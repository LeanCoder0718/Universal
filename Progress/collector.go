package progress

import (
	"context"
	"io"
	"sync/atomic"
	"time"
)

// MetricsCounterInterface define la interfaz desacoplada para contadores CPM/CPS.
type MetricsCounterInterface interface {
	Add(n uint64)
	CPM() uint64
	MaxCPM() uint64
	ResetCPM()
}

// LatencyBucket representa los límites de los buckets de latencia en milisegundos.
var latencyBuckets = [10]int64{
	10,   // < 10ms
	25,   // 10-25ms
	50,   // 25-50ms
	100,  // 50-100ms
	250,  // 100-250ms
	500,  // 250-500ms
	1000, // 500ms-1s
	2500, // 1s-2.5s
	5000, // 2.5s-5s
	// > 5s en bucket 9
}

// LatencyMetrics contiene el desglose percentil y estadístico de latencias observadas.
type LatencyMetrics struct {
	TotalSamples uint64
	AvgMs        float64
	P50Ms        float64
	P90Ms        float64
	P95Ms        float64
	P99Ms        float64
}

// Collector recaba métricas en tiempo real de consumo de memoria, goroutines, throughput y latencia.
type Collector struct {
	counter        MetricsCounterInterface
	interval       time.Duration
	writer         io.Writer
	chunksCreated  atomic.Uint64
	processedLines atomic.Uint64

	// Histograma de latencias lock-free basado en atomic buckets
	buckets      [10]atomic.Uint64
	totalSamples atomic.Uint64
	totalTimeMs  atomic.Uint64

	// Throughput rate en tiempo real
	startTime     time.Time
	lastSample    atomic.Int64 // timestamp unix nano
	lastLineCount atomic.Uint64
	currentLPS    atomic.Uint64 // Líneas por segundo calculadas
}

// NewCollector crea una nueva instancia de Collector.
func NewCollector(counter MetricsCounterInterface, interval time.Duration, writer io.Writer) *Collector {
	if interval <= 0 {
		interval = 500 * time.Millisecond
	}
	c := &Collector{
		counter:   counter,
		interval:  interval,
		writer:    writer,
		startTime: time.Now(),
	}
	c.lastSample.Store(time.Now().UnixNano())
	return c
}

// IncrChunksCreated incrementa el contador de chunks creados por el reader.
func (c *Collector) IncrChunksCreated() {
	c.chunksCreated.Add(1)
}

// ChunksCreated retorna la cantidad total de chunks emitidos.
func (c *Collector) ChunksCreated() uint64 {
	return c.chunksCreated.Load()
}

// IncrProcessedLines registra líneas procesadas y actualiza los cálculos de tasa.
func (c *Collector) IncrProcessedLines(count uint64) {
	total := c.processedLines.Add(count)
	if c.counter != nil {
		c.counter.Add(count)
	}

	// Cálculo ligero de LPS (Lines Per Second)
	now := time.Now().UnixNano()
	last := c.lastSample.Load()
	elapsed := now - last
	if elapsed >= int64(time.Second) {
		if c.lastSample.CompareAndSwap(last, now) {
			prevLines := c.lastLineCount.Swap(total)
			deltaLines := total - prevLines
			seconds := float64(elapsed) / float64(time.Second)
			if seconds > 0 {
				c.currentLPS.Store(uint64(float64(deltaLines) / seconds))
			}
		}
	}
}

// RecordLatency registra la latencia observada de un chunk o tarea en los buckets atómicos.
func (c *Collector) RecordLatency(d time.Duration) {
	ms := d.Milliseconds()
	if ms < 0 {
		ms = 0
	}

	c.totalSamples.Add(1)
	c.totalTimeMs.Add(uint64(ms))

	for i, limit := range latencyBuckets[:9] {
		if ms <= limit {
			c.buckets[i].Add(1)
			return
		}
	}
	c.buckets[9].Add(1) // Bucket para > 5000ms
}

// LatencySummary retorna una estimación de percentiles P50, P90, P95, P99 y promedio.
func (c *Collector) LatencySummary() LatencyMetrics {
	total := c.totalSamples.Load()
	if total == 0 {
		return LatencyMetrics{}
	}

	totalMs := c.totalTimeMs.Load()
	avg := float64(totalMs) / float64(total)

	var counts [10]uint64
	for i := 0; i < 10; i++ {
		counts[i] = c.buckets[i].Load()
	}

	calcPercentile := func(p float64) float64 {
		target := uint64(float64(total) * p)
		if target == 0 {
			target = 1
		}
		var accum uint64
		for i, count := range counts {
			accum += count
			if accum >= target {
				if i < 9 {
					return float64(latencyBuckets[i])
				}
				return 5000.0
			}
		}
		return 5000.0
	}

	return LatencyMetrics{
		TotalSamples: total,
		AvgMs:        avg,
		P50Ms:        calcPercentile(0.50),
		P90Ms:        calcPercentile(0.90),
		P95Ms:        calcPercentile(0.95),
		P99Ms:        calcPercentile(0.99),
	}
}

// TotalProcessedLines retorna el total acumulado de líneas procesadas.
func (c *Collector) TotalProcessedLines() uint64 {
	return c.processedLines.Load()
}

// LinesPerSecond retorna el throughput instantáneo en líneas procesadas por segundo.
func (c *Collector) LinesPerSecond() uint64 {
	lps := c.currentLPS.Load()
	if lps > 0 {
		return lps
	}
	// Fallback a promedio global si aún no hay muestreo de 1s
	elapsed := time.Since(c.startTime).Seconds()
	if elapsed > 0 {
		return uint64(float64(c.processedLines.Load()) / elapsed)
	}
	return 0
}

// CPM retorna el CPM (Conexiones/Líneas por Minuto) calculado.
func (c *Collector) CPM() uint64 {
	if c.counter != nil {
		return c.counter.CPM()
	}
	return c.LinesPerSecond() * 60
}

// MaxCPM retorna el CPM máximo alcanzado.
func (c *Collector) MaxCPM() uint64 {
	if c.counter != nil {
		return c.counter.MaxCPM()
	}
	return c.CPM()
}

// Run ejecuta el recolector de métricas si se desea reporte periódico a un io.Writer.
func (c *Collector) Run(ctx context.Context) {
	if c.writer == nil {
		return
	}
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// No-op liviano o escritura periódica sin bloquear
		}
	}
}
