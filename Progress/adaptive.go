package progress

import (
	"sync"
	"sync/atomic"
	"time"
)

// AdaptiveSchedulerOptions contiene los parámetros del programador adaptativo.
type AdaptiveSchedulerOptions struct {
	TotalItems    int           // Total de líneas / combos (N)
	NumWorkers    int           // Número de goroutines / threads (W)
	MinChunkSize  int           // Tamaño mínimo permitido (default 1)
	MaxChunkSize  int           // Tamaño máximo permitido (default 5000)
	TargetLatency time.Duration // Latencia objetivo por chunk (default 150ms)
	Alpha         float64       // Factor de suavizado EWMA (default 0.2)
}

// AdaptiveScheduler ajusta dinámicamente el chunkSize en función de N, W y la latencia observada.
type AdaptiveScheduler struct {
	mu             sync.Mutex
	opts           AdaptiveSchedulerOptions
	currentChunkSz atomic.Int64
	ewmaLatency    float64
	sampleCounter  atomic.Uint64
}

// NewAdaptiveScheduler crea una nueva instancia de AdaptiveScheduler con cálculo de base inicial N/4W.
func NewAdaptiveScheduler(opts AdaptiveSchedulerOptions) *AdaptiveScheduler {
	if opts.MinChunkSize <= 0 {
		opts.MinChunkSize = 1
	}
	if opts.MaxChunkSize <= 0 {
		opts.MaxChunkSize = 5000
	}
	if opts.TargetLatency <= 0 {
		opts.TargetLatency = 150 * time.Millisecond
	}
	if opts.Alpha <= 0 {
		opts.Alpha = 0.2
	}

	// Cálculo de bloque base inicial: N / (4 * W)
	initialSz := 1000
	if opts.TotalItems > 0 && opts.NumWorkers > 0 {
		baseCalculated := opts.TotalItems / (4 * opts.NumWorkers)
		if baseCalculated < opts.MinChunkSize {
			baseCalculated = opts.MinChunkSize
		}
		if baseCalculated > opts.MaxChunkSize {
			baseCalculated = opts.MaxChunkSize
		}
		initialSz = baseCalculated
	}

	s := &AdaptiveScheduler{
		opts: opts,
	}
	s.currentChunkSz.Store(int64(initialSz))
	return s
}

// CurrentChunkSize retorna el tamaño de chunk recomendado considerando el remanente (Tapering anti-straggler).
// Es completamente lock-free para permitir acceso ultra-rápido desde el reader.
func (s *AdaptiveScheduler) CurrentChunkSize(remainingItems int) int {
	baseSz := int(s.currentChunkSz.Load())
	if baseSz <= 0 {
		baseSz = s.opts.MinChunkSize
	}

	// Tapering de Cola: Si quedan pocos ítems (menos de 2 * NumWorkers), reducir el tamaño
	// para reparto uniforme y evitar stragglers al final del archivo.
	if remainingItems > 0 && s.opts.NumWorkers > 0 {
		taperLimit := s.opts.NumWorkers * 2
		if remainingItems <= taperLimit {
			taperSz := remainingItems / s.opts.NumWorkers
			if taperSz < s.opts.MinChunkSize {
				taperSz = s.opts.MinChunkSize
			}
			if taperSz < baseSz {
				return taperSz
			}
		}
	}

	return baseSz
}

// RecordLatency registra la latencia observada de un chunk y ajusta el tamaño recomendado mediante EWMA.
func (s *AdaptiveScheduler) RecordLatency(elapsed time.Duration) {
	// A alta concurrencia (>= 500 workers), aplicar muestreo para evitar que miles
	// de goroutines compitan por el lock de actualización EWMA.
	if s.opts.NumWorkers >= 500 {
		if s.sampleCounter.Add(1)%16 != 0 {
			return
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	latMs := float64(elapsed.Milliseconds())
	if latMs <= 0 {
		latMs = 1.0
	}

	if s.ewmaLatency == 0 {
		s.ewmaLatency = latMs
	} else {
		s.ewmaLatency = s.opts.Alpha*latMs + (1-s.opts.Alpha)*s.ewmaLatency
	}

	targetMs := float64(s.opts.TargetLatency.Milliseconds())
	if targetMs <= 0 {
		targetMs = 150.0
	}

	curr := int(s.currentChunkSz.Load())
	ratio := targetMs / s.ewmaLatency
	newSize := int(float64(curr) * ratio)

	if newSize < s.opts.MinChunkSize {
		newSize = s.opts.MinChunkSize
	}
	if newSize > s.opts.MaxChunkSize {
		newSize = s.opts.MaxChunkSize
	}

	s.currentChunkSz.Store(int64(newSize))
}
