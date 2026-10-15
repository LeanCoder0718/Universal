package progress

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// WorkloadType define el perfil de carga de trabajo del procesamiento.
type WorkloadType string

const (
	WorkloadCPU WorkloadType = "cpu" // Asigna NumWorkers = NumCPU
	WorkloadIO  WorkloadType = "io"  // Asigna NumWorkers = NumCPU * 4
)

// WorkerOptions contiene los parámetros de configuración requeridos para el WorkerPool.
type WorkerOptions struct {
	NumWorkers   int                // Número manual de workers (0 = auto-calcular)
	Workload     WorkloadType       // Perfil de carga: WorkloadCPU o WorkloadIO
	ForceWorkers bool               // Si es true, omite cualquier auto-corrección manual
	ControlPlane *Plane             // Plano de control P/C/G para pausar/reanudar
	Checkpoint   *Tracker           // Tracker para registrar el progreso por worker
	RetryPool    *RetryPool         // Subsistema desacoplado para encolamiento de reintentos
	Collector    *Collector         // Recolector atómico de throughput (RABCOUNT)
	ChunkPool    *ChunkPool         // Pool sync.Pool para reciclado de chunks
	Scheduler    *AdaptiveScheduler // Programador adaptativo opcional
	RateLimiter  RateLimiter        // Limitador de tasa (RPS) opcional
	RampUpDelay  time.Duration      // Intervalo de espera para levantar threads 1 a 1 progresivamente
}

// GoroutineMetrics encapsula los contadores de estado de workers y chunks.
type GoroutineMetrics struct {
	Configured        int
	Active            int
	Busy              int
	Free              int
	Paused            int
	Blocked           int
	Finished          int
	RuntimeGoroutines int
	WorkerUtilization float64
	AllocMB           float64
	GCCPUFraction     float64

	TotalChunks     uint64
	PendingChunks   int
	ProcessedChunks uint64
	ActiveChunks    int
}

// WorkerPool administra la colección de W goroutines persistentes del Worker Pool.
type WorkerPool struct {
	numWorkers      atomic.Int64
	opts            WorkerOptions
	processFn       ProcessFunc
	busyWorkers     atomic.Int64
	spawnedWorkers  atomic.Int64
	finishedWorkers atomic.Int64
	activeChunks    atomic.Int64
	processedChunks atomic.Uint64

	// Escalado de concurrencia en caliente
	scaleDown   atomic.Int64
	ctx         context.Context
	chunkQueue  <-chan *Chunk
	resultQueue chan<- *ChunkResult
	wg          *sync.WaitGroup
	mu          sync.Mutex
}

// --- Cached runtime.ReadMemStats (evita stop-the-world en cada llamada) ---
var (
	cachedMem   runtime.MemStats
	lastMemRead time.Time
	memCacheMu  sync.Mutex
	memCacheTTL = 5 * time.Second

	cachedNumGoroutines int
	lastGoroutineRead   time.Time
	goroutineCacheMu    sync.Mutex
	goroutineCacheTTL   = 1 * time.Second
)

// getCachedMemStats retorna MemStats cacheadas con TTL de 5s.
// runtime.ReadMemStats() requiere parar el mundo (stop-the-world GC pause);
// llamarlo cada segundo desde el UI thread congelaba todas las goroutines.
func getCachedMemStats() runtime.MemStats {
	memCacheMu.Lock()
	defer memCacheMu.Unlock()
	if time.Since(lastMemRead) > memCacheTTL {
		runtime.ReadMemStats(&cachedMem)
		lastMemRead = time.Now()
	}
	return cachedMem
}

// getCachedNumGoroutines retorna el número de goroutines con cache de 1s.
// runtime.NumGoroutine() es barato pero a tasas altas de llamada (UI 60fps)
// genera presión innecesaria de scheduler.
func getCachedNumGoroutines() int {
	goroutineCacheMu.Lock()
	defer goroutineCacheMu.Unlock()
	if time.Since(lastGoroutineRead) > goroutineCacheTTL {
		cachedNumGoroutines = runtime.NumGoroutine()
		lastGoroutineRead = time.Now()
	}
	return cachedNumGoroutines
}

// NewWorkerPool crea y configura una instancia de WorkerPool calculando automáticamente W óptimo.
func NewWorkerPool(opts WorkerOptions, fn ProcessFunc) *WorkerPool {
	numCPU := runtime.NumCPU()
	finalWorkers := opts.NumWorkers

	if !opts.ForceWorkers || finalWorkers <= 0 {
		if opts.Workload == WorkloadIO {
			finalWorkers = numCPU * 4
		} else {
			finalWorkers = numCPU
		}
	}

	opts.NumWorkers = finalWorkers

	wp := &WorkerPool{
		opts:      opts,
		processFn: fn,
	}
	wp.numWorkers.Store(int64(finalWorkers))
	return wp
}

// NumWorkers retorna el número W de goroutines persistentes configuradas.
func (p *WorkerPool) NumWorkers() int {
	return int(p.numWorkers.Load())
}

// CurrentWorkers retorna el número de workers activos actualmente.
func (p *WorkerPool) CurrentWorkers() int {
	return int(p.numWorkers.Load())
}

// AddWorkers añade delta goroutines en caliente al pool con throttle de spawn.
// Para deltas grandes (ej. 5000), las goroutines se lanzan en lotes para evitar
// saturar el scheduler del runtime con miles de goroutines simultáneas.
func (p *WorkerPool) AddWorkers(delta int) int {
	if delta <= 0 {
		return int(p.numWorkers.Load())
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.chunkQueue == nil || p.ctx == nil {
		p.numWorkers.Add(int64(delta))
		return int(p.numWorkers.Load())
	}

	// Throttle: lanzar en batches de 50 con micro-yield entre cada batch
	// para que el scheduler distribuya las goroutines en los P's disponibles.
	const spawnBatch = 50
	for i := 0; i < delta; i++ {
		current := p.numWorkers.Add(1)
		if p.wg != nil {
			p.wg.Add(1)
		}
		go func(workerID int) {
			if p.wg != nil {
				defer p.wg.Done()
			}
			p.workerLoop(p.ctx, workerID, p.chunkQueue, p.resultQueue)
		}(int(current))

		// Cada spawnBatch goroutines, ceder CPU al scheduler
		if (i+1)%spawnBatch == 0 && i < delta-1 {
			runtime.Gosched()
		}
	}
	return int(p.numWorkers.Load())
}

// RemoveWorkers reduce delta goroutines en caliente de forma suave (al terminar el chunk actual).
func (p *WorkerPool) RemoveWorkers(delta int) int {
	if delta <= 0 {
		return int(p.numWorkers.Load())
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	current := p.numWorkers.Load()
	if current-int64(delta) < 1 {
		delta = int(current - 1)
	}
	if delta <= 0 {
		return int(current)
	}
	p.scaleDown.Add(int64(delta))
	p.numWorkers.Add(-int64(delta))
	return int(p.numWorkers.Load())
}

// Start lanza las W goroutines persistentes ejecutoras e inicia el procesamiento en paralelo de forma progresiva (1 en 1).
// Usa time.NewTimer en lugar de time.After para evitar timer leaks durante el ramp-up.
func (p *WorkerPool) Start(ctx context.Context, chunkQueue <-chan *Chunk, resultQueue chan<- *ChunkResult, wg *sync.WaitGroup) {
	p.mu.Lock()
	p.ctx = ctx
	p.chunkQueue = chunkQueue
	p.resultQueue = resultQueue
	p.wg = wg
	total := int(p.numWorkers.Load())
	p.mu.Unlock()

	delay := p.opts.RampUpDelay

	wg.Add(1)
	go func() {
		defer wg.Done()

		// Timer reutilizable — se crea una sola vez y se reutiliza en cada iteración.
		// time.After() creaba un timer nuevo por iteración que no se liberaba hasta expirar.
		var rampTimer *time.Timer
		if delay > 0 {
			rampTimer = time.NewTimer(delay)
			rampTimer.Stop()
			defer rampTimer.Stop()
		}

		for i := 0; i < total; i++ {
			select {
			case <-ctx.Done():
				return
			default:
			}

			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				p.workerLoop(ctx, workerID, chunkQueue, resultQueue)
			}(i)

			if i < total-1 && rampTimer != nil {
				rampTimer.Reset(delay)
				select {
				case <-ctx.Done():
					return
				case <-rampTimer.C:
				}
			}
		}
	}()
}

func (p *WorkerPool) workerLoop(ctx context.Context, workerID int, chunkQueue <-chan *Chunk, resultQueue chan<- *ChunkResult) {
	const batchSize = 64
	localCount := uint64(0)

	p.spawnedWorkers.Add(1)
	defer func() {
		p.spawnedWorkers.Add(-1)
		p.finishedWorkers.Add(1)
	}()

	for {
		// Chequeo de reducción en caliente: si hay scaleDown pendiente, el worker se retira suavemente
		if p.scaleDown.Load() > 0 {
			if p.scaleDown.Add(-1) >= 0 {
				if localCount > 0 && p.opts.Collector != nil {
					p.opts.Collector.IncrProcessedLines(localCount)
				}
				return
			}
			p.scaleDown.Add(1)
		}

		select {
		case <-ctx.Done():
			if localCount > 0 && p.opts.Collector != nil {
				p.opts.Collector.IncrProcessedLines(localCount)
			}
			return

		case chk, ok := <-chunkQueue:
			if !ok {
				if localCount > 0 && p.opts.Collector != nil {
					p.opts.Collector.IncrProcessedLines(localCount)
				}
				return
			}

			p.busyWorkers.Add(1)
			p.activeChunks.Add(1)

			startChunkTime := time.Now()
			chunkRes := &ChunkResult{
				SequenceID: chk.SequenceID,
				Results:    make([]LineResult, 0, len(chk.Lines)),
			}

			if p.opts.Checkpoint != nil {
				p.opts.Checkpoint.UpdateWorkerProgress(workerID, chk, 0)
			}

			for _, line := range chk.Lines {

				if p.opts.RateLimiter != nil {
					if err := p.opts.RateLimiter.Wait(ctx); err != nil {
						p.busyWorkers.Add(-1)
						p.activeChunks.Add(-1)
						if p.opts.ChunkPool != nil {
							p.opts.ChunkPool.Put(chk)
						}
						return
					}
				}

				var out []byte
				var err error
				func() {
					defer func() {
						if r := recover(); r != nil {
							err = fmt.Errorf("panic in processFn: %v", r)
						}
					}()
					out, err = p.processFn(&line)
				}()
				if err != nil {
					if p.opts.RetryPool != nil {
						p.opts.RetryPool.Enqueue(&RetryTask{
							Line:        line,
							Attempt:     1,
							MaxAttempts: p.opts.RetryPool.opts.MaxAttempts,
							LastError:   err,
						})
					}

					chunkRes.Results = append(chunkRes.Results, LineResult{
						LineID:  line.ID,
						Line:    line,
						Success: false,
						Output:  nil,
						Err:     err,
					})
				} else {
					chunkRes.Results = append(chunkRes.Results, LineResult{
						LineID:  line.ID,
						Line:    line,
						Success: true,
						Output:  out,
						Err:     nil,
					})
				}

				localCount++
				if localCount >= batchSize {
					if p.opts.Collector != nil {
						p.opts.Collector.IncrProcessedLines(localCount)
					}
					localCount = 0
				}
			}

			p.busyWorkers.Add(-1)
			p.activeChunks.Add(-1)
			p.processedChunks.Add(1)

			if p.opts.Checkpoint != nil {
				p.opts.Checkpoint.ClearWorkerProgress(workerID)
			}

			elapsedChunk := time.Since(startChunkTime)
			if p.opts.Scheduler != nil {
				p.opts.Scheduler.RecordLatency(elapsedChunk)
			}
			if p.opts.Collector != nil {
				p.opts.Collector.RecordLatency(elapsedChunk)
			}

			select {
			case <-ctx.Done():
				if localCount > 0 && p.opts.Collector != nil {
					p.opts.Collector.IncrProcessedLines(localCount)
				}
				if p.opts.ChunkPool != nil {
					p.opts.ChunkPool.Put(chk)
				}
				return
			case resultQueue <- chunkRes:
			}

			if p.opts.ChunkPool != nil {
				p.opts.ChunkPool.Put(chk)
			}
		}
	}
}

// GetGoroutineMetrics retorna la métrica en tiempo real de goroutines y chunks.
func (p *WorkerPool) GetGoroutineMetrics(chunkQueueLen int) GoroutineMetrics {
	busy := int(p.busyWorkers.Load())
	if busy < 0 {
		busy = 0
	}
	fin := int(p.finishedWorkers.Load())
	configured := int(p.numWorkers.Load())
	spawned := int(p.spawnedWorkers.Load())
	active := spawned
	if active <= 0 {
		active = configured - fin
		if active < 0 {
			active = 0
		}
	}
	free := active - busy
	if free < 0 {
		free = 0
	}

	paused := 0
	if p.opts.ControlPlane != nil && p.opts.ControlPlane.IsPaused() {
		paused = active
		busy = 0
		free = 0
	}

	m := getCachedMemStats()
	allocMB := float64(m.Alloc) / (1024 * 1024)
	gcCPU := m.GCCPUFraction * 100.0
	numGoroutines := getCachedNumGoroutines()

	utilization := 0.0
	if configured > 0 {
		utilization = (float64(busy) / float64(configured)) * 100.0
		if utilization > 100.0 {
			utilization = 100.0
		}
	}

	activeC := int(p.activeChunks.Load())
	if activeC < 0 {
		activeC = 0
	}
	processedC := p.processedChunks.Load()
	totalC := processedC + uint64(chunkQueueLen) + uint64(activeC)

	return GoroutineMetrics{
		Configured:        configured,
		Active:            active,
		Busy:              busy,
		Free:              free,
		Paused:            paused,
		Blocked:           0,
		Finished:          fin,
		RuntimeGoroutines: numGoroutines,
		WorkerUtilization: utilization,
		AllocMB:           allocMB,
		GCCPUFraction:     gcCPU,
		TotalChunks:       totalC,
		PendingChunks:     chunkQueueLen,
		ProcessedChunks:   processedC,
		ActiveChunks:      activeC,
	}
}
