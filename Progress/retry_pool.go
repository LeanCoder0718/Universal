package progress

import (
	"bufio"
	"context"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

// RetryOptions define la configuración para el RetryPool.
type RetryOptions struct {
	NumWorkers       int              // Número R de workers dedicados a reintentos
	QueueSize        int              // Capacidad del canal buffered de reintentos
	MaxAttempts      int              // Intentos máximos por línea (default 3)
	InitialBackoff   time.Duration    // Espera inicial para el primer reintento (default 10ms)
	MaxBackoff       time.Duration    // Techo del backoff exponencial (default 500ms)
	ControlPlane     *Plane           // Plano de control P/C/G (opcional)
	DeadLetterSink   *DeadLetterSink  // Sink para fallos definitivos o desbordamiento
	IsRetryableClass IsRetryableError // Función clasificadora de errores
}

// RetryPool gestiona los workers dedicados a reintentar líneas fallidas fuera del hot path principal.
type RetryPool struct {
	mu         sync.RWMutex
	isClosed   bool
	opts       RetryOptions
	queue      chan *RetryTask
	processFn  ProcessFunc
	dlx        *DeadLetterSink
	plane      *Plane
	classifier IsRetryableError

	retryEnqueued  atomic.Uint64
	retrySuccess   atomic.Uint64
	retryFailedDLX atomic.Uint64
	retryOverflow  atomic.Uint64
}

// NewRetryPool crea una nueva instancia de RetryPool.
func NewRetryPool(opts RetryOptions, fn ProcessFunc) *RetryPool {
	if opts.NumWorkers <= 0 {
		opts.NumWorkers = 1
	}
	if opts.QueueSize <= 0 {
		opts.QueueSize = 5000
	}
	if opts.MaxAttempts <= 0 {
		opts.MaxAttempts = 3
	}
	if opts.InitialBackoff <= 0 {
		opts.InitialBackoff = 10 * time.Millisecond
	}
	if opts.MaxBackoff <= 0 {
		opts.MaxBackoff = 500 * time.Millisecond
	}
	if opts.IsRetryableClass == nil {
		opts.IsRetryableClass = DefaultRetryableClassifier
	}
	if opts.DeadLetterSink == nil {
		opts.DeadLetterSink = NewDeadLetterSink(nil)
	}

	return &RetryPool{
		opts:       opts,
		queue:      make(chan *RetryTask, opts.QueueSize),
		processFn:  fn,
		dlx:        opts.DeadLetterSink,
		plane:      opts.ControlPlane,
		classifier: opts.IsRetryableClass,
	}
}

// Queue retorna el canal subyacente de reintentos.
func (p *RetryPool) Queue() chan *RetryTask {
	return p.queue
}

// DrainQueue drena las líneas pendientes en la cola de reintentos y las escribe en el bufio.Writer.
func (p *RetryPool) DrainQueue(w *bufio.Writer) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// H-11 Fix: Labeled break para salir del for desde dentro del select.
	// `break` dentro de `select` solo sale del select, no del for.
drainLoop:
	for len(p.queue) > 0 {
		select {
		case task := <-p.queue:
			if task != nil {
				if _, err := w.Write(task.Line.Content); err != nil {
					return err
				}
				if _, err := w.WriteString("\n"); err != nil {
					return err
				}
			}
		default:
			break drainLoop
		}
	}
	return nil
}

// Enqueue intenta encolar una tarea para reintento de forma NO BLOQUEANTE y Thread-Safe.
func (p *RetryPool) Enqueue(task *RetryTask) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.isClosed {
		p.retryOverflow.Add(1)
		p.dlx.Record(RecordItem{
			Line:        task.Line,
			Attempt:     task.Attempt,
			MaxAttempts: task.MaxAttempts,
			LastError:   task.LastError,
			Reason:      "retry_pool_closed",
		})
		return false
	}

	if task.MaxAttempts <= 0 {
		task.MaxAttempts = p.opts.MaxAttempts
	}

	p.retryEnqueued.Add(1)

	select {
	case p.queue <- task:
		return true
	default:
		p.retryOverflow.Add(1)
		p.dlx.Record(RecordItem{
			Line:        task.Line,
			Attempt:     task.Attempt,
			MaxAttempts: task.MaxAttempts,
			LastError:   task.LastError,
			Reason:      "retry_queue_overflow",
		})
		return false
	}
}

// Start inicia los R workers de reintento.
func (p *RetryPool) Start(ctx context.Context, resultQueue chan<- *ChunkResult, wg *sync.WaitGroup) {
	for i := 0; i < p.opts.NumWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			p.workerLoop(ctx, resultQueue)
		}(i)
	}
}

func (p *RetryPool) workerLoop(ctx context.Context, resultQueue chan<- *ChunkResult) {
	for {
		select {
		case <-ctx.Done():
			return
		case task, ok := <-p.queue:
			if !ok {
				return
			}
			p.processRetryTask(ctx, task, resultQueue)
		}
	}
}

func (p *RetryPool) processRetryTask(ctx context.Context, task *RetryTask, resultQueue chan<- *ChunkResult) {
	if p.plane != nil {
		if err := p.plane.WaitIfPaused(ctx); err != nil {
			return
		}
	}

	task.Attempt++

	if task.Attempt > 1 {
		backoff := time.Duration(float64(p.opts.InitialBackoff) * math.Pow(2, float64(task.Attempt-2)))
		if backoff > p.opts.MaxBackoff {
			backoff = p.opts.MaxBackoff
		}

		// H-01 Fix: time.After() crea un timer que no se libera hasta expirar.
		// Con miles de reintentos concurrentes, los timers se acumulan en el heap.
		// time.NewTimer + Stop() previene la fuga de memoria.
		backoffTimer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			backoffTimer.Stop()
			return
		case <-backoffTimer.C:
		}
	}

	out, err := p.processFn(&task.Line)
	if err == nil {
		p.retrySuccess.Add(1)
		res := &ChunkResult{
			SequenceID: 0,
			Results: []LineResult{
				{
					LineID:  task.Line.ID,
					Success: true,
					Output:  out,
					Err:     nil,
				},
			},
		}

		select {
		case <-ctx.Done():
			return
		case resultQueue <- res:
		}
		return
	}

	task.LastError = err
	if task.Attempt < task.MaxAttempts && p.classifier(err) {
		p.Enqueue(task)
	} else {
		p.retryFailedDLX.Add(1)
		p.dlx.Record(RecordItem{
			Line:        task.Line,
			Attempt:     task.Attempt,
			MaxAttempts: task.MaxAttempts,
			LastError:   task.LastError,
			Reason:      "max_attempts_exceeded",
		})
	}
}

// MetricsSnapshot retorna un desglose de las métricas acumuladas del subsistema de reintentos.
type MetricsSnapshot struct {
	Enqueued  uint64
	Success   uint64
	FailedDLX uint64
	Overflow  uint64
}

// Metrics retorna la captura actual de contadores de reintento.
func (p *RetryPool) Metrics() MetricsSnapshot {
	return MetricsSnapshot{
		Enqueued:  p.retryEnqueued.Load(),
		Success:   p.retrySuccess.Load(),
		FailedDLX: p.retryFailedDLX.Load(),
		Overflow:  p.retryOverflow.Load(),
	}
}

// Close cierra el canal de reintentos de forma segura.
func (p *RetryPool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.isClosed {
		p.isClosed = true
		close(p.queue)
	}
}
