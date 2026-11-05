package progress

import (
	"bufio"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Plane administra las señales de control global (P/C/G) y sincroniza la pausa de los workers.
// Utiliza un canal de señalización para despertar a 5.000+ workers en O(1) sin contención
// y sin crear miles de goroutines efímeras watchdog.
type Plane struct {
	paused     atomic.Bool
	mu         sync.Mutex
	cond       *sync.Cond
	pauseChan  chan struct{}
	checkpoint *Tracker
}

// NewPlane crea una nueva instancia de Control Plane.
func NewPlane(cp *Tracker) *Plane {
	p := &Plane{
		checkpoint: cp,
	}
	p.cond = sync.NewCond(&p.mu)
	return p
}

// IsPaused indica si el sistema se encuentra en estado de pausa.
func (p *Plane) IsPaused() bool {
	return p.paused.Load()
}

// Pause activa el estado de pausa global (tecla P).
func (p *Plane) Pause() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.paused.Load() {
		p.paused.Store(true)
		p.pauseChan = make(chan struct{})
	}
}

// Resume desactiva la pausa y despierta a todas las goroutines suspendidas en O(1) (tecla C).
func (p *Plane) Resume() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.paused.Load() {
		p.paused.Store(false)
		if p.pauseChan != nil {
			close(p.pauseChan)
			p.pauseChan = nil
		}
		p.cond.Broadcast()
	}
}

// Save ejecuta el guardado en caliente de las líneas pendientes (tecla G).
func (p *Plane) Save(
	sourceFilePath string,
	outputPath string,
	chunkQueue <-chan *Chunk,
	drainRetryQueue func(w *bufio.Writer) error,
) (string, error) {
	if p.checkpoint == nil {
		return "", fmt.Errorf("checkpoint tracker no configurado")
	}

	wasPaused := p.IsPaused()
	if !wasPaused {
		p.Pause()
		defer p.Resume()
	}

	// Pequeña espera de reposo (quiescence) para permitir que los workers
	// que estén procesando una línea terminen y queden en pausa.
	time.Sleep(50 * time.Millisecond)

	return p.checkpoint.SavePendingSnapshot(sourceFilePath, outputPath, chunkQueue, drainRetryQueue)
}

// WaitIfPaused es invocado por los workers entre cada línea procesada.
// Espera en el canal de pausa sin crear goroutines efímeras y respetando
// la cancelación del contexto de forma instantánea.
func (p *Plane) WaitIfPaused(ctx context.Context) error {
	if !p.IsPaused() {
		return nil
	}

	p.mu.Lock()
	ch := p.pauseChan
	p.mu.Unlock()

	if ch == nil {
		return nil
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-ch:
		return nil
	}
}

// Stop desactiva la pausa y despierta a todos los workers para permitir un shutdown limpio.
func (p *Plane) Stop() {
	p.Resume()
}
