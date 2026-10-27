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
type Plane struct {
	paused     atomic.Bool
	mu         sync.Mutex
	cond       *sync.Cond
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
	p.paused.Store(true)
}

// Resume desactiva la pausa y despierta a todas las goroutines suspendidas (tecla C).
func (p *Plane) Resume() {
	p.paused.Store(false)
	p.cond.Broadcast()
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
// Incluye un watchdog que despierta a la goroutine si el contexto se cancela
// mientras está bloqueada en cond.Wait(), evitando un bloqueo infinito.
func (p *Plane) WaitIfPaused(ctx context.Context) error {
	if !p.IsPaused() {
		return nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	// Watchdog: si el context se cancela mientras estamos en Wait(),
	// necesitamos despertar la goroutine con Broadcast.
	watchdogDone := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			p.cond.Broadcast()
		case <-watchdogDone:
		}
	}()
	defer close(watchdogDone)

	for p.IsPaused() {
		if err := ctx.Err(); err != nil {
			return err
		}
		p.cond.Wait()
	}

	return nil
}

// Stop desactiva la pausa y despierta a todos los workers para permitir un shutdown limpio.
func (p *Plane) Stop() {
	p.paused.Store(false)
	p.cond.Broadcast()
}
