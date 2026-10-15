package HTTPClient

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// SessionHandle envuelve una Session HTTP con metadatos del ciclo de vida.
type SessionHandle struct {
	Session         *Session
	ID              string
	CreatedAt       time.Time
	RequestCount    atomic.Uint64
	SuccessCount    atomic.Uint64
	ConsecutiveErrs atomic.Uint32
	IsRetired       atomic.Bool
	PreWarmed       atomic.Bool
}

// NewSessionHandle envuelve una sesión recién creada.
func NewSessionHandle(s *Session) *SessionHandle {
	return &SessionHandle{
		Session:   s,
		ID:        NewStickyID(),
		CreatedAt: time.Now(),
	}
}

// IsHealthy evalúa si la sesión cumple la política adaptativa híbrida.
func (h *SessionHandle) IsHealthy(cfg LifecycleConfig) bool {
	if h == nil || h.IsRetired.Load() {
		return false
	}
	if cfg.MaxRequestsPerSession > 0 && h.RequestCount.Load() >= cfg.MaxRequestsPerSession {
		return false
	}
	if cfg.MaxSessionAge > 0 && time.Since(h.CreatedAt) >= cfg.MaxSessionAge {
		return false
	}
	if cfg.MaxConsecutiveErrors > 0 && h.ConsecutiveErrs.Load() >= cfg.MaxConsecutiveErrors {
		return false
	}
	return true
}

// LifecycleManager administra el ciclo de vida, rotación adaptativa,
// pre-calentamiento en background y drenaje gracioso asíncrono.
type LifecycleManager struct {
	cfg         *Config
	sessionOpts []Option
	pool        chan *SessionHandle
	drainQueue  chan *SessionHandle
	connMgr     *ConnectionManager

	closed atomic.Bool
	wg     sync.WaitGroup

	createdCount atomic.Uint64
	reusedCount  atomic.Uint64
	retiredCount atomic.Uint64
	preWarmCount atomic.Uint64
	activePreWarms atomic.Int32

	closeMu sync.RWMutex
}

// NewLifecycleManager construye e inicializa el administrador del ciclo de vida.
func NewLifecycleManager(cfg *Config, poolSize int, sessionOpts ...Option) (*LifecycleManager, error) {
	if cfg == nil {
		cfg = Default()
	}
	if poolSize <= 0 {
		poolSize = 10
	}

	cm := NewConnectionManager(cfg, nil, 50, cfg.Transport.IdleConnTimeout)
	optsWithCM := append([]Option{WithConnectionManager(cm)}, sessionOpts...)

	m := &LifecycleManager{
		cfg:         cfg,
		sessionOpts: optsWithCM,
		pool:        make(chan *SessionHandle, poolSize*4),
		drainQueue:  make(chan *SessionHandle, poolSize*4),
		connMgr:     cm,
	}

	m.wg.Add(1)
	go m.drainWorker()

	return m, nil
}

// Acquire obtiene una sesión sana del pool o crea una nueva inmediatamente.
func (m *LifecycleManager) Acquire(ctx context.Context) (*SessionHandle, error) {
	if m.closed.Load() {
		return nil, fmt.Errorf("lifecycle: manager cerrado")
	}

	for {
		select {
		case h := <-m.pool:
			if h.IsHealthy(m.cfg.Lifecycle) {
				m.reusedCount.Add(1)
				_ = h.Session.ResetComboState(false)
				return h, nil
			}
			m.retire(h)
		default:
			return m.createHandle()
		}
	}
}

func (m *LifecycleManager) createHandle() (*SessionHandle, error) {
	s, err := NewSession(m.cfg, m.sessionOpts...)
	if err != nil {
		return nil, err
	}
	m.createdCount.Add(1)
	return NewSessionHandle(s), nil
}

// Release retorna la sesión administrada al pool tras evaluar su estado de salud.
func (m *LifecycleManager) Release(h *SessionHandle, execErr error) {
	if h == nil {
		return
	}

	h.RequestCount.Add(1)

	if execErr != nil {
		h.ConsecutiveErrs.Add(1)
	} else {
		h.ConsecutiveErrs.Store(0)
		h.SuccessCount.Add(1)
	}

	if m.closed.Load() || !h.IsHealthy(m.cfg.Lifecycle) {
		m.retire(h)
		return
	}

	// Limpieza automática de cookies y rotación de identidad entre combos manteniendo el socket TCP vivo
	if m.cfg.Lifecycle.AutoResetOnRelease && h.Session != nil {
		_ = h.Session.ResetComboState(m.cfg.Lifecycle.RotateIdentityOnReset)
	}

	// Trigger de Pre-Warm: al alcanzar el 75% del uso tolerado, pre-calentar una sesión de reemplazo
	// H-09 Fix: Asegurar que cada sesión dispare pre-warm una sola vez y limitar goroutines concurrentes
	if m.cfg.Lifecycle.EnablePreWarm && m.cfg.Lifecycle.MaxRequestsPerSession > 0 {
		threshold := uint64(float64(m.cfg.Lifecycle.MaxRequestsPerSession) * 0.75)
		if h.RequestCount.Load() >= threshold && h.PreWarmed.CompareAndSwap(false, true) {
			if m.activePreWarms.Add(1) <= 8 {
				go func() {
					defer m.activePreWarms.Add(-1)
					m.triggerPreWarm()
				}()
			} else {
				m.activePreWarms.Add(-1)
			}
		}
	}

	m.closeMu.RLock()
	if m.closed.Load() {
		m.closeMu.RUnlock()
		m.retire(h)
		return
	}

	select {
	case m.pool <- h:
		m.closeMu.RUnlock()
	default:
		m.closeMu.RUnlock()
		m.retire(h)
	}
}

func (m *LifecycleManager) triggerPreWarm() {
	if m.closed.Load() {
		return
	}
	h, err := m.createHandle()
	if err != nil {
		return
	}
	m.preWarmCount.Add(1)

	m.closeMu.RLock()
	if m.closed.Load() {
		m.closeMu.RUnlock()
		m.retire(h)
		return
	}
	select {
	case m.pool <- h:
		m.closeMu.RUnlock()
	default:
		m.closeMu.RUnlock()
		m.retire(h)
	}
}

func (m *LifecycleManager) retire(h *SessionHandle) {
	if h == nil {
		return
	}
	if h.IsRetired.CompareAndSwap(false, true) {
		m.retiredCount.Add(1)
		m.closeMu.RLock()
		if m.closed.Load() {
			m.closeMu.RUnlock()
			if h.Session != nil {
				h.Session.Close()
			}
			return
		}
		select {
		case m.drainQueue <- h:
			m.closeMu.RUnlock()
		default:
			m.closeMu.RUnlock()
			if h.Session != nil {
				h.Session.Close()
			}
		}
	}
}

func (m *LifecycleManager) drainWorker() {
	defer m.wg.Done()
	for h := range m.drainQueue {
		if h != nil && h.Session != nil {
			h.Session.Close()
		}
	}
}

// Close detiene el manager y realiza el drenaje gracioso de todas las sesiones.
func (m *LifecycleManager) Close() {
	if !m.closed.CompareAndSwap(false, true) {
		return
	}
	m.closeMu.Lock()
	close(m.pool)
	for h := range m.pool {
		if h != nil && h.IsRetired.CompareAndSwap(false, true) {
			m.retiredCount.Add(1)
			if h.Session != nil {
				h.Session.Close()
			}
		}
	}
	close(m.drainQueue)
	if m.connMgr != nil {
		m.connMgr.Close()
	}
	m.closeMu.Unlock()

	m.wg.Wait()
}

// FlushSessions retira y drena todas las sesiones inactivas en el pool channel,
// forzando la creación de nuevas sesiones limpias en las siguientes peticiones.
func (m *LifecycleManager) FlushSessions() int {
	if m.closed.Load() {
		return 0
	}

	m.closeMu.RLock()
	defer m.closeMu.RUnlock()

	flushed := 0
	for {
		select {
		case h := <-m.pool:
			if h != nil {
				m.retire(h)
				flushed++
			}
		default:
			if m.connMgr != nil {
				m.connMgr.CloseIdleConnections()
			}
			return flushed
		}
	}
}

// Stats retorna las métricas de creación, reutilización, retiros y pre-calentamientos.
func (m *LifecycleManager) Stats() (created, reused, retired, preWarmed uint64) {
	return m.createdCount.Load(), m.reusedCount.Load(), m.retiredCount.Load(), m.preWarmCount.Load()
}
