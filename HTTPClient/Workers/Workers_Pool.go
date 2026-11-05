// Package workers implementa un pool concurrente de HTTPClient.Session
// con ESCALADO DINÁMICO y backpressure real, reemplazando el round-robin
// estático original.
//
// Problema resuelto: con PoolSize=10 fijo y 2.000 goroutines llamando
// Pool.Do() simultáneamente, el diseño anterior las serializaba detrás
// de 10 Session.mu (severidad crítica, ver auditoría). Con el fix de
// Session.go (mutex ya no envuelve I/O), cada Session soporta alta
// concurrencia por sí misma; aun así, seguimos necesitando LÍMITE de
// concurrencia real (no infinita) para no saturar sockets/FDs del OS,
// y una cola con backpressure para que 2.000 goroutines no floodeen el
// scheduler de Go simultáneamente.
package Workers

import (
	"github.com/LeanCoder0718/Universal/HTTPClient"
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Job es una unidad de trabajo ejecutada por una Session del pool.
type Job func(ctx context.Context, s *HTTPClient.Session) (*HTTPClient.Result, error)

// Pool administra la concurrencia y genera sesiones efímeras por combo.
type Pool struct {
	cfg         *HTTPClient.Config
	limiter     *HTTPClient.PerHost
	sessionOpts []HTTPClient.Option

	sem       chan struct{}
	lifecycle *HTTPClient.LifecycleManager

	closed     atomic.Bool
	maxRetries int

	createdClients   atomic.Uint64
	reusedClients    atomic.Uint64
	activeSessions   atomic.Int64
	activeKeepAlives atomic.Int64
	reusedConns      atomic.Uint64
	newConns         atomic.Uint64
	closedConns      atomic.Uint64
	totalRequests    atomic.Uint64
	pendingRequests  atomic.Int64
	executingReqs    atomic.Int64
	timeouts         atomic.Uint64
	retries          atomic.Uint64
	connErrors       atomic.Uint64
	proxy502Errors   atomic.Uint64
	target403Errors  atomic.Uint64
	bytesTransferred atomic.Uint64

	totalReqCount atomic.Uint64
	succReqCount  atomic.Uint64
	failReqCount  atomic.Uint64
	resetReqCount atomic.Uint64

	lastRPSCheck    time.Time
	lastReqSnapshot uint64
	calculatedRPS   float64
	rpsMu           sync.Mutex
}

// HTTPDetailedMetrics encapsula el estado completo del subsistema HTTP.
type HTTPDetailedMetrics struct {
	ActiveProtocol   string
	CreatedClients   uint64
	ReusedClients    uint64
	ActiveSessions   int
	FreeSessions     int
	ActiveKeepAlives int
	ReusedConns      uint64
	NewConns         uint64
	ClosedConns      uint64
	KeepAliveReuse   float64
	RPS              float64
	TotalRequests    uint64
	PendingRequests  int
	ExecutingReqs    int
	Timeouts         uint64
	Retries          uint64
	ConnErrors       uint64
	Proxy502Errors   uint64
	Target403Errors  uint64
	TotalBytesMB     float64
}

// Config de escalado. Se mantiene separado de HTTPClient.WorkerConfig
// porque estas son preocupaciones del ORQUESTADOR (cuántas requests en
// vuelo), no de cada Session individual.
type PoolOptions struct {
	// MaxInFlight limita cuántos Do()/Run() concurrentes se permiten en
	// todo el pool. 0 = sin límite (no recomendado en producción).
	MaxInFlight int
	// MaxJobRetries configura la cantidad máxima de intentos a nivel de Job
	// en pool.Run() / pool.RunWithRetry(). Si no se define (0), hereda cfg.Retry.MaxAttempts.
	MaxJobRetries int
}

// New crea el pool administrando la concurrencia global via semáforo.
func New(cfg *HTTPClient.Config, opts PoolOptions, sessionOpts ...HTTPClient.Option) (*Pool, error) {
	if cfg == nil {
		cfg = HTTPClient.Default()
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	cfgCopy := *cfg
	maxRetries := opts.MaxJobRetries
	if maxRetries <= 0 {
		maxRetries = cfgCopy.Retry.MaxAttempts
	}
	if maxRetries <= 0 {
		maxRetries = 1
	}

	// Reintentos limpios por petición (Session.Do): por defecto 1 intento si no está configurado
	// para evitar multiplicación oculta contra los reintentos del orquestador.
	if cfgCopy.Retry.MaxAttempts <= 0 {
		cfgCopy.Retry.MaxAttempts = 1
	}

	p := &Pool{
		cfg:         &cfgCopy,
		sessionOpts: sessionOpts,
		maxRetries:  maxRetries,
	}
	if cfgCopy.Worker.RateLimitPerHost > 0 {
		p.limiter = HTTPClient.NewRateLimiter(cfgCopy.Worker.RateLimitPerHost, cfgCopy.Worker.RateLimitBurst)
	}

	maxInFlight := opts.MaxInFlight
	if maxInFlight <= 0 {
		maxInFlight = cfgCopy.Worker.PoolSize * 4
		if maxInFlight <= 0 {
			maxInFlight = 100
		}
	}
	p.sem = make(chan struct{}, maxInFlight)

	lManager, err := HTTPClient.NewLifecycleManager(&cfgCopy, maxInFlight, sessionOpts...)
	if err != nil {
		return nil, fmt.Errorf("workers: creando lifecycle manager: %w", err)
	}
	p.lifecycle = lManager

	return p, nil
}

// acquireSlot bloquea hasta obtener un slot de concurrencia global o
// hasta que el contexto se cancele. Esto es el backpressure real: si
// 2.000 goroutines llaman Do() y maxInFlight=200, solo 200 ejecutan
// request a la vez; las otras 1.800 esperan aquí (bloqueadas en un
// channel, sin costo de CPU, sin busy-wait).
func (p *Pool) acquireSlot(ctx context.Context) error {
	p.pendingRequests.Add(1)
	select {
	case p.sem <- struct{}{}:
		p.pendingRequests.Add(-1)
		p.executingReqs.Add(1)
		p.activeSessions.Add(1)
		p.totalRequests.Add(1)
		p.reusedClients.Add(1)
		return nil
	case <-ctx.Done():
		p.pendingRequests.Add(-1)
		p.timeouts.Add(1)
		return ctx.Err()
	}
}

func (p *Pool) releaseSlot() {
	select {
	case <-p.sem:
		p.executingReqs.Add(-1)
		p.activeSessions.Add(-1)
	default:
	}
}

func (p *Pool) acquireSession(ctx context.Context) (*HTTPClient.SessionHandle, error) {
	return p.lifecycle.Acquire(ctx)
}

func (p *Pool) releaseSession(handle *HTTPClient.SessionHandle, execErr error) {
	if handle == nil {
		return
	}
	p.totalReqCount.Add(1)
	if execErr == nil {
		p.succReqCount.Add(1)
	} else {
		p.failReqCount.Add(1)
	}
	p.lifecycle.Release(handle, execErr)
}

func (p *Pool) Do(ctx context.Context, spec HTTPClient.Spec) (*HTTPClient.Result, error) {
	if p.closed.Load() {
		return nil, HTTPClient.ErrPoolClosed
	}
	if err := p.acquireSlot(ctx); err != nil {
		return nil, fmt.Errorf("workers: backpressure - %w: %v", HTTPClient.ErrBackpressureTimeout, err)
	}
	defer p.releaseSlot()

	if p.closed.Load() {
		return nil, HTTPClient.ErrPoolClosed
	}

	if p.limiter != nil {
		if err := p.limiter.Wait(ctx, spec.URL); err != nil {
			return nil, fmt.Errorf("workers: %w: %v", HTTPClient.ErrRateLimitExceeded, err)
		}
	}

	handle, err := p.acquireSession(ctx)
	if err != nil {
		p.connErrors.Add(1)
		return nil, fmt.Errorf("workers: obteniendo sesión: %w", err)
	}

	var execErr error
	defer func() {
		p.releaseSession(handle, execErr)
	}()

	res, execErr := handle.Session.Do(ctx, spec)
	if execErr != nil {
		p.connErrors.Add(1)
	} else if res != nil && res.Response != nil {
		if len(res.Response.BodyBytes) > 0 {
			p.bytesTransferred.Add(uint64(len(res.Response.BodyBytes)))
		}
		switch res.Response.StatusCode {
		case 502, 504:
			p.proxy502Errors.Add(1)
		case 403, 429:
			p.target403Errors.Add(1)
		}
	}
	return res, execErr
}

func (p *Pool) Run(ctx context.Context, job Job) (*HTTPClient.Result, error) {
	return p.RunWithRetry(ctx, p.maxRetries, job)
}

// RunWithRetry ejecuta un Job con reintentos a nivel de intento (attempt-level routing),
// garantizando que cada intento fallido cambie de proxy, excluya los proxies ya probados
// en este intento, aplique backoff con jitter y reporte métricas de salud al pool.
func (p *Pool) RunWithRetry(ctx context.Context, maxAttempts int, job Job) (*HTTPClient.Result, error) {
	if p.closed.Load() {
		return nil, HTTPClient.ErrPoolClosed
	}
	if err := p.acquireSlot(ctx); err != nil {
		return nil, fmt.Errorf("workers: backpressure - %w: %v", HTTPClient.ErrBackpressureTimeout, err)
	}
	defer p.releaseSlot()

	if p.closed.Load() {
		return nil, HTTPClient.ErrPoolClosed
	}

	handle, err := p.acquireSession(ctx)
	if err != nil {
		p.connErrors.Add(1)
		return nil, fmt.Errorf("workers: obteniendo sesión por combo: %w", err)
	}

	if maxAttempts <= 0 {
		maxAttempts = 1
	}

	var (
		res           *HTTPClient.Result
		execErr       error
		excludedHosts []string
	)

	defer func() {
		p.releaseSession(handle, execErr)
	}()

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		if attempt > 1 {
			p.retries.Add(1)
			p.backoff(ctx, attempt)
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			default:
			}

			// Rotación garantizada excluyendo proxies previamente fallidos para esta tarea
			if pool := handle.Session.ProxyPool(); pool != nil {
				node, _, aErr := pool.Acquire(ctx, HTTPClient.AcquirePolicy{
					ExcludedHosts: excludedHosts,
					AllowFallback: true,
				})
				if aErr == nil && node != nil {
					_ = handle.Session.SetExplicitProxy(node.Entry)
				}
			} else {
				_ = handle.Session.RotateProxy()
			}
		}

		res, execErr = job(ctx, handle.Session)

		if res != nil && res.Response != nil {
			if len(res.Response.BodyBytes) > 0 {
				p.bytesTransferred.Add(uint64(len(res.Response.BodyBytes)))
			}
			switch res.Response.StatusCode {
			case 502, 504:
				p.proxy502Errors.Add(1)
			case 403, 429:
				p.target403Errors.Add(1)
			}
		}

		if execErr == nil {
			if pool := handle.Session.ProxyPool(); pool != nil {
				if cur := handle.Session.CurrentProxy(); cur != nil {
					pool.ReportSuccess(*cur, 0)
				}
			}
			return res, nil
		}

		p.connErrors.Add(1)
		if cur := handle.Session.CurrentProxy(); cur != nil {
			excludedHosts = append(excludedHosts, cur.Host, cur.Host+":"+cur.Port)
			if pool := handle.Session.ProxyPool(); pool != nil {
				pool.ReportFailure(*cur, execErr)
			}
		}

		if !isRetryableError(execErr) || attempt == maxAttempts {
			break
		}
	}

	return res, execErr
}

// H-02 Fix: time.After() crea un timer que no se libera hasta expirar,
// causando fuga de memoria bajo reintentos concurrentes masivos.
func (p *Pool) backoff(ctx context.Context, attempt int) {
	wait := 50 * time.Millisecond * time.Duration(1<<(attempt-1))
	if wait > 500*time.Millisecond {
		wait = 500 * time.Millisecond
	}
	timer := time.NewTimer(wait)
	select {
	case <-timer.C:
	case <-ctx.Done():
		timer.Stop()
	}
}

func isRetryableError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) {
		return false
	}
	return true
}

// GetDetailedMetrics calcula y retorna una captura en tiempo real de todas las métricas del pool HTTP.
func (p *Pool) GetDetailedMetrics() HTTPDetailedMetrics {
	activeS := int(p.activeSessions.Load())
	if activeS < 0 {
		activeS = 0
	}

	executing := int(p.executingReqs.Load())
	if executing < 0 {
		executing = 0
	}
	pending := int(p.pendingRequests.Load())
	if pending < 0 {
		pending = 0
	}

	totReqs, succReqs, failReqs, resets := p.Metrics()

	p.rpsMu.Lock()
	now := time.Now()
	if p.lastRPSCheck.IsZero() {
		p.lastRPSCheck = now
		p.lastReqSnapshot = totReqs
	} else {
		elapsed := now.Sub(p.lastRPSCheck).Seconds()
		if elapsed >= 0.5 {
			delta := float64(totReqs - p.lastReqSnapshot)
			p.calculatedRPS = delta / elapsed
			p.lastRPSCheck = now
			p.lastReqSnapshot = totReqs
		}
	}
	currentRPS := p.calculatedRPS
	p.rpsMu.Unlock()

	var createdCount, reusedCount uint64
	if p.lifecycle != nil {
		createdCount, reusedCount, _, _ = p.lifecycle.Stats()
	} else {
		createdCount = p.createdClients.Load()
		reusedCount = p.reusedClients.Load()
	}

	totalConns := succReqs + createdCount
	keepAliveReusePct := 0.0
	if totalConns > 0 {
		keepAliveReusePct = (float64(succReqs) / float64(totalConns)) * 100.0
	}

	bytesMB := float64(p.bytesTransferred.Load()) / (1024 * 1024)

	activeProto := "HTTP/1.1"
	if p.cfg != nil {
		if p.cfg.HTTP.EnableHTTP3 {
			activeProto = "HTTP/3 (QUIC)"
		} else if p.cfg.HTTP.EnableHTTP2 {
			activeProto = "HTTP/2 (h2)"
		}
	}

	return HTTPDetailedMetrics{
		ActiveProtocol:   activeProto,
		CreatedClients:   createdCount,
		ReusedClients:    reusedCount,
		ActiveSessions:   activeS,
		FreeSessions:     0,
		ActiveKeepAlives: activeS,
		ReusedConns:      succReqs,
		NewConns:         createdCount,
		ClosedConns:      failReqs,
		KeepAliveReuse:   keepAliveReusePct,
		RPS:              currentRPS,
		TotalRequests:    totReqs,
		PendingRequests:  pending,
		ExecutingReqs:    executing,
		Timeouts:         p.timeouts.Load(),
		Retries:          resets,
		ConnErrors:       p.connErrors.Load(),
		Proxy502Errors:   p.proxy502Errors.Load(),
		Target403Errors:  p.target403Errors.Load(),
		TotalBytesMB:     bytesMB,
	}
}

func (p *Pool) Close() {
	if !p.closed.CompareAndSwap(false, true) {
		return
	}
	if p.limiter != nil {
		p.limiter.Close()
	}
	if p.lifecycle != nil {
		p.lifecycle.Close()
	}
}

// ResetPool purga todas las sesiones existentes en el pool para que se creen nuevas
// desde cero (nuevos sockets, TLS y cookies) y resetea las métricas transitorias.
// Ideal para transiciones entre rondas de reintento o ante ráfagas de MaxRetry.
func (p *Pool) ResetPool() int {
	if p.lifecycle != nil {
		return p.lifecycle.FlushSessions()
	}
	return 0
}

// InFlight retorna cuántos requests están actualmente en vuelo, útil
// para dashboards de observabilidad y para decidir si escalar PoolSize.
func (p *Pool) InFlight() int {
	return len(p.sem)
}

// Capacity retorna el límite configurado de concurrencia global.
func (p *Pool) Capacity() int {
	return cap(p.sem)
}

// Sessions expone las sesiones subyacentes para casos avanzados.
func (p *Pool) Sessions() []*HTTPClient.Session {
	return nil
}

func (p *Pool) Get(url string, headers map[string]string) (*HTTPClient.Result, error) {
	return p.Do(context.Background(), HTTPClient.Spec{
		Method: "GET", URL: url, Kind: HTTPClient.KindDocument, Extra: headers,
	})
}

func (p *Pool) Post(url string, body []byte, headers map[string]string) (*HTTPClient.Result, error) {
	return p.Do(context.Background(), HTTPClient.Spec{
		Method: "POST", URL: url, Kind: HTTPClient.KindDocument, Body: body, Extra: headers,
	})
}

func (p *Pool) GetXHR(url string, headers map[string]string) (*HTTPClient.Result, error) {
	return p.Do(context.Background(), HTTPClient.Spec{
		Method: "GET", URL: url, Kind: HTTPClient.KindXHR, Extra: headers,
	})
}

func (p *Pool) PostXHR(url string, body []byte, headers map[string]string) (*HTTPClient.Result, error) {
	return p.Do(context.Background(), HTTPClient.Spec{
		Method: "POST", URL: url, Kind: HTTPClient.KindXHR, Body: body, ContentType: "application/json", Extra: headers,
	})
}

// Metrics retorna las métricas agregadas de todas las sesiones ejecutadas en el pool.
func (p *Pool) Metrics() (total, success, failure, resets uint64) {
	return p.totalReqCount.Load(), p.succReqCount.Load(), p.failReqCount.Load(), p.resetReqCount.Load()
}
