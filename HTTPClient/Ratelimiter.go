package HTTPClient

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"
)

// entryTTL: tiempo tras el cual un limiter de dominio inactivo se elimina.
// Resuelve la fuga de memoria: el diseño original nunca hacía Delete()
// del sync.Map, así que escanear miles de dominios distintos (checker
// multi-target) crecía indefinidamente.
const entryTTL = 10 * time.Minute

type limiterEntry struct {
	limiter    *rate.Limiter
	lastUsedNs atomic.Int64
}

// PerHost throttla requests por dominio raíz, con un token bucket
// independiente por dominio y limpieza periódica de dominios inactivos.
type PerHost struct {
	limiters sync.Map // map[string]*limiterEntry
	rps      float64
	burst    int

	stopCleanup chan struct{}
	closed      atomic.Bool
}

// New crea el limiter y lanza una goroutine de limpieza en background
// que elimina entradas inactivas por más de entryTTL. Llamar Close()
// para detenerla y evitar goroutine leak.
func NewRateLimiter(rps float64, burst int) *PerHost {
	if burst <= 0 {
		burst = 3
	}
	p := &PerHost{rps: rps, burst: burst, stopCleanup: make(chan struct{})}
	go p.cleanupLoop()
	return p
}

func (p *PerHost) cleanupLoop() {
	ticker := time.NewTicker(entryTTL / 2)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			p.evictStale()
		case <-p.stopCleanup:
			return
		}
	}
}

func (p *PerHost) evictStale() {
	cutoff := time.Now().Add(-entryTTL).UnixNano()
	p.limiters.Range(func(key, value any) bool {
		entry := value.(*limiterEntry)
		if entry.lastUsedNs.Load() < cutoff {
			p.limiters.Delete(key)
		}
		return true
	})
}

// Wait bloquea hasta que el dominio de rawURL tenga un token disponible.
func (p *PerHost) Wait(ctx context.Context, rawURL string) error {
	if p.rps <= 0 {
		return nil
	}
	domain := RootDomainFromURL(rawURL)
	if domain == "" {
		return nil
	}
	entry := p.getOrCreate(domain)
	entry.lastUsedNs.Store(time.Now().UnixNano())
	return entry.limiter.Wait(ctx)
}

func (p *PerHost) getOrCreate(domain string) *limiterEntry {
	if v, ok := p.limiters.Load(domain); ok {
		return v.(*limiterEntry)
	}
	newEntry := &limiterEntry{limiter: rate.NewLimiter(rate.Limit(p.rps), p.burst)}
	newEntry.lastUsedNs.Store(time.Now().UnixNano())
	actual, _ := p.limiters.LoadOrStore(domain, newEntry)
	return actual.(*limiterEntry)
}

// ActiveDomains retorna cuántos dominios tienen limiter activo, útil
// para observabilidad (detectar crecimiento anómalo).
func (p *PerHost) ActiveDomains() int {
	count := 0
	p.limiters.Range(func(_, _ any) bool { count++; return true })
	return count
}

// Close detiene la goroutine de limpieza en background. Debe llamarse
// al finalizar el uso del limiter para evitar goroutine leak.
func (p *PerHost) Close() {
	if p.closed.CompareAndSwap(false, true) {
		close(p.stopCleanup)
	}
}
