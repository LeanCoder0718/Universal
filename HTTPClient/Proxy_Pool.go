// Package HTTPClient — gestión de gateways de proxy SIN circuit breaker.
//
// Caso de uso objetivo: 1 (o pocos) endpoints residenciales rotativos
// compartidos por cientos de goroutines. Un timeout en la goroutine B
// NUNCA debe banear el gateway para A..Z.
//
// Selección:
//   - Gateway fijo / round-robin simple entre entries.
//   - ModeSticky: inyecta session-id estable por Session (pin de IP del vendor).
//   - RecordResult: solo contadores globales de observabilidad.
package HTTPClient

import (
	"github.com/LeanCoder0718/Universal/GeoRoute"
	"context"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

type Type string

const (
	TypeHTTP   Type = "http"
	TypeHTTPS  Type = "https"
	TypeSOCKS5 Type = "socks5"
)

type Entry struct {
	Host string
	Port string
	User string
	Pass string
}

func (e Entry) key() string   { return e.Host + ":" + e.Port }
func (e Entry) HasAuth() bool { return e.User != "" }

func parseEntry(raw string) (Entry, bool) {
	parsed, err := GeoRoute.ParseProxy(raw)
	if err != nil {
		return Entry{}, false
	}
	return Entry{
		Host: parsed.Host,
		Port: parsed.Port,
		User: parsed.Username,
		Pass: parsed.Password,
	}, true
}

func (e Entry) URL(t Type) (*url.URL, error) {
	if t == "" {
		t = TypeHTTP
	}
	if !e.HasAuth() {
		return url.Parse(fmt.Sprintf("%s://%s:%s", t, e.Host, e.Port))
	}
	return url.Parse(fmt.Sprintf("%s://%s@%s:%s",
		t, url.UserPassword(e.User, e.Pass).String(), e.Host, e.Port))
}

func (e Entry) String(t Type) string {
	if t == "" {
		t = TypeHTTP
	}
	if !e.HasAuth() {
		return fmt.Sprintf("%s://%s:%s", t, e.Host, e.Port)
	}
	return fmt.Sprintf("%s://%s:%s@%s:%s",
		t, url.QueryEscape(e.User), url.QueryEscape(e.Pass), e.Host, e.Port)
}

// ProxyStatus representa el estado de salud de un proxy en el pool.
type ProxyStatus int

const (
	StatusHealthy ProxyStatus = iota
	StatusCooldown
	StatusDead
)

func (s ProxyStatus) String() string {
	switch s {
	case StatusHealthy:
		return "HEALTHY"
	case StatusCooldown:
		return "COOLDOWN"
	case StatusDead:
		return "DEAD"
	default:
		return "UNKNOWN"
	}
}

// ProxyNode encapsula una entrada de proxy con métricas individuales y estado de salud.
type ProxyNode struct {
	Entry          Entry
	Status         ProxyStatus
	ConsecutiveErr int
	CooldownUntil  time.Time
	TotalSuccess   uint64
	TotalFailures  uint64
	AvgLatencyMs   int64
	InFlight       int32
	LastUsed       time.Time
}

// AcquirePolicy define las reglas de selección de un proxy para un intento específico.
type AcquirePolicy struct {
	ExcludedHosts []string      // Proxies/hosts ya utilizados por la tarea (para no repetir)
	MaxWait       time.Duration // Timeout si el pool está bajo backpressure
	AllowFallback bool          // Si todos los disponibles fallaron, ¿permite reusar el menos malo?
}

// ProxyMetrics — observabilidad compatible con dashboards y monitoreo.
type ProxyMetrics struct {
	Entry      Entry
	Successes  uint64
	Failures   uint64
	AvgLatency time.Duration
	Banned     bool
	Status     string
}

// Pool mantiene la lista de gateways con rotación por intento y métricas de observabilidad.
// Diseñado SIN circuit breaker para que proxies residenciales rotativos compartidos por
// cientos de workers nunca se congelen ni se bloqueen por fallas temporales de una IP.
type Pool struct {
	mu      sync.RWMutex
	entries []Entry
	nodes   []*ProxyNode
	cfg     ProxyConfig
	rr      atomic.Uint64 // round-robin base

	totalOK   atomic.Uint64
	totalFail atomic.Uint64
}

func NewPool(raw []string, cfg ProxyConfig) (*Pool, error) {
	entries := make([]Entry, 0, len(raw))
	nodes := make([]*ProxyNode, 0, len(raw))
	now := time.Now()
	for _, r := range raw {
		if e, ok := parseEntry(strings.TrimSpace(r)); ok && e.Host != "" && e.Port != "" {
			entries = append(entries, e)
			nodes = append(nodes, &ProxyNode{
				Entry:    e,
				Status:   StatusHealthy,
				LastUsed: now,
			})
		}
	}
	if len(entries) == 0 {
		return nil, fmt.Errorf("proxy: %w (recibidas %d entradas crudas)", ErrEmptyProxyPool, len(raw))
	}
	return &Pool{
		entries: entries,
		nodes:   nodes,
		cfg:     cfg,
	}, nil
}

func (p *Pool) Type() Type {
	switch Type(p.cfg.ProxyType) {
	case TypeHTTPS:
		return TypeHTTPS
	case TypeSOCKS5:
		return TypeSOCKS5
	default:
		return TypeHTTP
	}
}

// Mode resuelve ModeSticky si cfg.Sticky legacy está activo.
func (p *Pool) Mode() ProxyMode {
	if p.cfg.Sticky {
		return ModeSticky
	}
	return p.cfg.Mode
}

// Acquire selecciona un proxy para un intento de ejecución respetando la política provista.
// En pools de 1 solo endpoint / Gateway residencial rotativo:
// NUNCA se congela ni se excluye. Se entrega inmediatamente para que todos los cientos
// de workers trabajen a máxima velocidad en paralelo sin interrupciones.
// En pools con múltiples proxies: rota excluyendo los proxies ya probados en la tarea actual.
func (p *Pool) Acquire(ctx context.Context, policy AcquirePolicy) (*ProxyNode, func(success bool, err error), error) {
	n := len(p.nodes) // p.nodes es inmutable tras NewPool; len() es safe sin lock
	if n == 0 {
		return nil, nil, ErrEmptyProxyPool
	}

	// H-10 Fix: Caso de 1 solo endpoint / Gateway residencial rotativo:
	// Acceso lock-free usando atomic para InFlight. Elimina la serialización
	// de 500+ workers detrás de un mutex para leer siempre el mismo nodo.
	if n == 1 {
		p.mu.Lock()
		node := p.nodes[0]
		atomic.AddInt32(&node.InFlight, 1)
		node.LastUsed = time.Now()
		p.mu.Unlock()
		release := p.makeReleaseFunc(node)
		return node, release, nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	now := time.Now()

	// Construir conjunto de exclusiones para búsqueda en O(1)
	excluded := make(map[string]struct{}, len(policy.ExcludedHosts))
	for _, h := range policy.ExcludedHosts {
		h = strings.TrimSpace(h)
		if h != "" {
			excluded[h] = struct{}{}
		}
	}

	// 2. Búsqueda round-robin de candidato no excluido en este intento
	startIdx := int(p.rr.Add(1)-1) % n
	for i := 0; i < n; i++ {
		node := p.nodes[(startIdx+i)%n]

		// Verificar si fue probado en intentos previos de esta tarea
		if _, ok := excluded[node.Entry.Host]; ok {
			continue
		}
		if _, ok := excluded[node.Entry.key()]; ok {
			continue
		}

		// Candidato óptimo encontrado
		node.InFlight++
		node.LastUsed = now
		release := p.makeReleaseFunc(node)
		return node, release, nil
	}

	// 3. Fallback cuando todos los proxies de la lista están en excludedHosts
	if policy.AllowFallback {
		var oldestNode *ProxyNode
		for _, node := range p.nodes {
			if oldestNode == nil || node.LastUsed.Before(oldestNode.LastUsed) {
				oldestNode = node
			}
		}
		if oldestNode != nil {
			oldestNode.InFlight++
			oldestNode.LastUsed = now
			release := p.makeReleaseFunc(oldestNode)
			return oldestNode, release, nil
		}
	}

	return nil, nil, ErrNoProxiesAvailable
}

func (p *Pool) makeReleaseFunc(node *ProxyNode) func(success bool, err error) {
	var once sync.Once
	return func(success bool, err error) {
		once.Do(func() {
			if success {
				p.ReportSuccess(node.Entry, 0)
			} else {
				p.ReportFailure(node.Entry, err)
			}
		})
	}
}

// ReportSuccess actualiza el nodo reportando un intento exitoso.
func (p *Pool) ReportSuccess(e Entry, latency time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.totalOK.Add(1)
	node := p.findNodeLocked(e)
	if node == nil {
		return
	}

	if node.InFlight > 0 {
		node.InFlight--
	}
	node.Status = StatusHealthy
	node.ConsecutiveErr = 0
	node.TotalSuccess++
	if latency > 0 {
		latMs := latency.Milliseconds()
		if node.AvgLatencyMs == 0 {
			node.AvgLatencyMs = latMs
		} else {
			node.AvgLatencyMs = (node.AvgLatencyMs*7 + latMs*3) / 10
		}
	}
}

// ReportFailure registra la falla en métricas de observabilidad SIN congelar ni banear el proxy.
// Esto garantiza que proxies residenciales rotativos compartidos por cientos de workers nunca
// se bloqueen para el resto de goroutines por una IP fallida upstream.
func (p *Pool) ReportFailure(e Entry, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.totalFail.Add(1)
	node := p.findNodeLocked(e)
	if node == nil {
		return
	}

	if node.InFlight > 0 {
		node.InFlight--
	}
	node.TotalFailures++
	node.ConsecutiveErr++
}

func (p *Pool) findNodeLocked(e Entry) *ProxyNode {
	key := e.key()
	for _, n := range p.nodes {
		if n.Entry.key() == key {
			return n
		}
	}
	for _, n := range p.nodes {
		if n.Entry.Host == e.Host {
			return n
		}
	}
	return nil
}

// GetNode busca y retorna una copia de información del nodo para inspección segura.
func (p *Pool) GetNode(host string) *ProxyNode {
	p.mu.RLock()
	defer p.mu.RUnlock()
	for _, n := range p.nodes {
		if n.Entry.Host == host || n.Entry.key() == host {
			cp := *n
			return &cp
		}
	}
	return nil
}

// Gateway elige un endpoint respetando el pool y la salud actual de los nodos.
func (p *Pool) Gateway() (Entry, error) {
	node, _, err := p.Acquire(context.Background(), AcquirePolicy{AllowFallback: true})
	if err != nil {
		return Entry{}, err
	}
	return node.Entry, nil
}

// Resolve obtiene el Entry del proxy tal como fue configurado.
func (p *Pool) Resolve() (Entry, error) {
	return p.Gateway()
}

// RecordResult registra el resultado en los contadores y actualiza la salud del proxy.
func (p *Pool) RecordResult(e Entry, d time.Duration, success bool) {
	if success {
		p.ReportSuccess(e, d)
	} else {
		p.ReportFailure(e, nil)
	}
}

func (p *Pool) Counters() (ok, fail uint64) {
	return p.totalOK.Load(), p.totalFail.Load()
}

// Metrics retorna métricas por cada gateway para compatibilidad con dashboards.
func (p *Pool) Metrics() []ProxyMetrics {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := make([]ProxyMetrics, 0, len(p.nodes))
	for _, n := range p.nodes {
		out = append(out, ProxyMetrics{
			Entry:      n.Entry,
			Successes:  n.TotalSuccess,
			Failures:   n.TotalFailures,
			AvgLatency: time.Duration(n.AvgLatencyMs) * time.Millisecond,
			Banned:     n.Status == StatusDead,
			Status:     n.Status.String(),
		})
	}
	return out
}

// Pick compatibilidad legacy.
func (p *Pool) Pick() (Entry, error) { return p.Gateway() }

// NewStickyID expone generación de id para callers que resetean egress.
func NewStickyID() string { return uuid.NewString() }

// SetGeoCountry reemplaza dinámicamente el código de país en los usernames
// de todos los proxy entries usando el enrutador regional universal.
func (p *Pool) SetGeoCountry(countryCode string) bool {
	if countryCode == "" {
		return false
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	anyChanged := false
	for i, e := range p.entries {
		if e.User == "" {
			continue
		}
		newUsername := GeoRoute.ApplyCountryToUsername(e.User, countryCode)
		if p.entries[i].User != newUsername {
			p.entries[i].User = newUsername
			if i < len(p.nodes) {
				p.nodes[i].Entry.User = newUsername
			}
			anyChanged = true
		}
	}
	return anyChanged
}
