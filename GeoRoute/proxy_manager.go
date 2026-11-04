package GeoRoute

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// --- Errors & Global Settings ---

var (
	ErrNoProxyForCountry  = errors.New("georoute: no hay proxies configurados para el país solicitado")
	ErrProxyPoolExhausted = errors.New("georoute: todos los proxies del país están en uso o agotados")
	ErrProxyTimeout       = errors.New("georoute: timeout esperando proxy disponible")
	ErrProxyAlreadyFree   = errors.New("georoute: proxy ya fue liberado (double release)")
	ErrManagerClosed      = errors.New("georoute: proxy manager cerrado")

	ActiveProviderConfig ProxyProviderConfig = DefaultCRProviderConfig()
	ActiveApplyGeo       bool                = true
)

// GeoPolicy define la política regional para un proceso o sesión de forma aislada,
// evitando la dependencia en variables globales compartidas.
type GeoPolicy struct {
	Enabled        bool
	ProviderConfig ProxyProviderConfig
}

// NewGeoPolicy crea una política regional con la configuración especificada.
func NewGeoPolicy(enabled bool, cfg ProxyProviderConfig) GeoPolicy {
	return GeoPolicy{
		Enabled:        enabled,
		ProviderConfig: cfg,
	}
}

// DefaultGeoPolicy retorna la política predeterminada basada en la configuración activa.
func DefaultGeoPolicy() GeoPolicy {
	return GeoPolicy{
		Enabled:        ActiveApplyGeo,
		ProviderConfig: ActiveProviderConfig,
	}
}

// Apply adapta un usuario de proxy según esta política de forma aislada.
func (p GeoPolicy) Apply(user, country string) string {
	if !p.Enabled || user == "" || country == "" {
		return user
	}
	country = strings.TrimSpace(country)
	if country == "" || strings.EqualFold(country, "XX") || strings.EqualFold(country, "GENERIC") || strings.EqualFold(country, "UNKNOWN") {
		return CleanCountryFromUsername(user)
	}
	return ApplyCountryToUsernameWithConfig(user, country, p.ProviderConfig)
}

// --- ProxyState ---

// ProxyState representa el estado actual de un proxy dentro de su pool.
type ProxyState int

const (
	ProxyAvailable  ProxyState = iota // Libre, en el canal disponible
	ProxyInUse                        // Reservado por un worker
	ProxyQuarantine                   // En enfriamiento temporal por fallo
	ProxyDead                         // Descartado definitivamente
)

func (s ProxyState) String() string {
	switch s {
	case ProxyAvailable:
		return "AVAILABLE"
	case ProxyInUse:
		return "IN_USE"
	case ProxyQuarantine:
		return "QUARANTINE"
	case ProxyDead:
		return "DEAD"
	default:
		return "UNKNOWN"
	}
}

// --- ProxyNode ---

// ProxyNode representa una instancia individual de proxy con su estado y metadatos.
type ProxyNode struct {
	ID       string // Identificador único (host:port:user)
	Raw      string // Cadena cruda original del proxy
	Parsed   ParsedProxy
	Country  CountryCode // País configurado (del tag del username)
	Resolved string      // Cadena final con el tag de país aplicado

	state           atomic.Int32 // ProxyState
	consecutiveErrs atomic.Uint32
	totalSuccesses  atomic.Uint64
	totalFailures   atomic.Uint64
	lastUsed        atomic.Int64 // Unix nano
	lastFailure     atomic.Int64 // Unix nano
	quarantineUntil atomic.Int64 // Unix nano

	// Métricas adaptativas para balanceo de carga
	activeConns atomic.Int64  // Conexiones actualmente activas sobre este proxy
	latencyEWMA atomic.Int64  // Latencia promedio EWMA en nanosegundos
	bytesTotal  atomic.Uint64 // Bytes totales transferidos a través de este proxy
}

// State retorna el estado actual del proxy de forma atómica.
func (n *ProxyNode) State() ProxyState {
	return ProxyState(n.state.Load())
}

// SetState cambia el estado del proxy de forma atómica.
func (n *ProxyNode) SetState(s ProxyState) {
	n.state.Store(int32(s))
}

// CompareAndSwapState realiza un CAS atómico sobre el estado del proxy.
func (n *ProxyNode) CompareAndSwapState(old, new ProxyState) bool {
	return n.state.CompareAndSwap(int32(old), int32(new))
}

// ActiveConns retorna el número de conexiones activas sobre este proxy.
func (n *ProxyNode) ActiveConns() int64 {
	return n.activeConns.Load()
}

// LatencyEWMA retorna la latencia promedio EWMA en nanosegundos.
func (n *ProxyNode) LatencyEWMA() time.Duration {
	return time.Duration(n.latencyEWMA.Load())
}

// RecordLatency actualiza la latencia EWMA con un factor de suavizado α=0.3.
// Thread-safe mediante CAS atómico.
func (n *ProxyNode) RecordLatency(d time.Duration) {
	const alpha = 30 // α × 100 para operar con enteros
	newVal := d.Nanoseconds()
	for {
		old := n.latencyEWMA.Load()
		if old == 0 {
			if n.latencyEWMA.CompareAndSwap(0, newVal) {
				return
			}
			continue
		}
		// EWMA = α×new + (1−α)×old = (30×new + 70×old) / 100
		ewma := (alpha*newVal + (100-alpha)*old) / 100
		if n.latencyEWMA.CompareAndSwap(old, ewma) {
			return
		}
	}
}

// RecordBytes agrega bytes transferidos al contador del proxy.
func (n *ProxyNode) RecordBytes(b uint64) {
	n.bytesTotal.Add(b)
}

// --- ManagerConfig ---

// ManagerConfig contiene la configuración del ProxyManager.
type ManagerConfig struct {
	// AcquireTimeout es el tiempo máximo de espera para obtener un proxy disponible.
	// Si se agota, se retorna ErrProxyTimeout. Default: 30s.
	AcquireTimeout time.Duration

	// MaxConsecutiveErrors es el número máximo de fallos consecutivos antes de
	// mover un proxy a cuarentena. Default: 3.
	MaxConsecutiveErrors uint32

	// QuarantineDuration es el tiempo que un proxy permanece en cuarentena
	// antes de volver a estar disponible. Default: 30s.
	QuarantineDuration time.Duration

	// MaxDeadErrors es el número total de cuarentenas acumuladas antes de marcar
	// un proxy como muerto definitivamente. Default: 10.
	MaxDeadErrors uint32

	// RecoveryInterval es el intervalo de la goroutine de recuperación que
	// revisa proxies en cuarentena. Default: 5s.
	RecoveryInterval time.Duration

	// AllowUnknownCountry, si es true, permite adquirir proxies para el país
	// "XX" (Unknown) usando el pool general sin tag regional. Default: true.
	AllowUnknownCountry bool

	// AdaptProxyToComboIP controla si se debe aplicar enrutamiento geográfico.
	//   true:  Cada cuenta obtiene un proxy cuyo país coincide con el del combo.
	//   false: Todos los combos usan el pool general con balanceo adaptativo
	//          (Least Active Connections + EWMA Latency). Default: true.
	AdaptProxyToComboIP bool
}

// DefaultManagerConfig retorna la configuración recomendada para producción.
func DefaultManagerConfig() ManagerConfig {
	return ManagerConfig{
		AcquireTimeout:       30 * time.Second,
		MaxConsecutiveErrors: 3,
		QuarantineDuration:   30 * time.Second,
		MaxDeadErrors:        10,
		RecoveryInterval:     5 * time.Second,
		AllowUnknownCountry:  true,
		AdaptProxyToComboIP:  true,
	}
}

// --- CountryProxyPool ---

// CountryProxyPool administra el conjunto de proxies disponibles de un país específico.
// Implementa acceso concurrente compartido y NO BLOQUEANTE (O(1)):
// Múltiples threads pueden utilizar simultáneamente todos los proxies disponibles
// sin que ningún thread quede suspendido en canales de espera.
type CountryProxyPool struct {
	country  CountryCode
	allNodes []*ProxyNode  // Todos los nodos del pool
	rrIndex  atomic.Uint64 // Contador Round-Robin lock-free

	mu         sync.RWMutex
	quarantine map[string]*ProxyNode // Nodos en cuarentena temporal (key: ID)
	dead       map[string]*ProxyNode // Nodos descartados (key: ID)

	totalCount    int
	quarantineCnt atomic.Int64
	deadCount     atomic.Int64
}

// newCountryProxyPool crea un pool vacío para el país dado.
func newCountryProxyPool(country CountryCode, capacity int) *CountryProxyPool {
	if capacity <= 0 {
		capacity = 1
	}
	return &CountryProxyPool{
		country:    country,
		allNodes:   make([]*ProxyNode, 0, capacity),
		quarantine: make(map[string]*ProxyNode),
		dead:       make(map[string]*ProxyNode),
		totalCount: 0,
	}
}

// addNode agrega un proxy al pool y lo marca como disponible.
func (p *CountryProxyPool) addNode(node *ProxyNode) {
	p.mu.Lock()
	p.allNodes = append(p.allNodes, node)
	p.totalCount++
	p.mu.Unlock()

	node.SetState(ProxyAvailable)
}

// acquire selecciona de forma INMEDIATA y NO BLOQUEANTE (O(1)) el proxy más óptimo del pool.
// Si hay 100 threads y 50 proxies, todos los 100 threads obtienen proxy instantáneamente
// compartiendo y balanceando la carga (Least Active Connections con desempate Round-Robin).
func (p *CountryProxyPool) acquire(ctx context.Context) (*ProxyNode, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("%w: %v (país: %s)", ErrProxyTimeout, err, p.country)
	}

	p.mu.RLock()
	nodesLen := len(p.allNodes)
	if nodesLen == 0 {
		p.mu.RUnlock()
		return nil, ErrNoProxiesAvailable
	}

	startIdx := int(p.rrIndex.Add(1) % uint64(nodesLen))
	var bestNode *ProxyNode
	var minConns int64 = 1<<62 - 1

	// Buscar el nodo saludable con menor número de conexiones activas
	for i := 0; i < nodesLen; i++ {
		idx := (startIdx + i) % nodesLen
		node := p.allNodes[idx]

		state := node.State()
		if state == ProxyQuarantine || state == ProxyDead {
			continue
		}

		conns := node.activeConns.Load()
		if conns < minConns {
			minConns = conns
			bestNode = node
			if conns == 0 {
				// Cero conexiones: selección inmediata
				break
			}
		}
	}
	p.mu.RUnlock()

	if bestNode == nil {
		return nil, fmt.Errorf("%w: todos los proxies de %s están en cuarentena o descartados", ErrProxyPoolExhausted, p.country)
	}

	bestNode.lastUsed.Store(time.Now().UnixNano())
	bestNode.activeConns.Add(1)
	return bestNode, nil
}

// release devuelve un proxy al pool tras un uso exitoso.
func (p *CountryProxyPool) release(node *ProxyNode) error {
	if node == nil {
		return nil
	}
	node.activeConns.Add(-1)
	node.consecutiveErrs.Store(0)
	node.totalSuccesses.Add(1)
	return nil
}

// releaseMildError procesa un fallo leve preservando errores consecutivos.
func (p *CountryProxyPool) releaseMildError(node *ProxyNode) error {
	if node == nil {
		return nil
	}
	node.activeConns.Add(-1)
	node.totalFailures.Add(1)
	return nil
}

// moveToQuarantine envía un proxy a cuarentena temporal.
func (p *CountryProxyPool) moveToQuarantine(node *ProxyNode, duration time.Duration) {
	if node == nil {
		return
	}
	if !node.CompareAndSwapState(ProxyAvailable, ProxyQuarantine) {
		return
	}

	node.activeConns.Add(-1)
	node.lastFailure.Store(time.Now().UnixNano())
	node.totalFailures.Add(1)
	node.quarantineUntil.Store(time.Now().Add(duration).UnixNano())
	p.quarantineCnt.Add(1)

	p.mu.Lock()
	p.quarantine[node.ID] = node
	p.mu.Unlock()
}

// moveToDead descarta permanentemente un proxy.
func (p *CountryProxyPool) moveToDead(node *ProxyNode) {
	if node == nil {
		return
	}
	old := ProxyState(node.state.Load())
	node.SetState(ProxyDead)

	p.mu.Lock()
	delete(p.quarantine, node.ID)
	p.dead[node.ID] = node
	p.mu.Unlock()

	if old == ProxyAvailable {
		node.activeConns.Add(-1)
	} else if old == ProxyQuarantine {
		p.quarantineCnt.Add(-1)
	}
	p.deadCount.Add(1)
}

// recoverQuarantined revisa los proxies en cuarentena y los reactiva si expiró el tiempo.
func (p *CountryProxyPool) recoverQuarantined() int {
	now := time.Now().UnixNano()
	recovered := 0

	p.mu.Lock()
	var toRecover []*ProxyNode
	for id, node := range p.quarantine {
		if node.quarantineUntil.Load() <= now {
			toRecover = append(toRecover, node)
			delete(p.quarantine, id)
		}
	}
	p.mu.Unlock()

	for _, node := range toRecover {
		if node.CompareAndSwapState(ProxyQuarantine, ProxyAvailable) {
			node.consecutiveErrs.Store(0)
			p.quarantineCnt.Add(-1)
			recovered++
		}
	}
	return recovered
}

// Stats retorna métricas del pool.
type PoolStats struct {
	Country    CountryCode
	Total      int
	Available  int64
	InUse      int64
	Quarantine int64
	Dead       int64
}

func (p *CountryProxyPool) Stats() PoolStats {
	p.mu.RLock()
	defer p.mu.RUnlock()

	var inUse int64
	var avail int64
	for _, n := range p.allNodes {
		if n.State() == ProxyAvailable {
			avail++
			inUse += n.activeConns.Load()
		}
	}

	return PoolStats{
		Country:    p.country,
		Total:      p.totalCount,
		Available:  avail,
		InUse:      inUse,
		Quarantine: p.quarantineCnt.Load(),
		Dead:       p.deadCount.Load(),
	}
}

// --- ProxyManager ---

// ProxyManager orquesta múltiples CountryProxyPool y gestiona el ciclo de vida
// completo de la asignación geográfica de proxies.
type ProxyManager struct {
	mu       sync.RWMutex
	pools    map[CountryCode]*CountryProxyPool
	provider *TemplateProvider
	cfg      ManagerConfig
	closed   atomic.Bool
	cancel   context.CancelFunc
	wg       sync.WaitGroup

	// globalPool es un pool unificado (sin tags de país) utilizado cuando
	// AdaptProxyToComboIP == false. Todos los workers comparten este pool
	// y el balanceador Least-Connections selecciona el proxy óptimo.
	globalPool *CountryProxyPool
}

// NewProxyManager crea un ProxyManager a partir de una lista cruda de proxies y un proveedor de plantillas.
// Los proxies se parsean, se les aplica la transformación de país configurada y se distribuyen
// en pools por CountryCode.
//
// Si countries es nil o vacío, se crean pools para todos los países configurados.
// Si countries tiene valores, solo se crean pools para esos países.
func NewProxyManager(rawProxies []string, providerCfg ProxyProviderConfig, countries []CountryCode, cfg ManagerConfig) (*ProxyManager, error) {
	if len(rawProxies) == 0 {
		return nil, ErrNoProxiesAvailable
	}

	provider := NewTemplateProvider(providerCfg)

	// Parsear y deduplicar proxies
	seen := make(map[string]struct{}, len(rawProxies))
	var parsedProxies []ParsedProxy
	for _, raw := range rawProxies {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		p, err := ParseProxy(raw)
		if err != nil {
			continue
		}
		key := p.Host + ":" + p.Port + ":" + p.Username
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		parsedProxies = append(parsedProxies, p)
	}

	if len(parsedProxies) == 0 {
		return nil, ErrNoProxiesAvailable
	}

	// Determinar qué países configurar
	countrySet := make(map[CountryCode]struct{})
	if len(countries) > 0 {
		for _, cc := range countries {
			countrySet[cc] = struct{}{}
		}
	}

	// Si no se especificaron países, crear pool general "XX" y uno por cada país
	// que se detecte necesario en runtime (lazy creation)

	ctx, cancel := context.WithCancel(context.Background())

	pm := &ProxyManager{
		pools:    make(map[CountryCode]*CountryProxyPool),
		provider: provider,
		cfg:      cfg,
		cancel:   cancel,
	}

	// Crear pools para los países especificados
	if len(countrySet) > 0 {
		for cc := range countrySet {
			pool := newCountryProxyPool(cc, len(parsedProxies))
			pm.pools[cc] = pool

			// Transformar cada proxy base para este país y agregarlo al pool
			for i := range parsedProxies {
				transformed, err := provider.Transform(parsedProxies[i], cc)
				if err != nil {
					continue
				}

				node := &ProxyNode{
					ID:       fmt.Sprintf("%s:%s:%d", cc, parsedProxies[i].Host, i),
					Raw:      parsedProxies[i].Raw,
					Parsed:   parsedProxies[i],
					Country:  cc,
					Resolved: transformed,
				}
				pool.addNode(node)
			}
		}
	}

	// Siempre crear un pool general para Unknown si está habilitado
	if cfg.AllowUnknownCountry {
		if _, exists := pm.pools[Unknown]; !exists {
			pool := newCountryProxyPool(Unknown, len(parsedProxies))
			pm.pools[Unknown] = pool

			for i := range parsedProxies {
				// Para Unknown, usar proxy base sin transformación regional
				transformed, err := provider.Transform(parsedProxies[i], Unknown)
				if err != nil {
					transformed = parsedProxies[i].String()
				}

				node := &ProxyNode{
					ID:       fmt.Sprintf("XX:%s:%d", parsedProxies[i].Host, i),
					Raw:      parsedProxies[i].Raw,
					Parsed:   parsedProxies[i],
					Country:  Unknown,
					Resolved: transformed,
				}
				pool.addNode(node)
			}
		}
	}

	// Crear pool global unificado (para modo AdaptProxyToComboIP=false)
	// Este pool contiene TODOS los proxies sin tag de país.
	globalPool := newCountryProxyPool("GLOBAL", len(parsedProxies))
	for i := range parsedProxies {
		node := &ProxyNode{
			ID:       fmt.Sprintf("GLOBAL:%s:%d", parsedProxies[i].Host, i),
			Raw:      parsedProxies[i].Raw,
			Parsed:   parsedProxies[i],
			Country:  "GLOBAL",
			Resolved: parsedProxies[i].String(),
		}
		globalPool.addNode(node)
	}
	pm.globalPool = globalPool

	// Iniciar goroutine de recuperación de cuarentena
	pm.wg.Add(1)
	go pm.recoveryLoop(ctx)

	return pm, nil
}

// EnsurePool crea dinámicamente un pool para un país que no fue pre-configurado.
// Thread-safe: puede llamarse concurrentemente desde múltiples workers.
func (pm *ProxyManager) EnsurePool(country CountryCode, baseProxies []ParsedProxy) {
	if pm.closed.Load() {
		return
	}

	pm.mu.RLock()
	_, exists := pm.pools[country]
	pm.mu.RUnlock()
	if exists {
		return
	}

	pm.mu.Lock()
	defer pm.mu.Unlock()

	// Double-check bajo write lock
	if _, exists := pm.pools[country]; exists {
		return
	}

	pool := newCountryProxyPool(country, len(baseProxies))
	for i := range baseProxies {
		transformed, err := pm.provider.Transform(baseProxies[i], country)
		if err != nil {
			continue
		}

		node := &ProxyNode{
			ID:       fmt.Sprintf("%s:%s:%d", country, baseProxies[i].Host, i),
			Raw:      baseProxies[i].Raw,
			Parsed:   baseProxies[i],
			Country:  country,
			Resolved: transformed,
		}
		pool.addNode(node)
	}
	pm.pools[country] = pool
}

// Acquire bloquea hasta obtener un proxy disponible del país solicitado.
// Si AdaptProxyToComboIP es false, ignora el país y usa el pool global.
// Si el país es Unknown y AllowUnknownCountry está habilitado, usa el pool general.
// Retorna ErrNoProxyForCountry si no existe pool para ese país.
// Retorna ErrProxyTimeout si expira el timeout de adquisición.
func (pm *ProxyManager) Acquire(ctx context.Context, country CountryCode) (*ProxyNode, error) {
	if pm.closed.Load() {
		return nil, ErrManagerClosed
	}

	// Si no se adapta por país, usar siempre el pool global
	if !pm.cfg.AdaptProxyToComboIP {
		return pm.acquireFromPool(ctx, pm.globalPool)
	}

	pm.mu.RLock()
	pool, exists := pm.pools[country]
	pm.mu.RUnlock()

	if !exists {
		// Si el país es desconocido y tenemos pool general, usar ese
		if country.IsUnknown() && pm.cfg.AllowUnknownCountry {
			pm.mu.RLock()
			pool, exists = pm.pools[Unknown]
			pm.mu.RUnlock()
		}
		if !exists {
			return nil, fmt.Errorf("%w: %s", ErrNoProxyForCountry, country)
		}
	}

	return pm.acquireFromPool(ctx, pool)
}

// acquireFromPool centraliza la lógica de adquisición con timeout y selección no-bloqueante.
func (pm *ProxyManager) acquireFromPool(ctx context.Context, pool *CountryProxyPool) (*ProxyNode, error) {
	if pool == nil {
		return nil, ErrNoProxiesAvailable
	}
	return pool.acquire(ctx)
}

// IsGeoMode retorna true si el ProxyManager está en modo de adaptación geográfica.
func (pm *ProxyManager) IsGeoMode() bool {
	return pm.cfg.AdaptProxyToComboIP
}

// Release devuelve un proxy al pool después de un uso exitoso o con error.
// Seguro para llamar múltiples veces de forma thread-safe.
func (pm *ProxyManager) Release(node *ProxyNode, execErr error) {
	if node == nil || pm.closed.Load() {
		return
	}

	// Resolver el pool correcto
	var pool *CountryProxyPool
	if !pm.cfg.AdaptProxyToComboIP && pm.globalPool != nil {
		pool = pm.globalPool
	} else {
		pm.mu.RLock()
		pool, _ = pm.pools[node.Country]
		pm.mu.RUnlock()
	}

	if execErr == nil {
		// Éxito: devolver al pool
		if pool != nil {
			_ = pool.release(node)
		}
		return
	}

	// Error: evaluar severidad
	errCount := node.consecutiveErrs.Add(1)

	if pool == nil {
		return
	}

	if errCount >= pm.cfg.MaxConsecutiveErrors {
		totalFails := node.totalFailures.Load()
		if uint32(totalFails) >= pm.cfg.MaxDeadErrors {
			pool.moveToDead(node)
		} else {
			pool.moveToQuarantine(node, pm.cfg.QuarantineDuration)
		}
	} else {
		// Error leve: devolver al pool preservando el contador de errores consecutivos
		_ = pool.releaseMildError(node)
	}
}

// Replace cuarentinea el proxy fallido y adquiere uno nuevo del mismo país.
func (pm *ProxyManager) Replace(ctx context.Context, failedNode *ProxyNode) (*ProxyNode, error) {
	if failedNode == nil {
		return nil, fmt.Errorf("proxy node nil")
	}

	// Reportar fallo del nodo actual
	pm.Release(failedNode, fmt.Errorf("proxy replacement requested"))

	// Adquirir un nuevo proxy del mismo país
	return pm.Acquire(ctx, failedNode.Country)
}

// Stats retorna las métricas de todos los pools.
func (pm *ProxyManager) Stats() []PoolStats {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	stats := make([]PoolStats, 0, len(pm.pools)+1)
	for _, pool := range pm.pools {
		stats = append(stats, pool.Stats())
	}
	if pm.globalPool != nil {
		stats = append(stats, pm.globalPool.Stats())
	}
	return stats
}

// HasPool verifica si existe un pool para el país dado.
func (pm *ProxyManager) HasPool(country CountryCode) bool {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	_, exists := pm.pools[country]
	return exists
}

// PoolCountries retorna los códigos de país con pools configurados.
func (pm *ProxyManager) PoolCountries() []CountryCode {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	countries := make([]CountryCode, 0, len(pm.pools))
	for cc := range pm.pools {
		countries = append(countries, cc)
	}
	return countries
}

// Close detiene la goroutine de recuperación y libera recursos.
func (pm *ProxyManager) Close() {
	if !pm.closed.CompareAndSwap(false, true) {
		return
	}
	if pm.cancel != nil {
		pm.cancel()
	}
	pm.wg.Wait()
}

// recoveryLoop revisa periódicamente los proxies en cuarentena y los devuelve
// al pool de disponibles cuando su período de enfriamiento ha expirado.
func (pm *ProxyManager) recoveryLoop(ctx context.Context) {
	defer pm.wg.Done()

	interval := pm.cfg.RecoveryInterval
	if interval <= 0 {
		interval = 5 * time.Second
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			pm.mu.RLock()
			pools := make([]*CountryProxyPool, 0, len(pm.pools)+1)
			for _, p := range pm.pools {
				pools = append(pools, p)
			}
			pm.mu.RUnlock()

			// Incluir globalPool en la recuperación
			if pm.globalPool != nil {
				pools = append(pools, pm.globalPool)
			}

			for _, pool := range pools {
				pool.recoverQuarantined()
			}
		}
	}
}

// ResolvedProxyString retorna la cadena del proxy resuelta lista para HTTPClient.
// Formato: "host:port:user:pass" o "host:port".
func (n *ProxyNode) ResolvedProxyString() string {
	return n.Resolved
}
