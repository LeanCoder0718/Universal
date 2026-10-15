package HTTPClient

import (
	"RABBIT/Universal/GeoRoute"
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ConnectionKey identifica unívocamente un contexto de transporte reutilizable.
// Permite que las conexiones TCP/TLS Keep-Alive se reutilicen por afinidad
// geográfica y de ruta sin acoplarse rígidamente a la identidad de un worker.
type ConnectionKey struct {
	CountryCode string
	RouteID     string
	Protocol    Protocol
}

// Normalized asegura que códigos vacíos o desconocidos se traten como GENERIC.
func (k ConnectionKey) Normalized() ConnectionKey {
	cc := strings.ToUpper(strings.TrimSpace(k.CountryCode))
	if cc == "" || cc == "XX" || cc == "UNKNOWN" {
		cc = "GENERIC"
	}
	return ConnectionKey{
		CountryCode: cc,
		RouteID:     strings.TrimSpace(k.RouteID),
		Protocol:    k.Protocol,
	}
}

func (k ConnectionKey) String() string {
	return fmt.Sprintf("%s:%s:%s", k.CountryCode, k.RouteID, k.Protocol)
}

// PooledClient encapsula un cliente HTTP con métricas de uso y control de ciclo de vida.
type PooledClient struct {
	Client       Client
	Proxy        *Entry
	CountryCode  string
	ActiveLeases atomic.Int64
	LastUsed     atomic.Int64 // Unix nano
	CreatedAt    time.Time
}

// Touch actualiza la marca de tiempo del último uso del cliente.
func (p *PooledClient) Touch() {
	p.LastUsed.Store(time.Now().UnixNano())
}

// ConnectionManager administra pools de transporte particionados por afinidad de clave.
// Proporciona reutilización de Keep-Alive por país/ruta, soporte para modo GENERIC
// sin sobrecoste, y desalojo ordenado de conexiones inactivas (LRU/TTL).
type ConnectionManager struct {
	mu      sync.RWMutex
	clients map[ConnectionKey]*PooledClient
	factory Factory
	cfg     *Config

	maxActivePools int
	idleTimeout    time.Duration
	closed         atomic.Bool
	cancel         context.CancelFunc // H-06 Fix: contexto para terminar cleanupLoop

	activePoolsCount atomic.Int64
	reusedPoolsCount atomic.Uint64
	createdPoolCount atomic.Uint64
}

// NewConnectionManager construye una nueva instancia del gestor de conexiones.
func NewConnectionManager(cfg *Config, factory Factory, maxActivePools int, idleTimeout time.Duration) *ConnectionManager {
	if factory == nil {
		factory = NewFactory()
	}
	if maxActivePools <= 0 {
		maxActivePools = 50
	}
	if idleTimeout <= 0 {
		idleTimeout = 120 * time.Second
	}

	// H-06 Fix: Contexto cancelable para que cleanupLoop salga en Close()
	// incluso si Close() se llama desde un panic handler o un shutdown forzado.
	ctx, cancel := context.WithCancel(context.Background())

	cm := &ConnectionManager{
		clients:        make(map[ConnectionKey]*PooledClient, maxActivePools),
		factory:        factory,
		cfg:            cfg,
		maxActivePools: maxActivePools,
		idleTimeout:    idleTimeout,
		cancel:         cancel,
	}

	go cm.cleanupLoop(ctx, idleTimeout/2)
	return cm
}

// GetClient obtiene un cliente cálido del pool compatible con la ConnectionKey o crea uno nuevo bajo demanda.
// Si la clave corresponde a GENERIC, utiliza el proxy base sin mutar etiquetas geográficas.
func (cm *ConnectionManager) GetClient(ctx context.Context, key ConnectionKey, baseParams Params) (Client, *PooledClient, error) {
	if cm.closed.Load() {
		return nil, nil, fmt.Errorf("connection_manager: cerrado")
	}

	normKey := key.Normalized()

	// 1. Búsqueda rápida bajo RLock
	cm.mu.RLock()
	pooled, exists := cm.clients[normKey]
	if exists && pooled != nil && pooled.Client != nil {
		pooled.ActiveLeases.Add(1)
		pooled.Touch()
		cm.reusedPoolsCount.Add(1)
		cm.mu.RUnlock()
		return pooled.Client, pooled, nil
	}
	cm.mu.RUnlock()

	// 2. Creación segura bajo Lock exclusivo
	cm.mu.Lock()
	defer cm.mu.Unlock()

	// Double-check tras adquirir Lock
	if pooled, exists = cm.clients[normKey]; exists && pooled != nil && pooled.Client != nil {
		pooled.ActiveLeases.Add(1)
		pooled.Touch()
		cm.reusedPoolsCount.Add(1)
		return pooled.Client, pooled, nil
	}

	// Si se superó el límite de pools activos, ejecutar desalojo forzado de inactivos
	if len(cm.clients) >= cm.maxActivePools {
		cm.evictOldestIdleLocked()
	}

	// Clonar parámetros y configurar proxy según el país
	params := baseParams
	params.Protocol = normKey.Protocol

	var targetProxy *Entry
	if baseParams.Proxy != nil {
		entryCopy := *baseParams.Proxy
		if normKey.CountryCode != "GENERIC" {
			// Adaptar el usuario del proxy al código de país requerido
			entryCopy.User = applyCountryToUsername(entryCopy.User, normKey.CountryCode)
		}
		targetProxy = &entryCopy
		params.Proxy = targetProxy
	}

	client, err := cm.factory.New(params)
	if err != nil {
		return nil, nil, fmt.Errorf("connection_manager: falló creando cliente para %s: %w", normKey, err)
	}

	newPooled := &PooledClient{
		Client:      client,
		Proxy:       targetProxy,
		CountryCode: normKey.CountryCode,
		CreatedAt:   time.Now(),
	}
	newPooled.ActiveLeases.Store(1)
	newPooled.Touch()

	cm.clients[normKey] = newPooled
	cm.createdPoolCount.Add(1)
	cm.activePoolsCount.Store(int64(len(cm.clients)))

	return client, newPooled, nil
}

// Release libera el lease sobre el cliente correspondiente a la clave.
func (cm *ConnectionManager) Release(key ConnectionKey) {
	normKey := key.Normalized()

	cm.mu.RLock()
	pooled, exists := cm.clients[normKey]
	cm.mu.RUnlock()

	if exists && pooled != nil {
		pooled.ActiveLeases.Add(-1)
		pooled.Touch()
	}
}

func (cm *ConnectionManager) evictOldestIdleLocked() {
	var oldestKey ConnectionKey
	var oldestTime int64 = 1<<62 - 1
	var found bool

	now := time.Now().UnixNano()
	for k, p := range cm.clients {
		if p.ActiveLeases.Load() <= 0 {
			last := p.LastUsed.Load()
			if last < oldestTime {
				oldestTime = last
				oldestKey = k
				found = true
			}
		}
	}

	if found && (now-oldestTime) > int64(cm.idleTimeout) {
		if p, ok := cm.clients[oldestKey]; ok {
			delete(cm.clients, oldestKey)
			if p.Client != nil {
				p.Client.CloseIdleConnections()
			}
		}
	}
}

// H-06 Fix: cleanupLoop acepta contexto para garantizar terminación
// limpia incluso si Close() no se llama (panic, crash, etc.).
func (cm *ConnectionManager) cleanupLoop(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if cm.closed.Load() {
				return
			}

			cm.mu.Lock()
			now := time.Now().UnixNano()
			idleThreshold := int64(cm.idleTimeout)

			for k, p := range cm.clients {
				if p.ActiveLeases.Load() <= 0 {
					last := p.LastUsed.Load()
					if (now - last) > idleThreshold {
						delete(cm.clients, k)
						if p.Client != nil {
							p.Client.CloseIdleConnections()
						}
					}
				}
			}
			cm.activePoolsCount.Store(int64(len(cm.clients)))
			cm.mu.Unlock()
		}
	}
}

// CloseIdleConnections cierra las conexiones inactivas de todos los clientes en el pool sin invalidar el gestor.
// Permite que las rondas subsiguientes de reintento sigan obteniendo conexiones limpias de ConnectionManager.
func (cm *ConnectionManager) CloseIdleConnections() {
	if cm.closed.Load() {
		return
	}

	cm.mu.Lock()
	defer cm.mu.Unlock()

	for k, p := range cm.clients {
		if p != nil && p.Client != nil {
			p.Client.CloseIdleConnections()
		}
		if p != nil && p.ActiveLeases.Load() <= 0 {
			delete(cm.clients, k)
		}
	}
	cm.activePoolsCount.Store(int64(len(cm.clients)))
}

// Close cierra ordenadamente todos los pools de transporte.
func (cm *ConnectionManager) Close() {
	if !cm.closed.CompareAndSwap(false, true) {
		return
	}

	// H-06 Fix: Cancelar el contexto de cleanupLoop para que salga inmediatamente.
	if cm.cancel != nil {
		cm.cancel()
	}

	cm.mu.Lock()
	defer cm.mu.Unlock()

	for k, p := range cm.clients {
		delete(cm.clients, k)
		if p != nil && p.Client != nil {
			p.Client.CloseIdleConnections()
		}
	}
	cm.activePoolsCount.Store(0)
}

// Stats retorna las métricas de pools activos, creados y reusados.
func (cm *ConnectionManager) Stats() (active int64, created, reused uint64) {
	return cm.activePoolsCount.Load(), cm.createdPoolCount.Load(), cm.reusedPoolsCount.Load()
}

// applyCountryToUsername reemplaza el tag de país en usernames usando el enrutador regional universal.
func applyCountryToUsername(user, countryCode string) string {
	return GeoRoute.ApplyCountryToUsername(user, countryCode)
}
