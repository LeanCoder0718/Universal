// Package browser contiene el núcleo absoluto de la librería: Session.
package HTTPClient

import (
	"context"
	"os"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/LeanCoder0718/Universal/GeoRoute"
	"github.com/LeanCoder0718/Universal/HTTPClient/Profiles"
	retry "github.com/LeanCoder0718/Universal/HTTPClient/Retry"

	fhttp "github.com/bogdanfinn/fhttp"
	"github.com/fatih/color"
)

// state agrupa TODO lo que attempt() necesita para ejecutar un request de
// forma aislada. Es inmutable una vez capturado: attempt() nunca toca
// s.client/s.builder/s.currentProxy directamente, solo lee este snapshot.
// Esto es lo que permite liberar el mutex de Session durante la I/O real.
type state struct {
	client       Client
	builder      *Builder
	jar          *Jar
	currentProxy *Entry
	stickyID     string
}

// Session es el núcleo único de la librería.
type Session struct {
	mu sync.Mutex // protege SOLO client/builder/profile/currentProxy (mutación de estado)

	cfg      *Config
	profile  Profile
	jar      *Jar
	builder  *Builder
	retryEng *retry.Engine
	factory  Factory

	client        Client
	directClient  Client
	proxyPool     *Pool
	currentProxy   *Entry
	clientCache    map[string]Client
	clientLastUsed map[string]time.Time
	connMgr        *ConnectionManager
	currentGeoKey ConnectionKey
	stickyID      string
	opts          sessionParams
	storage       map[string]any

	// generation se incrementa en cada refresh/rotate. Permite detectar,
	// tras liberar y reintentar, si el estado que capturamos sigue vigente
	// (evita pisar una rotación más reciente con una más antigua bajo
	// alta concurrencia sobre la MISMA Session, ver hallazgo de carrera).
	generation atomic.Uint64

	requestCount atomic.Uint64
	successCount atomic.Uint64
	failureCount atomic.Uint64
	resetCount   atomic.Uint64
}

type Option func(*sessionParams)

type sessionParams struct {
	identityOpts   []IdentityOption
	classifier     retry.Classifier
	factory        Factory
	proxyRaw       []string
	proxyEntry     *Entry
	proxyPool      *Pool
	connMgr        *ConnectionManager
	profileSpec    *Profiles.ProfileSpec // nil = usa ProfileManager o sistema legacy
	profileManager *Profiles.Manager     // nil = usa ProfileSpec o sistema legacy
	allowedHosts    []string
	enableSessionID *bool
}

// WithAllowedSessionHosts restringe la inyección de x-session-id a los hosts/dominios especificados.
func WithAllowedSessionHosts(hosts ...string) Option {
	return func(p *sessionParams) { p.allowedHosts = append(p.allowedHosts, hosts...) }
}

// WithEnableSessionID habilita o deshabilita la inyección de x-session-id a nivel de Session.
func WithEnableSessionID(enabled ...bool) Option {
	val := true
	if len(enabled) > 0 {
		val = enabled[0]
	}
	return func(p *sessionParams) { p.enableSessionID = &val }
}

// WithSessionID configura explícitamente si se inyecta x-session-id a nivel de Session.
func WithSessionID(enabled bool) Option {
	return func(p *sessionParams) { p.enableSessionID = &enabled }
}

func WithIdentityOptions(opts ...IdentityOption) Option {
	return func(p *sessionParams) { p.identityOpts = append(p.identityOpts, opts...) }
}

func WithClassifier(c retry.Classifier) Option {
	return func(p *sessionParams) { p.classifier = c }
}

func WithTransportFactory(f Factory) Option {
	return func(p *sessionParams) { p.factory = f }
}

// WithProxyURL configura un proxy HTTP estático a partir de una URL completa (ej. http://user:pass@host:port).
func WithProxyURL(rawURL string) Option {
	return func(p *sessionParams) {
		if rawURL == "" {
			return
		}
		entry, err := ParseProxyURL(rawURL)
		if err == nil {
			p.proxyEntry = &entry
		}
	}
}

// WithEnvProxy configura el proxy leyendo la URL desde una variable de entorno
// (por defecto "HTTP_PROXY_URL" si no se proporciona ninguna clave).
// Si la variable está vacía o no existe, la sesión opera en conexión directa.
func WithEnvProxy(envKey ...string) Option {
	key := "HTTP_PROXY_URL"
	if len(envKey) > 0 && envKey[0] != "" {
		key = envKey[0]
	}
	val := strings.TrimSpace(os.Getenv(key))
	if val == "" {
		return func(p *sessionParams) {}
	}
	return WithProxyURL(val)
}

func WithProxies(rawProxies []string) Option {
	return func(p *sessionParams) { p.proxyRaw = rawProxies }
}

// WithProxyPool inyecta un Pool de proxies compartido entre múltiples sesiones
// para que distribuyan y roten sus peticiones equitativamente por round-robin.
func WithProxyPool(pool *Pool) Option {
	return func(p *sessionParams) { p.proxyPool = pool }
}

func WithConnectionManager(cm *ConnectionManager) Option {
	return func(p *sessionParams) { p.connMgr = cm }
}

// WithProfileSpec configura la Session para usar un ProfileSpec estático del
// ProfileManager en lugar del sistema legacy de Identity.
func WithProfileSpec(spec *Profiles.ProfileSpec) Option {
	return func(p *sessionParams) { p.profileSpec = spec }
}

// WithProfileManager configura la Session para resolver perfiles dinámicamente
// desde el ProfileManager en cada creación de sesión y en cada rotación.
// Esto permite que múltiples sesiones en un pool obtengan perfiles diversos
// de acuerdo a la estrategia configurada (RoundRobin, Random, etc.).
func WithProfileManager(mgr *Profiles.Manager) Option {
	return func(p *sessionParams) { p.profileManager = mgr }
}

func NewSession(cfg *Config, opts ...Option) (*Session, error) {
	if cfg == nil {
		cfg = Default()
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("browser: config inválida: %w", err)
	}
	cfg = cfg.Clone()

	params := sessionParams{}
	for _, opt := range opts {
		opt(&params)
	}
	if params.factory == nil {
		params.factory = NewFactory()
	}
	if params.classifier == nil {
		params.classifier = retry.NewDefaultClassifier()
	}

	// Resolver la identidad: ProfileManager (dinámico), ProfileSpec (estático) o NewIdentity (legacy)
	var id Identity
	var err error
	if params.profileManager != nil {
		spec, pErr := params.profileManager.GetProfile(context.Background())
		if pErr != nil {
			return nil, fmt.Errorf("browser: obteniendo perfil desde ProfileManager: %w", pErr)
		}
		params.profileSpec = spec
		id, err = IdentityFromSpec(spec)
		if err != nil {
			return nil, fmt.Errorf("browser: generando identidad desde ProfileSpec: %w", err)
		}
	} else if params.profileSpec != nil {
		id, err = IdentityFromSpec(params.profileSpec)
		if err != nil {
			return nil, fmt.Errorf("browser: generando identidad desde ProfileSpec: %w", err)
		}
	} else {
		id, err = NewIdentity(params.identityOpts...)
		if err != nil {
			return nil, fmt.Errorf("browser: generando identidad: %w", err)
		}
	}

	_, resolvedProto, err := ResolveHTTPVersion(cfg.HTTP)
	if err != nil {
		return nil, fmt.Errorf("browser: resolviendo versión HTTP: %w", err)
	}

	profile := FromIdentity(id).WithProtocol(resolvedProto)
	jar := NewJAR(cfg.Cookie)
	builder := NewBuild(id, profile.HeaderLayout)
	retryEng := retry.New(cfg.Retry, params.classifier)

	var pool *Pool
	if params.proxyPool != nil {
		pool = params.proxyPool
	} else if len(params.proxyRaw) > 0 {
		cfg.Proxy.Enabled = true
		pool, err = NewPool(params.proxyRaw, cfg.Proxy)
		if err != nil {
			return nil, fmt.Errorf("browser: configurando proxy pool: %w", err)
		}
	} else if params.proxyEntry != nil {
		cfg.Proxy.Enabled = true
		pool, err = NewPool([]string{params.proxyEntry.String(TypeHTTP)}, cfg.Proxy)
		if err != nil {
			return nil, fmt.Errorf("browser: configurando proxy entry: %w", err)
		}
	}

	if len(params.allowedHosts) > 0 {
		cfg.Browser.AllowedSessionHosts = append(cfg.Browser.AllowedSessionHosts, params.allowedHosts...)
	}

	if params.enableSessionID != nil {
		cfg.Browser.EnableSessionID = *params.enableSessionID
	}

	s := &Session{
		cfg:         cfg,
		profile:     profile,
		jar:         jar,
		builder:     builder,
		retryEng:    retryEng,
		factory:     params.factory,
		proxyPool:   pool,
		connMgr:     params.connMgr,
		opts:           params,
		clientCache:    make(map[string]Client),
		clientLastUsed: make(map[string]time.Time),
		stickyID:       NewStickyID(),
	}

	if err := s.buildClient(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Session) getOrCreateClientLocked(entry *Entry, followRedirects bool) (Client, error) {
	key := fmt.Sprintf("direct_redir=%v", followRedirects)
	if entry != nil {
		key = fmt.Sprintf("%s@%s_redir=%v", entry.key(), entry.User, followRedirects)
	}

	if s.clientCache == nil {
		s.clientCache = make(map[string]Client)
	}
	if s.clientLastUsed == nil {
		s.clientLastUsed = make(map[string]time.Time)
	}

	if cached, ok := s.clientCache[key]; ok && cached != nil {
		s.clientLastUsed[key] = time.Now()
		return cached, nil
	}

	// LRU Eviction: Desalojar el cliente menos recientemente usado al superar el límite
	const maxCachedClients = 8
	if len(s.clientCache) >= maxCachedClients {
		var oldestKey string
		var oldestTime time.Time
		first := true
		for k := range s.clientCache {
			t := s.clientLastUsed[k]
			if first || t.Before(oldestTime) {
				oldestTime = t
				oldestKey = k
				first = false
			}
		}
		if oldestKey != "" {
			if oldClient := s.clientCache[oldestKey]; oldClient != nil {
				oldClient.CloseIdleConnections()
			}
			delete(s.clientCache, oldestKey)
			delete(s.clientLastUsed, oldestKey)
		}
	}

	proxyType := TypeHTTP
	if s.proxyPool != nil {
		proxyType = s.proxyPool.Type()
	}

	client, err := s.factory.New(Params{
		Transport:       s.cfg.Transport,
		TLSProfile:      s.profile.TLSProfile,
		Jar:             s.jar,
		Proxy:           entry,
		ProxyType:       proxyType,
		FollowRedirects: followRedirects,
		Protocol:        s.profile.Protocol,
	})
	if err != nil {
		return nil, err
	}

	s.clientCache[key] = client
	s.clientLastUsed[key] = time.Now()
	return client, nil
}

// buildClient DEBE llamarse con s.mu ya adquirido por el caller.
func (s *Session) buildClient() error {
	var entry *Entry
	if s.proxyPool != nil {
		resolved, err := s.proxyPool.Resolve()
		if err != nil {
			return fmt.Errorf("browser: resolviendo proxy: %w", err)
		}
		if (s.proxyPool.Mode() == ModeSticky || s.cfg.Proxy.Sticky) && s.stickyID != "" && resolved.User != "" {
			resolved.User = FormatProxyUserWithSession(resolved.User, s.stickyID)
		}
		entry = &resolved
	}

	client, err := s.getOrCreateClientLocked(entry, s.cfg.Browser.FollowRedirects)
	if err != nil {
		return fmt.Errorf("browser: creando transporte: %w", err)
	}

	s.client = client
	s.currentProxy = entry
	s.generation.Add(1)
	return nil
}

// reconnectLocked: nueva conexión / nuevo client con el mismo jar.
func (s *Session) reconnectLocked() error {
	return s.buildClient()
}

// refreshLocked: identidad nueva ⇒ jar NUEVO.
func (s *Session) refreshLocked() error {
	var id Identity
	var err error
	if s.opts.profileManager != nil {
		spec, pErr := s.opts.profileManager.GetProfile(context.Background())
		if pErr != nil {
			return fmt.Errorf("browser: refrescando perfil desde ProfileManager: %w", pErr)
		}
		s.opts.profileSpec = spec
		id, err = IdentityFromSpec(spec)
	} else if s.opts.profileSpec != nil {
		// ProfileSpec: la plantilla inmutable se mantiene, solo se genera
		// un nuevo session seed (nuevo fingerprint de sesión).
		id, err = IdentityFromSpec(s.opts.profileSpec)
	} else {
		id, err = NewIdentity(s.opts.identityOpts...)
	}
	if err != nil {
		return err
	}
	_, resolvedProto, _ := ResolveHTTPVersion(s.cfg.HTTP)
	s.profile = FromIdentity(id).WithProtocol(resolvedProto)
	s.jar = NewJAR(s.cfg.Cookie)
	s.builder = NewBuild(id, s.profile.HeaderLayout)
	s.stickyID = NewStickyID()
	s.requestCount.Store(0)
	s.resetCount.Add(1)

	// Limpieza obligatoria de clientes cacheados para que la nueva identidad (TLSProfile y Jar)
	// no sea reemplazada accidentalmente por un cliente antiguo con la misma clave de proxy.
	for _, c := range s.clientCache {
		if c != nil {
			c.CloseIdleConnections()
		}
	}
	s.clientCache = make(map[string]Client)
	s.clientLastUsed = make(map[string]time.Time)

	return s.buildClient()
}

// RotateProxy API pública: rota el cliente de la sesión seleccionando un proxy
// saludable del pool, excluyendo preferentemente el proxy actual.
func (s *Session) RotateProxy() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rotateProxyLocked(false)
}

// rotateProxyLocked reconstruye el transport seleccionando un proxy distinto si hay pool.
func (s *Session) rotateProxyLocked(clearCookies bool) error {
	if clearCookies && s.jar != nil {
		s.jar.Reset()
	}
	s.stickyID = NewStickyID()
	if s.proxyPool != nil {
		var excluded []string
		if s.currentProxy != nil {
			excluded = append(excluded, s.currentProxy.Host, s.currentProxy.key())
		}
		node, _, err := s.proxyPool.Acquire(context.Background(), AcquirePolicy{
			ExcludedHosts: excluded,
			AllowFallback: true,
		})
		if err == nil && node != nil {
			return s.setExplicitProxyLocked(node.Entry)
		}
	}
	return s.buildClient()
}

func (s *Session) reportProxy(st state, latency time.Duration, netOK bool) {
	if s.proxyPool == nil || st.currentProxy == nil {
		return
	}
	s.proxyPool.RecordResult(*st.currentProxy, latency, netOK)
}

// applyAction aplica la transición de estado solo si generation no cambió.
func (s *Session) applyAction(gen uint64, action retry.Action, class retry.ErrorClass) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.generation.Load() != gen {
		return nil
	}
	switch action {
	case retry.ActionRotateProxy:
		return s.rotateProxyLocked(class == retry.ClassBlocked)
	case retry.ActionReconnect:
		return s.reconnectLocked()
	case retry.ActionRebuildIdentity:
		return s.refreshLocked()
	case retry.ActionWaitSameConn, retry.ActionWaitSameProxy:
		return nil
	default:
		return nil
	}
}

func (s *Session) ResetEgress(clearCookies bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if clearCookies && s.jar != nil {
		s.jar.Reset()
	}
	return s.buildClient()
}

func proxyTypeFrom(pool *Pool) Type {
	if pool == nil {
		return TypeHTTP
	}
	return pool.Type()
}

// StickyID retorna el identificador de sesión lógica activo de forma thread-safe.
// Si no existe, realiza inicialización perezosa (lazy-init) garantizada.
func (s *Session) StickyID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stickyID == "" {
		s.stickyID = NewStickyID()
	}
	return s.stickyID
}

// ResetStickyID regenera de forma thread-safe un nuevo identificador para la sesión lógica.
func (s *Session) ResetStickyID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stickyID = NewStickyID()
	return s.stickyID
}

// isAuthorizedTarget determina si la URL objetivo califica para recibir cabeceras de sesión sensibles.
// Si cfg.Browser.AllowedSessionHosts está definido, exige coincidencia de host o subdominio.
// Si no está definido, se autoriza por defecto siempre que la URL sea válida y no apunte a un esquema ajeno.
func (s *Session) isAuthorizedTarget(u *url.URL) bool {
	if u == nil || u.Host == "" {
		return false
	}
	if s.cfg == nil || !s.cfg.Browser.EnableSessionID {
		return false
	}
	targetHost := strings.ToLower(u.Host)
	targetHostname := strings.ToLower(u.Hostname())

	if s.cfg != nil && len(s.cfg.Browser.AllowedSessionHosts) > 0 {
		for _, allowed := range s.cfg.Browser.AllowedSessionHosts {
			allowed = strings.ToLower(strings.TrimSpace(allowed))
			if allowed == "" {
				continue
			}
			// Si allowed especifica puerto explícito (ej. "127.0.0.1:8080"), comparar contra host:port
			if strings.Contains(allowed, ":") {
				if targetHost == allowed {
					return true
				}
				continue
			}
			// Si allowed no especifica puerto, comparar contra hostname o sufijo de dominio
			if targetHostname == allowed || strings.HasSuffix(targetHostname, "."+allowed) {
				return true
			}
		}
		return false
	}

	return true
}

// Identity expone la identidad actual (BrowserName, Version, etc.) de la sesión.
func (s *Session) Identity() Identity {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.profile.Identity
}

// Cookies expone el Jar para depuración. El Jar es thread-safe internamente
// (su propio RWMutex), por eso no necesita el lock de Session.
func (s *Session) Cookies() *Jar {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.jar
}

// ProxyURL retorna la URL completa del proxy actual en formato genérico (ej: "http://user:pass@host:port").
// Si no hay proxy configurado o activo en la sesión, retorna una cadena vacía.
func (s *Session) ProxyURL() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.currentProxy == nil {
		return ""
	}
	return s.currentProxy.String(proxyTypeFrom(s.proxyPool))
}

// RedactedProxyURL retorna la URL del proxy con la contraseña enmascarada para observabilidad segura.
func (s *Session) RedactedProxyURL() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.currentProxy == nil {
		return ""
	}
	return s.currentProxy.RedactedString(proxyTypeFrom(s.proxyPool))
}

// SetGeoCountry actualiza el proxy de la sesión para enrutar el tráfico a través del país especificado.
// Si el proxy soporta country routing (ej: country-XX), reutiliza o arrienda el cliente HTTP con la nueva salida geográfica.
// Si el país es vacío o genérico ("XX", "GENERIC"), restablece la sesión al proxy general por defecto sin tag regional.
func (s *Session) SetGeoCountry(countryCode string) error {
	if !GeoRoute.ActiveApplyGeo {
		return nil
	}
	countryCode = strings.ToUpper(strings.TrimSpace(countryCode))
	if countryCode == "" || countryCode == "XX" || countryCode == "UNKNOWN" {
		countryCode = "GENERIC"
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	// Si disponemos de ConnectionManager, delegamos el arriendo de transporte
	// con caché y afinidad de clave para preservar Keep-Alive entre países
	if s.connMgr != nil {
		normKey := ConnectionKey{
			SessionID:   s.stickyID,
			CountryCode: countryCode,
			RouteID:     "default",
			Protocol:    s.profile.Protocol,
		}.Normalized()

		if s.currentGeoKey == normKey && s.client != nil {
			return nil // Mismo país y cliente activo: PRESERVAR Keep-Alive al 100%
		}

		var baseProxy *Entry
		if s.proxyPool != nil {
			resolved, err := s.proxyPool.Resolve()
			if err != nil {
				return fmt.Errorf("session: error resolviendo proxy base: %w", err)
			}
			baseProxy = &resolved
		}
		proxyType := proxyTypeFrom(s.proxyPool)

		client, pooled, err := s.connMgr.GetClient(context.Background(), normKey, Params{
			Transport:       s.cfg.Transport,
			TLSProfile:      s.profile.TLSProfile,
			Jar:             s.jar,
			Proxy:           baseProxy,
			ProxyType:       proxyType,
			FollowRedirects: s.cfg.Browser.FollowRedirects,
			Protocol:        s.profile.Protocol,
		})
		if err != nil {
			return fmt.Errorf("session: error obteniendo cliente de ConnectionManager para %s: %w", countryCode, err)
		}

		// Hallazgo 10 Fix: Liberar el lease sobre la clave previa si existía
		if s.currentGeoKey.CountryCode != "" {
			s.connMgr.Release(s.currentGeoKey)
		}

		s.client = client
		if pooled != nil && pooled.Proxy != nil {
			s.currentProxy = pooled.Proxy
		}
		s.currentGeoKey = normKey
		s.generation.Add(1)
		return nil
	}

	// Hallazgo 17 Fix: Si no hay ConnectionManager, resolver y clonar el proxy
	// aplicando el país sólo a la copia de esta sesión para no mutar el pool compartido concurrentemente.
	if s.proxyPool != nil {
		resolved, err := s.proxyPool.Resolve()
		if err != nil {
			return fmt.Errorf("session: error resolviendo proxy base: %w", err)
		}
		if resolved.User != "" {
			resolved.User = GeoRoute.ApplyCountryToUsername(resolved.User, countryCode)
		}
		if s.client != nil && s.currentProxy != nil &&
			s.currentProxy.Host == resolved.Host &&
			s.currentProxy.Port == resolved.Port &&
			s.currentProxy.User == resolved.User &&
			s.currentProxy.Pass == resolved.Pass {
			return nil // Preservar Keep-Alive
		}
		return s.setExplicitProxyLocked(resolved)
	}
	return nil
}

// ProxyDisplay retorna el username del proxy (ej: "FoNNOdky_lightning_proxy-country-us") o "host:port" si no tiene usuario.
func (s *Session) ProxyDisplay() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.currentProxy == nil {
		return ""
	}
	if s.currentProxy.User != "" {
		return s.currentProxy.User
	}
	return s.currentProxy.Host + ":" + s.currentProxy.Port
}

// SetExplicitProxy reconfigura el transport de la sesión para usar un proxy específico
// inyectado externamente por el ProxyManager, sin tocar el Pool general.
// Preserva cookies, identity y builder; reusa transportes cacheados por proxy para
// no destruir sockets Keep-Alive innecesariamente.
// Thread-safe: adquiere el lock de Session durante la mutación.
func (s *Session) SetExplicitProxy(e Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.setExplicitProxyLocked(e)
}

func (s *Session) setExplicitProxyLocked(e Entry) error {
	entry := e
	if (s.proxyPool != nil && (s.proxyPool.Mode() == ModeSticky || s.cfg.Proxy.Sticky)) && s.stickyID != "" && entry.User != "" {
		entry.User = FormatProxyUserWithSession(entry.User, s.stickyID)
	}

	// Si el proxy es exactamente idéntico al actual y el cliente ya existe,
	// PRESERVAR el pool de conexiones TCP Keep-Alive y sesiones TLS sin destruirlo.
	if s.client != nil && s.currentProxy != nil &&
		s.currentProxy.Host == entry.Host &&
		s.currentProxy.Port == entry.Port &&
		s.currentProxy.User == entry.User &&
		s.currentProxy.Pass == entry.Pass {
		return nil
	}
	client, err := s.getOrCreateClientLocked(&entry, s.cfg.Browser.FollowRedirects)
	if err != nil {
		return fmt.Errorf("browser: reconfigurando proxy explícito: %w", err)
	}

	s.client = client
	s.currentProxy = &entry
	s.generation.Add(1)
	return nil
}

// snapshot captura, bajo lock BREVE, todo lo que attempt() necesita.
// Esta es la pieza central del fix: el lock se libera inmediatamente
// después de leer los punteros, NUNCA durante la llamada de red.
func (s *Session) snapshot() (state, uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.client == nil {
		return state{}, 0, fmt.Errorf("browser: %w", ErrSessionClosed)
	}
	if s.stickyID == "" {
		s.stickyID = NewStickyID()
	}
	return state{
		client:       s.client,
		builder:      s.builder,
		jar:          s.jar,
		currentProxy: s.currentProxy,
		stickyID:     s.stickyID,
	}, s.generation.Load(), nil
}
func (s *Session) Do(ctx context.Context, spec Spec) (*Result, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	targetCountry := spec.Country
	if targetCountry == "" {
		targetCountry = GeoRoute.CountryFromContext(ctx)
	}
	if targetCountry != "" {
		if err := s.SetGeoCountry(targetCountry); err != nil {
			return nil, fmt.Errorf("browser: adaptando proxy para país %s: %w", targetCountry, err)
		}
	}

	maxPerSession := s.cfg.Worker.MaxRequestsPerSession
	if maxPerSession > 0 && s.requestCount.Load() >= uint64(maxPerSession) {
		s.mu.Lock()
		var refreshErr error
		if s.requestCount.Load() >= uint64(maxPerSession) {
			refreshErr = s.refreshLocked()
		}
		s.mu.Unlock()
		if refreshErr != nil {
			return nil, fmt.Errorf("browser: refresh forzado fallido: %w", refreshErr)
		}
	}
	s.requestCount.Add(1)

	maxAttempts := s.cfg.Retry.MaxAttempts
	var lastResult *Result

	for attempt := 1; maxAttempts <= 0 || attempt <= maxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		reqTimeout := s.cfg.Transport.RequestTimeout
		if reqTimeout <= 0 {
			reqTimeout = 30 * time.Second
		}
		attemptCtx, cancel := context.WithTimeout(ctx, reqTimeout)

		st, gen, err := s.snapshot()
		if err != nil {
			cancel()
			s.failureCount.Add(1)
			return nil, err
		}
		result, decision := s.attemptWith(attemptCtx, st, spec, attempt)
		cancel()

		// Liberar el buffer del intento anterior para no acumular memoria (P0 Fix)
		if lastResult != nil {
			lastResult.Release()
		}
		lastResult = result

		if s.cfg.Debug {
			s.debugLog(spec, result)
		}

		if !decision.Retry {
			if decision.Class == retry.ClassSuccess {
				s.successCount.Add(1)
			} else {
				s.failureCount.Add(1)
			}

			rErr := resultErr(result)
			// En caso de error final, reciclamos el buffer al pool pero conservamos
			// una copia segura desacoplada en BodyBytes para el llamador (diagnóstico),
			// evitando data races y fugas de buffers.
			if rErr != nil && result != nil {
				result.PreserveBodyForError()
			}
			return result, rErr
		}

		if decision.Wait > 0 {
			timer := time.NewTimer(decision.Wait)
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil, ctx.Err()
			case <-timer.C:
			}
		}

		if err := s.applyAction(gen, decision.Action, decision.Class); err != nil {
			s.failureCount.Add(1)
			if result != nil {
				result.PreserveBodyForError()
			}
			return result, err
		}
	}

	s.failureCount.Add(1)
	if lastResult != nil {
		lastResult.PreserveBodyForError()
	}
	return lastResult, fmt.Errorf("browser: agotados %d intentos: %w", maxAttempts, ErrMaxRetriesExceeded)
}

func resultErr(r *Result) error {
	if r == nil {
		return ErrNilResponse
	}
	if r.Err != nil {
		return r.Err
	}
	if r.Response != nil && r.Response.Err != nil {
		return r.Response.Err
	}
	return nil
}

// attemptWith ejecuta UN intento usando un snapshot inmutable de estado,
// sin tocar s.mu en ningún momento. Esto es lo que hace posible que N
// goroutines llamen a Do() concurrentemente sobre la misma Session.
// attemptWith ejecuta UN intento usando un snapshot inmutable de estado.
func (s *Session) attemptWith(ctx context.Context, st state, spec Spec, attemptNum int) (*Result, retry.Decision) {
	start := time.Now()
	proxyStr := proxyURLString(st.currentProxy)

	var req *fhttp.Request
	failNet := func(err error) (*Result, retry.Decision) {
		s.reportProxy(st, time.Since(start), false)
		resp := &Response{Err: err, Duration: time.Since(start), Request: req, Complete: false}
		result := &Result{Response: resp, Attempts: attemptNum, ProxyURL: proxyStr, Err: err}
		dec := s.retryEng.Decide(attemptNum, retry.Signal{
			NetErr:     err,
			Method:     spec.Method,
			Idempotent: spec.Idempotent,
		})
		if spec.NoRetry {
			dec = retry.Decision{Retry: false, Class: retry.ClassPermanent}
		}
		return result, dec
	}

	u, parseErr := url.Parse(spec.URL)
	if parseErr != nil {
		return failNet(fmt.Errorf("browser: URL inválida: %w", parseErr))
	}

	cookieHeader := ""
	if len(spec.CookieFilter) > 0 {
		isNone := false
		for _, n := range spec.CookieFilter {
			if strings.EqualFold(n, "__NONE__") || n == "" {
				isNone = true
				break
			}
		}
		if !isNone {
			list := st.jar.CookiesFor(u.Hostname(), u.Path, u.Scheme == "https")
			filtered := make([]*fhttp.Cookie, 0, len(spec.CookieFilter))
			allowed := make(map[string]bool, len(spec.CookieFilter))
			for _, n := range spec.CookieFilter {
				allowed[strings.ToLower(n)] = true
			}
			for _, c := range list {
				if allowed[strings.ToLower(c.Name)] {
					filtered = append(filtered, c)
				}
			}
			cookieHeader = CookieHeaderFromList(filtered)
		}
	} else {
		list := st.jar.CookiesFor(u.Hostname(), u.Path, u.Scheme == "https")
		cookieHeader = CookieHeaderFromList(list)
	}

	var err error
	req, err = st.builder.Build(spec, cookieHeader)
	if err != nil {
		return failNet(err)
	}

	// Inyección obligatoria y centralizada de la identidad de sesión:
	// 1. Ocurre DESPUÉS de combinar headers del llamador (spec.Extra), garantizando
	//    que ningún llamador pueda sobrescribir accidentalmente la identidad lógica.
	// 2. Solo se inyecta en destinos autorizados para evitar fuga a terceros en redirecciones externas.
	if s.isAuthorizedTarget(u) {
		// Purgar cualquier intento previo del llamador con cualquier capitalización en el mapa de headers
		for k := range req.Header {
			if strings.EqualFold(k, "x-session-id") {
				delete(req.Header, k)
			}
		}
		req.Header.Set("x-session-id", st.stickyID)
		if order, ok := req.Header[HeaderOrderKey]; ok {
			newOrder := make([]string, 0, len(order)+1)
			for _, k := range order {
				if !strings.EqualFold(k, "x-session-id") {
					newOrder = append(newOrder, k)
				}
			}
			newOrder = append(newOrder, "x-session-id")
			req.Header[HeaderOrderKey] = newOrder
		}
	}

	// CRÍTICO: sin esto el ctx no cancela dial/TLS/body.
	req = req.WithContext(ctx)

	targetClient := st.client
	targetProxy := st.currentProxy

	followRedirects := s.cfg.Browser.FollowRedirects
	if spec.Redirect != nil {
		followRedirects = *spec.Redirect
	} else if spec.Redict != nil {
		followRedirects = *spec.Redict
	}

	useProxyOverride := false
	isLoopback := u.Hostname() == "localhost" || u.Hostname() == "127.0.0.1" || u.Hostname() == "::1"
	if isLoopback && (spec.UseProxy == nil || !*spec.UseProxy) {
		targetProxy = nil
		useProxyOverride = true
	} else if spec.UseProxy != nil {
		if !*spec.UseProxy {
			targetProxy = nil
			useProxyOverride = true
		} else if st.currentProxy == nil && s.proxyPool != nil {
			if resolved, pErr := s.proxyPool.Resolve(); pErr == nil {
				targetProxy = &resolved
				useProxyOverride = true
			}
		}
	}

	if useProxyOverride || followRedirects != s.cfg.Browser.FollowRedirects {
		s.mu.Lock()
		client, cErr := s.getOrCreateClientLocked(targetProxy, followRedirects)
		s.mu.Unlock()
		if cErr == nil && client != nil {
			targetClient = client
		}
	}

	rawResp, doErr := targetClient.Do(req)
	if doErr != nil {
		return failNet(doErr)
	}

	resp, err := FromRaw(rawResp, req, start, s.cfg.Transport.MaxResponseBodyBytes)
	if err != nil && (resp == nil || !resp.Truncated) {
		// Error de lectura de body (no solo truncado): cuenta como fallo de red.
		s.reportProxy(st, time.Since(start), false)
		if resp == nil {
			resp = &Response{Err: err, Duration: time.Since(start), Request: req, Complete: false}
		}
		result := &Result{Response: resp, Attempts: attemptNum, ProxyURL: proxyStr, Err: err}
		dec := s.retryEng.Decide(attemptNum, retry.Signal{
			NetErr:     err,
			Method:     spec.Method,
			Idempotent: spec.Idempotent,
		})
		return result, dec
	}

	// Respuesta HTTP recibida (aunque truncada): el proxy entregó bytes.
	s.reportProxy(st, time.Since(start), true)

	_ = resp.DecompressIfNeeded()

	// Solo Set-Cookie de la respuesta real (nunca eco del request Cookie).
	st.jar.SetCookies(u.Hostname(), u.Path, ParseSetCookies(rawResp.Header))

	result := &Result{Response: resp, Attempts: attemptNum, ProxyURL: proxyStr}
	sample := resp.BodyBytes
	if len(sample) > 4096 {
		sample = sample[:4096]
	}

	dec := s.retryEng.Decide(attemptNum, retry.Signal{
		StatusCode:    resp.StatusCode,
		BodySample:    sample,
		Method:        spec.Method,
		Idempotent:    spec.Idempotent,
		BodyComplete:  resp.Complete,
		BodyTruncated: resp.Truncated,
		RetryAfter:    resp.RetryAfter(),
	})
	return result, dec
}

func proxyURLString(e *Entry) string {
	if e == nil {
		return ""
	}
	return e.Host + ":" + e.Port
}

func (s *Session) Metrics() (total, success, failure, resets uint64) {
	return s.requestCount.Load(), s.successCount.Load(), s.failureCount.Load(), s.resetCount.Load()
}

// Profile retorna una copia del Profile activo en la sesión.
func (s *Session) Profile() Profile {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.profile
}

func (s *Session) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.connMgr != nil && s.currentGeoKey.CountryCode != "" {
		s.connMgr.Release(s.currentGeoKey)
		s.currentGeoKey = ConnectionKey{}
	}
	if s.client != nil {
		s.client.CloseIdleConnections()
		s.client = nil
	}
	if s.directClient != nil {
		s.directClient.CloseIdleConnections()
		s.directClient = nil
	}
	for _, c := range s.clientCache {
		if c != nil {
			c.CloseIdleConnections()
		}
	}
	s.clientCache = nil
	s.clientLastUsed = nil
}

// CurrentProxy retorna una copia de la entrada del proxy activo o nil si no hay proxy.
func (s *Session) CurrentProxy() *Entry {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.currentProxy == nil {
		return nil
	}
	cp := *s.currentProxy
	return &cp
}

// CurrentProxyString devuelve el string representativo del proxy actual asignado a la sesión.
func (s *Session) CurrentProxyString() string {
	if s == nil {
		return ""
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.currentProxy == nil {
		return ""
	}
	proxyType := TypeHTTP
	if s.proxyPool != nil {
		proxyType = s.proxyPool.Type()
	}
	return s.currentProxy.String(proxyType)
}

// ProxyPool retorna el pool de proxies asociado a la sesión, si existe.
func (s *Session) ProxyPool() *Pool {
	return s.proxyPool
}

// ResetComboState vacía el Jar de cookies y prepara la sesión para el siguiente
// combo. Mantiene la conexión TCP/TLS viva (connection reuse). Si rotateIdentity
// es true, asigna un nuevo fingerprint de navegador sin cerrar sockets.
func (s *Session) ResetComboState(rotateIdentity bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.jar != nil {
		s.jar.Clear()
	}

	// Renovación obligatoria de la identidad de sesión lógica para aislar el siguiente flujo
	s.stickyID = NewStickyID()

	if rotateIdentity {
		var id Identity
		var err error
		if s.opts.profileManager != nil {
			spec, pErr := s.opts.profileManager.GetProfile(context.Background())
			if pErr == nil {
				s.opts.profileSpec = spec
				id, err = IdentityFromSpec(spec)
			} else {
				err = pErr
			}
		} else if s.opts.profileSpec != nil {
			id, err = IdentityFromSpec(s.opts.profileSpec)
		} else {
			id, err = NewIdentity(s.opts.identityOpts...)
		}
		if err == nil {
			_, resolvedProto, _ := ResolveHTTPVersion(s.cfg.HTTP)
			s.profile = FromIdentity(id).WithProtocol(resolvedProto)
			s.builder = NewBuild(id, s.profile.HeaderLayout)

			// Cerrar y limpiar clientes anteriores para que coincidan con el nuevo TLS Profile
			for _, c := range s.clientCache {
				if c != nil {
					c.CloseIdleConnections()
				}
			}
			s.clientCache = make(map[string]Client)
			s.clientLastUsed = make(map[string]time.Time)
			_ = s.buildClient()
		}
	}

	if s.proxyPool != nil && (s.proxyPool.Mode() == ModeSticky || s.cfg.Proxy.Sticky) {
		return s.buildClient()
	}

	return nil
}

// SetData almacena un valor arbitrario asociado a la sesión de forma thread-safe (ej. Tokens de autenticación).
func (s *Session) SetData(key string, val any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.storage == nil {
		s.storage = make(map[string]any)
	}
	s.storage[key] = val
}

// GetData recupera un valor arbitrario asociado a la sesión de forma thread-safe.
func (s *Session) GetData(key string) (any, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.storage == nil {
		return nil, false
	}
	val, ok := s.storage[key]
	return val, ok
}

// ClearData elimina una clave o todo el almacenamiento de la sesión de forma thread-safe.
func (s *Session) ClearData(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.storage != nil {
		if key == "" {
			s.storage = make(map[string]any)
		} else {
			delete(s.storage, key)
		}
	}
}

func HeaderKindForHost(isTopLevelNavigation bool) RequestKind {
	if isTopLevelNavigation {
		return KindDocument
	}
	return KindXHR
}

var debugPrintMu sync.Mutex

func (s *Session) debugLog(spec Spec, result *Result) {
	debugPrintMu.Lock()
	defer debugPrintMu.Unlock()

	dateStr := time.Now().Format("2006-01-02 15:04:05")

	method := spec.Method
	if method == "" {
		method = "GET"
	}

	// 1. [fecha] POST →  https://...  (color cyan)
	cCyan := color.New(color.FgCyan, color.Bold)
	cCyan.Printf("[%s] %s →  %s\n", dateStr, method, sanitizeURL(spec.URL))

	// 2. [fecha] PROXY → http://xxx:xxx@xxx:xx [200]
	// si en caso no se usa proxy solo seria NO PROXY [200 (color verde para status code buenos y color rojo para status code malos)]
	proxyDisplay := "NO PROXY"
	if result != nil && result.ProxyURL != "" {
		proxyDisplay = result.ProxyURL
	} else if s.currentProxy != nil {
		proxyType := TypeHTTP
		if s.proxyPool != nil {
			proxyType = s.proxyPool.Type()
		}
		proxyDisplay = s.currentProxy.RedactedString(proxyType)
	}

	fmt.Printf("[%s] PROXY → %s ", dateStr, proxyDisplay)

	var statusCode int
	var respErr error
	var req *fhttp.Request
	var resp *Response

	if result != nil && result.Response != nil {
		resp = result.Response
		statusCode = resp.StatusCode
		respErr = resp.Err
		req = resp.Request
	}

	if respErr != nil {
		cRed := color.New(color.FgRed, color.Bold)
		cRed.Printf("[ERROR: %v]\n", respErr)
	} else if statusCode > 0 {
		if statusCode >= 200 && statusCode < 300 {
			cGreen := color.New(color.FgGreen, color.Bold)
			cGreen.Printf("[%d]\n", statusCode)
		} else if statusCode >= 300 && statusCode < 400 {
			cYellow := color.New(color.FgYellow, color.Bold)
			cYellow.Printf("[%d]\n", statusCode)
		} else {
			cRed := color.New(color.FgRed, color.Bold)
			cRed.Printf("[%d]\n", statusCode)
		}
	} else {
		cDim := color.New(color.FgHiBlack)
		cDim.Println("[NO STATUS]")
	}
	fmt.Println()

	// 3. [Send Headers] (este lo pones de color azul y todo su contenido recibido)
	cBlueBold := color.New(color.FgHiBlue, color.Bold)
	cBlue := color.New(color.FgHiBlue)
	cBlueBold.Println("[Send Headers]")

	var sendCookies string
	if req != nil && req.Header != nil {
		var keys []string
		maxKeyLen := 0
		for k := range req.Header {
			if strings.EqualFold(k, HeaderOrderKey) || strings.EqualFold(k, "header-order:") || strings.EqualFold(k, fhttp.PHeaderOrderKey) {
				continue
			}
			if strings.EqualFold(k, "Cookie") {
				if vals, ok := req.Header[k]; ok && len(vals) > 0 {
					sendCookies = strings.Join(vals, "; ")
				} else {
					sendCookies = req.Header.Get(k)
				}
				continue
			}
			keys = append(keys, k)
			if len(k) > maxKeyLen {
				maxKeyLen = len(k)
			}
		}
		var orderList []string
		if order, ok := req.Header[HeaderOrderKey]; ok && len(order) > 0 {
			orderList = order
		} else if len(spec.HeaderOrder) > 0 {
			orderList = spec.HeaderOrder
		}
		if len(orderList) > 0 {
			orderMap := make(map[string]int, len(orderList))
			for i, ord := range orderList {
				lowerOrd := strings.ToLower(strings.TrimSpace(ord))
				if _, exists := orderMap[lowerOrd]; !exists {
					orderMap[lowerOrd] = i
				}
			}
			sort.SliceStable(keys, func(i, j int) bool {
				idxI, okI := orderMap[strings.ToLower(keys[i])]
				idxJ, okJ := orderMap[strings.ToLower(keys[j])]
				if okI && okJ {
					return idxI < idxJ
				}
				if okI && !okJ {
					return true
				}
				if !okI && okJ {
					return false
				}
				return keys[i] < keys[j]
			})
		} else {
			sort.Strings(keys)
		}
		for _, k := range keys {
			var val string
			if vals, ok := req.Header[k]; ok && len(vals) > 0 {
				val = strings.Join(vals, ", ")
			} else {
				val = req.Header.Get(k)
			}
			if isSensitiveHeader(k) {
				val = "[REDACTED]"
			} else if strings.EqualFold(k, "x-session-id") {
				if len(val) > 8 {
					val = val[:4] + "***" + val[len(val)-4:]
				} else if len(val) > 0 {
					val = "***"
				}
			}
			padding := strings.Repeat(" ", maxKeyLen-len(k)+1)
			cBlue.Printf("\"%s\":%s\"%s\",\n", k, padding, val)
		}
	} else if len(spec.Extra) > 0 {
		var keys []string
		maxKeyLen := 0
		for k := range spec.Extra {
			keys = append(keys, k)
			if len(k) > maxKeyLen {
				maxKeyLen = len(k)
			}
		}
		var orderList []string
		if len(spec.HeaderOrder) > 0 {
			orderList = spec.HeaderOrder
		}
		if len(orderList) > 0 {
			orderMap := make(map[string]int, len(orderList))
			for i, ord := range orderList {
				lowerOrd := strings.ToLower(strings.TrimSpace(ord))
				if _, exists := orderMap[lowerOrd]; !exists {
					orderMap[lowerOrd] = i
				}
			}
			sort.SliceStable(keys, func(i, j int) bool {
				idxI, okI := orderMap[strings.ToLower(keys[i])]
				idxJ, okJ := orderMap[strings.ToLower(keys[j])]
				if okI && okJ {
					return idxI < idxJ
				}
				if okI && !okJ {
					return true
				}
				if !okI && okJ {
					return false
				}
				return keys[i] < keys[j]
			})
		} else {
			sort.Strings(keys)
		}
		for _, k := range keys {
			val := spec.Extra[k]
			if strings.EqualFold(k, "x-session-id") {
				if len(val) > 8 {
					val = val[:4] + "***" + val[len(val)-4:]
				} else if len(val) > 0 {
					val = "***"
				}
			}
			padding := strings.Repeat(" ", maxKeyLen-len(k)+1)
			cBlue.Printf("\"%s\":%s\"%s\",\n", k, padding, val)
		}
	}

	// 4. [Send Cookies] (este lo pones de color amarillo y todo su contenido recibido)
	cYellowBold := color.New(color.FgYellow, color.Bold)
	cYellow := color.New(color.FgYellow)
	cYellowBold.Println("[Send Cookies]")
	if sendCookies != "" {
		cYellow.Println(sendCookies)
	}
	fmt.Println()

	// 5. [BODY] (este lo pones de color naranja y todo su contenido recibido)
	fmt.Print("\033[38;5;208;1m[BODY]\033[0m\n")
	if len(spec.Body) > 0 {
		fmt.Printf("\033[38;5;208m%s\033[0m\n", string(spec.Body))
	}
	fmt.Println()

	// 6. [RESPONSE]
	cWhiteBold := color.New(color.FgHiWhite, color.Bold)
	cWhiteBold.Println("[RESPONSE]")

	// [Headers Recivied] (este lo pones de color azul y todo su contenido recibido)
	cBlueBold.Println("[Headers Recivied]")
	var recvCookies []string
	if resp != nil && resp.Headers != nil {
		var keys []string
		for k := range resp.Headers {
			if strings.EqualFold(k, "Set-Cookie") {
				recvCookies = append(recvCookies, resp.Headers.Values(k)...)
				continue
			}
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			vals := resp.Headers.Values(k)
			for _, v := range vals {
				cBlue.Printf("%s: %s\n", k, v)
			}
		}
	}
	fmt.Println()

	// [Cookies Recivied] (este lo pones de color amarillo y todo su contenido recibido)
	cYellowBold.Println("[Cookies Recivied]")
	if len(recvCookies) > 0 {
		for _, ck := range recvCookies {
			cYellow.Println(ck)
		}
	}
	fmt.Println()

	// [Body] (este lo pones de color verde y todo su contenido recibido)
	cGreenBold := color.New(color.FgHiGreen, color.Bold)
	cGreen := color.New(color.FgHiGreen)
	cGreenBold.Println("[Body]")
	if resp != nil && len(resp.BodyBytes) > 0 {
		cGreen.Println(string(resp.BodyBytes))
	} else if resp != nil && resp.Err != nil {
		cRed := color.New(color.FgRed)
		cRed.Printf("Error: %v\n", resp.Err)
	}

	fmt.Println()
	color.New(color.FgHiBlack).Println("────────────────────────────────────────────────────────────────")
	fmt.Println()
}

// WithCountry inyecta un código de país en el contexto para auto-enrutamiento regional en Session.Do.
func WithCountry(ctx context.Context, country string) context.Context {
	return GeoRoute.WithCountry(ctx, country)
}

// CountryFromContext extrae el código de país presente en el contexto si existe.
func CountryFromContext(ctx context.Context) string {
	return GeoRoute.CountryFromContext(ctx)
}



func sanitizeURL(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return rawURL
	}
	q := u.Query()
	sensitiveParams := []string{"password", "pass", "pwd", "token", "secret", "api_key", "apikey", "auth", "credential", "signature"}
	for param := range q {
		lowerParam := strings.ToLower(param)
		for _, sens := range sensitiveParams {
			if strings.Contains(lowerParam, sens) {
				q.Set(param, "[REDACTED]")
				break
			}
		}
	}
	u.RawQuery = q.Encode()
	return u.String()
}

func isSensitiveHeader(name string) bool {
	lower := strings.ToLower(name)
	return strings.Contains(lower, "authorization") ||
		strings.Contains(lower, "proxy-authorization") ||
		strings.Contains(lower, "x-api-key")
}

func maskSetCookie(raw string) string {
	parts := strings.Split(raw, ";")
	if len(parts) == 0 {
		return raw
	}
	nv := strings.TrimSpace(parts[0])
	if idx := strings.Index(nv, "="); idx != -1 {
		parts[0] = nv[:idx] + "=[REDACTED]"
	}
	return strings.Join(parts, ";")
}
