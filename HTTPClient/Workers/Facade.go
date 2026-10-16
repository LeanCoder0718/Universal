package Workers

import (
	"github.com/LeanCoder0718/Universal/HTTPClient"
	"github.com/LeanCoder0718/Universal/HTTPClient/Profiles"
	"fmt"
	"runtime"
	"strings"
	"time"
)

// AutoTuneConfig calibra e inspecciona el entorno del sistema (cores CPU, RDP/VPS)
// y escala de forma 100% automática todos los parámetros de red, pooling de sockets,
// buffers de memoria y ciclo de vida de sesiones según los workers configurados:
//   - Tier Bajo  (1 a 50 workers):   Ahorro de memoria, timeouts cortos, pre-warm suave.
//   - Tier Medio (51 a 250 workers):  Rendimiento equilibrado para VPS/RDPs de 4-8 cores.
//   - Tier Alto  (251 a 1000+ workers): Máxima concurrencia, buffers Gigabit, Keep-Alive masivo.
func AutoTuneConfig(cfg *HTTPClient.Config, poolSize int) {
	if cfg == nil {
		return
	}
	if poolSize <= 0 {
		poolSize = 10
	}

	cfg.Worker.PoolSize = poolSize
	cfg.Worker.MaxRequestsPerSession = 0

	// 1. TIER BAJO: 1 a 50 workers (depuración, RDP modesto o pocos hilos)
	if poolSize <= 50 {
		maxIdle := poolSize * 4
		if maxIdle < 50 {
			maxIdle = 50
		}
		cfg.Transport.MaxIdleConns = maxIdle
		cfg.Transport.MaxIdleConnsPerHost = 25
		cfg.Transport.DialTimeout = 10 * time.Second
		cfg.Transport.RequestTimeout = 30 * time.Second
		cfg.Transport.KeepAlive = 45 * time.Second
		cfg.Transport.IdleConnTimeout = 120 * time.Second
		cfg.Transport.ReadBufferSize = 32768
		cfg.Transport.WriteBufferSize = 32768
		cfg.Lifecycle.MaxRequestsPerSession = 0 // 0 = Reutilización infinita/persistente de sesiones
		cfg.Lifecycle.MaxSessionAge = 0         // 0 = Sin expiración por tiempo (mantiene socket vivo)
		cfg.Lifecycle.EnablePreWarm = false

		// 2. TIER MEDIO: 51 a 250 workers (producción estándar, VPS / RDP 4-8 vCPUs)
	} else if poolSize <= 250 {
		cfg.Transport.MaxIdleConns = poolSize * 4
		cfg.Transport.MaxIdleConnsPerHost = poolSize
		cfg.Transport.DialTimeout = 4 * time.Second
		cfg.Transport.RequestTimeout = 15 * time.Second
		cfg.Transport.KeepAlive = 45 * time.Second
		cfg.Transport.IdleConnTimeout = 120 * time.Second
		cfg.Transport.ReadBufferSize = 32768
		cfg.Transport.WriteBufferSize = 32768
		cfg.Lifecycle.MaxRequestsPerSession = 0 // 0 = Reutilización infinita/persistente de sesiones
		cfg.Lifecycle.MaxSessionAge = 0         // 0 = Sin expiración por tiempo (mantiene socket vivo)
		cfg.Lifecycle.EnablePreWarm = false

		// 3. TIER ALTO: 251 a 1000 workers (alto rendimiento, servidor dedicado, fibra gigabit, 8-32 vCPUs)
	} else if poolSize <= 1000 {
		cfg.Transport.MaxIdleConns = poolSize * 4
		cfg.Transport.MaxIdleConnsPerHost = poolSize
		cfg.Transport.DialTimeout = 3 * time.Second
		cfg.Transport.RequestTimeout = 12 * time.Second
		cfg.Transport.KeepAlive = 45 * time.Second
		cfg.Transport.IdleConnTimeout = 120 * time.Second
		cfg.Transport.ReadBufferSize = 32768
		cfg.Transport.WriteBufferSize = 32768
		cfg.Lifecycle.MaxRequestsPerSession = 0 // 0 = Reutilización infinita/persistente de sesiones
		cfg.Lifecycle.MaxSessionAge = 0         // 0 = Sin expiración por tiempo (mantiene socket vivo)
		cfg.Lifecycle.EnablePreWarm = false

		// 4. TIER ULTRA / INTUITIVO: 1001 a 5000+ workers (mega-concurrencia, auto-escalado dinámico)
	} else {
		cfg.Transport.MaxIdleConns = poolSize * 4
		maxPerHost := poolSize
		if maxPerHost > 3000 {
			maxPerHost = 3000
		}
		if maxPerHost < 1000 {
			maxPerHost = 1000
		}
		cfg.Transport.MaxIdleConnsPerHost = maxPerHost
		cfg.Transport.DialTimeout = 3 * time.Second
		cfg.Transport.RequestTimeout = 12 * time.Second
		cfg.Transport.KeepAlive = 45 * time.Second
		cfg.Transport.IdleConnTimeout = 120 * time.Second
		// Buffers de 16KB para mantener el consumo de RAM plano aun con 3,000 a 5,000 conexiones concurrentes
		cfg.Transport.ReadBufferSize = 16384
		cfg.Transport.WriteBufferSize = 16384
		cfg.Lifecycle.MaxRequestsPerSession = 0
		cfg.Lifecycle.MaxSessionAge = 0
		cfg.Lifecycle.EnablePreWarm = false
	}

	// Adaptación al hardware: si es un RDP de 1-2 vCPUs, acotar buffers para no inflar la RAM
	if runtime.NumCPU() <= 2 {
		cfg.Transport.ReadBufferSize = 32768
		cfg.Transport.WriteBufferSize = 32768
	}

	cfg.Transport.MaxConnsPerHost = 0
	cfg.Transport.DisableKeepAlives = false
	cfg.Browser.FollowRedirects = true
	cfg.Cookie.DisableHostOnly = false
	cfg.Worker.RateLimitPerHost = 0
	cfg.Worker.RateLimitBurst = 0
	cfg.Lifecycle.MaxConsecutiveErrors = 2 // Rotar y destruir automáticamente sesiones que acumulan 2 errores consecutivos (MaxRetry)
}

// PoolOption permite personalizar opciones del pool (ej. versiones HTTP) de forma limpia y directa.
type PoolOption func(*HTTPClient.Config)

// WithHTTP2 fuerza HTTP/2 en el pool.
func WithHTTP2() PoolOption {
	return func(cfg *HTTPClient.Config) {
		cfg.HTTP.EnableHTTP2 = true
		cfg.HTTP.EnableHTTP3 = false
	}
}

// WithHTTP3 fuerza HTTP/3 (QUIC) en el pool.
func WithHTTP3() PoolOption {
	return func(cfg *HTTPClient.Config) {
		cfg.HTTP.EnableHTTP2 = false
		cfg.HTTP.EnableHTTP3 = true
	}
}

// WithHTTP1 fuerza HTTP/1.1 en el pool.
func WithHTTP1() PoolOption {
	return func(cfg *HTTPClient.Config) {
		cfg.HTTP.EnableHTTP2 = false
		cfg.HTTP.EnableHTTP3 = false
	}
}

// WithProtocolRacing habilita HTTP/3 con fallback automático a HTTP/2.
func WithProtocolRacing() PoolOption {
	return func(cfg *HTTPClient.Config) {
		cfg.HTTP.EnableHTTP2 = true
		cfg.HTTP.EnableHTTP3 = true
	}
}

// WithAutoProtocol habilita la negociación automática TLS ALPN exclusivamente entre HTTP/2 y HTTP/1.1 sobre TCP.
// Deshabilita HTTP/3 (QUIC/UDP) para evitar conexiones UDP fallidas o timeouts a través de proxies residenciales.
func WithAutoProtocol() PoolOption {
	return func(cfg *HTTPClient.Config) {
		cfg.HTTP.EnableHTTP2 = true
		cfg.HTTP.EnableHTTP3 = false
	}
}

// WithHTTPConfig aplica una configuración HTTP completa al pool.
func WithHTTPConfig(httpCfg HTTPClient.HTTPConfig) PoolOption {
	return func(cfg *HTTPClient.Config) {
		cfg.HTTP = httpCfg
	}
}

// WithEphemeralSessions fuerza la creación de una nueva sesión HTTP limpia por cada combo
// y la destruye/cierra inmediatamente al finalizar el procesamiento (sin reutilización entre combos).
func WithEphemeralSessions() PoolOption {
	return func(cfg *HTTPClient.Config) {
		cfg.Lifecycle.MaxRequestsPerSession = 1
		cfg.Lifecycle.AutoResetOnRelease = false
		cfg.Lifecycle.EnablePreWarm = false
	}
}

// WithAutoReset activa la limpieza automática de cookies al terminar cada combo
// y permite elegir si se rota la huella/dispositivo (rotateIdentity = true/false)
// manteniendo el socket TCP/TLS caliente con el proxy.
func WithAutoReset(rotateIdentity bool) PoolOption {
	return func(cfg *HTTPClient.Config) {
		cfg.Lifecycle.AutoResetOnRelease = true
		cfg.Lifecycle.RotateIdentityOnReset = rotateIdentity
	}
}

// WithSessionLifecycle configura los límites de peticiones y tiempo de vida de sesiones.
func WithSessionLifecycle(maxReqs uint64, maxAge time.Duration, preWarm bool) PoolOption {
	return func(cfg *HTTPClient.Config) {
		cfg.Lifecycle.MaxRequestsPerSession = maxReqs
		cfg.Lifecycle.MaxSessionAge = maxAge
		cfg.Lifecycle.EnablePreWarm = preWarm
	}
}

// WithTimeouts configura tiempos límite de conexión y respuesta.
func WithTimeouts(dialTimeout, requestTimeout time.Duration) PoolOption {
	return func(cfg *HTTPClient.Config) {
		cfg.Transport.DialTimeout = dialTimeout
		cfg.Transport.RequestTimeout = requestTimeout
	}
}

// WithRateLimit configura el limitador de peticiones por segundo por host.
func WithRateLimit(perHost float64, burst int) PoolOption {
	return func(cfg *HTTPClient.Config) {
		cfg.Worker.RateLimitPerHost = perHost
		cfg.Worker.RateLimitBurst = burst
	}
}

// WithDebug configura el logging detallado de peticiones HTTP en consola.
func WithDebug(debug bool) PoolOption {
	return func(cfg *HTTPClient.Config) {
		cfg.Debug = debug
	}
}

// EnableSessionID habilita o deshabilita la inyección de la cabecera x-session-id en las peticiones.
// Si se llama sin argumentos (Workers.EnableSessionID()), la activa (true).
// También permite especificar un valor booleano explícito: Workers.EnableSessionID(false).
func EnableSessionID(enabled ...bool) PoolOption {
	val := true
	if len(enabled) > 0 {
		val = enabled[0]
	}
	return func(cfg *HTTPClient.Config) {
		cfg.Browser.EnableSessionID = val
	}
}

// EneableSessionID es un alias de conveniencia para EnableSessionID.
func EneableSessionID(enabled ...bool) PoolOption {
	return EnableSessionID(enabled...)
}

// DisableSessionID deshabilita la inyección de la cabecera x-session-id (comportamiento seguro por defecto).
func DisableSessionID() PoolOption {
	return func(cfg *HTTPClient.Config) {
		cfg.Browser.EnableSessionID = false
	}
}

// WithSessionID configura explícitamente si se debe inyectar la cabecera x-session-id en las peticiones.
func WithSessionID(enabled bool) PoolOption {
	return func(cfg *HTTPClient.Config) {
		cfg.Browser.EnableSessionID = enabled
	}
}

// QuickStart crea un pool HTTP listo para producción auto-calibrado automáticamente según poolSize.
func QuickStart(poolSize int, proxies []string, proxyType string, maxRetries int, poolOpts ...PoolOption) (*Pool, error) {
	cfg := HTTPClient.Default()
	AutoTuneConfig(cfg, poolSize)
	cfg.Debug = false

	for _, opt := range poolOpts {
		opt(cfg)
	}

	if maxRetries <= 0 {
		maxRetries = 1
	}
	cfg.Retry.MaxAttempts = 1
	cfg.Retry.WaitMin = 100 * time.Millisecond
	cfg.Retry.WaitMax = 1 * time.Second
	cfg.Retry.JitterFactor = 0.3

	// Proxies
	cfg.Proxy.Enabled = len(proxies) > 0
	cfg.Proxy.ProxyType = proxyType
	//cfg.Proxy.Mode = HTTPClient.ModeSticky
	//cfg.Proxy.Sticky = true

	cfg.Cookie.DisableHostOnly = false // trade-off ya documentado

	// Smart Session Lifecycle Manager (SSLM): rotación adaptativa híbrida
	cfg.Lifecycle.MaxRequestsPerSession = 30
	cfg.Lifecycle.MaxSessionAge = 3 * time.Minute
	cfg.Lifecycle.MaxConsecutiveErrors = 2
	cfg.Lifecycle.EnablePreWarm = true

	var opts []HTTPClient.Option
	if len(proxies) > 0 {
		clean := make([]string, 0, len(proxies))
		for _, p := range proxies {
			if idx := strings.Index(p, "://"); idx >= 0 {
				clean = append(clean, p[idx+3:])
			} else {
				clean = append(clean, p)
			}
		}
		sharedPool, pErr := HTTPClient.NewPool(clean, cfg.Proxy)
		if pErr != nil {
			return nil, fmt.Errorf("workers: configurando pool de proxies compartido: %w", pErr)
		}
		opts = append(opts, HTTPClient.WithProxyPool(sharedPool))
	}

	opts = append(opts, HTTPClient.WithIdentityOptions(

		HTTPClient.WithFirefoxRatio(0.50),
		HTTPClient.WithPlatform(HTTPClient.PlatformWindows),
		HTTPClient.WithLanguage("en-US,en;q=0.9"),
	))

	// Desactivar el rate limiter por host para maximizar el throughput de la línea gigabit
	cfg.Worker.RateLimitPerHost = 0
	cfg.Worker.RateLimitBurst = 0

	// MaxInFlight holgado (poolSize * 4) para permitir paralelismo completo sin frenar las 150 goroutines
	return New(cfg, PoolOptions{MaxInFlight: poolSize * 4}, opts...)
}

// QuickStartWithProfile crea un pool HTTP usando el nuevo ProfileManager.
//
// Cada sesión creada por el pool recibirá un perfil seleccionado por la
// estrategia del ProfileManager (RoundRobin, Random, etc.) según el
// ProfileMode elegido por el usuario.
//
// Ejemplo de uso:
//
//	registry := Profiles.DefaultCatalog()
//	manager, _ := Profiles.NewManager(Profiles.ManagerConfig{
//	    Registry: registry,
//	    Mode:     Profiles.Browser,
//	})
//	pool, _ := Workers.QuickStartWithProfile(100, proxies, "http", 3, manager)
func QuickStartWithProfile(poolSize int, proxies []string, proxyType string, maxRetries int, profileMgr *Profiles.Manager, poolOpts ...PoolOption) (*Pool, error) {
	if profileMgr == nil {
		return nil, fmt.Errorf("workers: %w, usa QuickStart para el modo legacy", HTTPClient.ErrNilProfileManager)
	}

	cfg := HTTPClient.Default()
	AutoTuneConfig(cfg, poolSize)
	cfg.Debug = false

	for _, opt := range poolOpts {
		opt(cfg)
	}

	if maxRetries <= 0 {
		maxRetries = 1
	}
	cfg.Retry.MaxAttempts = maxRetries
	cfg.Retry.WaitMin = 100 * time.Millisecond
	cfg.Retry.WaitMax = 1 * time.Second
	cfg.Retry.JitterFactor = 0.3

	// Proxies
	cfg.Proxy.Enabled = len(proxies) > 0
	cfg.Proxy.ProxyType = proxyType
	cfg.Proxy.Mode = HTTPClient.ModeRotating
	cfg.Proxy.Sticky = false

	var opts []HTTPClient.Option
	if len(proxies) > 0 {
		clean := make([]string, 0, len(proxies))
		for _, p := range proxies {
			if idx := strings.Index(p, "://"); idx >= 0 {
				clean = append(clean, p[idx+3:])
			} else {
				clean = append(clean, p)
			}
		}
		sharedPool, pErr := HTTPClient.NewPool(clean, cfg.Proxy)
		if pErr != nil {
			return nil, fmt.Errorf("workers: configurando pool de proxies compartido: %w", pErr)
		}
		opts = append(opts, HTTPClient.WithProxyPool(sharedPool))
	}

	// Inyectar el ProfileManager a las opciones del pool.
	opts = append(opts, HTTPClient.WithProfileManager(profileMgr))

	return New(cfg, PoolOptions{MaxInFlight: poolSize * 4}, opts...)
}

// QuickStartWithHTTPConfig crea un pool HTTP permitiendo especificar la configuración de versiones HTTP (HTTP/1.1, HTTP/2, HTTP/3).
func QuickStartWithHTTPConfig(poolSize int, proxies []string, proxyType string, maxRetries int, httpCfg HTTPClient.HTTPConfig, profileMgr *Profiles.Manager) (*Pool, error) {
	cfg := HTTPClient.Default()
	AutoTuneConfig(cfg, poolSize)
	cfg.HTTP = httpCfg
	cfg.Debug = false

	if maxRetries <= 0 {
		maxRetries = 1
	}
	cfg.Retry.MaxAttempts = 1
	cfg.Retry.WaitMin = 100 * time.Millisecond
	cfg.Retry.WaitMax = 1 * time.Second
	cfg.Retry.JitterFactor = 0.3

	cfg.Proxy.Enabled = len(proxies) > 0
	cfg.Proxy.ProxyType = proxyType
	cfg.Proxy.Mode = HTTPClient.ModeRotating
	cfg.Proxy.Sticky = false

	var opts []HTTPClient.Option
	if len(proxies) > 0 {
		clean := make([]string, 0, len(proxies))
		for _, p := range proxies {
			if idx := strings.Index(p, "://"); idx >= 0 {
				clean = append(clean, p[idx+3:])
			} else {
				clean = append(clean, p)
			}
		}
		sharedPool, pErr := HTTPClient.NewPool(clean, cfg.Proxy)
		if pErr != nil {
			return nil, fmt.Errorf("workers: configurando pool de proxies compartido: %w", pErr)
		}
		opts = append(opts, HTTPClient.WithProxyPool(sharedPool))
	}

	if profileMgr != nil {
		opts = append(opts, HTTPClient.WithProfileManager(profileMgr))
	} else {
		opts = append(opts, HTTPClient.WithIdentityOptions(
			HTTPClient.WithFirefoxRatio(0.50),
			HTTPClient.WithPlatform(HTTPClient.PlatformWindows),
			HTTPClient.WithLanguage("en-US,en;q=0.9"),
		))
	}

	return New(cfg, PoolOptions{MaxInFlight: poolSize * 4}, opts...)
}
