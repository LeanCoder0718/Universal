package HTTPClient

import (
	retry "github.com/LeanCoder0718/Universal/HTTPClient/Retry"
	"fmt"
	"time"
)

// HTTPConfig centraliza la configuración de versiones y capacidades HTTP (HTTP/1.1, HTTP/2, HTTP/3).
// Por diseño arquitectónico:
//   - EnableHTTP2=false, EnableHTTP3=false -> HTTP/1.1 (Comportamiento predeterminado)
//   - EnableHTTP2=true,  EnableHTTP3=false -> HTTP/2
//   - EnableHTTP2=false, EnableHTTP3=true  -> HTTP/3 (QUIC / UDP)
//   - EnableHTTP2=true,  EnableHTTP3=true  -> Protocol Racing (Prioridad HTTP/3 con fallback a HTTP/2)
type HTTPConfig struct {
	EnableHTTP2 bool `json:"enable_http2" yaml:"enable_http2"`
	EnableHTTP3 bool `json:"enable_http3" yaml:"enable_http3"`
}

// HTTPVersion identifica formalmente la versión del protocolo resuelta.
type HTTPVersion string

const (
	HTTPVersion11 HTTPVersion = "HTTP/1.1"
	HTTPVersion2  HTTPVersion = "HTTP/2"
	HTTPVersion3  HTTPVersion = "HTTP/3"
)

type Protocol string

const (
	ProtocolHTTP1       Protocol = "http1"
	ProtocolHTTP2       Protocol = "http2"
	ProtocolHTTP3       Protocol = "http3"
	ProtocolHTTP3Racing Protocol = "http3_racing"
	ProtocolAuto        Protocol = "auto"
)

// ResolveHTTPVersion es la ÚNICA fuente de verdad para resolver la versión de protocolo HTTP
// a partir de la configuración provista. Evita condicionales dispersos por el código.
func ResolveHTTPVersion(cfg HTTPConfig) (HTTPVersion, Protocol, error) {
	if cfg.EnableHTTP3 && cfg.EnableHTTP2 {
		// Protocol Racing: intenta QUIC/UDP (HTTP/3) y conmuta a HTTP/2 sobre TLS ALPN si UDP no está disponible
		return HTTPVersion3, ProtocolHTTP3Racing, nil
	}
	if cfg.EnableHTTP3 {
		return HTTPVersion3, ProtocolHTTP3, nil
	}
	if cfg.EnableHTTP2 {
		return HTTPVersion2, ProtocolHTTP2, nil
	}
	// Comportamiento predeterminado: HTTP/1.1
	return HTTPVersion11, ProtocolHTTP1, nil
}

type Config struct {
	HTTP      HTTPConfig
	Cookie    CookieConfig
	Retry     retry.RetryConfig
	Proxy     ProxyConfig
	Transport TransportConfig
	Browser   BrowserConfig
	Worker    WorkerConfig
	Lifecycle LifecycleConfig
	Debug     bool
}

type CookieConfig struct {
	RejectPublicSuffixes bool
	MaxCookiesPerDomain  int
	// DisableHostOnly, si es true, comparte cookies sin 'Domain' con
	// todos los subdominios. ADVERTENCIA (hallazgo de auditoría): esto
	// REINTRODUCE la vulnerabilidad de fuga de cookies entre subdominios
	// que Jar.go corrigió explícitamente. Úsalo solo si conoces el
	// trade-off; por eso el default es false.
	DisableHostOnly bool
}

// ProxyMode define cómo el proveedor asigna IP de salida.
type ProxyMode int

const (
	// ModeRotating: cada nueva conexión TCP puede traer IP distinta.
	// Típico de gateways "rotating residential" sin sticky.
	ModeRotating ProxyMode = iota
	// ModeSticky: se inyecta session-id en el username; el proveedor
	// pinnea la misma IP de salida mientras el id no cambie (TTL vendor).
	ModeSticky
)

type ProxyConfig struct {
	Enabled                 bool
	Sticky                  bool
	ProxyType               string
	Mode                    ProxyMode
	AffinityMaxConnsPerHost int
}

type TransportConfig struct {
	MaxResponseBodyBytes   int64
	RequestTimeout         time.Duration
	DialTimeout            time.Duration
	KeepAlive              time.Duration
	IdleConnTimeout        time.Duration
	DisableKeepAlives      bool
	MaxIdleConns           int
	MaxIdleConnsPerHost    int
	MaxConnsPerHost        int
	DisableCompression     bool
	WriteBufferSize        int
	ReadBufferSize         int
	MaxResponseHeaderBytes int64
}

type BrowserConfig struct {
	FollowRedirects     bool
	AllowedSessionHosts []string
}

type WorkerConfig struct {
	RateLimitPerHost      float64
	RateLimitBurst        int
	PoolSize              int
	MaxRequestsPerSession int // 0 = infinito
}

type LifecycleConfig struct {
	MaxRequestsPerSession uint64        // Peticiones máximas por sesión (ej. 30)
	MaxSessionAge         time.Duration // Edad máxima por sesión (ej. 3m)
	MaxConsecutiveErrors  uint32        // Errores consecutivos tolerados antes de rotar (ej. 2)
	EnablePreWarm         bool          // Pre-calentar sesión de reemplazo en background
	AutoResetOnRelease    bool          // Limpia cookies y estado entre combos al liberar la sesión
	RotateIdentityOnReset bool          // Rota fingerprint/dispositivo en cada reset de combo
}

// Default retorna una configuración de producción razonable con HTTP/1.1 por defecto.
func Default() *Config {
	return &Config{
		HTTP: HTTPConfig{
			EnableHTTP2: false, // HTTP/1.1 por defecto
			EnableHTTP3: false,
		},
		Cookie: CookieConfig{
			RejectPublicSuffixes: true,
			MaxCookiesPerDomain:  180,
			DisableHostOnly:      false,
		},
		Retry: retry.RetryConfig{
			MaxAttempts:  3,
			WaitMin:      1 * time.Second,
			WaitMax:      5 * time.Second,
			JitterFactor: 0.2,
		},
		Proxy: ProxyConfig{
			Enabled:                 false,
			ProxyType:               "http",
			Mode:                    ModeRotating,
			Sticky:                  false,
			AffinityMaxConnsPerHost: 0,
		},
		Transport: TransportConfig{
			RequestTimeout:         30 * time.Second,
			DialTimeout:            10 * time.Second,
			IdleConnTimeout:        90 * time.Second,
			KeepAlive:              30 * time.Second,
			ReadBufferSize:         4096,
			WriteBufferSize:        4096,
			MaxIdleConns:           1000,
			MaxIdleConnsPerHost:    100,
			MaxConnsPerHost:        0,
			DisableKeepAlives:      false,
			DisableCompression:     false,
			MaxResponseHeaderBytes: 1 << 20, // 1MB
			MaxResponseBodyBytes:   4 * 1024 * 1024,
		},
		Browser: BrowserConfig{
			FollowRedirects: true,
		},
		Worker: WorkerConfig{
			RateLimitPerHost:      100,
			RateLimitBurst:        20,
			PoolSize:              10,
			MaxRequestsPerSession: 0,
		},
		Lifecycle: LifecycleConfig{
			MaxRequestsPerSession: 30,
			MaxSessionAge:         3 * time.Minute,
			MaxConsecutiveErrors:  2,
			EnablePreWarm:         true,
			AutoResetOnRelease:    false,
			RotateIdentityOnReset: false,
		},
	}
}

// DefaultHTTP2 retorna una configuración con HTTP/2 forzado.
func DefaultHTTP2() *Config {
	cfg := Default()
	cfg.HTTP.EnableHTTP2 = true
	cfg.HTTP.EnableHTTP3 = false
	return cfg
}

// DefaultHTTP3 retorna una configuración con HTTP/3 (QUIC) forzado.
func DefaultHTTP3() *Config {
	cfg := Default()
	cfg.HTTP.EnableHTTP2 = false
	cfg.HTTP.EnableHTTP3 = true
	return cfg
}

// DefaultProtocolRacing retorna una configuración con HTTP/3 y fallback a HTTP/2.
func DefaultProtocolRacing() *Config {
	cfg := Default()
	cfg.HTTP.EnableHTTP2 = true
	cfg.HTTP.EnableHTTP3 = true
	return cfg
}

// Validate hace Fail Fast sobre configuraciones inconsistentes que antes
// pasaban silenciosamente (el Validate original era un no-op total).
func (c *Config) Validate() error {
	if c == nil {
		return fmt.Errorf("config: config es nil")
	}
	if _, _, err := ResolveHTTPVersion(c.HTTP); err != nil {
		return fmt.Errorf("config: versión HTTP inválida: %w", err)
	}
	if c.Worker.PoolSize <= 0 {
		return fmt.Errorf("config: Worker.PoolSize debe ser > 0, got %d", c.Worker.PoolSize)
	}
	if c.Transport.RequestTimeout <= 0 {
		return fmt.Errorf("config: Transport.RequestTimeout debe ser > 0")
	}
	if c.Transport.DialTimeout <= 0 {
		return fmt.Errorf("config: Transport.DialTimeout debe ser > 0")
	}
	if c.Transport.MaxIdleConnsPerHost < 0 {
		return fmt.Errorf("config: Transport.MaxIdleConnsPerHost no puede ser negativo")
	}
	if c.Retry.MaxAttempts < 0 {
		return fmt.Errorf("config: Retry.MaxAttempts no puede ser negativo")
	}
	if c.Cookie.DisableHostOnly {
		// No es un error, pero se documenta como advertencia explícita
		// en el propio tipo; aquí solo lo dejamos pasar (decisión del caller).
		_ = "advertencia: DisableHostOnly=true reintroduce riesgo de fuga de cookies entre subdominios"
	}
	if c.Retry.WaitMin > c.Retry.WaitMax && c.Retry.WaitMax > 0 {
		return fmt.Errorf("config: Retry.WaitMin no puede ser mayor que WaitMax")
	}
	if c.Worker.RateLimitPerHost < 0 {
		return fmt.Errorf("config: Worker.RateLimitPerHost no puede ser negativo")
	}
	return nil
}

func (c *Config) Clone() *Config {
	if c == nil {
		return nil
	}
	clone := *c
	return &clone
}
