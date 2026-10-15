package GeoRoute

import (
	"fmt"
	"strings"
)

// GeoRouterConfig contiene los parámetros necesarios para inicializar el motor de Geo-Routing.
type GeoRouterConfig struct {
	Provider       ProxyProviderConfig // Configuración del proveedor de proxy regional
	BaseProxies    []string            // Lista de proxies base
	ProxyType      string              // "http", "socks5"
	MaxCacheSize   int                 // Tamaño máximo de la caché de dominios (default: 50,000)
	ConflictPolicy ConflictPolicy      // Política de resolución de conflictos de país (default: PreferService)
}

// GeoRouter es el orquestador principal del motor de Geo-Routing.
// Coordina la detección de país, el agrupamiento, la asignación de proxy y las métricas.
type GeoRouter struct {
	detector       *CountryDetector
	grouping       *GroupingEngine
	assignment     *ProxyAssignmentEngine
	metrics        *Metrics
	provider       ProxyProvider
	conflictPolicy ConflictPolicy
}

// NewGeoRouter construye e inicializa una instancia lista para producción del GeoRouter.
func NewGeoRouter(cfg GeoRouterConfig) (*GeoRouter, error) {
	if cfg.MaxCacheSize <= 0 {
		cfg.MaxCacheSize = 50000
	}

	// Si no se especificó un proveedor, auto-detectar de la lista de proxies base
	if cfg.Provider.RegionTemplate == "" {
		if detected, ok := DetectProviderFromProxies(cfg.BaseProxies); ok {
			cfg.Provider = detected
		} else {
			cfg.Provider = DefaultCRProviderConfig()
		}
	}

	detector := NewCountryDetector(cfg.MaxCacheSize)
	grouping := NewGroupingEngine(detector)
	provider := NewTemplateProvider(cfg.Provider)

	assignment, err := NewProxyAssignmentEngine(cfg.BaseProxies, provider)
	if err != nil {
		return nil, fmt.Errorf("georoute: error inicializando assignment engine: %w", err)
	}

	return &GeoRouter{
		detector:       detector,
		grouping:       grouping,
		assignment:     assignment,
		metrics:        NewMetrics(),
		provider:       provider,
		conflictPolicy: cfg.ConflictPolicy,
	}, nil
}

// NewGeoRouterInteractive inicializa el GeoRouter auto-detectando el proveedor o solicitando la configuración al usuario si es ambiguo.
func NewGeoRouterInteractive(baseProxies []string, proxyType string) (*GeoRouter, error) {
	providerCfg := AutoDetectOrPromptProvider(baseProxies)
	return NewGeoRouter(GeoRouterConfig{
		Provider:       providerCfg,
		BaseProxies:    baseProxies,
		ProxyType:      proxyType,
		ConflictPolicy: PolicyPreferService,
	})
}

// Route es la función central para el hot-path: recibe un registro/combo (ej: "user@domain.es:pass" o "movistarplay.cl:user@gmail.com:pass"),
// detecta su país, selecciona y transforma el proxy adecuado, y actualiza las métricas.
// Es 100% thread-safe y libre de bloqueos de contención.
func (r *GeoRouter) Route(combo string) (proxyStr string, country CountryCode, err error) {
	combo = strings.TrimSpace(combo)
	if combo == "" {
		r.metrics.RecordClassification(Unknown, true)
		r.metrics.RecordProxyAssignment(Unknown, ErrInvalidDomain)
		return "", Unknown, ErrInvalidDomain
	}

	// 1. Detectar País
	country = r.detector.Detect(combo)
	r.metrics.RecordClassification(country, false)

	// 2. Si no hay proxies configurados, devolver solo la clasificación sin error fatal
	if r.assignment.TotalProxies() == 0 {
		return "", country, nil
	}

	// 3. Asignar y transformar proxy
	proxyStr, err = r.assignment.Assign(country)
	r.metrics.RecordProxyAssignment(country, err)

	return proxyStr, country, err
}

// ParseAndRoute parsea una línea completa según el formato (Combo o ULP), detecta país, asigna proxy y registra métricas detalladas.
func (r *GeoRouter) ParseAndRoute(rawLine string, format InputFormat) (proxyStr string, rec NormalizedRecord, err error) {
	rec, err = r.detector.DetectNormalized(rawLine, format, r.conflictPolicy)
	if err != nil {
		r.metrics.RecordNormalized(rec, true)
		r.metrics.RecordProxyAssignment(Unknown, err)
		return "", rec, err
	}

	r.metrics.RecordNormalized(rec, false)

	if r.assignment.TotalProxies() == 0 {
		return "", rec, nil
	}

	proxyStr, err = r.assignment.Assign(rec.Country)
	r.metrics.RecordProxyAssignment(rec.Country, err)

	return proxyStr, rec, err
}

// RouteNormalized recibe un registro ya normalizado y le asigna el proxy correspondiente.
func (r *GeoRouter) RouteNormalized(rec NormalizedRecord) (proxyStr string, country CountryCode, err error) {
	r.metrics.RecordNormalized(rec, false)

	if r.assignment.TotalProxies() == 0 {
		return "", rec.Country, nil
	}

	proxyStr, err = r.assignment.Assign(rec.Country)
	r.metrics.RecordProxyAssignment(rec.Country, err)

	return proxyStr, rec.Country, err
}

// ClassifyOnly devuelve el país de un registro sin asignar proxy.
func (r *GeoRouter) ClassifyOnly(combo string) CountryCode {
	return r.detector.Detect(combo)
}

// Detector devuelve la instancia interna del detector de país.
func (r *GeoRouter) Detector() *CountryDetector {
	return r.detector
}

// Grouping devuelve el motor de agrupamiento.
func (r *GeoRouter) Grouping() *GroupingEngine {
	return r.grouping
}

// ReorderCombos reorganiza la lista de combos según la estrategia de orden deseada.
func (r *GeoRouter) ReorderCombos(combos []string, order ProcessingOrder, customPriority []CountryCode) []string {
	return r.grouping.ReorderBatch(combos, order, customPriority)
}

// Metrics retorna el recolector de métricas activo.
func (r *GeoRouter) Metrics() *Metrics {
	return r.metrics
}

// Stats retorna una instantánea inmutable de las métricas en tiempo real.
func (r *GeoRouter) Stats() MetricsSnapshot {
	return r.metrics.Snapshot()
}

// RenderDashboard devuelve la visualización ASCII formateada para consolas y dashboards.
func (r *GeoRouter) RenderDashboard() string {
	return r.metrics.Snapshot().RenderDashboard()
}

// RenderDetailedReport devuelve el reporte ASCII completo con desglose de fuentes y conflictos.
func (r *GeoRouter) RenderDetailedReport() string {
	return r.metrics.Snapshot().RenderDetailedReport()
}
