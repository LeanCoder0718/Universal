package GeoRoute

import (
	"fmt"
	"strings"
)

// ProxyProviderConfig define las reglas de parametrización regional para un proveedor.
type ProxyProviderConfig struct {
	Name               string // Nombre identificador (ej: "default_cr", "711proxy", "brightdata", "oxylabs")
	RegionTemplate     string // Plantilla de región (ej: "__cr.{country}", "-zone-custom-region-{country}", "-country-{country}")
	CountryPlaceholder string // Marcador a reemplazar (ej: "{country}")
	LowercaseCountry   bool   // Si es true, transforma el country code a minúsculas (ej: "es" vs "ES")
}

// DefaultCRProviderConfig retorna la configuración estándar para proveedores como DataImpulse ("__cr.<country>")
func DefaultCRProviderConfig() ProxyProviderConfig {
	return ProxyProviderConfig{
		Name:               "dataimpulse",
		RegionTemplate:     "__cr.{country}",
		CountryPlaceholder: "{country}",
		LowercaseCountry:   true,
	}
}

// SevenElevenProxyProviderConfig retorna la configuración para 711Proxy ("USER368076-zone-custom-region-MX")
func SevenElevenProxyProviderConfig() ProxyProviderConfig {
	return ProxyProviderConfig{
		Name:               "711proxy",
		RegionTemplate:     "-zone-custom-region-{country}",
		CountryPlaceholder: "{country}",
		LowercaseCountry:   false, // 711proxy usa mayúsculas (ej: MX, ES, US)
	}
}

// OxylabsProviderConfig retorna la configuración para Oxylabs ("customer-user-cc-es")
func OxylabsProviderConfig() ProxyProviderConfig {
	return ProxyProviderConfig{
		Name:               "oxylabs",
		RegionTemplate:     "-cc-{country}",
		CountryPlaceholder: "{country}",
		LowercaseCountry:   true,
	}
}

// BrightDataProviderConfig retorna la configuración para BrightData / Luminati ("-country-es")
func BrightDataProviderConfig() ProxyProviderConfig {
	return ProxyProviderConfig{
		Name:               "brightdata",
		RegionTemplate:     "-country-{country}",
		CountryPlaceholder: "{country}",
		LowercaseCountry:   true,
	}
}

// SmartproxyProviderConfig retorna la configuración para Smartproxy ("user-country-es")
func SmartproxyProviderConfig() ProxyProviderConfig {
	return ProxyProviderConfig{
		Name:               "smartproxy",
		RegionTemplate:     "-country-{country}",
		CountryPlaceholder: "{country}",
		LowercaseCountry:   true,
	}
}

// IPRoyalProviderConfig retorna la configuración para IPRoyal / Pawns ("_country-es")
func IPRoyalProviderConfig() ProxyProviderConfig {
	return ProxyProviderConfig{
		Name:               "iproyal",
		RegionTemplate:     "_country-{country}",
		CountryPlaceholder: "{country}",
		LowercaseCountry:   true,
	}
}

// WebshareProviderConfig retorna la configuración para Webshare ("-country-es")
func WebshareProviderConfig() ProxyProviderConfig {
	return ProxyProviderConfig{
		Name:               "webshare",
		RegionTemplate:     "-country-{country}",
		CountryPlaceholder: "{country}",
		LowercaseCountry:   true,
	}
}

// CustomProviderConfig crea una configuración personalizada de plantilla regional.
func CustomProviderConfig(name, template string, lowercase bool) ProxyProviderConfig {
	if template == "" {
		template = "__cr.{country}"
	}
	return ProxyProviderConfig{
		Name:               name,
		RegionTemplate:     template,
		CountryPlaceholder: "{country}",
		LowercaseCountry:   lowercase,
	}
}

// DetectProviderFromProxy analiza el host y username de un proxy individual para inferir su proveedor y plantilla.
func DetectProviderFromProxy(p ParsedProxy) (ProxyProviderConfig, bool) {
	hostLower := strings.ToLower(p.Host)

	// 1. Detección por Host / Dominio del Proveedor
	if strings.Contains(hostLower, "711proxy.com") || strings.Contains(hostLower, "711.proxy.com") || strings.Contains(hostLower, "711.proxy") || strings.Contains(hostLower, "rotgb.711") {
		return SevenElevenProxyProviderConfig(), true
	}
	if strings.Contains(hostLower, "dataimpulse.com") {
		return DefaultCRProviderConfig(), true
	}
	if strings.Contains(hostLower, "oxylabs.io") {
		return OxylabsProviderConfig(), true
	}
	if strings.Contains(hostLower, "brightdata.com") || strings.Contains(hostLower, "luminati.io") {
		return BrightDataProviderConfig(), true
	}
	if strings.Contains(hostLower, "smartproxy.com") {
		return SmartproxyProviderConfig(), true
	}
	if strings.Contains(hostLower, "iproyal.com") || strings.Contains(hostLower, "pawns.app") {
		return IPRoyalProviderConfig(), true
	}
	if strings.Contains(hostLower, "webshare.io") {
		return WebshareProviderConfig(), true
	}

	// 2. Detección por Username / Tag Regional Embebido
	u := p.Username
	if u == "" {
		return ProxyProviderConfig{}, false
	}

	tag, prefixStr, _, isUpper, found := ExtractCountryFromUsername(u)
	if found && len(tag) >= 2 {
		knownPrefixes := []struct {
			prefix string
			name   string
		}{
			{"-zone-custom-region-", "711proxy_custom"},
			{"-zone-custom-", "zone_custom"},
			{"-zone-", "zone"},
			{"__cr.", "dataimpulse"},
			{"-country-", "brightdata_smartproxy"},
			{"_country-", "iproyal"},
			{"_country_", "country_underscore"},
			{":country:", "colon_country"},
			{"-region-", "region_dash"},
			{"_region-", "region_underscore"},
			{"-cc-", "oxylabs"},
			{"_cc-", "cc_underscore"},
			{"-loc-", "loc_dash"},
			{"_loc-", "loc_underscore"},
			{"-geo-", "geo_dash"},
			{"_geo-", "geo_underscore"},
		}

		for _, kp := range knownPrefixes {
			if strings.Contains(prefixStr, kp.prefix) {
				return ProxyProviderConfig{
					Name:               kp.name,
					RegionTemplate:     kp.prefix + "{country}",
					CountryPlaceholder: "{country}",
					LowercaseCountry:   !isUpper,
				}, true
			}
		}

		// Delimitador simple detectado (ej: "-" en "user-US" -> "-{country}")
		if len(prefixStr) > 0 {
			lastDelim := prefixStr[len(prefixStr)-1:]
			return ProxyProviderConfig{
				Name:               "custom_delimiter",
				RegionTemplate:     lastDelim + "{country}",
				CountryPlaceholder: "{country}",
				LowercaseCountry:   !isUpper,
			}, true
		}
	}

	return ProxyProviderConfig{}, false
}

// DetectProviderFromProxies escanea una lista de proxies crudos buscando patrones conocidos de proveedores.
func DetectProviderFromProxies(rawProxies []string) (ProxyProviderConfig, bool) {
	for _, raw := range rawProxies {
		p, err := ParseProxy(raw)
		if err != nil {
			continue
		}
		if cfg, ok := DetectProviderFromProxy(p); ok {
			return cfg, true
		}
	}
	return ProxyProviderConfig{}, false
}

// PromptUserForProviderConfig despliega un menú interactivo en consola para que el usuario elija o ingrese su extensión de proxy.
func PromptUserForProviderConfig() ProxyProviderConfig {
	fmt.Println()
	fmt.Println("\033[36m┌─────────────────────────────────────────────────────────────┐\033[0m")
	fmt.Println("\033[36m│          CONFIGURACIÓN REGIONAL DE PROXIES (GeoRoute)       │\033[0m")
	fmt.Println("\033[36m└─────────────────────────────────────────────────────────────┘\033[0m")
	fmt.Println("No se detectó automáticamente el formato regional en la lista de proxies.")
	fmt.Println("Selecciona el proveedor o la extensión de país correspondiente:")
	fmt.Println()
	fmt.Println("  \033[33m[1]\033[0m DataImpulse / Estándar      (\033[32m__cr.{country}\033[0m   ej: __cr.es)")
	fmt.Println("  \033[33m[2]\033[0m 711Proxy / Zone Custom      (\033[32m-zone-custom-region-{country}\033[0m  ej: -zone-custom-region-MX)")
	fmt.Println("  \033[33m[3]\033[0m BrightData / Smartproxy     (\033[32m-country-{country}\033[0m  ej: -country-es)")
	fmt.Println("  \033[33m[4]\033[0m Oxylabs                     (\033[32m-cc-{country}\033[0m       ej: -cc-es)")
	fmt.Println("  \033[33m[5]\033[0m IPRoyal / Pawns             (\033[32m_country-{country}\033[0m  ej: _country-es)")
	fmt.Println("  \033[33m[6]\033[0m Personalizado / Otro        (Ingresar plantilla manual)")
	fmt.Println()

	var choice int
	fmt.Print("\033[31m → \033[0mSelecciona una opción [1 por defecto]: ")
	_, err := fmt.Scanln(&choice)
	if err != nil || choice < 1 || choice > 6 {
		choice = 1
	}

	switch choice {
	case 1:
		return DefaultCRProviderConfig()
	case 2:
		return SevenElevenProxyProviderConfig()
	case 3:
		return BrightDataProviderConfig()
	case 4:
		return OxylabsProviderConfig()
	case 5:
		return IPRoyalProviderConfig()
	case 6:
		var customTemplate string
		fmt.Print("\033[31m → \033[0mIngresa tu plantilla (usa {country}, ej: -region-{country}): ")
		_, _ = fmt.Scanln(&customTemplate)
		customTemplate = strings.TrimSpace(customTemplate)
		if customTemplate == "" {
			customTemplate = "__cr.{country}"
		}
		if !strings.Contains(customTemplate, "{country}") {
			customTemplate = customTemplate + "{country}"
		}

		var casing string
		fmt.Print("\033[31m → \033[0m¿El código de país debe ser en mayúsculas? (S/N) [N por defecto]: ")
		_, _ = fmt.Scanln(&casing)
		isUpper := strings.EqualFold(strings.TrimSpace(casing), "s") || strings.EqualFold(strings.TrimSpace(casing), "si") || strings.EqualFold(strings.TrimSpace(casing), "y")

		return CustomProviderConfig("custom_user", customTemplate, !isUpper)
	default:
		return DefaultCRProviderConfig()
	}
}

// AutoDetectOrPromptProvider detecta automáticamente la configuración regional de los proxies o solicita la entrada del usuario si no es detectable.
func AutoDetectOrPromptProvider(rawProxies []string) ProxyProviderConfig {
	if cfg, ok := DetectProviderFromProxies(rawProxies); ok {
		casingText := "minúsculas"
		if !cfg.LowercaseCountry {
			casingText = "mayúsculas"
		}
		fmt.Printf("\033[32m[GeoRoute]\033[0m Formato de proxy regional detectado automáticamente: \033[33m%q\033[0m (%s)\n", cfg.RegionTemplate, casingText)
		return cfg
	}

	return PromptUserForProviderConfig()
}

// ProxyProvider es la abstracción decoupled para transformar proxys por región.
type ProxyProvider interface {
	Name() string
	Transform(proxy ParsedProxy, country CountryCode) (string, error)
}

// TemplateProvider implementa ProxyProvider usando plantillas configurables.
type TemplateProvider struct {
	cfg ProxyProviderConfig
}

// NewTemplateProvider crea una nueva instancia de TemplateProvider.
func NewTemplateProvider(cfg ProxyProviderConfig) *TemplateProvider {
	if cfg.CountryPlaceholder == "" {
		cfg.CountryPlaceholder = "{country}"
	}
	if cfg.RegionTemplate == "" {
		cfg.RegionTemplate = "__cr.{country}"
		cfg.LowercaseCountry = true
	}
	return &TemplateProvider{cfg: cfg}
}

func (t *TemplateProvider) Name() string {
	return t.cfg.Name
}

// Transform transforma el username de forma estructural SIN tocar host, port ni password.
// Si el país es UNKNOWN o vacío, entrega el proxy GENERAL con BaseUsername limpio.
func (t *TemplateProvider) Transform(proxy ParsedProxy, country CountryCode) (string, error) {
	// 1. Caso: Proxy sin autenticación -> no hay username que regionalizar
	if proxy.BaseUsername == "" && proxy.Password == "" {
		return proxy.String(), nil
	}

	// 2. Caso: País UNKNOWN o no determinado -> usar PROXY GENERAL (Regla 11)
	if country == Unknown || country == "" {
		transformed := proxy
		transformed.Username = proxy.BaseUsername
		transformed.Region = ""
		return transformed.String(), nil
	}

	// 3. Formatear Country Code (Regla 15: normalización coherente)
	ccStr := string(country)
	if t.cfg.LowercaseCountry {
		ccStr = strings.ToLower(ccStr)
	} else {
		ccStr = strings.ToUpper(ccStr)
	}

	// 4. Construir tag de región usando la plantilla
	regionTag := strings.ReplaceAll(t.cfg.RegionTemplate, t.cfg.CountryPlaceholder, ccStr)

	// 5. Aplicar sobre el BaseUsername limpio (Regla 12: sustitución exclusiva de la región)
	transformed := proxy
	transformed.Username = proxy.BaseUsername + regionTag
	transformed.Region = ccStr

	// Validar que el resultado siga siendo un proxy íntegro
	if err := transformed.Validate(); err != nil {
		return "", fmt.Errorf("%w: %v", ErrProxyTransformation, err)
	}

	return transformed.String(), nil
}
