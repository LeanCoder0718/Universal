package Profiles

import (
	"github.com/bogdanfinn/tls-client/profiles"
)

// ProfileSpec es la plantilla INMUTABLE de un perfil de cliente HTTP/TLS.
//
// Un ProfileSpec describe COMPLETAMENTE cómo se ve un cliente desde la
// perspectiva del servidor: huella TLS, User-Agent, cabeceras de identidad,
// orden de headers y capacidades de protocolo.
//
// REGLA DE ORO: un ProfileSpec NUNCA contiene estado mutable (cookies,
// buffers, contadores). Todo estado mutable vive en HTTPClient.Session.
// Esto permite que N goroutines lean el MISMO ProfileSpec concurrentemente
// sin locks, sin copias y sin riesgo de contaminación cruzada.
type ProfileSpec struct {
	// ID único del perfil dentro del Registry (ej: "chrome_146_windows").
	// Se genera automáticamente al registrar: platform_client_version.
	id string

	// Clasificación
	platform   PlatformType
	clientType ClientType

	// Versión del cliente (ej: "146", "148", "18.5")
	version string

	// Identidad HTTP
	userAgent       string
	secChUA         string // "" si el cliente no soporta Client Hints
	secChUAMobile   string // "?0" desktop, "?1" mobile, "" si no aplica
	secChUAPlatform string // ej: `"Windows"`, `"macOS"`, "" para apps nativas

	// Metadatos de plataforma
	platformName string // "Windows", "macOS", "Linux", "iOS", "Android"
	uaFragment   string // Fragmento del OS en el User-Agent (ej: "Windows NT 10.0; Win64; x64")

	// Huella TLS
	tlsProfile profiles.ClientProfile

	// Capacidades del perfil
	capabilities Capabilities

	// Accept-Language por defecto (puede ser override por la Session)
	defaultLocale string

	// secGPC: "1" si el perfil debe enviar Sec-GPC (Global Privacy Control)
	secGPC string

	// NotABrand: token "Not-A-Brand" para Chrome/Edge/Brave Client Hints
	notABrand string
}

// --- Getters públicos (read-only, thread-safe por inmutabilidad) ---

func (p *ProfileSpec) ID() string                         { return p.id }
func (p *ProfileSpec) Platform() PlatformType             { return p.platform }
func (p *ProfileSpec) ClientType() ClientType             { return p.clientType }
func (p *ProfileSpec) Version() string                    { return p.version }
func (p *ProfileSpec) UserAgent() string                  { return p.userAgent }
func (p *ProfileSpec) SecChUA() string                    { return p.secChUA }
func (p *ProfileSpec) SecChUAMobile() string              { return p.secChUAMobile }
func (p *ProfileSpec) SecChUAPlatform() string            { return p.secChUAPlatform }
func (p *ProfileSpec) PlatformName() string               { return p.platformName }
func (p *ProfileSpec) UAFragment() string                 { return p.uaFragment }
func (p *ProfileSpec) TLSProfile() profiles.ClientProfile { return p.tlsProfile }
func (p *ProfileSpec) Caps() Capabilities                 { return p.capabilities }
func (p *ProfileSpec) DefaultLocale() string              { return p.defaultLocale }
func (p *ProfileSpec) SecGPC() string                     { return p.secGPC }
func (p *ProfileSpec) NotABrand() string                  { return p.notABrand }

// ProfileBuilder facilita la construcción de ProfileSpec de forma segura
// y validada. Una vez que se llama Build(), el ProfileSpec resultante es
// inmutable y listo para registrar en el Registry.
type ProfileBuilder struct {
	spec ProfileSpec
}

// NewProfileBuilder crea un builder con los campos mínimos obligatorios.
func NewProfileBuilder(platform PlatformType, clientType ClientType, version string) *ProfileBuilder {
	return &ProfileBuilder{
		spec: ProfileSpec{
			platform:   platform,
			clientType: clientType,
			version:    version,
		},
	}
}

func (b *ProfileBuilder) UserAgent(ua string) *ProfileBuilder {
	b.spec.userAgent = ua
	return b
}

func (b *ProfileBuilder) SecChUA(val string) *ProfileBuilder {
	b.spec.secChUA = val
	return b
}

func (b *ProfileBuilder) SecChUAMobile(val string) *ProfileBuilder {
	b.spec.secChUAMobile = val
	return b
}

func (b *ProfileBuilder) SecChUAPlatform(val string) *ProfileBuilder {
	b.spec.secChUAPlatform = val
	return b
}

func (b *ProfileBuilder) PlatformName(name string) *ProfileBuilder {
	b.spec.platformName = name
	return b
}

func (b *ProfileBuilder) UAFragment(frag string) *ProfileBuilder {
	b.spec.uaFragment = frag
	return b
}

func (b *ProfileBuilder) TLSProfile(p profiles.ClientProfile) *ProfileBuilder {
	b.spec.tlsProfile = p
	return b
}

func (b *ProfileBuilder) Capabilities(caps Capabilities) *ProfileBuilder {
	b.spec.capabilities = caps
	return b
}

func (b *ProfileBuilder) DefaultLocale(locale string) *ProfileBuilder {
	b.spec.defaultLocale = locale
	return b
}

func (b *ProfileBuilder) SecGPC(val string) *ProfileBuilder {
	b.spec.secGPC = val
	return b
}

func (b *ProfileBuilder) NotABrand(val string) *ProfileBuilder {
	b.spec.notABrand = val
	return b
}

// Build construye el ProfileSpec inmutable, generando su ID automáticamente.
func (b *ProfileBuilder) Build() *ProfileSpec {
	spec := b.spec // copia por valor — el builder no retiene referencia mutable
	spec.id = generateProfileID(spec.platform, spec.clientType, spec.version)

	// Inferir defaults si no se proporcionaron
	if spec.platformName == "" {
		spec.platformName = spec.platform.String()
	}
	if spec.defaultLocale == "" {
		spec.defaultLocale = "en-US,en;q=0.9"
	}

	return &spec
}

// generateProfileID produce un ID determinístico y legible.
func generateProfileID(platform PlatformType, client ClientType, version string) string {
	return platform.String() + "_" + client.String() + "_" + version
}
