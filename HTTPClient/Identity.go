package HTTPClient

import (
	"github.com/LeanCoder0718/Universal/HTTPClient/Profiles"
	"crypto/rand"
	"encoding/binary"
	"fmt"

	"github.com/bogdanfinn/tls-client/profiles"
)

// Browser enumera las familias de navegador soportadas.
// Cerrado a propósito (no string libre) para que sea imposible construir
// una Identity con un valor de familia no soportado por el resto del sistema.
type Browser int

const (
	BrowserChrome Browser = iota
	BrowserFirefox
)

// Platform enumera plataformas soportadas con toda su metadata coherente.
type Platform int

const (
	PlatformWindows Platform = iota
	PlatformMacOS
)

// chromeRelease agrupa TODA la info de una versión de Chrome en un solo
// lugar: versión, profile TLS y el token "Not-A-Brand" correcto para esa
// versión. Antes esta información estaba fragmentada en un slice anónimo
// dentro de BrowserClient.go sin relación explícita con el profile TLS.
type chromeRelease struct {
	Major     string
	Full      string
	Profile   profiles.ClientProfile
	NotABrand string
}

var chromeReleases = []chromeRelease{
	{"131", "131.0.0.0", profiles.Chrome_131, `"Not(A:Brand"`},
	{"133", "133.0.0.0", profiles.Chrome_133, `"Not/A)Brand"`},
}

type firefoxRelease struct {
	Major   string
	Full    string
	Profile profiles.ClientProfile
}

var firefoxReleases = []firefoxRelease{
	{"132", "132.0", profiles.Firefox_132},
	{"133", "133.0", profiles.Firefox_133},
	{"135", "135.0", profiles.Firefox_135},
}

type platformInfo struct {
	Name        string // "Windows" | "macOS"
	SecPlatform string // valor exacto para sec-ch-ua-platform
	UAFragment  func(browser Browser) string
}

var platforms = []platformInfo{
	{
		Name:        "Windows",
		SecPlatform: `"Windows"`,
		UAFragment: func(_ Browser) string {
			return "Windows NT 10.0; Win64; x64"
		},
	},
}

// Identity es el conjunto COMPLETO y COHERENTE de señales de cliente.
// Es inmutable: todos los campos son exportados solo lectura por convención
// (no hay setters). Para cambiar identidad se crea una nueva instancia.
type Identity struct {
	Browser         Browser
	BrowserName     string // "chrome" | "firefox"
	BrowserVersion  string // major, ej "133"
	UserAgent       string
	SecChUA         string // "" en Firefox (no soporta Client Hints)
	SecChUAMobile   string
	SecChUAPlatform string
	PlatformName    string // "Windows" | "macOS"
	AcceptLanguage  string
	SecGPC          string // "1" o "" — presente en ~8% de navegadores reales
	Profile         profiles.ClientProfile

	// seed determinístico derivado de esta identidad, usado por headers.Engine
	// para mantener un orden de cabeceras ESTABLE durante toda la sesión
	// (nunca aleatorio por request — ver auditoría, hallazgo #2).
	sessionSeed uint64
}

// SessionSeed expone el seed determinístico de esta identidad.
func (id Identity) SessionSeed() uint64 { return id.sessionSeed }

// Option permite personalizar la generación sin romper la API si se
// agregan más ejes de configuración en el futuro (Open/Closed).
type IdentityOption func(*buildParams)

type buildParams struct {
	forceBrowser  *Browser
	forcePlatform *Platform
	forceLanguage *string
	firefoxRatio  float64 // probabilidad de elegir Firefox si no se fuerza
}

// WithBrowser fuerza una familia de navegador específica.
func WithBrowser(b Browser) IdentityOption {
	return func(p *buildParams) { p.forceBrowser = &b }
}

// WithPlatform fuerza una plataforma específica.
func WithPlatform(pl Platform) IdentityOption {
	return func(p *buildParams) { p.forcePlatform = &pl }
}

// WithFirefoxRatio ajusta la probabilidad [0,1] de generar Firefox cuando
// no se fuerza un browser. Por defecto 0: Chrome-only, recomendado para
// objetivos que verifican fuertemente coherencia Chromium (ej. Netflix web).
func WithFirefoxRatio(ratio float64) IdentityOption {
	return func(p *buildParams) { p.firefoxRatio = ratio }
}

// WithLanguage fuerza un header Accept-Language específico en lugar de aleatorio.
func WithLanguage(lang string) IdentityOption {
	return func(p *buildParams) { p.forceLanguage = &lang }
}

var acceptLanguages = []string{
	"en-US,en;q=0.9",
	"en-US,en;q=0.9,es;q=0.8",
	"en-GB,en;q=0.9,en-US;q=0.8",
	"en-US,en;q=0.9,pt;q=0.8",
}

// New es el ÚNICO constructor autorizado de Identity en todo el proyecto.
// Cualquier código que necesite un UserAgent, sec-ch-ua o profile TLS DEBE
// pasar por aquí. Esto elimina la posibilidad estructural de mismatch
// entre TLS profile y headers de navegador.
func NewIdentity(opts ...IdentityOption) (Identity, error) {
	params := buildParams{firefoxRatio: 0}
	for _, opt := range opts {
		opt(&params)
	}

	seed, err := randomSeed()
	if err != nil {
		return Identity{}, fmt.Errorf("identity: generando seed de sesión: %w", err)
	}
	rng := newSeededRNG(seed)

	browser := pickBrowser(params, rng)
	platform, err := pickPlatform(params, rng)
	if err != nil {
		return Identity{}, fmt.Errorf("identity: seleccionando plataforma: %w", err)
	}
	pinfo := platforms[platform]

	var id Identity
	id.sessionSeed = seed
	id.PlatformName = pinfo.Name
	id.SecChUAPlatform = pinfo.SecPlatform
	if params.forceLanguage != nil {
		id.AcceptLanguage = *params.forceLanguage
	} else {
		id.AcceptLanguage = acceptLanguages[int(rng()%uint64(len(acceptLanguages)))]
	}

	switch browser {
	case BrowserFirefox:
		rel := firefoxReleases[int(rng()%uint64(len(firefoxReleases)))]
		id.Browser = BrowserFirefox
		id.BrowserName = "firefox"
		id.BrowserVersion = rel.Major
		id.Profile = rel.Profile
		id.UserAgent = fmt.Sprintf(
			"Mozilla/5.0 (%s; rv:%s) Gecko/20100101 Firefox/%s",
			pinfo.UAFragment(browser), rel.Full, rel.Full,
		)
		id.SecChUA = "" // Firefox no envía Client Hints
		id.SecChUAMobile = ""

	default: // BrowserChrome
		rel := chromeReleases[int(rng()%uint64(len(chromeReleases)))]
		id.Browser = BrowserChrome
		id.BrowserName = "chrome"
		id.BrowserVersion = rel.Major
		id.Profile = rel.Profile
		id.UserAgent = fmt.Sprintf(
			"Mozilla/5.0 (%s) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/%s Safari/537.36",
			pinfo.UAFragment(browser), rel.Full,
		)
		id.SecChUA = fmt.Sprintf(
			`%s;v="99", "Google Chrome";v="%s", "Chromium";v="%s"`,
			rel.NotABrand, rel.Major, rel.Major,
		)
		id.SecChUAMobile = "?0"
	}

	if rng()%100 < 8 {
		id.SecGPC = "1"
	}

	return id, nil
}

func pickBrowser(p buildParams, rng func() uint64) Browser {
	if p.forceBrowser != nil {
		return *p.forceBrowser
	}
	if p.firefoxRatio <= 0 {
		return BrowserChrome
	}
	threshold := uint64(p.firefoxRatio * 100)
	if rng()%100 < threshold {
		return BrowserFirefox
	}
	return BrowserChrome
}

func pickPlatform(p buildParams, rng func() uint64) (Platform, error) {
	if p.forcePlatform != nil {
		pl := *p.forcePlatform
		if int(pl) < 0 || int(pl) >= len(platforms) {
			return 0, fmt.Errorf("identity: plataforma %d no implementada (solo Windows disponible)", pl)
		}
		return pl, nil
	}
	return Platform(rng() % uint64(len(platforms))), nil
}

// randomSeed usa crypto/rand para la semilla inicial: la generación de
// identidad NO debe depender de math/rand global compartido (evita
// contención y correlación entre goroutines concurrentes).
func randomSeed() (uint64, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(b[:]), nil
}

// newSeededRNG crea un generador determinístico simple (splitmix64) a
// partir del seed. Se usa splitmix64 en vez de math/rand para no acoplar
// este paquete a estado global ni a *rand.Rand (menos allocations).
func newSeededRNG(seed uint64) func() uint64 {
	state := seed
	return func() uint64 {
		state += 0x9E3779B97F4A7C15
		z := state
		z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9
		z = (z ^ (z >> 27)) * 0x94D049BB133111EB
		return z ^ (z >> 31)
	}
}

// IdentityFromSpec construye una Identity a partir de un ProfileSpec del
// ProfileManager. Es el puente entre el sistema modular de perfiles y la
// infraestructura existente de Session/Builder/Transport.
//
// El ProfileSpec es inmutable (solo lectura). La Identity resultante
// también es inmutable por convención. No hay estado compartido.
func IdentityFromSpec(spec *Profiles.ProfileSpec) (Identity, error) {
	if spec == nil {
		return Identity{}, fmt.Errorf("identity: ProfileSpec es nil")
	}

	seed, err := randomSeed()
	if err != nil {
		return Identity{}, fmt.Errorf("identity: generando seed de sesión: %w", err)
	}

	// Mapear ClientType del ProfileManager al Browser enum existente.
	var browser Browser
	switch spec.ClientType() {
	case Profiles.ClientFirefox:
		browser = BrowserFirefox
	default:
		// Chrome, Edge, Brave, Opera, Safari, OkHTTP — todos usan el
		// path de Chrome para el Builder/Layout (los headers se overridean
		// via Spec.Extra en el flujo de negocio cuando sea necesario).
		browser = BrowserChrome
	}

	browserName := spec.ClientType().String()
	if browser == BrowserChrome {
		browserName = "chrome"
	} else {
		browserName = "firefox"
	}

	id := Identity{
		Browser:         browser,
		BrowserName:     browserName,
		BrowserVersion:  spec.Version(),
		UserAgent:       spec.UserAgent(),
		SecChUA:         spec.SecChUA(),
		SecChUAMobile:   spec.SecChUAMobile(),
		SecChUAPlatform: spec.SecChUAPlatform(),
		PlatformName:    spec.PlatformName(),
		AcceptLanguage:  spec.DefaultLocale(),
		SecGPC:          spec.SecGPC(),
		Profile:         spec.TLSProfile(),
		sessionSeed:     seed,
	}

	return id, nil
}
