package HTTPClient

import (
	"strings"
	"time"

	tlsprofiles "github.com/bogdanfinn/tls-client/profiles"
)

// Capabilities describe qué features de protocolo soporta el perfil.
// Permite que Transport y HeaderEngine tomen decisiones sin acoplarse
// a switch/case dispersos por todo el código (como ocurría en
// buildBaseOptions dentro de Client.go).
type Capabilities struct {
	SupportsHTTP2       bool
	SupportsHTTP3       bool
	SupportsClientHints bool // Chrome sí, Firefox no
}

// Profile es la unidad ATÓMICA de "cómo se comporta este navegador".
// Es inmutable y se construye una sola vez por identity.Identity.
type Profile struct {
	Identity      Identity
	TLSProfile    tlsprofiles.ClientProfile
	Protocol      Protocol
	Capabilities  Capabilities
	DefaultLocale string
	HeaderLayout  LayoutSet
}

// FromIdentity construye un Profile coherente a partir de una Identity ya
// creada por identity.New(). Esta es la ÚNICA función que decide protocolo
// y capacidades a partir del navegador: nadie más debe hacer ese switch.
func FromIdentity(id Identity) Profile {
	bName := strings.ToLower(id.BrowserName)
	supportsHints := id.Browser == BrowserChrome &&
		!strings.Contains(bName, "safari") &&
		!strings.Contains(bName, "okhttp") &&
		!strings.Contains(bName, "native")

	caps := Capabilities{
		SupportsHTTP2:       true,
		SupportsHTTP3:       false,
		SupportsClientHints: supportsHints,
	}

	layout := resolveLayout(id.BrowserName)

	return Profile{
		Identity:      id,
		TLSProfile:    id.Profile,
		Protocol:      ProtocolHTTP1, // HTTP/1.1 por defecto
		Capabilities:  caps,
		DefaultLocale: id.AcceptLanguage,
		HeaderLayout:  layout,
	}
}

// resolveLayout selecciona el LayoutSet correcto según el nombre del
// cliente. Este es el ÚNICO punto de decisión: agregar un nuevo layout
// requiere solo añadir un case aquí y definir el LayoutSet en Layout*.go.
func resolveLayout(browserName string) LayoutSet {
	switch browserName {
	case "firefox":
		return FirefoxLayoutSet
	case "Safari", "Safari iOS":
		return SafariLayoutSet
	case "OkHTTP", "native":
		return NativeAppLayoutSet
	case "Edge":
		return EdgeLayoutSet
	case "Brave":
		return BraveLayoutSet
	case "Opera":
		return OperaLayoutSet
	default:
		// Chrome y todos los Chromium-based
		return ChromeLayoutSet
	}
}

// WithProtocol retorna una copia del perfil forzando un protocolo distinto.
// No muta el original (inmutabilidad): evita condiciones de carrera si el
// mismo Profile base es compartido entre sesiones antes de especializarse.
func (p Profile) WithProtocol(proto Protocol) Profile {
	clone := p
	clone.Protocol = proto
	return clone
}

// TLSHandshakeBudget calcula un timeout de TLS handshake razonable en
// función del protocolo (HTTP/3 racing necesita más margen).
func (p Profile) TLSHandshakeBudget(base time.Duration) time.Duration {
	if p.Protocol == ProtocolHTTP3Racing {
		return base + 5*time.Second
	}
	return base
}
