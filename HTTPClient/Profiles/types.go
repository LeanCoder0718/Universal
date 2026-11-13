// Package Profiles implementa la gestión modular, extensible y thread-safe
// de perfiles de cliente HTTP/TLS para múltiples plataformas (iOS, Android,
// Windows, macOS, Linux) y tipos de cliente (Chrome, Firefox, Safari, Edge,
// OkHttp, aplicaciones nativas).
//
// Principio arquitectónico: los ProfileSpec son INMUTABLES una vez registrados.
// La mutabilidad (cookies, session state, headers dinámicos) vive exclusivamente
// en HTTPClient.Session. Esto elimina estructuralmente toda posibilidad de
// contaminación cruzada entre goroutines concurrentes.
package Profiles

// PlatformType identifica la plataforma del sistema operativo.
type PlatformType int

const (
	PlatformWindows PlatformType = iota
	PlatformMacOS
	PlatformLinux
	PlatformIOS
	PlatformAndroid
)

// String devuelve el nombre legible de la plataforma.
func (p PlatformType) String() string {
	switch p {
	case PlatformWindows:
		return "Windows"
	case PlatformMacOS:
		return "macOS"
	case PlatformLinux:
		return "Linux"
	case PlatformIOS:
		return "iOS"
	case PlatformAndroid:
		return "Android"
	default:
		return "Unknown"
	}
}

// ClientType identifica la familia del cliente HTTP.
type ClientType int

const (
	ClientChrome ClientType = iota
	ClientFirefox
	ClientSafari
	ClientEdge
	ClientBrave
	ClientOpera
	ClientOkHTTP   // Android native HTTP client
	ClientSafariOS // Safari en dispositivo iOS (distinto al de macOS)
	ClientCustom   // Perfil personalizado (ej: Nike, Zalando, Cloudflare)
)

// String devuelve el nombre legible del tipo de cliente.
func (c ClientType) String() string {
	switch c {
	case ClientChrome:
		return "Chrome"
	case ClientFirefox:
		return "Firefox"
	case ClientSafari:
		return "Safari"
	case ClientEdge:
		return "Edge"
	case ClientBrave:
		return "Brave"
	case ClientOpera:
		return "Opera"
	case ClientOkHTTP:
		return "OkHTTP"
	case ClientSafariOS:
		return "Safari iOS"
	case ClientCustom:
		return "Custom"
	default:
		return "Unknown"
	}
}

// ProfileMode define la categoría o familia de perfiles que el
// usuario elige para la ejecución de combos/tareas.
type ProfileMode int

const (
	Browser       ProfileMode = iota + 1 // Todos los browsers de escritorio (Chrome, Firefox, Safari macOS, Edge, Brave, Opera)
	Android                              // Todos los perfiles de Android (OkHTTP)
	IOS                                  // Todos los perfiles de iOS (Safari iOS)
	Windows                              // Todos los browsers de Windows Desktop (Chrome, Edge, Firefox Windows)
	WindowsChrome                        // Perfiles netamente de Windows Chrome (Chrome 131 hasta 152)
	AndroidChrome                        // Perfiles de Android Chrome / WebView (Chrome 131, 133 Mobile)

	// Aliases de compatibilidad
	ModeBrowser       = Browser
	ModeAndroid       = Android
	ModeIOS           = IOS
	ModeWindows       = Windows
	ModeWindowsChrome = WindowsChrome
	ModeAndroidChrome = AndroidChrome
)

// String devuelve el nombre legible del modo de perfil.
func (m ProfileMode) String() string {
	switch m {
	case Browser:
		return "Browser"
	case Android:
		return "Android"
	case IOS:
		return "iOS"
	case Windows:
		return "Windows"
	case WindowsChrome:
		return "Windows Chrome"
	case AndroidChrome:
		return "Android Chrome"
	default:
		return "Unknown"
	}
}

// Capabilities describe las capacidades del perfil respecto a protocolos y
// señales de navegador. Permite que Transport y HeaderEngine tomen decisiones
// sin switch/case dispersos.
type Capabilities struct {
	SupportsHTTP2       bool
	SupportsHTTP3       bool
	SupportsClientHints bool // Chrome/Edge/Brave sí, Firefox/Safari/OkHTTP no
	SupportsTLS13       bool
	IsMobile            bool // true para iOS/Android
	IsNativeApp         bool // true para OkHTTP, apps nativas (sin sec-fetch-*)
}
