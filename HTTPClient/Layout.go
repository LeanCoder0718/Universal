package HTTPClient

// RequestKind clasifica el tipo de request para elegir el layout correcto.
// Un navegador real envía distintos headers (sec-fetch-*, etc.) según el
// tipo de navegación: esto no puede ser una decisión ad-hoc por endpoint.
type RequestKind int

const (
	KindDocument RequestKind = iota // navegación completa (document)
	KindXHR                         // fetch/XHR (cors, same-origin API)
	KindNavigate                    // click de usuario a nueva URL top-level
	KindRaw                         // petición directa/cruda sin cabeceras automáticas de navegador
)

// Slot representa una cabecera dentro de un layout.
type Slot struct {
	Name     string
	Required bool
	// StaticValue, si no vacío, es un valor fijo que no depende del contexto
	// (ej. "gzip, deflate, br, zstd"). Si está vacío, el valor se resuelve
	// dinámicamente desde el Context al construir la request.
	StaticValue string
}

// Layout define el orden EXACTO y el conjunto de cabeceras para un
// RequestKind concreto, dentro de una familia de navegador.
type Layout struct {
	Kind  RequestKind
	Slots []Slot
}

// LayoutSet agrupa todos los layouts de una familia de navegador
// (Chrome, Firefox). Mantiene juntos los layouts relacionados: cambiar
// el comportamiento de Chrome se hace en un solo lugar.
type LayoutSet struct {
	BrowserName string
	Layouts     map[RequestKind]Layout
}

// Get devuelve el layout para un RequestKind, con fallback a KindXHR si
// no existe uno específico (todas las LayoutSet deben definir KindXHR).
func (s LayoutSet) Get(kind RequestKind) Layout {
	if l, ok := s.Layouts[kind]; ok {
		return l
	}
	if kind == KindRaw {
		return Layout{Kind: KindRaw, Slots: nil}
	}
	return s.Layouts[KindXHR]
}

// pseudoHeaderSlots son comunes a HTTP/2 en cualquier navegador y SIEMPRE
// van primero, en este orden exacto. Nunca se reordenan (romper esto
// rompe la negociación HTTP/2 real).
var pseudoHeaderSlots = []Slot{
	{Name: ":method", Required: true},
	{Name: ":authority", Required: true},
	{Name: ":scheme", Required: true},
	{Name: ":path", Required: true},
}

func withPseudo(rest ...Slot) []Slot {
	slots := make([]Slot, 0, len(pseudoHeaderSlots)+len(rest))
	slots = append(slots, pseudoHeaderSlots...)
	slots = append(slots, rest...)
	return slots
}

// ChromeLayoutSet reproduce el orden real de Chrome/Chromium 124-137
// observado en tráfico HTTP/2 (capturado vía Burp/JADX por el usuario).
var ChromeLayoutSet = LayoutSet{
	BrowserName: "chrome",
	Layouts: map[RequestKind]Layout{
		KindDocument: {
			Kind: KindDocument,
			Slots: withPseudo(
				Slot{Name: "cache-control", Required: false},
				Slot{Name: "sec-ch-ua", Required: false},
				Slot{Name: "sec-ch-ua-mobile", Required: false},
				Slot{Name: "sec-ch-ua-platform", Required: false},
				Slot{Name: "origin", Required: false},
				Slot{Name: "content-type", Required: false},
				Slot{Name: "upgrade-insecure-requests", Required: true, StaticValue: "1"},
				Slot{Name: "user-agent", Required: true},
				Slot{Name: "accept", Required: true, StaticValue: "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8"},
				Slot{Name: "sec-fetch-site", Required: true, StaticValue: "same-origin"},
				Slot{Name: "sec-fetch-mode", Required: true, StaticValue: "navigate"},
				Slot{Name: "sec-fetch-user", Required: true, StaticValue: "?1"},
				Slot{Name: "sec-fetch-dest", Required: true, StaticValue: "document"},
				Slot{Name: "referer", Required: false},
				Slot{Name: "accept-encoding", Required: true},
				Slot{Name: "accept-language", Required: true},
				Slot{Name: "cookie", Required: false},
			),
		},
		KindXHR: {
			Kind: KindXHR,
			Slots: withPseudo(
				Slot{Name: "sec-ch-ua", Required: false},
				Slot{Name: "sec-ch-ua-mobile", Required: false},
				Slot{Name: "sec-ch-ua-platform", Required: false},
				Slot{Name: "user-agent", Required: true},
				Slot{Name: "accept", Required: true, StaticValue: "*/*"},
				Slot{Name: "content-type", Required: false},
				Slot{Name: "origin", Required: false},
				Slot{Name: "sec-fetch-site", Required: true, StaticValue: "same-origin"},
				Slot{Name: "sec-fetch-mode", Required: true, StaticValue: "cors"},
				Slot{Name: "sec-fetch-dest", Required: true, StaticValue: "empty"},
				Slot{Name: "referer", Required: false},
				Slot{Name: "accept-encoding", Required: true},
				Slot{Name: "accept-language", Required: true},
				Slot{Name: "cookie", Required: false},
			),
		},
		KindNavigate: {
			Kind: KindNavigate,
			Slots: withPseudo(
				Slot{Name: "cache-control", Required: false},
				Slot{Name: "sec-ch-ua", Required: false},
				Slot{Name: "sec-ch-ua-mobile", Required: false},
				Slot{Name: "sec-ch-ua-platform", Required: false},
				Slot{Name: "origin", Required: false},
				Slot{Name: "content-type", Required: false},
				Slot{Name: "upgrade-insecure-requests", Required: true, StaticValue: "1"},
				Slot{Name: "user-agent", Required: true},
				Slot{Name: "accept", Required: true, StaticValue: "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8"},
				Slot{Name: "sec-fetch-site", Required: true, StaticValue: "same-origin"},
				Slot{Name: "sec-fetch-mode", Required: true, StaticValue: "navigate"},
				Slot{Name: "sec-fetch-user", Required: true, StaticValue: "?1"},
				Slot{Name: "sec-fetch-dest", Required: true, StaticValue: "document"},
				Slot{Name: "referer", Required: false},
				Slot{Name: "accept-encoding", Required: true},
				Slot{Name: "accept-language", Required: true},
				Slot{Name: "cookie", Required: false},
			),
		},
	},
}

// FirefoxLayoutSet: Firefox no soporta Client Hints (sin sec-ch-ua-*) y
// tiene un orden ligeramente distinto de sec-fetch-*. Mantenerlo separado
// evita mezclar accidentalmente headers de Chromium en tráfico Firefox.
var FirefoxLayoutSet = LayoutSet{
	BrowserName: "firefox",
	Layouts: map[RequestKind]Layout{
		KindDocument: {
			Kind: KindDocument,
			Slots: withPseudo(
				Slot{Name: "user-agent", Required: true},
				Slot{Name: "accept", Required: true, StaticValue: "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"},
				Slot{Name: "accept-language", Required: true},
				Slot{Name: "accept-encoding", Required: true},
				Slot{Name: "sec-fetch-dest", Required: true, StaticValue: "document"},
				Slot{Name: "sec-fetch-mode", Required: true, StaticValue: "navigate"},
				Slot{Name: "sec-fetch-site", Required: true, StaticValue: "none"},
				Slot{Name: "sec-fetch-user", Required: true, StaticValue: "?1"},
				Slot{Name: "cookie", Required: false},
			),
		},
		KindXHR: {
			Kind: KindXHR,
			Slots: withPseudo(
				Slot{Name: "user-agent", Required: true},
				Slot{Name: "accept", Required: true, StaticValue: "*/*"},
				Slot{Name: "accept-language", Required: true},
				Slot{Name: "accept-encoding", Required: true},
				Slot{Name: "content-type", Required: false},
				Slot{Name: "sec-fetch-dest", Required: true, StaticValue: "empty"},
				Slot{Name: "sec-fetch-mode", Required: true, StaticValue: "cors"},
				Slot{Name: "sec-fetch-site", Required: true, StaticValue: "same-origin"},
				Slot{Name: "cookie", Required: false},
			),
		},
	},
}
