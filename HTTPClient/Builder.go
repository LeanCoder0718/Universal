// Package request implementa el ÚNICO Request Builder del proyecto.
// Ningún flujo de negocio (login, checker, etc.) debe construir
// *fhttp.Request manualmente: todo pasa por Builder.Build(), que
// garantiza que método, URL, headers (vía Engine), cuerpo y
// cookies queden siempre coherentes con la Identity de la sesión.
//
// Esto elimina el patrón repetido en el diseño original donde cada
// flujo llamaba OH()/OHAdd() y fhttp.NewRequest() por separado,
// duplicando el conocimiento de "cómo se construye un request válido".
package HTTPClient

import (
	"bytes"
	"fmt"
	"io"
	"net/url"
	"strings"

	fhttp "github.com/bogdanfinn/fhttp"
)

// Spec describe la INTENCIÓN de un request, sin detalles de bajo nivel.
// El caller de negocio solo llena esto; el Builder decide layout,
// pseudo-headers y orden.
type Spec struct {
	Method       string
	URL          string
	Kind         RequestKind
	Body         []byte
	ContentType  string
	Country      string            // Opcional: código de país para geo-enrutamiento dinámico en esta petición ("ES", "PE", "XX", etc.)
	Extra        map[string]string // headers puntuales (Authorization, etc.)
	HeaderOrder  []string          // Si se define, fuerza el orden wire exacto de las cabeceras (fhttp.HeaderOrderKey)
	CookieFilter []string          // Si no está vacío, el Session filtrará el Jar para enviar SOLO estas cookies en este request.
	Idempotent   bool

	// Opciones personalizables por petición:
	UseProxy *bool // nil = usa proxy de sesión (si hay), &false = fuerza directo sin proxy, &true = fuerza proxy
	Redirect *bool // nil = usa config de sesión, &true = sigue redirecciones, &false = detiene en 301/302
	Redict   *bool // Alias de compatibilidad para "Redict"
	NoRetry         bool  // true = deshabilita reintentos automáticos para este request
	MaxAttempts     int   // Si es > 0, define intentos máximos específicos para esta petición en Session.Do
	PreserveCookies bool  // true = ante caídas de proxy o bloqueos WAF, rota el proxy pero NUNCA borra las cookies del Jar
}

// True retorna un puntero a bool con valor true.
func True() *bool { b := true; return &b }

// False retorna un puntero a bool con valor false.
func False() *bool { b := false; return &b }

// NoRedirect es un alias para False() para deshabilitar redirecciones en Spec.
func NoRedirect() *bool { return False() }

// Bool retorna un puntero a bool con el valor proporcionado.
func Bool(b bool) *bool { return &b }

// Builder construye *fhttp.Request completos y coherentes.
type Builder struct {
	engine Engine
	layout LayoutSet
	id     Identity
}

// New crea un Builder atado a una Identity y su LayoutSet concreto.
// Un Builder vive tanto como la Session que lo posee; nunca se comparte
// entre sesiones con identidades distintas (evitaría el mismatch que
// motivó toda esta reingeniería).
func NewBuild(id Identity, layout LayoutSet) *Builder {
	return &Builder{engine: NewEngine(), layout: layout, id: id}
}

// Build materializa un *fhttp.Request listo para Transport.Do().
func (b *Builder) Build(spec Spec, cookieHeader string) (*fhttp.Request, error) {
	if spec.Method == "" {
		spec.Method = fhttp.MethodGet
	}
	u, err := url.Parse(spec.URL)
	if err != nil {
		return nil, fmt.Errorf("request: URL inválida %q: %w", spec.URL, err)
	}

	var bodyReader io.Reader
	if len(spec.Body) > 0 {
		bodyReader = bytes.NewReader(spec.Body)
	}

	req, err := fhttp.NewRequest(spec.Method, spec.URL, bodyReader)
	if err != nil {
		return nil, fmt.Errorf("request: creando request: %w", err)
	}

	layout := b.layout.Get(spec.Kind)

	ua := b.id.UserAgent
	secUA := b.id.SecChUA
	secMobile := b.id.SecChUAMobile
	secPlatform := b.id.SecChUAPlatform

	for k, v := range spec.Extra {
		if strings.EqualFold(k, "User-Agent") {
			ua = v
			if strings.Contains(v, "Android") || strings.Contains(v, "okhttp") {
				secUA = ""
				secMobile = ""
				secPlatform = ""
			}
		}
	}

	isOkHttp := strings.Contains(strings.ToLower(ua), "okhttp")
	acceptLang := b.id.AcceptLanguage
	acceptEncoding := "gzip, deflate, br"
	if isOkHttp {
		secUA = ""
		secMobile = ""
		secPlatform = ""
		acceptLang = ""
		acceptEncoding = "gzip"
	}

	ctx := Context{
		Method:          spec.Method,
		Authority:       u.Host,
		Scheme:          u.Scheme,
		Path:            u.RequestURI(),
		UserAgent:       ua,
		SecChUA:         secUA,
		SecChUAMobile:   secMobile,
		SecChUAPlatform: secPlatform,
		AcceptLanguage:  acceptLang,
		ContentType:     spec.ContentType,
		AcceptEncoding:  acceptEncoding,
		Cookie:          cookieHeader,
		Extra:           spec.Extra,
	}

	req.Header = b.engine.Build(layout, ctx)
	if len(spec.Extra) > 0 {
		req.Header = b.engine.Merge(req.Header, spec.Extra)
		for k, v := range spec.Extra {
			if strings.EqualFold(k, "Host") {
				req.Host = v
				req.Header["Host"] = []string{v}
			}
		}
	}

	if isOkHttp {
		// Native OkHttp nunca envía cabeceras W3C Sec-Fetch-* ni Client Hints de navegador
		browserHeaders := []string{
			"sec-fetch-dest", "sec-fetch-mode", "sec-fetch-site", "sec-fetch-user",
			"sec-ch-ua", "sec-ch-ua-mobile", "sec-ch-ua-platform",
		}
		for _, bh := range browserHeaders {
			for k := range req.Header {
				if strings.EqualFold(k, bh) {
					delete(req.Header, k)
				}
			}
		}
		if order, ok := req.Header[HeaderOrderKey]; ok {
			cleanOrder := make([]string, 0, len(order))
			for _, ord := range order {
				shouldKeep := true
				for _, bh := range browserHeaders {
					if strings.EqualFold(ord, bh) {
						shouldKeep = false
						break
					}
				}
				if shouldKeep {
					cleanOrder = append(cleanOrder, ord)
				}
			}
			req.Header[HeaderOrderKey] = cleanOrder
		}
	}
	if len(spec.HeaderOrder) > 0 {
		customOrder := make([]string, 0, len(spec.HeaderOrder))
		for _, h := range spec.HeaderOrder {
			cleaned := strings.ToLower(strings.TrimSpace(h))
			if cleaned != "" {
				customOrder = append(customOrder, cleaned)
			}
		}
		req.Header[HeaderOrderKey] = customOrder
	}
	if spec.ContentType != "" {
		hasCT := false
		for k := range req.Header {
			if strings.EqualFold(k, "Content-Type") {
				hasCT = true
				break
			}
		}
		if !hasCT {
			req.Header.Set("Content-Type", spec.ContentType)
		}
	}
	if cookieHeader != "" {
		hasCookie := false
		for k := range req.Header {
			if strings.EqualFold(k, "Cookie") {
				hasCookie = true
				break
			}
		}
		if !hasCookie {
			req.Header.Set("Cookie", cookieHeader)
		}
	}
	return req, nil
}

// BuildJSON es un atajo de conveniencia para el caso más común (API JSON),
// evitando que cada flujo repita el mismo Content-Type manualmente.
func (b *Builder) BuildJSON(method, rawURL string, body []byte, cookieHeader string) (*fhttp.Request, error) {
	return b.Build(Spec{
		Method:      method,
		URL:         rawURL,
		Kind:        KindXHR,
		Body:        body,
		ContentType: "application/json",
	}, cookieHeader)
}

// BuildDocument construye una navegación de documento completo (GET
// inicial de página), usando el layout Document (con sec-fetch-user, etc.)
func (b *Builder) BuildDocument(rawURL string, cookieHeader string) (*fhttp.Request, error) {
	return b.Build(Spec{
		Method: fhttp.MethodGet,
		URL:    rawURL,
		Kind:   KindDocument,
	}, cookieHeader)
}

// CookieHeaderFromList serializa cookies al formato de header "Cookie:"
// estándar (name=value; name2=value2), función pura reutilizable.
func CookieHeaderFromList(cookies []*fhttp.Cookie) string {
	if len(cookies) == 0 {
		return ""
	}
	parts := make([]string, 0, len(cookies))
	for _, c := range cookies {
		parts = append(parts, c.Name+"="+c.Value)
	}
	return strings.Join(parts, "; ")
}
