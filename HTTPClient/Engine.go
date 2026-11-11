package HTTPClient

import (
	"net/url"
	"sort"
	"strings"

	fhttp "github.com/bogdanfinn/fhttp"
)

// HeaderOrderKey re-exporta la clave especial de fhttp para no obligar a
// otros paquetes (request, workers) a importar fhttp directamente solo
// para leer/escribir el orden.
const HeaderOrderKey = fhttp.HeaderOrderKey

// Context contiene todos los valores dinámicos que un Layout puede
// necesitar para resolver sus Slots. Es el único "input" que el Engine
// acepta: nada de leer identity.Identity a medias desde otros paquetes.
type Context struct {
	Method          string
	Authority       string
	Scheme          string
	Path            string
	UserAgent       string
	SecChUA         string
	SecChUAMobile   string
	SecChUAPlatform string
	AcceptLanguage  string
	ContentType     string
	AcceptEncoding  string
	Cookie          string
	// Extra permite valores puntuales por request (ej. Authorization)
	// sin ensuciar el Context genérico con cada caso de negocio posible.
	Extra map[string]string
}

// GetHeader busca de forma insensible a mayúsculas/minúsculas un valor en fhttp.Header.
// Esto es vital porque en HTTP/2 las cabeceras se almacenan en minúsculas (layout wire),
// mientras que Go CanonicalMIMEHeaderKey busca exclusivamente con formato Título-Case.
func GetHeader(h fhttp.Header, name string) string {
	if h == nil {
		return ""
	}
	if v := h.Get(name); v != "" {
		return v
	}
	for k, vv := range h {
		if strings.EqualFold(k, name) && len(vv) > 0 {
			return vv[0]
		}
	}
	return ""
}

// Engine construye fhttp.Header a partir de un Layout + Context.
// Es el ÚNICO punto de materialización de cabeceras de todo el proyecto.
type Engine struct{}

// NewEngine construye el motor de cabeceras. Sin estado propio: toda la
// variabilidad viene de Layout (estático) y Context (dinámico por request),
// lo que lo hace trivialmente testeable y thread-safe sin locks.
func NewEngine() Engine { return Engine{} }

// Build materializa un fhttp.Header respetando EXACTAMENTE el orden del
// Layout. No hay shuffle, no hay aleatoriedad: el orden de una sesión es
// estable durante toda su vida, como en un navegador real.
func (Engine) Build(layout Layout, ctx Context) fhttp.Header {
	h := fhttp.Header{}
	order := make([]string, 0, len(layout.Slots))

	resolve := func(name string) (string, bool) {
		switch name {
		case ":method":
			return ctx.Method, ctx.Method != ""
		case ":authority":
			return ctx.Authority, ctx.Authority != ""
		case ":scheme":
			return ctx.Scheme, ctx.Scheme != ""
		case ":path":
			return ctx.Path, ctx.Path != ""
		case "user-agent":
			return ctx.UserAgent, ctx.UserAgent != ""
		case "sec-ch-ua":
			return ctx.SecChUA, ctx.SecChUA != ""
		case "sec-ch-ua-mobile":
			return ctx.SecChUAMobile, ctx.SecChUAMobile != ""
		case "sec-ch-ua-platform":
			return ctx.SecChUAPlatform, ctx.SecChUAPlatform != ""
		case "accept-language":
			return ctx.AcceptLanguage, ctx.AcceptLanguage != ""
		case "accept-encoding":
			return ctx.AcceptEncoding, ctx.AcceptEncoding != ""
		case "content-type":
			return ctx.ContentType, ctx.ContentType != ""
		case "cookie":
			return ctx.Cookie, ctx.Cookie != ""
		case "sec-fetch-site":
			if ctx.Extra != nil {
				if v, ok := ctx.Extra["sec-fetch-site"]; ok && v != "" {
					return v, true
				}
				ref := ""
				for k, v := range ctx.Extra {
					if strings.EqualFold(k, "referer") && v != "" {
						ref = v
						break
					}
				}
				if ref != "" {
					if refURL, err := url.Parse(ref); err == nil && refURL.Host != "" {
						refHost := refURL.Hostname()
						targetHost := ctx.Authority
						if idx := strings.Index(targetHost, ":"); idx != -1 {
							targetHost = targetHost[:idx]
						}
						if strings.EqualFold(refHost, targetHost) {
							return "same-origin", true
						}
						if RootDomain(refHost) == RootDomain(targetHost) {
							return "same-site", true
						}
						return "cross-site", true
					}
				}
			}
			return "none", true
		default:
			if ctx.Extra != nil {
				v, ok := ctx.Extra[name]
				return v, ok && v != ""
			}
			return "", false
		}
	}

	for _, slot := range layout.Slots {
		value := slot.StaticValue
		present := value != ""
		if !present {
			value, present = resolve(slot.Name)
		}
		if !present {
			if slot.Required {
				// Fail Fast silencioso: un slot requerido sin valor indica
				// un Context incompleto para este Layout. No agregamos la
				// cabecera vacía (rompería el fingerprint igual), pero el
				// caller debería validar con Validate() antes de enviar.
				continue
			}
			continue
		}

		// Los pseudo-headers (:method, :scheme, etc.) no deben añadirse al mapa Header
		// porque fhttp/net/http los rechaza como nombres inválidos. Solo se añaden al orden.
		if !strings.HasPrefix(slot.Name, ":") {
			for existingKey := range h {
				if existingKey != HeaderOrderKey && strings.EqualFold(existingKey, slot.Name) {
					delete(h, existingKey)
				}
			}
			h[slot.Name] = []string{value}
		}
		order = append(order, slot.Name)
	}

	h[HeaderOrderKey] = order
	return h
}

// Validate detecta slots Required sin valor resuelto ANTES de enviar el
// request (Fail Fast), en vez de descubrirlo como un 400 del servidor.
func (Engine) Validate(layout Layout, ctx Context) []string {
	var missing []string
	tmp := NewEngine().Build(layout, ctx)
	order, _ := tmp[HeaderOrderKey]
	present := make(map[string]bool, len(order))
	for _, k := range order {
		present[strings.ToLower(k)] = true
	}
	for _, slot := range layout.Slots {
		if slot.Required && !present[strings.ToLower(slot.Name)] {
			missing = append(missing, slot.Name)
		}
	}
	return missing
}

// Merge combina un fhttp.Header generado por Build con overrides puntuales
// (ej. Authorization dinámico) SIN duplicar claves con distinto casing en el mapa.
// Si el valor de un override es "", la cabecera correspondiente se elimina de Header y de HeaderOrderKey.
func (Engine) Merge(base fhttp.Header, overrides map[string]string) fhttp.Header {
	if len(overrides) == 0 {
		return base
	}
	keys := make([]string, 0, len(overrides))
	for k := range overrides {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	order, _ := base[HeaderOrderKey]
	for _, k := range keys {
		v := overrides[k]
		if v == "" {
			for existingKey := range base {
				if existingKey != HeaderOrderKey && strings.EqualFold(existingKey, k) {
					delete(base, existingKey)
				}
			}
			newOrder := make([]string, 0, len(order))
			for _, ord := range order {
				if !strings.EqualFold(ord, k) {
					newOrder = append(newOrder, ord)
				}
			}
			order = newOrder
			continue
		}

		// Eliminar cualquier versión anterior del header (insensible a mayúsculas/minúsculas)
		// para no duplicar cabeceras en el wire format de fhttp
		for existingKey := range base {
			if existingKey != HeaderOrderKey && strings.EqualFold(existingKey, k) {
				delete(base, existingKey)
			}
		}

		base[k] = []string{v}

		// Asegurar que el header esté registrado en HeaderOrderKey en una sola posición
		found := false
		for _, ord := range order {
			if strings.EqualFold(ord, k) {
				found = true
				break
			}
		}
		if !found {
			order = append(order, k)
		}
	}
	base[HeaderOrderKey] = order
	return base
}
