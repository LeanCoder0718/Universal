package HTTPClient

import (
	"strconv"
	"strings"
	"time"

	fhttp "github.com/bogdanfinn/fhttp"
)

// isWeekday evalúa si una palabra corresponde a un día de la semana en formato RFC 1123 / RFC 850.
func isWeekday(s string) bool {
	s = strings.TrimSpace(s)
	switch strings.ToLower(s) {
	case "mon", "tue", "wed", "thu", "fri", "sat", "sun",
		"monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday":
		return true
	}
	return false
}

// SplitSetCookieHeader separa una línea Set-Cookie que contenga múltiples cookies unidas por coma
// (comportamiento común de proxies o multiplexación HTTP/2) sin romper las comas dentro del atributo Expires
// (ej. "Expires=Thu, 30-Oct-1980 16:00:00 GMT").
func SplitSetCookieHeader(line string) []string {
	line = strings.TrimSpace(line)
	if line == "" {
		return nil
	}

	var result []string
	start := 0
	inQuotes := false
	n := len(line)

	for i := 0; i < n; i++ {
		ch := line[i]
		if ch == '"' {
			inQuotes = !inQuotes
			continue
		}
		if ch == ',' && !inQuotes {
			// Miramos la palabra inmediatamente anterior a la coma para ver si es un día de la semana
			wordStart := i - 1
			for wordStart >= start && (line[wordStart] == ' ' || line[wordStart] == '\t') {
				wordStart--
			}
			wordEnd := wordStart + 1
			for wordStart >= start && line[wordStart] != ' ' && line[wordStart] != '\t' && line[wordStart] != '=' && line[wordStart] != ';' {
				wordStart--
			}
			word := line[wordStart+1 : wordEnd]

			if isWeekday(word) {
				// Coma dentro de una fecha en Expires (ej: "Thu,"), no separar
				continue
			}

			// Es un separador entre cookies independientes
			cookieStr := strings.TrimSpace(line[start:i])
			if cookieStr != "" {
				result = append(result, cookieStr)
			}
			start = i + 1
		}
	}

	if start < n {
		cookieStr := strings.TrimSpace(line[start:])
		if cookieStr != "" {
			result = append(result, cookieStr)
		}
	}

	return result
}

// ParseSetCookie parsea una cookie individual desde su representación textual completa.
func ParseSetCookie(sc string) *fhttp.Cookie {
	sc = strings.TrimSpace(sc)
	if sc == "" {
		return nil
	}

	parts := strings.Split(sc, ";")
	if len(parts) == 0 {
		return nil
	}

	nameVal := strings.TrimSpace(parts[0])
	eqIdx := strings.IndexByte(nameVal, '=')
	if eqIdx <= 0 {
		return nil
	}

	name := strings.TrimSpace(nameVal[:eqIdx])
	val := strings.TrimSpace(nameVal[eqIdx+1:])
	if len(val) >= 2 && val[0] == '"' && val[len(val)-1] == '"' {
		val = val[1 : len(val)-1]
	}

	cookie := &fhttp.Cookie{
		Name:  name,
		Value: val,
		Raw:   sc,
	}

	for i := 1; i < len(parts); i++ {
		p := strings.TrimSpace(parts[i])
		if len(p) == 0 {
			continue
		}
		kvAttr := strings.SplitN(p, "=", 2)
		k := strings.ToLower(strings.TrimSpace(kvAttr[0]))
		var v string
		if len(kvAttr) == 2 {
			v = strings.TrimSpace(kvAttr[1])
			if len(v) >= 2 && v[0] == '"' && v[len(v)-1] == '"' {
				v = v[1 : len(v)-1]
			}
		}

		switch k {
		case "domain":
			cookie.Domain = strings.TrimPrefix(v, ".")
		case "path":
			cookie.Path = v
		case "secure":
			cookie.Secure = true
		case "httponly":
			cookie.HttpOnly = true
		case "max-age":
			if secs, err := strconv.Atoi(v); err == nil {
				cookie.MaxAge = secs
			}
		case "expires":
			cookie.RawExpires = v
			if t, err := time.Parse(time.RFC1123, v); err == nil {
				cookie.Expires = t.UTC()
			} else if t, err := time.Parse("Mon, 02-Jan-2006 15:04:05 MST", v); err == nil {
				cookie.Expires = t.UTC()
			} else if t, err := time.Parse("Mon, 02-Jan-2006 15:04:05 -0700", v); err == nil {
				cookie.Expires = t.UTC()
			} else if t, err := time.Parse(time.RFC850, v); err == nil {
				cookie.Expires = t.UTC()
			} else if t, err := time.Parse(time.ANSIC, v); err == nil {
				cookie.Expires = t.UTC()
			}
		case "samesite":
			switch strings.ToLower(v) {
			case "lax":
				cookie.SameSite = fhttp.SameSiteLaxMode
			case "strict":
				cookie.SameSite = fhttp.SameSiteStrictMode
			case "none":
				cookie.SameSite = fhttp.SameSiteNoneMode
			default:
				cookie.SameSite = fhttp.SameSiteDefaultMode
			}
		}
	}

	return cookie
}

// ParseSetCookies extrae y parsea todas las cookies de un header fhttp.Header,
// manejando adecuadamente cabeceras repetidas y unidas con comas.
func ParseSetCookies(header fhttp.Header) []*fhttp.Cookie {
	if header == nil {
		return nil
	}

	var rawLines []string
	for k, v := range header {
		if strings.EqualFold(k, "Set-Cookie") {
			rawLines = append(rawLines, v...)
		}
	}

	if len(rawLines) == 0 {
		return nil
	}

	var cookies []*fhttp.Cookie
	for _, line := range rawLines {
		split := SplitSetCookieHeader(line)
		for _, s := range split {
			if c := ParseSetCookie(s); c != nil && c.Name != "" {
				cookies = append(cookies, c)
			}
		}
	}

	return cookies
}
