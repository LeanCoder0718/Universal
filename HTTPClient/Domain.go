package HTTPClient

import (
	"net/url"
	"strings"
)

// RootDomain reduce un host a su dominio "raíz" de dos niveles.
// "accounts.netflix.com" -> "netflix.com"
// "netflix.com"          -> "netflix.com"
//
// ÚNICA implementación en todo el proyecto. Antes existían dos copias
// (una en Client.go para cookies, otra en RateLimiter.go para throttling)
// que podían desincronizarse silenciosamente. Ahora cookies, proxy y
// rate-limiting usan esta misma función: Single Source of Truth.
func RootDomain(host string) string {
	host = strings.ToLower(strings.TrimSpace(host))
	host = strings.TrimSuffix(host, ".")
	parts := strings.Split(host, ".")
	if len(parts) >= 2 {
		return parts[len(parts)-2] + "." + parts[len(parts)-1]
	}
	return host
}

// RootDomainFromURL extrae el host de una URL cruda y aplica RootDomain.
// Retorna "" si la URL es inválida, permitiendo a los llamadores decidir
// si eso es un error fatal (Fail Fast) o un caso ignorable.
func RootDomainFromURL(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}
	return RootDomain(u.Hostname())
}

// Truncate corta un string a n runes preservando UTF-8, usado únicamente
// para logging/debug, nunca para lógica de negocio.
func Truncate(s string, n int) string {
	runes := []rune(s)
	if len(runes) <= n {
		return s
	}
	return string(runes[:n]) + "..."
}
