package HTTPClient

import (
	"net/url"

	fhttp "github.com/bogdanfinn/fhttp"
)

// TLSClientAdapter adapta nuestro Jar (RFC 6265 completo) a la interfaz
// tlsclient.CookieJar exigida por bogdanfinn/tls-client, SIN filtrar esa
// dependencia externa hacia el resto de los paquetes de la librería
// (Dependency Inversion: transport es el único que conoce tls-client).
//
// Esto reemplaza el uso de SharedDomainJar directamente como
// tlsclient.CookieJar en Client.go, que mezclaba responsabilidades de
// RFC 6265 con la interfaz específica de la librería TLS de terceros.
type TLSClientAdapter struct {
	jar *Jar
}

// NewTLSClientAdapter envuelve un *Jar existente.
func NewTLSClientAdapter(jar *Jar) *TLSClientAdapter {
	return &TLSClientAdapter{jar: jar}
}

// SetCookies implementa tlsclient.CookieJar.
func (a *TLSClientAdapter) SetCookies(u *url.URL, fcookies []*fhttp.Cookie) {
	if u == nil {
		return
	}
	a.jar.SetCookies(u.Hostname(), u.Path, fcookies)
}

// Cookies implementa tlsclient.CookieJar.
func (a *TLSClientAdapter) Cookies(u *url.URL) []*fhttp.Cookie {
	if u == nil {
		return nil
	}
	isSecure := u.Scheme == "https"
	return a.jar.CookiesFor(u.Hostname(), u.Path, isSecure)
}

// Underlying expone el *Jar real para operaciones avanzadas (AddManual,
// Snapshot, Reset) sin obligar a re-implementar esos métodos aquí.
func (a *TLSClientAdapter) Underlying() *Jar {
	return a.jar
}
