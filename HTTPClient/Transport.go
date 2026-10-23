// Package transport es la ÃšNICA capa del proyecto que importa
// github.com/bogdanfinn/tls-client directamente. Todo el resto del
// sistema (browser, headers, cookies, retry, workers) depende solo de
// la interfaz Client definida aquÃ­ (Dependency Inversion Principle).
package HTTPClient

import (
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/bogdanfinn/tls-client/profiles"

	fhttp "github.com/bogdanfinn/fhttp"
	tlsclient "github.com/bogdanfinn/tls-client"
)

// DefaultCertificatePins almacena los pines criptogrÃ¡ficos SPKI SHA-256 (Base64 puro) de los servidores legÃ­timos.
// Si un capturador (Fiddler, Charles, Burp, HTTP Toolkit, etc.) intenta interceptar el trÃ¡fico,
// el pin no coincidirÃ¡ y la conexiÃ³n se abortarÃ¡ automÃ¡ticamente en el handshake TLS antes de enviar datos.
var DefaultCertificatePins = map[string][]string{
	// 1. Servidor de Licencias / Seguridad (KeyGuard en Railway)
	"kodelock-server-production.up.railway.app": {
		"HLb3HRWGSbybWXPPGKsaL3NwYvwWHrTYTCYU15lfeQ4=", // *.up.railway.app
		"s/tdAOmUzd8syaTuqfgGvFcn6DzA5Cmb+Vby1ST+U3Y=", // YE2
		"sCkq5UWXjg+7mKu9lMhhYF5bGLsy7VI/UNW3tccdR7w=", // Root YE
		"diGVwiVYbubAI3RW4hB9xU8e/CH2GnkuvVFZE8zmgzI=", // ISRG Root X2
	},

	// 2. ViX+ APIs (RABBIT VIX)
	"vix.com": {
		"XGB5cPsfxwQ41b3QzL1R1/BVvmNKb4HOh2WFmKDrqGU=", // vix.com leaf
		"biIcgxJw7HM1TbdJxioNUtXUL4DAGP3v1bLiXlQJxHw=", // GlobalSign Atlas R3 CA
	},
	"identity-api.vix.com": {
		"XGB5cPsfxwQ41b3QzL1R1/BVvmNKb4HOh2WFmKDrqGU=", // identity-api.vix.com leaf
		"biIcgxJw7HM1TbdJxioNUtXUL4DAGP3v1bLiXlQJxHw=", // GlobalSign Atlas R3 CA
	},
	"client-api.vix.com": {
		"XGB5cPsfxwQ41b3QzL1R1/BVvmNKb4HOh2WFmKDrqGU=", // client-api.vix.com leaf
		"biIcgxJw7HM1TbdJxioNUtXUL4DAGP3v1bLiXlQJxHw=", // GlobalSign Atlas R3 CA
	},

	// 3. Microsoft / Hotmail / Outlook (ComÃºn a Disney, Vix y Paramount)
	"login.live.com": {
		"1SLeNF99B9POrF/Ms/GGW2fiD9cuBFzeFCNS8QOZXWc=", // login.live.com leaf
		"Wec45nQiFwKvHtuHxSAMGkt19k+uPSw9JlEkxhvYPHk=", // DigiCert Global G2 CA
	},
	"login.microsoftonline.com": {
		"Uhp5jER6NT+KX9locs2DnhTrcarmbJ423u77v4RSfA4=", // stamp2.login.microsoftonline.com
		"Cgcdqp4tWwaW/5zVT0SzM3JVarwR9qZzaApjgFqeVic=", // Microsoft TLS G2 CA
		"SwPJlmyGOywAipWl7ZJUBwRIx7IZ0oMQL2psW26OKs0=", // Microsoft TLS RSA Root G2
	},
	"outlook.live.com": {
		"eHwepDm5W1oNUyYHK9ihJED5aNxC3T+O0nq3ZpaclXI=", // outlook.com leaf
		"Wec45nQiFwKvHtuHxSAMGkt19k+uPSw9JlEkxhvYPHk=", // DigiCert Global G2 CA
	},

	// 4. Disney+ APIs (RABBIT DISNEY)
	"disney.api.edge.bamgrid.com": {
		"urI9D63Un7M1KTD5L2eN58jaGnFRPgoSc3fMWCJCk3g=", // *.api.edge.bamgrid.com
		"G9LNNAql897egYsabashkzUCTEJkWBzgoEtk8X/678c=", // Amazon RSA 2048 M04
		"++MBgDH5WGvL9Bcn5Be30cRcL0f5O+NyoXuWtQdX1aI=", // Amazon Root CA 1
	},
	"www.disneyplus.com": {
		"c+15aDnoMyBhAZfKOecDW/eQ8SEF+2H6mSpPJp+owE4=", // *.disneyplus.com
		"EgNpQklEUNXn9Nl6RoIOC532j1g5+EFw0ZpLxxJq9Ms=", // COMODO RSA OV Secure Server CA
		"grX4Ta9HpZx6tSHkmCrvpApTQGo67CYDnvprLg5yRME=", // COMODO RSA CA
	},
	"disneyplus.com": {
		"Nl7c81Q7dNwkcgSoDyfGpmnjMDq/jYLL/Jj1YsVc8EE=", // disneyplus.com
		"OdSlmQD9NWJh4EbcOHBxkhygPwNSwA9Q91eounfbcoE=", // WR3
		"hxqRlPTu1bMS/0DITB1SSu0vd4u/8l8TjPgfaAp63Gc=", // GTS Root R1
	},

	// 5. Paramount+ APIs (RABBIT PARAMOUNT)
	"www.paramountplus.com": {
		"axVO/+yviN7l2QnpivPtS8xaXJWJjTmRFPiAf3lsk+A=", // *.paramountplus.com
		"KqkYYX5LYAYP7XGemqzbtPPIA8x7BS/BbOIcAXf3j2k=", // Sectigo Server Auth CA
		"Douxi77vs4G+Ib/BogbTFymEYq0QSFXwSgVCaZcI09Q=", // Sectigo Root R46
	},
	"paramountplus.com": {
		"g7diQu0zVrTiGJG1UwGQO2PkdT6WWDW47Pp5Gb/TGXw=", // paramountplus.com
		"brzvtCELCIZUo4sD/qPX0ccRtPsd3DY6RfmxpOU9oB4=", // YE1
		"sCkq5UWXjg+7mKu9lMhhYF5bGLsy7VI/UNW3tccdR7w=", // Root YE
		"diGVwiVYbubAI3RW4hB9xU8e/CH2GnkuvVFZE8zmgzI=", // ISRG Root X2
	},
}

// cleanPinsMap normaliza los pines asegurando que se use la cadena Base64 pura
// que espera el comparador de hpkp, removiendo prefijos como pin-sha256= y comillas si existieran.
func cleanPinsMap(rawMap map[string][]string) map[string][]string {
	if rawMap == nil {
		return nil
	}
	clean := make(map[string][]string, len(rawMap))
	for host, pins := range rawMap {
		cleanList := make([]string, 0, len(pins))
		for _, p := range pins {
			p = strings.TrimSpace(p)
			p = strings.TrimPrefix(p, "pin-sha256=")
			p = strings.Trim(p, "\"")
			if p != "" {
				cleanList = append(cleanList, p)
			}
		}
		if len(cleanList) > 0 {
			clean[host] = cleanList
		}
	}
	return clean
}

// Client abstrae completamente el motor TLS/HTTP subyacente.
type Client interface {
	Do(req *fhttp.Request) (*fhttp.Response, error)
	CloseIdleConnections()
}

// Params agrupa TODO lo que Factory.New necesita para construir un
// Client, evitando firmas de funciÃ³n con 6+ parÃ¡metros posicionales
// (que son propensas a errores de orden) y dejando explÃ­cito quÃ©
// depende de quÃ© (Explicit Configuration).
type Params struct {
	Transport       TransportConfig
	TLSProfile      profiles.ClientProfile
	Jar             *Jar
	Proxy           *Entry // nil = sin proxy
	ProxyType       Type
	FollowRedirects bool // vive en BrowserConfig; se pasa explÃ­cito aquÃ­
	Protocol        Protocol
}

// Factory construye instancias de Client. Es una interfaz (no una funciÃ³n
// suelta) para poder inyectar un Factory falso en tests unitarios de
// browser.Session sin abrir sockets reales.
type Factory interface {
	New(p Params) (Client, error)
}

// TLSClientFactory es la implementaciÃ³n real sobre bogdanfinn/tls-client.
type TLSClientFactory struct{}

// NewFactory retorna la factory de producciÃ³n. Un test puede sustituir
// esta factory por un mock que implemente la misma interfaz Factory.
func NewFactory() Factory { return TLSClientFactory{} }

func (TLSClientFactory) New(p Params) (Client, error) {
	timeoutSec := int(p.Transport.RequestTimeout.Seconds())
	if timeoutSec <= 0 {
		timeoutSec = 30
	}

	options := []tlsclient.HttpClientOption{
		tlsclient.WithTimeoutSeconds(timeoutSec),
		tlsclient.WithClientProfile(p.TLSProfile),
		tlsclient.WithTransportOptions(buildTransportOptions(p.Transport)),
		tlsclient.WithCookieJar(NewTLSClientAdapter(p.Jar)),
		tlsclient.WithDialer(net.Dialer{
			Timeout:   p.Transport.DialTimeout,
			KeepAlive: p.Transport.KeepAlive,
		}),
	}

	// Activar Certificate Pinning nativo normalizado (Base64 puro)
	if DefaultCertificatePins != nil && len(DefaultCertificatePins) > 0 {
		options = append(options, tlsclient.WithCertificatePinning(cleanPinsMap(DefaultCertificatePins), nil))
	}

	switch p.Protocol {
	case ProtocolHTTP1:
		options = append(options, tlsclient.WithForceHttp1())
	case ProtocolHTTP2:
		options = append(options, tlsclient.WithDisableHttp3())
	case ProtocolHTTP3, ProtocolHTTP3Racing:
		options = append(options, tlsclient.WithProtocolRacing())
	default:
		// Default: HTTP/1.1
		options = append(options, tlsclient.WithForceHttp1())
	}

	if p.Proxy != nil {
		proxyURL, err := p.Proxy.URL(p.ProxyType)
		if err != nil {
			return nil, fmt.Errorf("transport: proxy URL invÃ¡lida: %w", err)
		}
		options = append(options, tlsclient.WithProxyUrl(proxyURL.String()))
	}

	if !p.FollowRedirects {
		options = append(options, tlsclient.WithNotFollowRedirects())
	}

	raw, err := tlsclient.NewHttpClient(tlsclient.NewNoopLogger(), options...)
	if err != nil {
		return nil, fmt.Errorf("transport: error creando cliente TLS: %w", err)
	}

	return &tlsClientWrapper{raw: raw}, nil
}

// tlsClientWrapper adapta tlsclient.HttpClient a nuestra interfaz Client
// mÃ­nima; evita exponer los ~15 mÃ©todos de tlsclient.HttpClient que no
// usamos (Interface Segregation Principle).
type tlsClientWrapper struct {
	raw tlsclient.HttpClient
}

func (w *tlsClientWrapper) Do(req *fhttp.Request) (*fhttp.Response, error) {
	return w.raw.Do(req)
}

func (w *tlsClientWrapper) CloseIdleConnections() {
	w.raw.CloseIdleConnections()
}

func buildTransportOptions(cfg TransportConfig) *tlsclient.TransportOptions {
	idleTimeout := cfg.IdleConnTimeout
	if idleTimeout <= 0 {
		idleTimeout = 60 * time.Second
	}
	return &tlsclient.TransportOptions{
		DisableKeepAlives:      cfg.DisableKeepAlives,
		MaxIdleConns:           cfg.MaxIdleConns,
		MaxIdleConnsPerHost:    cfg.MaxIdleConnsPerHost,
		MaxConnsPerHost:        cfg.MaxConnsPerHost,
		DisableCompression:     cfg.DisableCompression,
		IdleConnTimeout:        &idleTimeout,
		WriteBufferSize:        cfg.WriteBufferSize,
		ReadBufferSize:         cfg.ReadBufferSize,
		MaxResponseHeaderBytes: cfg.MaxResponseHeaderBytes,
	}
}