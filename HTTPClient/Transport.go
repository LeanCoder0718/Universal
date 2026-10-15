// Package transport es la ÚNICA capa del proyecto que importa
// github.com/bogdanfinn/tls-client directamente. Todo el resto del
// sistema (browser, headers, cookies, retry, workers) depende solo de
// la interfaz Client definida aquí (Dependency Inversion Principle).
//
// Esto resuelve el hallazgo de auditoría #11: en el diseño original,
// TLSClient y WorkerClient (Client.go) importaban tlsclient en el mismo
// archivo donde vivía la lógica de retry, de pooling y de cookies,
// haciendo imposible reemplazar el motor TLS sin reescribir todo el core.
package HTTPClient

import (
	"fmt"
	"net"
	"time"

	"github.com/bogdanfinn/tls-client/profiles"

	fhttp "github.com/bogdanfinn/fhttp"
	tlsclient "github.com/bogdanfinn/tls-client"
)

// Client abstrae completamente el motor TLS/HTTP subyacente.
type Client interface {
	Do(req *fhttp.Request) (*fhttp.Response, error)
	CloseIdleConnections()
}

// Params agrupa TODO lo que Factory.New necesita para construir un
// Client, evitando firmas de función con 6+ parámetros posicionales
// (que son propensas a errores de orden) y dejando explícito qué
// depende de qué (Explicit Configuration).
type Params struct {
	Transport       TransportConfig
	TLSProfile      profiles.ClientProfile
	Jar             *Jar
	Proxy           *Entry // nil = sin proxy
	ProxyType       Type
	FollowRedirects bool // vive en BrowserConfig; se pasa explícito aquí
	Protocol        Protocol
}

// Factory construye instancias de Client. Es una interfaz (no una función
// suelta) para poder inyectar un Factory falso en tests unitarios de
// browser.Session sin abrir sockets reales.
type Factory interface {
	New(p Params) (Client, error)
}

// TLSClientFactory es la implementación real sobre bogdanfinn/tls-client.
type TLSClientFactory struct{}

// NewFactory retorna la factory de producción. Un test puede sustituir
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
		tlsclient.WithInsecureSkipVerify(),
		tlsclient.WithDialer(net.Dialer{
			Timeout:   p.Transport.DialTimeout,
			KeepAlive: p.Transport.KeepAlive,
		}),
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
			return nil, fmt.Errorf("transport: proxy URL inválida: %w", err)
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
// mínima; evita exponer los ~15 métodos de tlsclient.HttpClient que no
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
