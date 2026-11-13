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

// Params agrupa todo lo que Factory.New necesita para construir un Client.
type Params struct {
	Transport       TransportConfig
	TLSProfile      profiles.ClientProfile
	Jar             *Jar
	Proxy           *Entry // nil = sin proxy
	ProxyType       Type
	FollowRedirects bool
	Protocol        Protocol
}

// Factory construye instancias de Client.
type Factory interface {
	New(p Params) (Client, error)
}

// TLSClientFactory es la implementación real sobre bogdanfinn/tls-client.
type TLSClientFactory struct{}

// NewFactory retorna la factory de producción.
func NewFactory() Factory { return TLSClientFactory{} }

func (TLSClientFactory) New(p Params) (Client, error) {
	timeoutSec := int(p.Transport.RequestTimeout.Seconds())
	if timeoutSec <= 0 {
		timeoutSec = 30
	}

	options := []tlsclient.HttpClientOption{
		tlsclient.WithTimeoutSeconds(timeoutSec),
		tlsclient.WithClientProfile(p.TLSProfile),
		tlsclient.WithRandomTLSExtensionOrder(),
		tlsclient.WithTransportOptions(buildTransportOptions(p.Transport)),
		tlsclient.WithDialer(net.Dialer{
			Timeout:   p.Transport.DialTimeout,
			KeepAlive: p.Transport.KeepAlive,
		}),
	}

	if p.Transport.InsecureSkipVerify {
		options = append(options, tlsclient.WithInsecureSkipVerify())
	}

	// Jar is managed directly by Session/Builder to avoid duplicate headers in tls-client
	// if p.Jar != nil {
	// 	options = append(options, tlsclient.WithCookieJar(NewTLSClientAdapter(p.Jar)))
	// }

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

// tlsClientWrapper adapta tlsclient.HttpClient a nuestra interfaz Client mínima.
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