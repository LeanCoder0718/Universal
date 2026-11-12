package HTTPClient

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestRedirectURLTracking verifica que GetURLRedirect, GetUrlRedict y GetURLRedict
// retornen la URL final después de seguir una o varias redirecciones 302/301.
func TestRedirectURLTracking(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/login":
			http.Redirect(w, r, "/pe/login", http.StatusFound)
		case "/pe/login":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`welcome to pe login`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	// Creamos un fake client que propague raw.Request tal como lo hace bogdanfinn/tls-client
	factory := &testLoopbackFactory{
		transport: &redirectAwareTransport{serverURL: ts.URL},
	}

	cfg := Default()
	sess, err := NewSession(cfg, WithTransportFactory(factory))
	if err != nil {
		t.Fatalf("error creando sesión: %v", err)
	}
	defer sess.Close()

	res, err := sess.Do(context.Background(), Spec{
		Method: "GET",
		URL:    ts.URL + "/login",
	})
	if err != nil {
		t.Fatalf("petición falló: %v", err)
	}
	defer res.Release()

	expectedFinalURL := ts.URL + "/pe/login"

	if res.GetURLRedirect() != expectedFinalURL {
		t.Errorf("GetURLRedirect() = %q, se esperaba %q", res.GetURLRedirect(), expectedFinalURL)
	}
	if res.GetUrlRedict() != expectedFinalURL {
		t.Errorf("GetUrlRedict() = %q, se esperaba %q", res.GetUrlRedict(), expectedFinalURL)
	}
	if res.GetURLRedict() != expectedFinalURL {
		t.Errorf("GetURLRedict() = %q, se esperaba %q", res.GetURLRedict(), expectedFinalURL)
	}
}

// TestProxyURLStringWithCredentials verifica que proxyURLString formatee host:port:user:pass completo.
func TestProxyURLStringWithCredentials(t *testing.T) {
	entry := &Entry{
		Host: "gw.dataimpulse.com",
		Port: "823",
		User: "usuario123",
		Pass: "claveSegura456",
	}

	result := proxyURLString(entry)
	expected := "gw.dataimpulse.com:823:usuario123:claveSegura456"

	if result != expected {
		t.Errorf("proxyURLString() = %q, se esperaba %q", result, expected)
	}
}

type redirectAwareTransport struct {
	serverURL string
}

func (t *redirectAwareTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// Cliente estándar de Go que sigue redirecciones
	client := &http.Client{}
	return client.Do(req)
}
