package HTTPClient

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	retry "github.com/LeanCoder0718/Universal/HTTPClient/Retry"
	fhttp "github.com/bogdanfinn/fhttp"
	tlsprofiles "github.com/bogdanfinn/tls-client/profiles"
)

func TestAudit_ConnectionManager_KeyIsolation(t *testing.T) {
	cm := NewConnectionManager(Default(), nil, 10, 60*time.Second)
	defer cm.Close()

	key1 := ConnectionKey{
		SessionID:   "session-1",
		CountryCode: "US",
		RouteID:     "default",
		Protocol:    ProtocolHTTP2,
	}

	key2 := ConnectionKey{
		SessionID:   "session-2",
		CountryCode: "US",
		RouteID:     "default",
		Protocol:    ProtocolHTTP2,
	}

	if key1.Normalized() == key2.Normalized() {
		t.Fatalf("ConnectionKeys de sesiones distintas no deben colisionar")
	}

	if key1.String() == key2.String() {
		t.Fatalf("ConnectionKey.String() debe diferenciar sesiones")
	}
}

func TestAudit_Profile_SupportsClientHints(t *testing.T) {
	// Safari
	idSafari := Identity{
		Browser:        BrowserChrome,
		BrowserName:    "Safari",
		BrowserVersion: "17",
		Profile:        tlsprofiles.Safari_16_0,
	}
	pSafari := FromIdentity(idSafari)
	if pSafari.Capabilities.SupportsClientHints {
		t.Errorf("Safari no debe tener SupportsClientHints=true")
	}

	// OkHttp
	idOkHttp := Identity{
		Browser:        BrowserChrome,
		BrowserName:    "OkHTTP",
		BrowserVersion: "4.9",
		Profile:        tlsprofiles.Okhttp4Android9,
	}
	pOkHttp := FromIdentity(idOkHttp)
	if pOkHttp.Capabilities.SupportsClientHints {
		t.Errorf("OkHTTP no debe tener SupportsClientHints=true")
	}

	// Chrome real
	idChrome := Identity{
		Browser:        BrowserChrome,
		BrowserName:    "chrome",
		BrowserVersion: "133",
		Profile:        tlsprofiles.Chrome_133,
	}
	pChrome := FromIdentity(idChrome)
	if !pChrome.Capabilities.SupportsClientHints {
		t.Errorf("Chrome real debe tener SupportsClientHints=true")
	}
}

func TestAudit_Firefox_PseudoHeaderOrder(t *testing.T) {
	docLayout := FirefoxLayoutSet.Get(KindDocument)
	if len(docLayout.Slots) < 4 {
		t.Fatalf("FirefoxLayoutSet debe contener al menos los pseudo-headers")
	}

	expected := []string{":method", ":path", ":authority", ":scheme"}
	for i := 0; i < 4; i++ {
		if docLayout.Slots[i].Name != expected[i] {
			t.Errorf("slot[%d] esperado %s, got %s", i, expected[i], docLayout.Slots[i].Name)
		}
	}
}

func TestAudit_Jar_ExpirationDoesNotDeleteSlashUnconditionally(t *testing.T) {
	jar := NewJAR(CookieConfig{})

	// Cookie en "/"
	jar.AddManual("example.com", "/", Cookie{
		Name:   "token",
		Value:  "slash_token",
		Domain: "example.com",
		Path:   "/",
	})

	// Cookie en "/api"
	jar.AddManual("example.com", "/api", Cookie{
		Name:   "token",
		Value:  "api_token",
		Domain: "example.com",
		Path:   "/api",
	})

	// Servidor envía expiración explícita para Path=/api
	deleteHeader := make(fhttp.Header)
	deleteHeader["Set-Cookie"] = []string{
		"token=DELETE; Max-Age=0; Domain=example.com; Path=/api",
	}
	jar.SetCookies("example.com", "/api/v1", ParseSetCookies(deleteHeader))

	// La cookie en "/" debe PERMANECER intacta
	slashCookies := jar.CookiesFor("example.com", "/home", true)
	if len(slashCookies) != 1 || slashCookies[0].Value != "slash_token" {
		t.Fatalf("la cookie en '/' fue borrada incorrectamente por una expiración con Path=/api: %+v", slashCookies)
	}
}

func TestAudit_Dynamic_SecFetchSite(t *testing.T) {
	engine := NewEngine()
	layout := ChromeLayoutSet.Get(KindXHR)

	// 1. Sin referer: debe ser none
	ctxNone := Context{
		Authority: "api.example.com",
		Extra:     map[string]string{},
	}
	hNone := engine.Build(layout, ctxNone)
	if vals := hNone["sec-fetch-site"]; len(vals) == 0 || vals[0] != "none" {
		t.Errorf("sin referer esperado 'none', got %v", vals)
	}

	// 2. Referer mismo origen: same-origin
	ctxSameOrigin := Context{
		Authority: "api.example.com",
		Extra: map[string]string{
			"referer": "https://api.example.com/dashboard",
		},
	}
	hSameOrigin := engine.Build(layout, ctxSameOrigin)
	if vals := hSameOrigin["sec-fetch-site"]; len(vals) == 0 || vals[0] != "same-origin" {
		t.Errorf("mismo host esperado 'same-origin', got %v", vals)
	}

	// 3. Referer mismo sitio (subdominio): same-site
	ctxSameSite := Context{
		Authority: "api.example.com",
		Extra: map[string]string{
			"referer": "https://auth.example.com/login",
		},
	}
	hSameSite := engine.Build(layout, ctxSameSite)
	if vals := hSameSite["sec-fetch-site"]; len(vals) == 0 || vals[0] != "same-site" {
		t.Errorf("subdominio esperado 'same-site', got %v", vals)
	}

	// 4. Referer externo: cross-site
	ctxCrossSite := Context{
		Authority: "api.example.com",
		Extra: map[string]string{
			"referer": "https://otherdomain.org/",
		},
	}
	hCrossSite := engine.Build(layout, ctxCrossSite)
	if vals := hCrossSite["sec-fetch-site"]; len(vals) == 0 || vals[0] != "cross-site" {
		t.Errorf("sitio externo esperado 'cross-site', got %v", vals)
	}
}

func TestAudit_ActionRotateProxy_ClearsCookiesOnBlock(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	sess, err := NewSession(Default(), WithTransportFactory(&testLoopbackFactory{transport: ts.Client().Transport}))
	if err != nil {
		t.Fatalf("error creando sesión: %v", err)
	}
	defer sess.Close()

	// Inyectar cookie en sesión
	sess.jar.AddManual("example.com", "/", Cookie{Name: "session_user", Value: "val1"})

	// Aplicar ActionRotateProxy por bloqueo (ClassBlocked)
	gen := sess.generation.Load()
	if err := sess.applyAction(gen, retry.ActionRotateProxy, retry.ClassBlocked); err != nil {
		t.Fatalf("applyAction falló: %v", err)
	}

	// Cookies deben haber sido purgadas para evitar contaminar la nueva IP
	cks := sess.Cookies().CookiesFor("example.com", "/", true)
	if len(cks) != 0 {
		t.Errorf("cookies no fueron purgadas tras rotación por bloqueo: %+v", cks)
	}
}

func TestAudit_Config_DefaultHTTP2(t *testing.T) {
	cfg := Default()
	if !cfg.HTTP.EnableHTTP2 {
		t.Errorf("Default().HTTP.EnableHTTP2 debe ser true por defecto para perfiles modernos")
	}
}
