package HTTPClient

import (
	"bytes"
	"compress/gzip"
	"compress/zlib"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	retry "github.com/LeanCoder0718/Universal/HTTPClient/Retry"
	"github.com/andybalholm/brotli"
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

func TestAudit_Decompression_GzipDeflateBrotli(t *testing.T) {
	originalText := "Hello, this is a compressed payload for audit verification tests!"

	// 1. Gzip
	var gzBuf bytes.Buffer
	gw := gzip.NewWriter(&gzBuf)
	gw.Write([]byte(originalText))
	gw.Close()

	respGz := &Response{
		StatusCode: 200,
		Headers: fhttp.Header{
			"Content-Encoding": []string{"gzip"},
		},
		BodyBytes: gzBuf.Bytes(),
	}
	if err := respGz.DecompressIfNeeded(); err != nil {
		t.Fatalf("falló descompresión gzip: %v", err)
	}
	if string(respGz.BodyBytes) != originalText {
		t.Errorf("gzip payload esperado %q, got %q", originalText, string(respGz.BodyBytes))
	}
	// Probar idempotencia (no descomprimir dos veces)
	if err := respGz.DecompressIfNeeded(); err != nil {
		t.Fatalf("segunda llamada a DecompressIfNeeded falló en gzip: %v", err)
	}
	if string(respGz.BodyBytes) != originalText {
		t.Errorf("segunda llamada a gzip corrompió payload: %q", string(respGz.BodyBytes))
	}

	// 2. Deflate (zlib)
	var zBuf bytes.Buffer
	zw := zlib.NewWriter(&zBuf)
	zw.Write([]byte(originalText))
	zw.Close()

	respDeflate := &Response{
		StatusCode: 200,
		Headers: fhttp.Header{
			"Content-Encoding": []string{"deflate"},
		},
		BodyBytes: zBuf.Bytes(),
	}
	if err := respDeflate.DecompressIfNeeded(); err != nil {
		t.Fatalf("falló descompresión deflate: %v", err)
	}
	if string(respDeflate.BodyBytes) != originalText {
		t.Errorf("deflate payload esperado %q, got %q", originalText, string(respDeflate.BodyBytes))
	}

	// 3. Brotli (br)
	var brBuf bytes.Buffer
	bw := brotli.NewWriter(&brBuf)
	bw.Write([]byte(originalText))
	bw.Close()

	respBr := &Response{
		StatusCode: 200,
		Headers: fhttp.Header{
			"Content-Encoding": []string{"br"},
		},
		BodyBytes: brBuf.Bytes(),
	}
	if err := respBr.DecompressIfNeeded(); err != nil {
		t.Fatalf("falló descompresión brotli: %v", err)
	}
	if string(respBr.BodyBytes) != originalText {
		t.Errorf("brotli payload esperado %q, got %q", originalText, string(respBr.BodyBytes))
	}
}

func TestAudit_ProxyURL_ParsingAndRedaction(t *testing.T) {
	// Proxy con caracteres especiales codificados
	rawURL := "http://user%40corp:p%40ss%23word@127.0.0.1:8080"
	entry, err := ParseProxyURL(rawURL)
	if err != nil {
		t.Fatalf("ParseProxyURL falló: %v", err)
	}

	if entry.User != "user@corp" {
		t.Errorf("esperado user 'user@corp', got %q", entry.User)
	}
	if entry.Pass != "p@ss#word" {
		t.Errorf("esperado pass 'p@ss#word', got %q", entry.Pass)
	}

	// Verificar redacción de credenciales en logs
	redacted := entry.RedactedString(TypeHTTP)
	if strings.Contains(redacted, "p@ss#word") {
		t.Errorf("RedactedString filtró la contraseña en texto claro: %s", redacted)
	}
	if !strings.Contains(redacted, "***") {
		t.Errorf("RedactedString no contiene máscara '***': %s", redacted)
	}

	// Probar WithEnvProxy
	os.Setenv("TEST_HTTP_PROXY", rawURL)
	defer os.Unsetenv("TEST_HTTP_PROXY")

	sess, err := NewSession(Default(), WithEnvProxy("TEST_HTTP_PROXY"))
	if err != nil {
		t.Fatalf("NewSession con WithEnvProxy falló: %v", err)
	}
	defer sess.Close()

	if sess.RedactedProxyURL() != redacted {
		t.Errorf("sess.RedactedProxyURL() esperado %q, got %q", redacted, sess.RedactedProxyURL())
	}
}

func TestAudit_Context_HeaderCoherence(t *testing.T) {
	engine := NewEngine()
	chromeSet := ChromeLayoutSet

	// Contexto Documento (navegación top-level)
	ctxDoc := Context{
		Method:    "GET",
		Authority: "example.com",
		Scheme:    "https",
		Path:      "/",
	}
	hDoc := engine.Build(chromeSet.Get(KindDocument), ctxDoc)
	if len(hDoc["sec-fetch-user"]) == 0 || hDoc["sec-fetch-user"][0] != "?1" {
		t.Errorf("KindDocument debe enviar sec-fetch-user: ?1")
	}
	if len(hDoc["sec-fetch-dest"]) == 0 || hDoc["sec-fetch-dest"][0] != "document" {
		t.Errorf("KindDocument debe enviar sec-fetch-dest: document")
	}

	// Contexto XHR (fetch/API)
	ctxXHR := Context{
		Method:    "POST",
		Authority: "example.com",
		Scheme:    "https",
		Path:      "/api/v1/auth",
	}
	hXHR := engine.Build(chromeSet.Get(KindXHR), ctxXHR)
	if len(hXHR["sec-fetch-user"]) != 0 {
		t.Errorf("KindXHR NUNCA debe enviar sec-fetch-user")
	}
	if len(hXHR["sec-fetch-dest"]) == 0 || hXHR["sec-fetch-dest"][0] != "empty" {
		t.Errorf("KindXHR debe enviar sec-fetch-dest: empty")
	}
	if len(hXHR["sec-fetch-mode"]) == 0 || hXHR["sec-fetch-mode"][0] != "cors" {
		t.Errorf("KindXHR debe enviar sec-fetch-mode: cors")
	}
}

func TestAudit_Classifier_403NoInfiniteLoop(t *testing.T) {
	classifier := retry.NewDefaultClassifier()
	engine := retry.New(retry.RetryConfig{
		MaxAttempts: 3,
		WaitMin:     10 * time.Millisecond,
		WaitMax:     50 * time.Millisecond,
	}, classifier)

	// 1. 403 sin keywords de WAF (lógica de negocio / permisos normales)
	// DEBE terminar inmediatamente sin retry
	decBusiness := engine.Decide(1, retry.Signal{
		StatusCode: 403,
		Method:     "GET",
		BodySample: []byte(`{"error":"insufficient_role_permissions"}`),
	})
	if decBusiness.Retry {
		t.Errorf("403 de lógica de negocio no debe reintentar")
	}
	if decBusiness.Class != retry.ClassBusinessLogic {
		t.Errorf("esperado ClassBusinessLogic, got %v", decBusiness.Class)
	}

	// 2. 403 con bloqueo de WAF/Cloudflare
	// Reintenta con rotación pero se detiene al alcanzar MaxAttempts (sin bucle infinito)
	decWAF1 := engine.Decide(1, retry.Signal{
		StatusCode: 403,
		Method:     "GET",
		BodySample: []byte(`<title>Attention Required! | Cloudflare</title>`),
	})
	if !decWAF1.Retry {
		t.Errorf("intento 1 con bloqueo WAF debe reintentar con rotación")
	}
	if decWAF1.Action != retry.ActionRotateProxy {
		t.Errorf("esperado ActionRotateProxy en bloqueo WAF")
	}

	// Al alcanzar intento 3 (MaxAttempts = 3), se agota y se detiene
	decWAFMax := engine.Decide(3, retry.Signal{
		StatusCode: 403,
		Method:     "GET",
		BodySample: []byte(`<title>Attention Required! | Cloudflare</title>`),
	})
	if decWAFMax.Retry {
		t.Errorf("al alcanzar MaxAttempts NUNCA debe continuar reintentando (bucle infinito prevenido)")
	}
}

func TestAudit_SanitizeURL_QueryParamRedaction(t *testing.T) {
	rawURL := "https://api.example.com/v1/login?user=admin&password=SuperSecret123&token=abc-xyz&redirect=https://home"
	sanitized := sanitizeURL(rawURL)

	if strings.Contains(sanitized, "SuperSecret123") {
		t.Errorf("sanitizeURL no ocultó el parámetro password: %s", sanitized)
	}
	if strings.Contains(sanitized, "abc-xyz") {
		t.Errorf("sanitizeURL no ocultó el parámetro token: %s", sanitized)
	}
	if !strings.Contains(sanitized, "password=%5BREDACTED%5D") && !strings.Contains(sanitized, "password=[REDACTED]") {
		t.Errorf("parámetro password no fue reemplazado por [REDACTED]: %s", sanitized)
	}
}

func TestAudit_CookieJar_ConnectedToTLSClientAdapter(t *testing.T) {
	jar := NewJAR(CookieConfig{})
	adapter := NewTLSClientAdapter(jar)

	// Simular SetCookies desde redirección interna
	adapter.SetCookies(nil, nil) // no-op safe
	reqURL, _ := http.NewRequest("GET", "https://auth.example.com/login", nil)
	adapter.SetCookies(reqURL.URL, []*fhttp.Cookie{
		{Name: "session_token", Value: "token_12345"},
	})

	cookies := adapter.Cookies(reqURL.URL)
	if len(cookies) != 1 || cookies[0].Value != "token_12345" {
		t.Fatalf("TLSClientAdapter no persistió la cookie en el Jar: %+v", cookies)
	}
}

func TestAudit_SessionIsolation_SeparateJars(t *testing.T) {
	cfg := Default()
	sess1, err := NewSession(cfg)
	if err != nil {
		t.Fatalf("error sess1: %v", err)
	}
	defer sess1.Close()

	sess2, err := NewSession(cfg)
	if err != nil {
		t.Fatalf("error sess2: %v", err)
	}
	defer sess2.Close()

	// Inyectar cookie en sess1
	sess1.Cookies().AddManual("example.com", "/", Cookie{Name: "user_a", Value: "111"})

	// Verificar que sess2 NO tiene la cookie de sess1
	s2Cookies := sess2.Cookies().CookiesFor("example.com", "/", true)
	if len(s2Cookies) != 0 {
		t.Errorf("aislamiento violado: sess2 heredó cookies de sess1: %+v", s2Cookies)
	}

	// Verificar StickyID distintos
	if sess1.StickyID() == sess2.StickyID() {
		t.Errorf("sess1 y sess2 no deben tener el mismo StickyID")
	}
}
