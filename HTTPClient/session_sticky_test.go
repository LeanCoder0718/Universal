package HTTPClient

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	fhttp "github.com/bogdanfinn/fhttp"
	"github.com/fatih/color"
)

// fakeRoundTripperFactory permite inyectar un transporte HTTP estándar hacia httptest.Server
// sin intermediación de TLS compleja para pruebas unitarias deterministas del contrato HTTPClient.
type testLoopbackClient struct {
	serverURL string
	transport http.RoundTripper
}

func (c *testLoopbackClient) Do(req *fhttp.Request) (*fhttp.Response, error) {
	// Mapear fhttp.Request a http.Request para el httptest.Server
	var bodyReader io.Reader
	if req.Body != nil {
		bodyBytes, err := io.ReadAll(req.Body)
		if err == nil {
			bodyReader = io.NopCloser(io.MultiReader(io.LimitReader(req.Body, 0), io.NopCloser(io.LimitReader(req.Body, 0))))
			_ = bodyBytes
		}
	}
	_ = bodyReader

	stdReq, err := http.NewRequestWithContext(req.Context(), req.Method, req.URL.String(), req.Body)
	if err != nil {
		return nil, err
	}
	for k, vv := range req.Header {
		if strings.HasPrefix(k, ":") || strings.EqualFold(k, HeaderOrderKey) || strings.EqualFold(k, "header-order:") {
			continue
		}
		for _, v := range vv {
			stdReq.Header.Add(k, v)
		}
	}

	rt := c.transport
	if rt == nil {
		rt = http.DefaultTransport
	}
	stdResp, err := rt.RoundTrip(stdReq)
	if err != nil {
		return nil, err
	}

	fResp := &fhttp.Response{
		StatusCode:    stdResp.StatusCode,
		Status:        stdResp.Status,
		Proto:         stdResp.Proto,
		ProtoMajor:    stdResp.ProtoMajor,
		ProtoMinor:    stdResp.ProtoMinor,
		Body:          stdResp.Body,
		ContentLength: stdResp.ContentLength,
		Header:        make(fhttp.Header),
	}
	for k, vv := range stdResp.Header {
		fResp.Header[k] = vv
	}
	return fResp, nil
}

func (c *testLoopbackClient) CloseIdleConnections() {}

type testLoopbackFactory struct {
	transport http.RoundTripper
}

func (f *testLoopbackFactory) New(p Params) (Client, error) {
	return &testLoopbackClient{transport: f.transport}, nil
}

// 1. Test: Dos peticiones de la misma sesión conservan el mismo identificador.
func TestSession_StickyID_StabilityAcrossRequests(t *testing.T) {
	var receivedIDs []string
	var mu sync.Mutex

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		receivedIDs = append(receivedIDs, r.Header.Get("x-session-id"))
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer ts.Close()

	cfg := Default()
	sess, err := NewSession(cfg, WithTransportFactory(&testLoopbackFactory{transport: ts.Client().Transport}))
	if err != nil {
		t.Fatalf("error creando session: %v", err)
	}
	defer sess.Close()

	ctx := context.Background()
	initialSticky := sess.StickyID()
	if initialSticky == "" {
		t.Fatalf("stickyID no debe ser vacío")
	}

	// Petición 1
	res1, err := sess.Do(ctx, Spec{Method: "GET", URL: ts.URL + "/req1", Kind: KindDocument})
	if err != nil {
		t.Fatalf("req1 falló: %v", err)
	}
	res1.Release()

	// Petición 2
	res2, err := sess.Do(ctx, Spec{Method: "POST", URL: ts.URL + "/req2", Kind: KindXHR, Body: []byte(`{"a":1}`)})
	if err != nil {
		t.Fatalf("req2 falló: %v", err)
	}
	res2.Release()

	mu.Lock()
	defer mu.Unlock()
	if len(receivedIDs) != 2 {
		t.Fatalf("esperadas 2 peticiones recibidas, obtenidas %d", len(receivedIDs))
	}
	if receivedIDs[0] != initialSticky {
		t.Errorf("req1 header x-session-id %q no coincide con session stickyID %q", receivedIDs[0], initialSticky)
	}
	if receivedIDs[1] != initialSticky {
		t.Errorf("req2 header x-session-id %q no coincide con session stickyID %q", receivedIDs[1], initialSticky)
	}
	if receivedIDs[0] != receivedIDs[1] {
		t.Errorf("peticiones consecutivas de la misma sesión tuvieron identificadores distintos: %q != %q", receivedIDs[0], receivedIDs[1])
	}
}

// 2. Test: Dos sesiones distintas no comparten el identificador ni cookies.
func TestSession_StickyID_IsolationBetweenSessions(t *testing.T) {
	var receivedIDs []string
	var mu sync.Mutex

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		receivedIDs = append(receivedIDs, r.Header.Get("x-session-id"))
		mu.Unlock()
		http.SetCookie(w, &http.Cookie{Name: "session_user", Value: r.Header.Get("x-session-id")})
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	factory := &testLoopbackFactory{transport: ts.Client().Transport}
	cfg := Default()

	sessA, err := NewSession(cfg, WithTransportFactory(factory))
	if err != nil {
		t.Fatalf("error creando sessA: %v", err)
	}
	defer sessA.Close()

	sessB, err := NewSession(cfg, WithTransportFactory(factory))
	if err != nil {
		t.Fatalf("error creando sessB: %v", err)
	}
	defer sessB.Close()

	if sessA.StickyID() == sessB.StickyID() {
		t.Fatalf("dos sesiones independientes generaron el mismo stickyID: %s", sessA.StickyID())
	}

	ctx := context.Background()
	_, _ = sessA.Do(ctx, Spec{Method: "GET", URL: ts.URL + "/a", Kind: KindDocument})
	_, _ = sessB.Do(ctx, Spec{Method: "GET", URL: ts.URL + "/b", Kind: KindDocument})

	mu.Lock()
	defer mu.Unlock()
	if len(receivedIDs) != 2 {
		t.Fatalf("esperadas 2 peticiones, recibidas %d", len(receivedIDs))
	}
	if receivedIDs[0] == receivedIDs[1] {
		t.Errorf("sesiones distintas enviaron el mismo x-session-id: %s", receivedIDs[0])
	}
}

// 3. Test: Los headers del llamador no pueden sobrescribir accidentalmente x-session-id.
func TestSession_StickyID_CallerCannotOverwrite(t *testing.T) {
	var receivedID string
	var mu sync.Mutex

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		receivedID = r.Header.Get("x-session-id")
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	sess, err := NewSession(Default(), WithTransportFactory(&testLoopbackFactory{transport: ts.Client().Transport}))
	if err != nil {
		t.Fatalf("error creando sesión: %v", err)
	}
	defer sess.Close()

	ctx := context.Background()
	// El llamador intenta inyectar un x-session-id falso en spec.Extra
	res, err := sess.Do(ctx, Spec{
		Method: "GET",
		URL:    ts.URL + "/test",
		Kind:   KindDocument,
		Extra: map[string]string{
			"x-session-id": "fake-caller-id-12345",
			"X-Session-ID": "fake-caller-id-67890",
		},
	})
	if err != nil {
		t.Fatalf("petición falló: %v", err)
	}
	res.Release()

	mu.Lock()
	defer mu.Unlock()
	if receivedID == "fake-caller-id-12345" || receivedID == "fake-caller-id-67890" {
		t.Fatalf("VULNERABILIDAD: El llamador pudo sobrescribir x-session-id con %q", receivedID)
	}
	if receivedID != sess.StickyID() {
		t.Errorf("servidor recibió %q, se esperaba el stickyID canónico %q", receivedID, sess.StickyID())
	}
}

// 4. Test: ResetComboState renueva la identidad de sesión pero mantiene la reutilización de sockets/recursos.
func TestSession_StickyID_ResetComboIsolation(t *testing.T) {
	sess, err := NewSession(Default(), WithTransportFactory(&testLoopbackFactory{}))
	if err != nil {
		t.Fatalf("error creando sesión: %v", err)
	}
	defer sess.Close()

	id1 := sess.StickyID()
	if id1 == "" {
		t.Fatal("id1 no debe ser vacío")
	}

	err = sess.ResetComboState(false)
	if err != nil {
		t.Fatalf("ResetComboState falló: %v", err)
	}

	id2 := sess.StickyID()
	if id2 == "" {
		t.Fatal("id2 no debe ser vacío tras ResetComboState")
	}
	if id1 == id2 {
		t.Fatalf("ResetComboState debió generar un nuevo stickyID para aislar el siguiente combo, pero repitió %s", id1)
	}
}

// 5. Test: Una sesión sin identificador previo tiene inicialización segura (Lazy Init).
func TestSession_StickyID_LazyInitSafe(t *testing.T) {
	sess := &Session{
		cfg:         Default(),
		clientCache: make(map[string]Client),
	}

	id := sess.StickyID()
	if id == "" {
		t.Fatal("StickyID() retornó vacío")
	}

	// Segundo llamado retorna el mismo ID
	if sess.StickyID() != id {
		t.Errorf("segundo llamado a StickyID retornó ID diferente")
	}
}

// 6. Test: Reintentos de red conservan la identidad de la sesión lógica.
func TestSession_StickyID_RetriesPreserveID(t *testing.T) {
	var attempts int
	var receivedIDs []string
	var mu sync.Mutex

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		attempts++
		receivedIDs = append(receivedIDs, r.Header.Get("x-session-id"))
		curAttempt := attempts
		mu.Unlock()

		if curAttempt == 1 {
			// Simular fallo transitorio 503
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`service temporary error`))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`ok`))
	}))
	defer ts.Close()

	cfg := Default()
	cfg.Retry.MaxAttempts = 3
	cfg.Retry.WaitMin = 10 * time.Millisecond
	cfg.Retry.WaitMax = 50 * time.Millisecond

	sess, err := NewSession(cfg, WithTransportFactory(&testLoopbackFactory{transport: ts.Client().Transport}))
	if err != nil {
		t.Fatalf("error creando sesión: %v", err)
	}
	defer sess.Close()

	ctx := context.Background()
	res, err := sess.Do(ctx, Spec{Method: "GET", URL: ts.URL + "/retry", Kind: KindDocument, Idempotent: true})
	if err != nil {
		t.Fatalf("Do falló en reintento: %v", err)
	}
	res.Release()

	mu.Lock()
	defer mu.Unlock()
	if len(receivedIDs) < 2 {
		t.Fatalf("se esperaban al menos 2 intentos, hubo %d", len(receivedIDs))
	}
	for i, id := range receivedIDs {
		if id != sess.StickyID() {
			t.Errorf("intento %d envió x-session-id %q distinto de %q", i+1, id, sess.StickyID())
		}
	}
}

// 7. Test: Política de alcance de destino seguro (Safe Targets).
func TestSession_StickyID_TargetRestriction(t *testing.T) {
	var allowedReceivedID, externalReceivedID string
	var mu sync.Mutex

	tsAllowed := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		allowedReceivedID = r.Header.Get("x-session-id")
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer tsAllowed.Close()

	tsExternal := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		externalReceivedID = r.Header.Get("x-session-id")
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer tsExternal.Close()

	uAllowed, err := url.Parse(tsAllowed.URL)
	if err != nil {
		t.Fatalf("error parseando tsAllowed URL: %v", err)
	}

	cfg := Default()
	sess, err := NewSession(cfg,
		WithTransportFactory(&testLoopbackFactory{transport: tsAllowed.Client().Transport}),
		WithAllowedSessionHosts(uAllowed.Host),
	)
	if err != nil {
		t.Fatalf("error creando sesión: %v", err)
	}
	defer sess.Close()

	ctx := context.Background()
	// Petición a host autorizado: debe recibir x-session-id
	resAllowed, err := sess.Do(ctx, Spec{Method: "GET", URL: tsAllowed.URL + "/auth", Kind: KindDocument})
	if err != nil {
		t.Fatalf("error en petición a host autorizado: %v", err)
	}
	if resAllowed != nil {
		resAllowed.Release()
	}

	// Petición a host no autorizado: no debe recibir x-session-id
	resExt, _ := sess.Do(ctx, Spec{Method: "GET", URL: tsExternal.URL + "/ext", Kind: KindDocument})
	if resExt != nil {
		resExt.Release()
	}

	mu.Lock()
	defer mu.Unlock()
	if allowedReceivedID != sess.StickyID() {
		t.Errorf("host autorizado no recibió stickyID esperado: got %q, want %q", allowedReceivedID, sess.StickyID())
	}
	if externalReceivedID != "" {
		t.Errorf("host no autorizado recibió x-session-id: %q (filtración no deseada)", externalReceivedID)
	}
}

// 8. Test: Acceso concurrente thread-safe sin carreras de datos.
func TestSession_StickyID_ConcurrentSafe(t *testing.T) {
	var requestCount atomic.Uint64
	var mismatchCount atomic.Uint64

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	sess, err := NewSession(Default(), WithTransportFactory(&testLoopbackFactory{transport: ts.Client().Transport}))
	if err != nil {
		t.Fatalf("error creando sesión: %v", err)
	}
	defer sess.Close()

	targetID := sess.StickyID()
	var wg sync.WaitGroup
	workers := 20
	reqsPerWorker := 10

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < reqsPerWorker; j++ {
				cur := sess.StickyID()
				if cur != targetID {
					mismatchCount.Add(1)
				}
				res, dErr := sess.Do(context.Background(), Spec{Method: "GET", URL: ts.URL + "/concur", Kind: KindDocument})
				if dErr == nil && res != nil {
					res.Release()
				}
			}
		}()
	}

	wg.Wait()

	if mismatchCount.Load() > 0 {
		t.Errorf("se detectaron %d discrepancias de stickyID bajo concurrencia", mismatchCount.Load())
	}
	if requestCount.Load() != uint64(workers*reqsPerWorker) {
		t.Errorf("esperadas %d peticiones, ejecutadas %d", workers*reqsPerWorker, requestCount.Load())
	}
}

// 9. Test: Anonimización de x-session-id en registros de depuración (Debug Log).
func TestSession_StickyID_DebugLogMasking(t *testing.T) {
	oldStdout := os.Stdout
	oldColorOutput := color.Output
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("error creando pipe: %v", err)
	}
	os.Stdout = w
	color.Output = w

	ts := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		rw.WriteHeader(http.StatusOK)
		_, _ = rw.Write([]byte(`ok`))
	}))
	defer ts.Close()

	cfg := Default()
	cfg.Debug = true

	sess, err := NewSession(cfg, WithTransportFactory(&testLoopbackFactory{transport: ts.Client().Transport}))
	if err != nil {
		color.Output = oldColorOutput
		os.Stdout = oldStdout
		t.Fatalf("error creando sesión: %v", err)
	}
	defer sess.Close()

	sid := sess.StickyID()
	res, err := sess.Do(context.Background(), Spec{Method: "GET", URL: ts.URL + "/logtest", Kind: KindDocument})
	if err != nil {
		color.Output = oldColorOutput
		os.Stdout = oldStdout
		t.Fatalf("error en Do: %v", err)
	}
	res.Release()

	_ = w.Close()
	os.Stdout = oldStdout
	color.Output = oldColorOutput

	var buf bytes.Buffer
	_, _ = io.Copy(&buf, r)
	output := buf.String()

	// El ID plano completo NO debe aparecer en el log
	if strings.Contains(output, sid) {
		t.Errorf("FUGA DE INFORMACIÓN: el x-session-id completo %q apareció en el log de depuración sin anonimizar", sid)
	}
	// Debe contener la forma anonimizada con asteriscos
	masked := sid[:4] + "***" + sid[len(sid)-4:]
	if !strings.Contains(output, masked) {
		t.Errorf("el log de depuración no contiene el valor anonimizado esperado %q", masked)
	}
}
