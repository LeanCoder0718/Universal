package HTTPClient

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/LeanCoder0718/Universal/HTTPClient/Profiles"
	retry "github.com/LeanCoder0718/Universal/HTTPClient/Retry"
	fhttp "github.com/bogdanfinn/fhttp"
	tlsprofiles "github.com/bogdanfinn/tls-client/profiles"
)

// trackingMockClient registra eventos para comprobar el ciclo de vida de conexiones.
type trackingMockClient struct {
	closedCount int
	profile     tlsprofiles.ClientProfile
	jar         *Jar
	handler     http.HandlerFunc
}

func (c *trackingMockClient) Do(req *fhttp.Request) (*fhttp.Response, error) {
	if c.handler != nil {
		rec := httptest.NewRecorder()
		stdReq, _ := http.NewRequest(req.Method, req.URL.String(), nil)
		c.handler(rec, stdReq)
		res := rec.Result()
		fResp := &fhttp.Response{
			StatusCode: res.StatusCode,
			Header:     make(fhttp.Header),
			Body:       res.Body,
			Request:    req,
		}
		for k, vv := range res.Header {
			fResp.Header[k] = vv
		}
		return fResp, nil
	}
	return &fhttp.Response{
		StatusCode: 200,
		Header:     make(fhttp.Header),
		Body:       http.NoBody,
		Request:    req,
	}, nil
}

func (c *trackingMockClient) CloseIdleConnections() {
	c.closedCount++
}

type trackingMockFactory struct {
	createdClients []*trackingMockClient
	handler        http.HandlerFunc
}

func (f *trackingMockFactory) New(p Params) (Client, error) {
	cli := &trackingMockClient{
		profile: p.TLSProfile,
		jar:     p.Jar,
		handler: f.handler,
	}
	f.createdClients = append(f.createdClients, cli)
	return cli, nil
}

// ----------------------------------------------------------------------------
// Test Paso 1: refreshLocked() limpia clientCache y cierra conexiones previas
// ----------------------------------------------------------------------------
func TestSession_RefreshLocked_ClearsClientCacheAndAdoptsNewProfile(t *testing.T) {
	catalog := Profiles.DefaultCatalog()
	mgr, err := Profiles.NewManager(Profiles.ManagerConfig{
		Registry: catalog,
		Mode:     Profiles.Browser,
	})
	if err != nil {
		t.Fatalf("error creando ProfileManager: %v", err)
	}

	factory := &trackingMockFactory{}
	cfg := Default()
	sess, err := NewSession(cfg, WithProfileManager(mgr), WithTransportFactory(factory))
	if err != nil {
		t.Fatalf("error creando sesión: %v", err)
	}
	defer sess.Close()

	// 1. Ejecutar petición inicial
	_, err = sess.Do(context.Background(), Spec{
		Method: "GET",
		URL:    "http://example.com/test",
	})
	if err != nil {
		t.Fatalf("error en Do inicial: %v", err)
	}

	if len(factory.createdClients) != 1 {
		t.Fatalf("se esperaba 1 cliente creado, got %d", len(factory.createdClients))
	}
	firstClient := factory.createdClients[0]

	// Verificar que clientCache contiene el cliente
	sess.mu.Lock()
	if len(sess.clientCache) != 1 {
		sess.mu.Unlock()
		t.Fatalf("clientCache debería tener 1 entrada, got %d", len(sess.clientCache))
	}

	// 2. Ejecutar refreshLocked (simulando rotación forzada o retry ActionRebuildIdentity)
	if err := sess.refreshLocked(); err != nil {
		sess.mu.Unlock()
		t.Fatalf("refreshLocked falló: %v", err)
	}

	// Verificar que el cliente anterior fue cerrado al desalojar el caché
	if firstClient.closedCount == 0 {
		sess.mu.Unlock()
		t.Errorf("el cliente anterior no cerró sus conexiones en refreshLocked")
	}

	// Verificar que se construyó un nuevo cliente
	if len(factory.createdClients) != 2 {
		sess.mu.Unlock()
		t.Fatalf("se esperaba 2 clientes creados tras refresh, got %d", len(factory.createdClients))
	}
	secondClient := factory.createdClients[1]

	if firstClient == secondClient {
		sess.mu.Unlock()
		t.Errorf("refreshLocked no generó un cliente nuevo, reutilizó el anterior")
	}

	// El nuevo cliente debe tener su propio Jar desacoplado
	if firstClient.jar == secondClient.jar {
		sess.mu.Unlock()
		t.Errorf("el nuevo cliente comparte el Jar antiguo")
	}
	sess.mu.Unlock()
}

// ----------------------------------------------------------------------------
// Test Paso 2: PreserveBodyForError conserva BodyBytes y recicla poolBuf
// ----------------------------------------------------------------------------
func TestResponse_PreserveBodyForError(t *testing.T) {
	buf := GetBuffer()
	payload := []byte("error: invalid credentials or cloudflare challenge")
	buf.Write(payload)

	resp := &Response{
		StatusCode: 403,
		poolBuf:    buf,
		BodyBytes:  buf.Bytes(),
	}

	result := &Result{
		Response: resp,
		Err:      fmt.Errorf("server forbidden 403"),
	}

	// Llamar a PreserveBodyForError
	result.PreserveBodyForError()

	// poolBuf debe ser nil (retornado a sync.Pool)
	if resp.poolBuf != nil {
		t.Errorf("poolBuf debería ser nil tras PreserveBodyForError")
	}

	// BodyBytes debe seguir intacto y legible
	if string(resp.BodyBytes) != string(payload) {
		t.Errorf("BodyBytes esperado %q, got %q", payload, resp.BodyBytes)
	}

	// Modificar un buffer nuevo del pool para verificar que resp.BodyBytes está desacoplado (Zero Data Race)
	newBuf := GetBuffer()
	newBuf.Write([]byte("DATA OVERWRITE"))
	if string(resp.BodyBytes) != string(payload) {
		t.Errorf("resp.BodyBytes sufrió corrupción por data race tras reciclar buffer al pool")
	}
	PutBuffer(newBuf)
}

// ----------------------------------------------------------------------------
// Test Paso 3: Evicción LRU en clientCache
// ----------------------------------------------------------------------------
func TestSession_ClientCache_LRUEviction(t *testing.T) {
	factory := &trackingMockFactory{}
	sess, err := NewSession(Default(), WithTransportFactory(factory))
	if err != nil {
		t.Fatalf("error creando sesión: %v", err)
	}
	defer sess.Close()

	sess.mu.Lock()
	defer sess.mu.Unlock()

	// Llenar clientCache hasta el límite (8 clientes)
	for i := 1; i <= 8; i++ {
		entry := &Entry{
			Host: fmt.Sprintf("proxy%d.net", i),
			Port: "8080",
			User: fmt.Sprintf("user%d", i),
		}
		_, err := sess.getOrCreateClientLocked(entry, false)
		if err != nil {
			t.Fatalf("error creando cliente %d: %v", i, err)
		}
		time.Sleep(2 * time.Millisecond) // Garantizar timestamps distintos
	}

	if len(sess.clientCache) != 8 {
		t.Fatalf("clientCache debería tener 8 clientes, got %d", len(sess.clientCache))
	}

	// Reutilizar / tocar el cliente más antiguo (proxy1) para actualizar su lastUsed
	proxy1 := &Entry{Host: "proxy1.net", Port: "8080", User: "user1"}
	_, _ = sess.getOrCreateClientLocked(proxy1, false)

	// El cliente proxy2 (creado en segundo lugar) es ahora el más antiguo (LRU).
	// Al insertar el cliente 9, proxy2 debe ser desalojado, NO proxy1.
	proxy9 := &Entry{Host: "proxy9.net", Port: "8080", User: "user9"}
	_, err = sess.getOrCreateClientLocked(proxy9, false)
	if err != nil {
		t.Fatalf("error insertando cliente 9: %v", err)
	}

	if len(sess.clientCache) != 8 {
		t.Errorf("clientCache excedió maxCachedClients=8, got %d", len(sess.clientCache))
	}

	// Verificar que proxy1 sigue vivo en el caché
	keyProxy1 := fmt.Sprintf("%s@%s_redir=false", proxy1.key(), proxy1.User)
	if _, exists := sess.clientCache[keyProxy1]; !exists {
		t.Errorf("proxy1 (recién usado) fue desalojado indebidamente")
	}

	// Verificar que proxy2 (el más antiguo no usado) fue desalojado por política LRU
	proxy2 := &Entry{Host: "proxy2.net", Port: "8080", User: "user2"}
	keyProxy2 := fmt.Sprintf("%s@%s_redir=false", proxy2.key(), proxy2.User)
	if _, exists := sess.clientCache[keyProxy2]; exists {
		t.Errorf("proxy2 (el más antiguo) debió haber sido desalojado por política LRU")
	}
}

// ----------------------------------------------------------------------------
// Test Paso 5 (Medición y Carga Concurrente): No fugas de buffers ni deadlocks
// ----------------------------------------------------------------------------
func TestSession_ConcurrentMemoryAndStress(t *testing.T) {
	factory := &trackingMockFactory{
		handler: func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(200)
			_, _ = w.Write([]byte(`{"status":"success","data":"ok"}`))
		},
	}

	sess, err := NewSession(Default(), WithTransportFactory(factory))
	if err != nil {
		t.Fatalf("error creando sesión: %v", err)
	}
	defer sess.Close()

	const workers = 30
	const requestsPerWorker = 50
	errChan := make(chan error, workers)

	for w := 0; w < workers; w++ {
		go func(workerID int) {
			for r := 0; r < requestsPerWorker; r++ {
				spec := Spec{
					Method: "GET",
					URL:    fmt.Sprintf("http://example.com/api/item?w=%d&r=%d", workerID, r),
				}
				res, err := sess.Do(context.Background(), spec)
				if err != nil {
					errChan <- fmt.Errorf("worker %d req %d error: %w", workerID, r, err)
					return
				}
				if !strings.Contains(res.String(), "success") {
					res.Release()
					errChan <- fmt.Errorf("worker %d respuesta inesperada: %s", workerID, res.String())
					return
				}
				res.Release()
			}
			errChan <- nil
		}(w)
	}

	for w := 0; w < workers; w++ {
		if err := <-errChan; err != nil {
			t.Fatal(err)
		}
	}

	total, success, failure, _ := sess.Metrics()
	if total != workers*requestsPerWorker {
		t.Errorf("total requests esperadas %d, got %d", workers*requestsPerWorker, total)
	}
	if success != total {
		t.Errorf("success esperadas %d, got %d (fallos: %d)", total, success, failure)
	}
}

// ----------------------------------------------------------------------------
// Test Mejora 1: Diferenciación de errores Proxy vs TLS Handshake
// ----------------------------------------------------------------------------
func TestRetry_ProxyErrorVsTLSFingerprint(t *testing.T) {
	classifier := retry.NewDefaultClassifier()
	engine := retry.New(retry.RetryConfig{MaxAttempts: 3, WaitMin: 100 * time.Millisecond, WaitMax: 1 * time.Second}, classifier)

	// Caso A: Error que contiene "proxy", "socks" o "gateway" con mención de handshake
	proxyErr := fmt.Errorf("proxyconnect tcp: dial tcp 1.2.3.4:8080: tls: first record does not look like a TLS handshake")
	decProxy := engine.Decide(1, retry.Signal{NetErr: proxyErr, Method: "GET"})
	if decProxy.Class != retry.ClassBlocked {
		t.Errorf("error de proxy clasificado como %v, esperado ClassBlocked", decProxy.Class)
	}
	if decProxy.Action != retry.ActionRotateProxy {
		t.Errorf("acción esperada ActionRotateProxy para proxy error, got %v", decProxy.Action)
	}

	// Caso B: Error TLS del servidor de destino directo (sin proxy)
	destTLSErr := fmt.Errorf("tls: handshake failure: certificate required")
	decTLS := engine.Decide(1, retry.Signal{NetErr: destTLSErr, Method: "GET"})
	if decTLS.Class != retry.ClassTLSFingerprint {
		t.Errorf("error de servidor TLS clasificado como %v, esperado ClassTLSFingerprint", decTLS.Class)
	}
	if decTLS.Action != retry.ActionRebuildIdentity {
		t.Errorf("acción esperada ActionRebuildIdentity para TLS error, got %v", decTLS.Action)
	}
}

// ----------------------------------------------------------------------------
// Test Mejora 2: Parseo y Respeto de Retry-After
// ----------------------------------------------------------------------------
func TestResponse_RetryAfter_Parsing(t *testing.T) {
	respSeconds := &Response{
		Headers: fhttp.Header{
			"Retry-After": []string{"15"},
		},
	}
	if respSeconds.RetryAfter() != 15*time.Second {
		t.Errorf("esperado 15s, got %v", respSeconds.RetryAfter())
	}

	futureDate := time.Now().Add(45 * time.Second).UTC().Format(http.TimeFormat)
	respDate := &Response{
		Headers: fhttp.Header{
			"Retry-After": []string{futureDate},
		},
	}
	parsed := respDate.RetryAfter()
	if parsed < 40*time.Second || parsed > 50*time.Second {
		t.Errorf("esperado ~45s, got %v", parsed)
	}

	respEmpty := &Response{
		Headers: fhttp.Header{},
	}
	if respEmpty.RetryAfter() != 0 {
		t.Errorf("esperado 0 para cabecera vacía, got %v", respEmpty.RetryAfter())
	}
}

func TestRetry_RetryAfter_DecideWaitAndRotate(t *testing.T) {
	classifier := retry.NewDefaultClassifier()
	engine := retry.New(retry.RetryConfig{MaxAttempts: 3, WaitMin: 1 * time.Second, WaitMax: 5 * time.Second}, classifier)

	// Caso A: Retry-After razonable (10 segundos) -> ajustar Wait a 10s
	decReasonable := engine.Decide(1, retry.Signal{
		StatusCode: 429,
		Method:     "GET",
		RetryAfter: 10 * time.Second,
	})
	if decReasonable.Wait < 10*time.Second {
		t.Errorf("esperado wait >= 10s por Retry-After, got %v", decReasonable.Wait)
	}
	if decReasonable.Action == retry.ActionRotateProxy {
		t.Errorf("no debía forzar rotación para Retry-After corto")
	}

	// Caso B: Retry-After excesivo (3600 segundos = 1 hora) -> Forzar ActionRotateProxy para no congelar worker
	decExcessive := engine.Decide(1, retry.Signal{
		StatusCode: 429,
		Method:     "GET",
		RetryAfter: 3600 * time.Second,
	})
	if decExcessive.Action != retry.ActionRotateProxy {
		t.Errorf("esperado ActionRotateProxy para Retry-After excesivo, got %v", decExcessive.Action)
	}
}

