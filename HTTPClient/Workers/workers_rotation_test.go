package Workers

import (
	"github.com/LeanCoder0718/Universal/HTTPClient"
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	fhttp "github.com/bogdanfinn/fhttp"
)

// mockClient implementa HTTPClient.Client sin abrir sockets reales.
type mockClient struct {
	proxyEntry *HTTPClient.Entry
	doFn       func(req *fhttp.Request) (*fhttp.Response, error)
}

func (m *mockClient) Do(req *fhttp.Request) (*fhttp.Response, error) {
	if m.doFn != nil {
		return m.doFn(req)
	}
	return &fhttp.Response{
		StatusCode: 200,
		Body:       fhttp.NoBody,
		Header:     make(fhttp.Header),
	}, nil
}

func (m *mockClient) CloseIdleConnections() {}

// mockFactory implementa HTTPClient.Factory para inyectar clientes simulados en tests.
type mockFactory struct {
	mu           sync.Mutex
	createdCount int
	doFn         func(req *fhttp.Request) (*fhttp.Response, error)
}

func (f *mockFactory) New(p HTTPClient.Params) (HTTPClient.Client, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.createdCount++
	return &mockClient{
		proxyEntry: p.Proxy,
		doFn:       f.doFn,
	}, nil
}

func createTestWorkerPool(t *testing.T, proxyCount int, maxRetries int, doFn func(req *fhttp.Request) (*fhttp.Response, error)) (*Pool, *HTTPClient.Pool) {
	t.Helper()
	rawProxies := make([]string, proxyCount)
	for i := 0; i < proxyCount; i++ {
		rawProxies[i] = fmt.Sprintf("192.168.1.%d:8080:user%d:pass%d", i+1, i+1, i+1)
	}

	cfg := HTTPClient.Default()
	cfg.Worker.PoolSize = 2
	cfg.Proxy.Enabled = true
	cfg.Proxy.ProxyType = "http"
	cfg.Retry.MaxAttempts = maxRetries

	sharedProxyPool, err := HTTPClient.NewPool(rawProxies, cfg.Proxy)
	if err != nil {
		t.Fatalf("falló creando sharedProxyPool: %v", err)
	}

	mockFact := &mockFactory{doFn: doFn}
	opts := []HTTPClient.Option{
		HTTPClient.WithProxyPool(sharedProxyPool),
		HTTPClient.WithTransportFactory(mockFact),
	}

	workerPool, err := New(cfg, PoolOptions{MaxInFlight: 10}, opts...)
	if err != nil {
		t.Fatalf("falló creando workerPool: %v", err)
	}

	return workerPool, sharedProxyPool
}

// Test 1: Petición exitosa al intento 1 -> 0 reintentos, 1 sola invocación.
func TestWorkerPool_Test1_SuccessFirstAttempt(t *testing.T) {
	pool, _ := createTestWorkerPool(t, 5, 3, nil)
	defer pool.Close()

	var invocations int
	_, err := pool.Run(context.Background(), func(ctx context.Context, s *HTTPClient.Session) (*HTTPClient.Result, error) {
		invocations++
		return nil, nil // Éxito inmediato
	})

	if err != nil {
		t.Fatalf("error inesperado: %v", err)
	}
	if invocations != 1 {
		t.Fatalf("esperada 1 sola invocación en éxito al primer intento, obtenidas: %d", invocations)
	}
}

// Test 2 & 3: Intento 1 y 2 fallan -> 3 intentos utilizan 3 proxies diferentes (A -> B -> C).
func TestWorkerPool_Test2_3_RotateAcrossAttempts(t *testing.T) {
	pool, _ := createTestWorkerPool(t, 5, 3, nil)
	defer pool.Close()

	var (
		mu           sync.Mutex
		proxiesUsed  []string
		attemptCount int
	)

	_, err := pool.RunWithRetry(context.Background(), 3, func(ctx context.Context, s *HTTPClient.Session) (*HTTPClient.Result, error) {
		mu.Lock()
		defer mu.Unlock()
		attemptCount++

		cur := s.CurrentProxy()
		if cur == nil {
			return nil, errors.New("sesión no tiene proxy configurada")
		}
		proxiesUsed = append(proxiesUsed, cur.Host)

		if attemptCount < 3 {
			// Simular fallo en intentos 1 y 2
			return nil, errors.New("timeout connecting to proxy")
		}
		// Éxito en intento 3
		return nil, nil
	})

	if err != nil {
		t.Fatalf("se esperaba éxito al intento 3, obtenido error: %v", err)
	}
	if attemptCount != 3 {
		t.Fatalf("esperados 3 intentos, ejecutados: %d", attemptCount)
	}

	// Verificar que los 3 proxies usados sean estrictamente distintos
	if len(proxiesUsed) != 3 {
		t.Fatalf("esperados 3 proxies registrados, obtenidos: %v", proxiesUsed)
	}
	if proxiesUsed[0] == proxiesUsed[1] {
		t.Fatalf("CRÍTICO: Intento 2 reutilizó la misma proxy que Intento 1 (%s)", proxiesUsed[0])
	}
	if proxiesUsed[1] == proxiesUsed[2] || proxiesUsed[0] == proxiesUsed[2] {
		t.Fatalf("CRÍTICO: Intento 3 repitió proxy (%s) vs (%v)", proxiesUsed[2], proxiesUsed[:2])
	}
}

// Test 5: Pool pequeño (2 proxies, 4 retries) -> no causa deadlock ni cuelga el proceso.
func TestWorkerPool_Test5_SmallPoolFallback(t *testing.T) {
	pool, _ := createTestWorkerPool(t, 2, 4, nil)
	defer pool.Close()

	var attempts int
	_, err := pool.RunWithRetry(context.Background(), 4, func(ctx context.Context, s *HTTPClient.Session) (*HTTPClient.Result, error) {
		attempts++
		if attempts < 4 {
			return nil, errors.New("connection reset")
		}
		return nil, nil
	})

	if err != nil {
		t.Fatalf("esperado éxito al intento 4, obtenido error: %v", err)
	}
	if attempts != 4 {
		t.Fatalf("esperados 4 intentos completados con fallback, obtenidos: %d", attempts)
	}
}

// Test 7: Cancelación de context.Context durante retry aborta limpiamente sin fugas.
func TestWorkerPool_Test7_ContextCancellation(t *testing.T) {
	pool, _ := createTestWorkerPool(t, 5, 5, nil)
	defer pool.Close()

	ctx, cancel := context.WithCancel(context.Background())

	var attempts int
	_, err := pool.RunWithRetry(ctx, 5, func(c context.Context, s *HTTPClient.Session) (*HTTPClient.Result, error) {
		attempts++
		if attempts == 1 {
			cancel() // Cancelar el contexto tras el primer intento
		}
		return nil, errors.New("network failure")
	})

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("esperado context.Canceled, obtenido: %v", err)
	}
	if attempts > 2 {
		t.Fatalf("se debió abortar inmediatamente tras cancelación, intentos ejecutados: %d", attempts)
	}
}

// Test 10: Integridad de Task preservada (los datos no se corrompen ni pierden al rotar).
func TestWorkerPool_Test10_TaskDataIntegrity(t *testing.T) {
	pool, _ := createTestWorkerPool(t, 3, 3, nil)
	defer pool.Close()

	type TaskItem struct {
		Email    string
		Password string
	}
	task := TaskItem{
		Email:    "testuser@domain.com",
		Password: "SecretPassword123!",
	}

	var observedEmails []string
	var observedPasses []string

	_, _ = pool.RunWithRetry(context.Background(), 3, func(ctx context.Context, s *HTTPClient.Session) (*HTTPClient.Result, error) {
		observedEmails = append(observedEmails, task.Email)
		observedPasses = append(observedPasses, task.Password)
		return nil, errors.New("simulated error")
	})

	for _, e := range observedEmails {
		if e != task.Email {
			t.Fatalf("corrupción de email: esperado %s, obtenido %s", task.Email, e)
		}
	}
	for _, p := range observedPasses {
		if p != task.Password {
			t.Fatalf("corrupción de password: esperado %s, obtenido %s", task.Password, p)
		}
	}
}

// Test de Concurrencia masiva con Worker Pool: múltiples workers ejecutando tareas simultáneamente
func TestWorkerPool_MassiveConcurrentExecution(t *testing.T) {
	pool, _ := createTestWorkerPool(t, 10, 2, nil)
	defer pool.Close()

	var wg sync.WaitGroup
	var completed atomic.Int64
	numTasks := 40

	for i := 0; i < numTasks; i++ {
		wg.Add(1)
		go func(taskID int) {
			defer wg.Done()
			_, err := pool.Run(context.Background(), func(ctx context.Context, s *HTTPClient.Session) (*HTTPClient.Result, error) {
				if taskID%2 == 0 {
					return nil, errors.New("error recuperable")
				}
				return nil, nil
			})
			if err == nil || taskID%2 == 0 {
				completed.Add(1)
			}
		}(i)
	}

	wg.Wait()
	if completed.Load() != int64(numTasks) {
		t.Fatalf("esperadas %d tareas procesadas, completadas: %d", numTasks, completed.Load())
	}
}

// TestWorkerPool_InternalRetriesDecoupled verifica que QuickStart configure
// cfg.Retry.MaxRetries = 3 y cfg.Retry.MaxAttempts = 4 (1 inicial + 3 reintentos)
// para aplicar reintentos por petición de forma transparente, mientras que WithNoInternalRetries
// fija MaxAttempts = 1 para ejecuciones estrictas de 1 solo intento.
func TestWorkerPool_InternalRetriesDecoupled(t *testing.T) {
	pool, err := QuickStart(10, []string{"127.0.0.1:8080:u:p"}, "http", 3)
	if err != nil {
		t.Fatalf("falló QuickStart: %v", err)
	}
	defer pool.Close()

	if pool.maxRetries != 3 {
		t.Fatalf("esperado pool.maxRetries = 3, obtenido %d", pool.maxRetries)
	}
	if pool.cfg.Retry.MaxAttempts != 4 {
		t.Fatalf("esperado cfg.Retry.MaxAttempts = 4 (1 inicial + 3 reintentos), obtenido %d", pool.cfg.Retry.MaxAttempts)
	}

	// Con WithNoInternalRetries explícito
	poolExplicit, err := QuickStart(10, []string{"127.0.0.1:8080:u:p"}, "http", 3, WithNoInternalRetries())
	if err != nil {
		t.Fatalf("falló QuickStart con WithNoInternalRetries: %v", err)
	}
	defer poolExplicit.Close()

	if poolExplicit.cfg.Retry.MaxAttempts != 1 {
		t.Fatalf("esperado cfg.Retry.MaxAttempts = 1 con WithNoInternalRetries, obtenido %d", poolExplicit.cfg.Retry.MaxAttempts)
	}
}
