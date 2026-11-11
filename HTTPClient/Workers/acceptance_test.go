package Workers

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LeanCoder0718/Universal/HTTPClient"
	"github.com/LeanCoder0718/Universal/HTTPClient/Profiles"
	fhttp "github.com/bogdanfinn/fhttp"
)

func mockOKResponse(body string) *fhttp.Response {
	return &fhttp.Response{
		StatusCode: http.StatusOK,
		Header:     make(fhttp.Header),
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}

func mockStatusResponse(status int, body string) *fhttp.Response {
	return &fhttp.Response{
		StatusCode: status,
		Header:     make(fhttp.Header),
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}

func createAcceptancePool(t *testing.T, maxRetries int, doFn func(req *fhttp.Request) (*fhttp.Response, error), extraOpts ...PoolOption) *Pool {
	t.Helper()
	proxies := []string{
		"192.168.1.1:8080:u1:p1",
		"192.168.1.2:8080:u2:p2",
		"192.168.1.3:8080:u3:p3",
		"192.168.1.4:8080:u4:p4",
	}
	mockFact := &mockFactory{doFn: doFn}
	opts := []PoolOption{
		WithSessionOptions(HTTPClient.WithTransportFactory(mockFact)),
		func(cfg *HTTPClient.Config) {
			cfg.Retry.WaitMin = 1 * time.Millisecond
			cfg.Retry.WaitMax = 5 * time.Millisecond
		},
	}
	opts = append(opts, extraOpts...)

	reg := Profiles.DefaultCatalog()
	mgr, err := Profiles.NewManager(Profiles.ManagerConfig{
		Registry: reg,
		Mode:     Profiles.Browser,
	})
	if err != nil {
		t.Fatalf("Profiles.NewManager falló: %v", err)
	}

	pool, err := QuickStartWithProfile(5, proxies, "http", maxRetries, mgr, opts...)
	if err != nil {
		t.Fatalf("QuickStartWithProfile falló: %v", err)
	}
	return pool
}

// 1. A succeeds, B fails once then succeeds: A=1, B=2.
func TestAcceptance_01_StepASucceeds_StepBFailsOnceThenSucceeds(t *testing.T) {
	var countA, countB, countC atomic.Int32
	pool := createAcceptancePool(t, 3, func(req *fhttp.Request) (*fhttp.Response, error) {
		switch req.URL.Path {
		case "/step-a":
			countA.Add(1)
			return mockOKResponse("ok-a"), nil
		case "/step-b":
			attempt := countB.Add(1)
			if attempt == 1 {
				// Simular fallo transitorio recuperable (503 Service Unavailable)
				return mockStatusResponse(http.StatusServiceUnavailable, "overloaded"), nil
			}
			return mockOKResponse("ok-b"), nil
		case "/step-c":
			countC.Add(1)
			return mockOKResponse("ok-c"), nil
		default:
			return mockOKResponse("ok"), nil
		}
	})
	defer pool.Close()

	var completedJob atomic.Bool
	res, err := pool.Run(context.Background(), func(ctx context.Context, s *HTTPClient.Session) (*HTTPClient.Result, error) {
		rA, err := s.Do(ctx, HTTPClient.Spec{Method: "GET", URL: "https://example.com/step-a"})
		if err != nil {
			return nil, fmt.Errorf("step A falló: %w", err)
		}
		if rA.StatusCode != 200 {
			return nil, fmt.Errorf("step A status != 200: %d", rA.StatusCode)
		}

		rB, err := s.Do(ctx, HTTPClient.Spec{Method: "GET", URL: "https://example.com/step-b"})
		if err != nil {
			return nil, fmt.Errorf("step B falló: %w", err)
		}
		if rB.StatusCode != 200 {
			return nil, fmt.Errorf("step B status != 200: %d", rB.StatusCode)
		}

		rC, err := s.Do(ctx, HTTPClient.Spec{Method: "GET", URL: "https://example.com/step-c"})
		if err != nil {
			return nil, fmt.Errorf("step C falló: %w", err)
		}
		completedJob.Store(true)
		return rC, nil
	})

	if err != nil {
		t.Fatalf("Job falló inesperadamente: %v", err)
	}
	if !completedJob.Load() || res == nil || res.StatusCode != 200 {
		t.Fatalf("Job no completó exitosamente: %v", res)
	}
	if a := countA.Load(); a != 1 {
		t.Errorf("Paso A debió ejecutarse exactamente 1 vez (sin repetición), pero fue: %d", a)
	}
	if b := countB.Load(); b != 2 {
		t.Errorf("Paso B debió ejecutarse 2 veces (1 fallo + 1 reintento exitoso), pero fue: %d", b)
	}
	if c := countC.Load(); c != 1 {
		t.Errorf("Paso C debió ejecutarse 1 vez, pero fue: %d", c)
	}
}

// 2. A succeeds, B exhausts maxRetries=3: A=1, B=4, C=0. Returns ErrMaxRetriesExceeded with 4 attempts.
func TestAcceptance_02_StepASucceeds_StepBExhaustsRetries(t *testing.T) {
	var countA, countB, countC atomic.Int32
	pool := createAcceptancePool(t, 3, func(req *fhttp.Request) (*fhttp.Response, error) {
		switch req.URL.Path {
		case "/step-a":
			countA.Add(1)
			return mockOKResponse("ok-a"), nil
		case "/step-b":
			countB.Add(1)
			return mockStatusResponse(http.StatusServiceUnavailable, "overloaded"), nil
		case "/step-c":
			countC.Add(1)
			return mockOKResponse("ok-c"), nil
		default:
			return mockOKResponse("ok"), nil
		}
	})
	defer pool.Close()

	_, err := pool.Run(context.Background(), func(ctx context.Context, s *HTTPClient.Session) (*HTTPClient.Result, error) {
		_, err := s.Do(ctx, HTTPClient.Spec{Method: "GET", URL: "https://example.com/step-a"})
		if err != nil {
			return nil, err
		}

		// Step B se reintenta internamente en s.Do
		_, err = s.Do(ctx, HTTPClient.Spec{Method: "GET", URL: "https://example.com/step-b"})
		if err != nil {
			return nil, err
		}

		return s.Do(ctx, HTTPClient.Spec{Method: "GET", URL: "https://example.com/step-c"})
	})

	if err == nil {
		t.Fatal("Se esperaba error por agotamiento de reintentos, pero fue nil")
	}
	if !errors.Is(err, HTTPClient.ErrMaxRetriesExceeded) {
		t.Errorf("errors.Is(err, ErrMaxRetriesExceeded) debió ser true, obtenido: %v", err)
	}
	var maxRetriesErr *HTTPClient.MaxRetriesError
	if !errors.As(err, &maxRetriesErr) {
		t.Fatalf("err no es de tipo *MaxRetriesError: %T", err)
	}
	if maxRetriesErr.Attempts != 4 {
		t.Errorf("maxRetriesErr.Attempts debió ser 4 (1 inicial + 3 reintentos), obtenido: %d", maxRetriesErr.Attempts)
	}

	if a := countA.Load(); a != 1 {
		t.Errorf("Paso A debió ejecutarse exactamente 1 vez (el Job no se repitió), obtenido: %d", a)
	}
	if b := countB.Load(); b != 4 {
		t.Errorf("Paso B debió intentar exactamente 4 veces antes de rendirse, obtenido: %d", b)
	}
	if c := countC.Load(); c != 0 {
		t.Errorf("Paso C NUNCA debió ejecutarse tras el fallo de B, obtenido: %d", c)
	}
}

// 3. A fails once then succeeds: retry uses clean state and rotates proxy (A=2).
func TestAcceptance_03_StepAFailsOnceThenSucceeds_CleanStateAndRotate(t *testing.T) {
	var countA atomic.Int32

	pool := createAcceptancePool(t, 3, func(req *fhttp.Request) (*fhttp.Response, error) {
		count := countA.Add(1)

		if count == 1 {
			// Intento 1: falla con WAF challenge / Cloudflare block
			hdr := make(fhttp.Header)
			hdr["cf-mitigated"] = []string{"challenge"}
			return &fhttp.Response{
				StatusCode: http.StatusForbidden,
				Header:     hdr,
				Body:       io.NopCloser(strings.NewReader("cf challenge")),
			}, nil
		}
		return mockOKResponse("ok-a"), nil
	})
	defer pool.Close()

	res, err := pool.Run(context.Background(), func(ctx context.Context, s *HTTPClient.Session) (*HTTPClient.Result, error) {
		return s.Do(ctx, HTTPClient.Spec{Method: "GET", URL: "https://example.com/step-a"})
	})

	if err != nil {
		t.Fatalf("Step A debió recuperarse al reintento, err: %v", err)
	}
	if res.StatusCode != 200 {
		t.Fatalf("Step A statusCode esperado 200, obtenido: %d", res.StatusCode)
	}
	if countA.Load() != 2 {
		t.Fatalf("Step A debió ejecutarse 2 veces, obtenido: %d", countA.Load())
	}
}

// 4. Cookie obtained in A arrives at B's retry.
func TestAcceptance_04_CookieFromASurvivesToBRetry(t *testing.T) {
	var bAttempt1Cookie, bAttempt2Cookie string
	var countB atomic.Int32

	pool := createAcceptancePool(t, 3, func(req *fhttp.Request) (*fhttp.Response, error) {
		switch req.URL.Path {
		case "/step-a":
			hdr := make(fhttp.Header)
			hdr["Set-Cookie"] = []string{"auth_token=super_secret_token_123; Path=/; Domain=example.com"}
			return &fhttp.Response{
				StatusCode: 200,
				Header:     hdr,
				Body:       io.NopCloser(strings.NewReader("logged in")),
			}, nil
		case "/step-b":
			attempt := countB.Add(1)
			if attempt == 1 {
				bAttempt1Cookie = HTTPClient.GetHeader(req.Header, "Cookie")
				// Retornamos 503 e intentamos inyectar una cookie sucia
				hdr := make(fhttp.Header)
				hdr["Set-Cookie"] = []string{"dirty_cookie=corrupted; Path=/; Domain=example.com"}
				return &fhttp.Response{
					StatusCode: http.StatusServiceUnavailable,
					Header:     hdr,
					Body:       io.NopCloser(strings.NewReader("try later")),
				}, nil
			}
			bAttempt2Cookie = HTTPClient.GetHeader(req.Header, "Cookie")
			return mockOKResponse("step-b ok"), nil
		default:
			return mockOKResponse("ok"), nil
		}
	})
	defer pool.Close()

	_, err := pool.Run(context.Background(), func(ctx context.Context, s *HTTPClient.Session) (*HTTPClient.Result, error) {
		if _, err := s.Do(ctx, HTTPClient.Spec{Method: "GET", URL: "https://example.com/step-a"}); err != nil {
			return nil, err
		}
		return s.Do(ctx, HTTPClient.Spec{Method: "GET", URL: "https://example.com/step-b"})
	})

	if err != nil {
		t.Fatalf("Job falló inesperadamente: %v", err)
	}
	if !strings.Contains(bAttempt1Cookie, "auth_token=super_secret_token_123") {
		t.Errorf("Intento 1 de B no recibió la cookie de A: %q", bAttempt1Cookie)
	}
	if !strings.Contains(bAttempt2Cookie, "auth_token=super_secret_token_123") {
		t.Errorf("Reintento 2 de B debió conservar la cookie de A tras el rollback: %q", bAttempt2Cookie)
	}
	if strings.Contains(bAttempt2Cookie, "dirty_cookie") {
		t.Errorf("Reintento 2 de B conservó la cookie sucia del intento 1 fallido: %q", bAttempt2Cookie)
	}
}

// 5. Token obtained in A remains available in B's closure without re-running A.
func TestAcceptance_05_TokenInClosureWithoutReRunningA(t *testing.T) {
	var countA, countB atomic.Int32
	var bReceivedAuthHeader string

	pool := createAcceptancePool(t, 3, func(req *fhttp.Request) (*fhttp.Response, error) {
		switch req.URL.Path {
		case "/login":
			countA.Add(1)
			return &fhttp.Response{
				StatusCode: 200,
				Header:     make(fhttp.Header),
				Body:       io.NopCloser(strings.NewReader(`token-xyz-789`)),
			}, nil
		case "/transfer":
			att := countB.Add(1)
			bReceivedAuthHeader = req.Header.Get("Authorization")
			if att == 1 {
				return mockStatusResponse(http.StatusServiceUnavailable, "overloaded"), nil
			}
			return mockOKResponse("transferred"), nil
		default:
			return mockOKResponse("ok"), nil
		}
	})
	defer pool.Close()

	_, err := pool.Run(context.Background(), func(ctx context.Context, s *HTTPClient.Session) (*HTTPClient.Result, error) {
		resA, err := s.Do(ctx, HTTPClient.Spec{Method: "GET", URL: "https://example.com/login"})
		if err != nil {
			return nil, err
		}
		// Extraer token en variable local del closure
		token := string(resA.Response.BodyBytes)

		return s.Do(ctx, HTTPClient.Spec{
			Method: "POST",
			URL:    "https://example.com/transfer",
			Extra: map[string]string{
				"Authorization": "Bearer " + token,
			},
			Idempotent: true,
		})
	})

	if err != nil {
		t.Fatalf("Job falló inesperadamente: %v", err)
	}
	if countA.Load() != 1 {
		t.Errorf("Paso A (login) se reejecutó indebidamente: %d veces", countA.Load())
	}
	if countB.Load() != 2 {
		t.Errorf("Paso B debió ejecutarse 2 veces, obtenido: %d", countB.Load())
	}
	if bReceivedAuthHeader != "Bearer token-xyz-789" {
		t.Errorf("Paso B no recibió el token del closure en su reintento: %q", bReceivedAuthHeader)
	}
}

// 6. maxRetries = 0 produces exactly 1 attempt.
func TestAcceptance_06_MaxRetriesZero_ExactlyOneAttempt(t *testing.T) {
	var count atomic.Int32
	pool := createAcceptancePool(t, 0, func(req *fhttp.Request) (*fhttp.Response, error) {
		count.Add(1)
		return mockStatusResponse(http.StatusServiceUnavailable, "overloaded"), nil
	})
	defer pool.Close()

	_, err := pool.Run(context.Background(), func(ctx context.Context, s *HTTPClient.Session) (*HTTPClient.Result, error) {
		return s.Do(ctx, HTTPClient.Spec{Method: "GET", URL: "https://example.com/endpoint"})
	})

	if err == nil {
		t.Fatal("Se esperaba error")
	}
	if count.Load() != 1 {
		t.Errorf("maxRetries = 0 debió generar exactamente 1 intento (0 reintentos), pero ejecutó: %d", count.Load())
	}
}

// 7. maxRetries = 1 produces at most 2 attempts.
func TestAcceptance_07_MaxRetriesOne_AtMostTwoAttempts(t *testing.T) {
	var count atomic.Int32
	pool := createAcceptancePool(t, 1, func(req *fhttp.Request) (*fhttp.Response, error) {
		count.Add(1)
		return mockStatusResponse(http.StatusServiceUnavailable, "overloaded"), nil
	})
	defer pool.Close()

	_, err := pool.Run(context.Background(), func(ctx context.Context, s *HTTPClient.Session) (*HTTPClient.Result, error) {
		return s.Do(ctx, HTTPClient.Spec{Method: "GET", URL: "https://example.com/endpoint"})
	})

	if err == nil {
		t.Fatal("Se esperaba error")
	}
	if count.Load() != 2 {
		t.Errorf("maxRetries = 1 debió generar exactamente 2 intentos (1 inicial + 1 reintento), pero ejecutó: %d", count.Load())
	}
}

// 8. Negative maxRetries (-1) returns ErrInvalidConfig.
func TestAcceptance_08_NegativeMaxRetries_ErrInvalidConfig(t *testing.T) {
	proxies := []string{"127.0.0.1:8080"}
	reg := Profiles.DefaultCatalog()
	mgr, _ := Profiles.NewManager(Profiles.ManagerConfig{Registry: reg, Mode: Profiles.Browser})

	// 1. QuickStart
	_, err := QuickStart(5, proxies, "http", -1)
	if !errors.Is(err, HTTPClient.ErrInvalidConfig) {
		t.Errorf("QuickStart con maxRetries=-1 debió retornar ErrInvalidConfig, obtenido: %v", err)
	}

	// 2. QuickStartWithProfile
	_, err = QuickStartWithProfile(5, proxies, "http", -1, mgr)
	if !errors.Is(err, HTTPClient.ErrInvalidConfig) {
		t.Errorf("QuickStartWithProfile con maxRetries=-1 debió retornar ErrInvalidConfig, obtenido: %v", err)
	}

	// 3. QuickStartWithHTTPConfig
	_, err = QuickStartWithHTTPConfig(5, proxies, "http", -1, HTTPClient.HTTPConfig{}, mgr)
	if !errors.Is(err, HTTPClient.ErrInvalidConfig) {
		t.Errorf("QuickStartWithHTTPConfig con maxRetries=-1 debió retornar ErrInvalidConfig, obtenido: %v", err)
	}

	// 4. Workers.New
	cfg := HTTPClient.Default()
	_, err = New(cfg, PoolOptions{MaxJobRetries: -1})
	if !errors.Is(err, HTTPClient.ErrInvalidConfig) {
		t.Errorf("Workers.New con MaxJobRetries=-1 debió retornar ErrInvalidConfig, obtenido: %v", err)
	}
}

// 9. Requests inherit pool configuration without specifying MaxAttempts or MaxRetries in Spec.
func TestAcceptance_09_RequestsInheritPoolConfigWithoutSpecOptions(t *testing.T) {
	var count atomic.Int32
	pool := createAcceptancePool(t, 3, func(req *fhttp.Request) (*fhttp.Response, error) {
		count.Add(1)
		return mockStatusResponse(http.StatusServiceUnavailable, "overloaded"), nil
	})
	defer pool.Close()

	_, err := pool.Run(context.Background(), func(ctx context.Context, s *HTTPClient.Session) (*HTTPClient.Result, error) {
		// Petición estándar y normal, sin campos de retry en Spec
		return s.Do(ctx, HTTPClient.Spec{
			Method: "GET",
			URL:    "https://example.com/resource",
		})
	})

	if err == nil {
		t.Fatal("Se esperaba error tras agotar intentos")
	}
	if count.Load() != 4 {
		t.Errorf("La petición debió heredar maxRetries=3 del pool (4 intentos totales), pero ejecutó: %d", count.Load())
	}
}

// 10. No multiplication of attempts between Session and Workers (B=4, not 16).
func TestAcceptance_10_NoAttemptMultiplication(t *testing.T) {
	var countA, countB atomic.Int32
	pool := createAcceptancePool(t, 3, func(req *fhttp.Request) (*fhttp.Response, error) {
		if req.URL.Path == "/step-a" {
			countA.Add(1)
			return mockOKResponse("ok-a"), nil
		}
		countB.Add(1)
		return mockStatusResponse(http.StatusServiceUnavailable, "overloaded"), nil
	})
	defer pool.Close()

	_, err := pool.Run(context.Background(), func(ctx context.Context, s *HTTPClient.Session) (*HTTPClient.Result, error) {
		if _, err := s.Do(ctx, HTTPClient.Spec{Method: "GET", URL: "https://example.com/step-a"}); err != nil {
			return nil, err
		}
		return s.Do(ctx, HTTPClient.Spec{Method: "GET", URL: "https://example.com/step-b"})
	})

	if err == nil {
		t.Fatal("Se esperaba error")
	}
	if countA.Load() != 1 {
		t.Errorf("Paso A debió ejecutarse 1 vez, obtenido: %d", countA.Load())
	}
	if countB.Load() != 4 {
		t.Errorf("Paso B debió ejecutarse exactamente 4 veces (NO 16 por multiplicación), obtenido: %d", countB.Load())
	}
}

// 11. Context cancellation stops retries.
func TestAcceptance_11_ContextCancellationStopsRetries(t *testing.T) {
	var count atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())

	pool := createAcceptancePool(t, 3, func(req *fhttp.Request) (*fhttp.Response, error) {
		att := count.Add(1)
		if att == 1 {
			// Cancelamos el contexto tras el intento 1
			cancel()
		}
		return mockStatusResponse(http.StatusServiceUnavailable, "overloaded"), nil
	}, func(cfg *HTTPClient.Config) {
		cfg.Retry.WaitMin = 50 * time.Millisecond
		cfg.Retry.WaitMax = 100 * time.Millisecond
	})
	defer pool.Close()

	_, err := pool.Run(ctx, func(c context.Context, s *HTTPClient.Session) (*HTTPClient.Result, error) {
		return s.Do(c, HTTPClient.Spec{Method: "GET", URL: "https://example.com/resource"})
	})

	if err == nil {
		t.Fatal("Se esperaba error de cancelación")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Error esperado context.Canceled, obtenido: %v", err)
	}
	if count.Load() > 2 {
		t.Errorf("Los reintentos debieron detenerse inmediatamente por cancelación de contexto, intentos ejecutados: %d", count.Load())
	}
}

// 12. Concurrent tasks do not share cookies or state.
func TestAcceptance_12_ConcurrentTasksDoNotShareState(t *testing.T) {
	pool := createAcceptancePool(t, 2, func(req *fhttp.Request) (*fhttp.Response, error) {
		taskID := req.URL.Query().Get("task")
		if strings.HasPrefix(req.URL.Path, "/init") {
			hdr := make(fhttp.Header)
			hdr["Set-Cookie"] = []string{fmt.Sprintf("session_task=%s; Path=/; Domain=example.com", taskID)}
			return &fhttp.Response{
				StatusCode: 200,
				Header:     hdr,
				Body:       io.NopCloser(strings.NewReader("ok")),
			}, nil
		}

		// En /check verificamos la cookie
		cookieHeader := HTTPClient.GetHeader(req.Header, "Cookie")
		expected := fmt.Sprintf("session_task=%s", taskID)
		if !strings.Contains(cookieHeader, expected) {
			return mockStatusResponse(http.StatusBadRequest, "cookie mismatch"), nil
		}
		// Verificar que no tenga cookies de otras tareas
		parts := strings.Split(cookieHeader, ";")
		for _, part := range parts {
			part = strings.TrimSpace(part)
			if strings.HasPrefix(part, "session_task=") && part != expected {
				return mockStatusResponse(http.StatusConflict, "cookie contamination"), nil
			}
		}
		return mockOKResponse("clean"), nil
	}, WithAutoReset(false))
	defer pool.Close()

	const tasks = 10
	var wg sync.WaitGroup
	var errCount atomic.Int32

	for i := 0; i < tasks; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			taskStr := fmt.Sprintf("task_%d", id)
			_, err := pool.Run(context.Background(), func(ctx context.Context, s *HTTPClient.Session) (*HTTPClient.Result, error) {
				_, err := s.Do(ctx, HTTPClient.Spec{
					Method: "GET",
					URL:    fmt.Sprintf("https://example.com/init?task=%s", taskStr),
				})
				if err != nil {
					return nil, err
				}

				res, err := s.Do(ctx, HTTPClient.Spec{
					Method: "GET",
					URL:    fmt.Sprintf("https://example.com/check?task=%s", taskStr),
				})
				if err != nil {
					return nil, err
				}
				if res.StatusCode != 200 {
					return nil, fmt.Errorf("bad status %d", res.StatusCode)
				}
				return res, nil
			})
			if err != nil {
				errCount.Add(1)
			}
		}(i)
	}

	wg.Wait()
	if errCount.Load() != 0 {
		t.Fatalf("%d tareas concurrentes fallaron por contaminación de estado", errCount.Load())
	}
}

// 13. Non-idempotent requests respect existing idempotency safety policy.
func TestAcceptance_13_NonIdempotentMethodSafetyPolicy(t *testing.T) {
	// A: POST con error 400 (Client/Permanent) sin Idempotent: true -> NO se reintenta (1 intento)
	{
		var count atomic.Int32
		pool := createAcceptancePool(t, 3, func(req *fhttp.Request) (*fhttp.Response, error) {
			count.Add(1)
			return mockStatusResponse(http.StatusBadRequest, "bad request"), nil
		})
		_, _ = pool.Run(context.Background(), func(ctx context.Context, s *HTTPClient.Session) (*HTTPClient.Result, error) {
			return s.Do(ctx, HTTPClient.Spec{Method: "POST", URL: "https://example.com/submit"})
		})
		pool.Close()
		if count.Load() != 1 {
			t.Errorf("POST con 400 debió abortar al intento 1, pero ejecutó: %d", count.Load())
		}
	}

	// B: POST con 503 (Server Overloaded / RateLimit) -> SÍ se reintenta (4 intentos)
	{
		var count atomic.Int32
		pool := createAcceptancePool(t, 3, func(req *fhttp.Request) (*fhttp.Response, error) {
			count.Add(1)
			return mockStatusResponse(http.StatusServiceUnavailable, "overloaded"), nil
		})
		_, _ = pool.Run(context.Background(), func(ctx context.Context, s *HTTPClient.Session) (*HTTPClient.Result, error) {
			return s.Do(ctx, HTTPClient.Spec{Method: "POST", URL: "https://example.com/submit"})
		})
		pool.Close()
		if count.Load() != 4 {
			t.Errorf("POST con 503 debió reintentarse 4 veces, pero ejecutó: %d", count.Load())
		}
	}

	// C: POST con Idempotent: true ante error recuperable -> SÍ se reintenta
	{
		var count atomic.Int32
		pool := createAcceptancePool(t, 2, func(req *fhttp.Request) (*fhttp.Response, error) {
			count.Add(1)
			return mockStatusResponse(http.StatusBadGateway, "bad gateway"), nil
		})
		_, _ = pool.Run(context.Background(), func(ctx context.Context, s *HTTPClient.Session) (*HTTPClient.Result, error) {
			return s.Do(ctx, HTTPClient.Spec{
				Method:     "POST",
				URL:        "https://example.com/payment",
				Idempotent: true,
			})
		})
		pool.Close()
		if count.Load() != 3 {
			t.Errorf("POST con Idempotent:true ante 502 debió intentar 3 veces, pero ejecutó: %d", count.Load())
		}
	}
}

// 14. No backoff or proxy rotation occurs after the final attempt.
func TestAcceptance_14_NoBackoffOrProxyRotationAfterFinalAttempt(t *testing.T) {
	var count atomic.Int32
	var finalAttemptTime time.Time

	pool := createAcceptancePool(t, 2, func(req *fhttp.Request) (*fhttp.Response, error) {
		att := count.Add(1)
		if att == 3 {
			finalAttemptTime = time.Now()
		}
		return mockStatusResponse(http.StatusServiceUnavailable, "overloaded"), nil
	}, func(cfg *HTTPClient.Config) {
		// Backoff relativamente perceptible para verificar que NO se duerme tras el último intento
		cfg.Retry.WaitMin = 200 * time.Millisecond
		cfg.Retry.WaitMax = 500 * time.Millisecond
	})
	defer pool.Close()

	_, err := pool.Run(context.Background(), func(ctx context.Context, s *HTTPClient.Session) (*HTTPClient.Result, error) {
		return s.Do(ctx, HTTPClient.Spec{Method: "GET", URL: "https://example.com/test"})
	})
	elapsedAfterFinal := time.Since(finalAttemptTime)

	if err == nil {
		t.Fatal("Se esperaba error tras agotar intentos")
	}
	if count.Load() != 3 {
		t.Errorf("Se esperaban 3 intentos, pero fueron: %d", count.Load())
	}
	// Si hubiera esperado backoff tras el último intento, elapsedAfterFinal sería >= 200ms
	if elapsedAfterFinal >= 150*time.Millisecond {
		t.Errorf("Se detectó backoff innecesario tras el último intento: tardó %v", elapsedAfterFinal)
	}
}

// 15. Precedence of explicit options (spec.NoRetry, spec.MaxRetries, spec.MaxAttempts) is verified.
func TestAcceptance_15_PrecedenceOfExplicitOptions(t *testing.T) {
	// Subtest 1: Spec.NoRetry gana sobre todo -> 1 intento
	{
		var count atomic.Int32
		pool := createAcceptancePool(t, 3, func(req *fhttp.Request) (*fhttp.Response, error) {
			count.Add(1)
			return mockStatusResponse(http.StatusServiceUnavailable, "overloaded"), nil
		})
		_, _ = pool.Run(context.Background(), func(ctx context.Context, s *HTTPClient.Session) (*HTTPClient.Result, error) {
			return s.Do(ctx, HTTPClient.Spec{
				Method:     "GET",
				URL:        "https://example.com/test",
				NoRetry:    true,
				MaxRetries: 3,
			})
		})
		pool.Close()
		if count.Load() != 1 {
			t.Errorf("NoRetry debió forzar 1 solo intento, ejecutados: %d", count.Load())
		}
	}

	// Subtest 2: Spec.MaxRetries = 1 gana sobre Spec.MaxAttempts y pool config (MaxRetries:1 -> 2 intentos)
	{
		var count atomic.Int32
		pool := createAcceptancePool(t, 3, func(req *fhttp.Request) (*fhttp.Response, error) {
			count.Add(1)
			return mockStatusResponse(http.StatusServiceUnavailable, "overloaded"), nil
		})
		_, _ = pool.Run(context.Background(), func(ctx context.Context, s *HTTPClient.Session) (*HTTPClient.Result, error) {
			return s.Do(ctx, HTTPClient.Spec{
				Method:      "GET",
				URL:         "https://example.com/test",
				MaxRetries:  1,
				MaxAttempts: 5,
			})
		})
		pool.Close()
		if count.Load() != 2 {
			t.Errorf("Spec.MaxRetries=1 debió prevalecer (2 intentos), pero ejecutó: %d", count.Load())
		}
	}

	// Subtest 3: Spec.MaxAttempts = 3 gana sobre pool config (3 intentos)
	{
		var count atomic.Int32
		pool := createAcceptancePool(t, 1, func(req *fhttp.Request) (*fhttp.Response, error) {
			count.Add(1)
			return mockStatusResponse(http.StatusServiceUnavailable, "overloaded"), nil
		})
		_, _ = pool.Run(context.Background(), func(ctx context.Context, s *HTTPClient.Session) (*HTTPClient.Result, error) {
			return s.Do(ctx, HTTPClient.Spec{
				Method:      "GET",
				URL:         "https://example.com/test",
				MaxAttempts: 3,
			})
		})
		pool.Close()
		if count.Load() != 3 {
			t.Errorf("Spec.MaxAttempts=3 debió prevalecer (3 intentos), pero ejecutó: %d", count.Load())
		}
	}
}
