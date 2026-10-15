package HTTPClient

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
)

func newTestProxyPool(t *testing.T, count int) *Pool {
	t.Helper()
	raw := make([]string, count)
	for i := 0; i < count; i++ {
		raw[i] = fmt.Sprintf("10.0.0.%d:8080:user%d:pass%d", i+1, i+1, i+1)
	}
	cfg := Default().Proxy
	cfg.Enabled = true
	cfg.ProxyType = "http"
	p, err := NewPool(raw, cfg)
	if err != nil {
		t.Fatalf("falló creando pool de prueba: %v", err)
	}
	return p
}

// Test 1: Petición exitosa al intento 1 -> 0 reintentos, proxy reportada exitosa.
func TestProxyPool_Test1_SingleSuccess(t *testing.T) {
	pool := newTestProxyPool(t, 5)

	node, release, err := pool.Acquire(context.Background(), AcquirePolicy{AllowFallback: true})
	if err != nil {
		t.Fatalf("Acquire falló: %v", err)
	}
	if node == nil {
		t.Fatal("node retornado es nil")
	}

	release(true, nil)

	ok, fail := pool.Counters()
	if ok != 1 || fail != 0 {
		t.Fatalf("esperado ok=1, fail=0; obtenido ok=%d, fail=%d", ok, fail)
	}
	if node.Status != StatusHealthy {
		t.Fatalf("esperado StatusHealthy, obtenido %v", node.Status)
	}
}

// Test 2: Intento 1 falla con timeout -> Intento 2 utiliza obligatoriamente una proxy distinta (Proxy B != Proxy A).
func TestProxyPool_Test2_RotateOnFailure(t *testing.T) {
	pool := newTestProxyPool(t, 5)

	// Intento 1
	node1, release1, err := pool.Acquire(context.Background(), AcquirePolicy{AllowFallback: true})
	if err != nil {
		t.Fatalf("intento 1 falló al adquirir: %v", err)
	}
	p1Host := node1.Entry.Host
	release1(false, errors.New("i/o timeout"))

	// Intento 2 excluyendo p1Host
	node2, release2, err := pool.Acquire(context.Background(), AcquirePolicy{
		ExcludedHosts: []string{p1Host},
		AllowFallback: true,
	})
	if err != nil {
		t.Fatalf("intento 2 falló al adquirir: %v", err)
	}
	p2Host := node2.Entry.Host
	release2(true, nil)

	if p2Host == p1Host {
		t.Fatalf("CRÍTICO: Intento 2 reutilizó la misma proxy (%s) en lugar de rotar", p1Host)
	}
}

// Test 3: Dos reintentos consecutivos con pool disponible -> 3 intentos utilizan 3 proxies diferentes (A -> B -> C).
func TestProxyPool_Test3_ThreeDistinctProxies(t *testing.T) {
	pool := newTestProxyPool(t, 5)

	var usedHosts []string

	for attempt := 1; attempt <= 3; attempt++ {
		node, release, err := pool.Acquire(context.Background(), AcquirePolicy{
			ExcludedHosts: usedHosts,
			AllowFallback: true,
		})
		if err != nil {
			t.Fatalf("intento %d falló al adquirir: %v", attempt, err)
		}

		for _, prev := range usedHosts {
			if node.Entry.Host == prev {
				t.Fatalf("CRÍTICO: Intento %d repitió el proxy %s ya usado en intentos previos (%v)", attempt, node.Entry.Host, usedHosts)
			}
		}

		usedHosts = append(usedHosts, node.Entry.Host)
		if attempt < 3 {
			release(false, errors.New("connection reset by peer"))
		} else {
			release(true, nil)
		}
	}

	if len(usedHosts) != 3 {
		t.Fatalf("esperadas 3 proxies distintas, obtenidas: %v", usedHosts)
	}
}

// Test 4: Proxy residencial único -> errores 403 o fallos NUNCA lo congelan ni bloquean a los demás workers.
func TestProxyPool_Test4_SingleResidentialNeverFrozen(t *testing.T) {
	pool := newTestProxyPool(t, 1) // 1 solo endpoint residencial rotativo

	// Worker 1 falla con 403 CloudFront
	node1, release1, err := pool.Acquire(context.Background(), AcquirePolicy{AllowFallback: true})
	if err != nil {
		t.Fatalf("falló adquiriendo proxy residencial: %v", err)
	}
	release1(false, errors.New("HTTP 403 Forbidden: CloudFront"))

	// Verificar que NO esté en cooldown y siga completamente operativo
	if node1.Status != StatusHealthy {
		t.Fatalf("el proxy residencial rotativo NO debe pasar a Cooldown, estado actual: %v", node1.Status)
	}

	// Worker 2 (u otro intento) solicita proxy inmediatamente: DEBE obtener el endpoint sin error
	node2, release2, err := pool.Acquire(context.Background(), AcquirePolicy{
		ExcludedHosts: []string{node1.Entry.Host},
		AllowFallback: true,
	})
	if err != nil {
		t.Fatalf("el pool no debe bloquear a los demás workers en proxy residencial: %v", err)
	}
	defer release2(true, nil)

	if node2 == nil {
		t.Fatal("node retornado es nil")
	}
}

// Test 5: Pool limitado (2 proxies, 4 retries) -> rotación alternada sin deadlock ni bloqueo infinito.
func TestProxyPool_Test5_SmallPoolFallback(t *testing.T) {
	pool := newTestProxyPool(t, 2) // Solo 2 proxies

	var usedHosts []string
	for attempt := 1; attempt <= 4; attempt++ {
		node, release, err := pool.Acquire(context.Background(), AcquirePolicy{
			ExcludedHosts: usedHosts,
			AllowFallback: true, // Permitir fallback cuando todos estén excluidos
		})
		if err != nil {
			t.Fatalf("intento %d falló con fallback activado: %v", attempt, err)
		}

		usedHosts = append(usedHosts, node.Entry.Host)
		release(false, errors.New("timeout"))
	}

	if len(usedHosts) != 4 {
		t.Fatalf("esperados 4 intentos completados, obtenidos %d: %v", len(usedHosts), usedHosts)
	}
}

// Test 6: Pool agotado (todas las proxies excluidas) y AllowFallback=false -> ErrNoProxiesAvailable.
func TestProxyPool_Test6_ExhaustedPoolError(t *testing.T) {
	pool := newTestProxyPool(t, 2)

	_, _, err := pool.Acquire(context.Background(), AcquirePolicy{
		ExcludedHosts: []string{"10.0.0.1", "10.0.0.2"},
		AllowFallback: false,
	})

	if !errors.Is(err, ErrNoProxiesAvailable) {
		t.Fatalf("esperado ErrNoProxiesAvailable, obtenido: %v", err)
	}
}

// Test 8: Concurrencia masiva (50 goroutines concurrentes) -> cero data races.
func TestProxyPool_Test8_ConcurrentRaceStress(t *testing.T) {
	pool := newTestProxyPool(t, 10)

	var wg sync.WaitGroup
	workers := 50
	iterations := 40

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				excluded := []string{}
				if j%2 == 1 {
					excluded = append(excluded, fmt.Sprintf("10.0.0.%d", (workerID%10)+1))
				}
				_, release, err := pool.Acquire(context.Background(), AcquirePolicy{
					ExcludedHosts: excluded,
					AllowFallback: true,
				})
				if err != nil {
					continue
				}
				success := (workerID+j)%3 != 0
				var reportErr error
				if !success {
					reportErr = errors.New("502 bad gateway")
				}
				release(success, reportErr)
			}
		}(i)
	}

	wg.Wait()
	ok, fail := pool.Counters()
	if ok+fail == 0 {
		t.Fatal("no se registraron operaciones en los contadores del pool")
	}
}
