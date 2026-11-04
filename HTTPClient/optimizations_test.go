package HTTPClient

import (
	"context"
	"sync"
	"testing"

	"github.com/LeanCoder0718/Universal/GeoRoute"
	fhttp "github.com/bogdanfinn/fhttp"
)

// 1. Test del Jar con atomic.Uint64 y caché de serialización
func TestOptimization_Jar_AtomicGeneration(t *testing.T) {
	jar := NewJAR(CookieConfig{})
	if jar.Generation() != 0 {
		t.Fatalf("generación inicial esperada 0, obtenida: %d", jar.Generation())
	}

	jar.SetCookies("example.com", "/api", []*fhttp.Cookie{
		{Name: "session", Value: "val1"},
	})
	if jar.Generation() != 1 {
		t.Fatalf("generación esperada 1 tras SetCookies, obtenida: %d", jar.Generation())
	}

	jar.AddManual("example.com", "/api", Cookie{Name: "manual", Value: "val2"})
	if jar.Generation() != 2 {
		t.Fatalf("generación esperada 2 tras AddManual, obtenida: %d", jar.Generation())
	}

	jar.Clear()
	if jar.Generation() != 3 {
		t.Fatalf("generación esperada 3 tras Clear, obtenida: %d", jar.Generation())
	}

	jar.Reset()
	if jar.Generation() != 4 {
		t.Fatalf("generación esperada 4 tras Reset, obtenida: %d", jar.Generation())
	}
}

// 2. Test de Proxy Pool Acquire y Gateway Lock-Free bajo alta concurrencia
func TestOptimization_ProxyPool_LockFreeAcquireAndGateway(t *testing.T) {
	// 1 nodo (gateway residencial)
	pool, err := NewPool([]string{"user:pass@127.0.0.1:8080"}, ProxyConfig{})
	if err != nil {
		t.Fatalf("error creando pool: %v", err)
	}

	const goroutines = 100
	const opsPerGoroutine = 100
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				node, release, aErr := pool.Acquire(context.Background(), AcquirePolicy{})
				if aErr != nil || node == nil {
					t.Errorf("Acquire falló: %v", aErr)
					return
				}
				gw, gErr := pool.Gateway()
				if gErr != nil || gw.Host != "127.0.0.1" {
					t.Errorf("Gateway falló: %v", gErr)
					return
				}
				release(true, nil)
			}
		}()
	}

	wg.Wait()

	metrics := pool.Metrics()
	if len(metrics) != 1 {
		t.Fatalf("esperado 1 nodo en métricas, obtenido: %d", len(metrics))
	}
	expectedOps := uint64(goroutines * opsPerGoroutine)
	if metrics[0].Successes != expectedOps {
		t.Fatalf("éxitos esperados %d, obtenidos: %d", expectedOps, metrics[0].Successes)
	}
}

// 3. Test de ConnectionKey compartida cuando Sticky=false
func TestOptimization_ConnectionManager_KeyReuseWithoutSticky(t *testing.T) {
	cfg := Default()
	cfg.Proxy.Sticky = false

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

	// Ambas sesiones tienen stickyIDs distintos
	if sess1.StickyID() == sess2.StickyID() {
		t.Fatalf("stickyIDs no deben ser iguales entre sesiones independientes")
	}

	// Con Sticky=false, la clave de conexión hacia el mismo país no incluye SessionID
	key1 := ConnectionKey{
		SessionID:   "",
		CountryCode: "MX",
		RouteID:     "default",
		Protocol:    sess1.Profile().Protocol,
	}.Normalized()

	key2 := ConnectionKey{
		SessionID:   "",
		CountryCode: "MX",
		RouteID:     "default",
		Protocol:    sess2.Profile().Protocol,
	}.Normalized()

	if key1 != key2 {
		t.Fatalf("claves de conexión para MX deberían ser idénticas para compartir Keep-Alive: %+v vs %+v", key1, key2)
	}
}

// 4. Test de GeoPolicy aislada de variables globales
func TestOptimization_GeoPolicy_IsolatedInstance(t *testing.T) {
	customCfg := GeoRoute.ProxyProviderConfig{
		RegionTemplate:    "-custom-{country}",
		CountryPlaceholder: "{country}",
		LowercaseCountry:  true,
	}
	policy := GeoRoute.NewGeoPolicy(true, customCfg)

	// Modificar variable global a false
	orig := GeoRoute.ActiveApplyGeo
	defer func() { GeoRoute.ActiveApplyGeo = orig }()
	GeoRoute.ActiveApplyGeo = false

	user := "myuser"
	adapted := policy.Apply(user, "ES")
	expected := "myuser-custom-es"

	if adapted != expected {
		t.Fatalf("GeoPolicy aislada debió aplicar '%s', obtuvo '%s' (posible fuga a variable global)", expected, adapted)
	}
}
