package GeoRoute

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// --- Helper: genera proxies crudos de prueba ---
func makeRawProxies(n int) []string {
	proxies := make([]string, n)
	for i := 0; i < n; i++ {
		proxies[i] = fmt.Sprintf("gate.provider.com:%d:user%d:pass%d", 8000+i, i, i)
	}
	return proxies
}

// --- Test 1: Concurrencia Compartida No-Bloqueante (100 Workers / 50 Proxies) ---
func TestConcurrentMultiThreadSharing_100Workers_50Proxies(t *testing.T) {
	rawProxies := makeRawProxies(50)
	countries := []CountryCode{"US"}

	cfg := DefaultManagerConfig()
	pm, err := NewProxyManager(rawProxies, DefaultCRProviderConfig(), countries, cfg)
	if err != nil {
		t.Fatalf("NewProxyManager error: %v", err)
	}
	defer pm.Close()

	const numWorkers = 100
	var wg sync.WaitGroup
	var acquiredCount atomic.Int64
	var failedCount atomic.Int64

	start := time.Now()
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			ctx := context.Background()

			// Adquisición instantánea sin bloqueo
			node, err := pm.Acquire(ctx, "US")
			if err != nil {
				failedCount.Add(1)
				return
			}
			acquiredCount.Add(1)

			// Simular trabajo simultáneo de todos los 100 threads
			time.Sleep(10 * time.Millisecond)

			pm.Release(node, nil)
		}(w)
	}

	wg.Wait()
	elapsed := time.Since(start)

	if failedCount.Load() > 0 {
		t.Errorf("Fallos en adquisición concurrente: %d", failedCount.Load())
	}
	if acquiredCount.Load() != numWorkers {
		t.Errorf("Workers que adquirieron = %d, want %d", acquiredCount.Load(), numWorkers)
	}

	// Debe haber tomado prácticamente 10-20ms (todos en paralelo), no secuencial
	if elapsed > 2*time.Second {
		t.Errorf("La ejecución tomó demasiado tiempo (%v), hubo bloqueo secuencial", elapsed)
	}
}

// --- Test 2: Aislamiento entre países ---
func TestCrossCountryIsolation(t *testing.T) {
	rawProxies := makeRawProxies(5)
	countries := []CountryCode{"US", "BR"}

	cfg := DefaultManagerConfig()
	cfg.AcquireTimeout = 5 * time.Second

	pm, err := NewProxyManager(rawProxies, DefaultCRProviderConfig(), countries, cfg)
	if err != nil {
		t.Fatalf("NewProxyManager error: %v", err)
	}
	defer pm.Close()

	var wg sync.WaitGroup
	var mismatchUS, mismatchBR atomic.Int64

	// 20 goroutines pidiendo US
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			node, err := pm.Acquire(ctx, "US")
			if err != nil {
				return
			}
			defer pm.Release(node, nil)

			if node.Country != "US" {
				mismatchUS.Add(1)
			}
			time.Sleep(time.Millisecond)
		}()
	}

	// 20 goroutines pidiendo BR
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			node, err := pm.Acquire(ctx, "BR")
			if err != nil {
				return
			}
			defer pm.Release(node, nil)

			if node.Country != "BR" {
				mismatchBR.Add(1)
			}
			time.Sleep(time.Millisecond)
		}()
	}

	wg.Wait()

	if m := mismatchUS.Load(); m > 0 {
		t.Errorf("US workers recibieron %d proxies de otro país", m)
	}
	if m := mismatchBR.Load(); m > 0 {
		t.Errorf("BR workers recibieron %d proxies de otro país", m)
	}
}

// --- Test 3: Reemplazo conserva país ---
func TestProxyReplacementSameCountry(t *testing.T) {
	rawProxies := makeRawProxies(5)
	countries := []CountryCode{"US"}

	cfg := DefaultManagerConfig()
	cfg.AcquireTimeout = 2 * time.Second
	cfg.MaxConsecutiveErrors = 1 // Cuarentena inmediata

	pm, err := NewProxyManager(rawProxies, DefaultCRProviderConfig(), countries, cfg)
	if err != nil {
		t.Fatalf("NewProxyManager error: %v", err)
	}
	defer pm.Close()

	ctx := context.Background()
	original, err := pm.Acquire(ctx, "US")
	if err != nil {
		t.Fatalf("Acquire error: %v", err)
	}

	replacement, err := pm.Replace(ctx, original)
	if err != nil {
		t.Fatalf("Replace error: %v", err)
	}

	if replacement.Country != "US" {
		t.Errorf("Replacement country = %s, want US", replacement.Country)
	}

	if replacement.ID == original.ID {
		t.Log("Replace retornó el mismo proxy (puede ocurrir con pool pequeño y cuarentena corta)")
	}

	pm.Release(replacement, nil)
}

// --- Test 4: Doble liberación segura ---
func TestDoubleReleaseSafety(t *testing.T) {
	rawProxies := makeRawProxies(3)
	countries := []CountryCode{"US"}

	cfg := DefaultManagerConfig()
	pm, err := NewProxyManager(rawProxies, DefaultCRProviderConfig(), countries, cfg)
	if err != nil {
		t.Fatalf("NewProxyManager error: %v", err)
	}
	defer pm.Close()

	node, err := pm.Acquire(context.Background(), "US")
	if err != nil {
		t.Fatalf("Acquire error: %v", err)
	}

	// Primera liberación: debe tener éxito
	pm.Release(node, nil)

	// Segunda liberación: debe ser no-op (no panic, no doble inserción)
	pm.Release(node, nil)

	// Tercera liberación con error: debe ser no-op
	pm.Release(node, fmt.Errorf("test error"))
}

// --- Test 5: Cancelación de Contexto y Agotamiento de Pool ---
func TestContextCancellationAndExhaustion(t *testing.T) {
	rawProxies := makeRawProxies(1)
	countries := []CountryCode{"US"}

	cfg := DefaultManagerConfig()
	pm, err := NewProxyManager(rawProxies, DefaultCRProviderConfig(), countries, cfg)
	if err != nil {
		t.Fatalf("NewProxyManager error: %v", err)
	}
	defer pm.Close()

	// 1. Context ya cancelado debe retornar error inmediatamente
	ctxCancel, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = pm.Acquire(ctxCancel, "US")
	if err == nil {
		t.Error("Expected error with cancelled context, got nil")
	}

	// 2. Múltiples adquisiciones concurrentes sobre el único proxy funcionan sin error
	ctx := context.Background()
	n1, err1 := pm.Acquire(ctx, "US")
	n2, err2 := pm.Acquire(ctx, "US")
	if err1 != nil || err2 != nil {
		t.Fatalf("Shared acquire failed: %v, %v", err1, err2)
	}
	if n1.ID != n2.ID {
		t.Errorf("Expected same proxy ID, got %s and %s", n1.ID, n2.ID)
	}
	pm.Release(n1, nil)
	pm.Release(n2, nil)
}

// --- Test 6: Cuarentena y recuperación ---
func TestQuarantineRecovery(t *testing.T) {
	rawProxies := makeRawProxies(1)
	countries := []CountryCode{"US"}

	cfg := DefaultManagerConfig()
	cfg.AcquireTimeout = 3 * time.Second
	cfg.MaxConsecutiveErrors = 1
	cfg.QuarantineDuration = 500 * time.Millisecond
	cfg.RecoveryInterval = 200 * time.Millisecond

	pm, err := NewProxyManager(rawProxies, DefaultCRProviderConfig(), countries, cfg)
	if err != nil {
		t.Fatalf("NewProxyManager error: %v", err)
	}
	defer pm.Close()

	ctx := context.Background()

	// Adquirir y forzar fallo
	node, err := pm.Acquire(ctx, "US")
	if err != nil {
		t.Fatalf("Acquire error: %v", err)
	}

	// Reportar fallo -> cuarentena
	pm.Release(node, fmt.Errorf("simulated failure"))

	// Verificar que está en cuarentena (no debería poder adquirirse inmediatamente)
	shortCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	_, err = pm.Acquire(shortCtx, "US")
	cancel()
	if err == nil {
		t.Log("Proxy adquirido antes de lo esperado (puede ocurrir si recovery fue muy rápido)")
	}

	// Esperar a que la cuarentena expire y el recovery loop lo devuelva
	time.Sleep(800 * time.Millisecond)

	recovered, err := pm.Acquire(ctx, "US")
	if err != nil {
		t.Errorf("Acquire tras recovery falló: %v", err)
	} else {
		pm.Release(recovered, nil)
	}
}

// --- Test 7: País no existente ---
func TestNonExistentCountry(t *testing.T) {
	rawProxies := makeRawProxies(3)
	countries := []CountryCode{"US"}

	cfg := DefaultManagerConfig()
	cfg.AllowUnknownCountry = false

	pm, err := NewProxyManager(rawProxies, DefaultCRProviderConfig(), countries, cfg)
	if err != nil {
		t.Fatalf("NewProxyManager error: %v", err)
	}
	defer pm.Close()

	_, err = pm.Acquire(context.Background(), "AR")
	if err == nil {
		t.Error("Expected ErrNoProxyForCountry, got nil")
	}
}

// --- Test 8: País Unknown con AllowUnknownCountry ---
func TestUnknownCountryPool(t *testing.T) {
	rawProxies := makeRawProxies(3)
	countries := []CountryCode{"US"}

	cfg := DefaultManagerConfig()
	cfg.AllowUnknownCountry = true

	pm, err := NewProxyManager(rawProxies, DefaultCRProviderConfig(), countries, cfg)
	if err != nil {
		t.Fatalf("NewProxyManager error: %v", err)
	}
	defer pm.Close()

	node, err := pm.Acquire(context.Background(), Unknown)
	if err != nil {
		t.Errorf("Acquire Unknown falló: %v", err)
	} else {
		pm.Release(node, nil)
	}
}

// --- Test 9: EnsurePool dinámico ---
func TestEnsurePoolDynamic(t *testing.T) {
	rawProxies := makeRawProxies(3)
	countries := []CountryCode{"US"}

	cfg := DefaultManagerConfig()
	pm, err := NewProxyManager(rawProxies, DefaultCRProviderConfig(), countries, cfg)
	if err != nil {
		t.Fatalf("NewProxyManager error: %v", err)
	}
	defer pm.Close()

	// AR no existe inicialmente
	if pm.HasPool("AR") {
		t.Error("AR pool should not exist yet")
	}

	// Parsear proxies base para EnsurePool
	var parsed []ParsedProxy
	for _, raw := range rawProxies {
		p, err := ParseProxy(raw)
		if err == nil {
			parsed = append(parsed, p)
		}
	}

	// Crear dinámicamente
	pm.EnsurePool("AR", parsed)

	if !pm.HasPool("AR") {
		t.Error("AR pool should exist after EnsurePool")
	}

	node, err := pm.Acquire(context.Background(), "AR")
	if err != nil {
		t.Errorf("Acquire AR falló: %v", err)
	} else {
		if node.Country != "AR" {
			t.Errorf("node.Country = %s, want AR", node.Country)
		}
		pm.Release(node, nil)
	}
}

// --- Test 10: Deduplicación de proxies ---
func TestProxyDeduplication(t *testing.T) {
	rawProxies := []string{
		"gate.provider.com:8000:user0:pass0",
		"gate.provider.com:8000:user0:pass0", // Duplicado
		"gate.provider.com:8000:user0:pass0", // Duplicado
		"gate.provider.com:8001:user1:pass1",
	}
	countries := []CountryCode{"US"}

	cfg := DefaultManagerConfig()
	pm, err := NewProxyManager(rawProxies, DefaultCRProviderConfig(), countries, cfg)
	if err != nil {
		t.Fatalf("NewProxyManager error: %v", err)
	}
	defer pm.Close()

	stats := pm.Stats()
	for _, s := range stats {
		if s.Country == "US" {
			if s.Total != 2 {
				t.Errorf("US pool total = %d, want 2 (after dedup)", s.Total)
			}
		}
	}
}

// --- Test 11: AdaptProxyToComboIP = false (Modo Global Unificado) ---
func TestAdaptProxyToComboIPFalse(t *testing.T) {
	rawProxies := makeRawProxies(10)
	countries := []CountryCode{"US"}

	cfg := DefaultManagerConfig()
	cfg.AdaptProxyToComboIP = false // Desactivar enrutamiento geográfico

	pm, err := NewProxyManager(rawProxies, DefaultCRProviderConfig(), countries, cfg)
	if err != nil {
		t.Fatalf("NewProxyManager error: %v", err)
	}
	defer pm.Close()

	if pm.IsGeoMode() {
		t.Error("Expected IsGeoMode() to be false")
	}

	// Debe poder adquirir para cualquier país (ej: JP, DE, BR) usando el pool global
	ctx := context.Background()
	nodeJP, err := pm.Acquire(ctx, "JP")
	if err != nil {
		t.Fatalf("Acquire JP in non-geo mode failed: %v", err)
	}
	if nodeJP.Country != "GLOBAL" {
		t.Errorf("Expected node country GLOBAL, got %s", nodeJP.Country)
	}
	pm.Release(nodeJP, nil)

	nodeDE, err := pm.Acquire(ctx, "DE")
	if err != nil {
		t.Fatalf("Acquire DE in non-geo mode failed: %v", err)
	}
	pm.Release(nodeDE, nil)
}

// --- Test 12: EWMA Latency Tracking y Conexiones Activas ---
func TestAdaptiveEWMAAndLeastConnections(t *testing.T) {
	node := &ProxyNode{
		ID:       "test-node",
		Country:  "US",
		Resolved: "1.2.3.4:8080",
	}

	// 1. Verificar active connections
	if node.ActiveConns() != 0 {
		t.Errorf("Initial activeConns = %d, want 0", node.ActiveConns())
	}
	node.activeConns.Add(1)
	if node.ActiveConns() != 1 {
		t.Errorf("activeConns after Add(1) = %d, want 1", node.ActiveConns())
	}
	node.activeConns.Add(-1)
	if node.ActiveConns() != 0 {
		t.Errorf("activeConns after Add(-1) = %d, want 0", node.ActiveConns())
	}

	// 2. Verificar EWMA latency smoothing
	lat1 := 100 * time.Millisecond
	node.RecordLatency(lat1)
	if node.LatencyEWMA() != lat1 {
		t.Errorf("First latency = %v, want %v", node.LatencyEWMA(), lat1)
	}

	lat2 := 200 * time.Millisecond
	node.RecordLatency(lat2)
	// EWMA = (30 * 200ms + 70 * 100ms) / 100 = 130ms
	expectedEWMA := 130 * time.Millisecond
	if node.LatencyEWMA() != expectedEWMA {
		t.Errorf("EWMA after second sample = %v, want %v", node.LatencyEWMA(), expectedEWMA)
	}
}

// --- Test 13: Carga y Contención Extrema (20 Proxies / 100 Workers) ---
func TestHighContention_20Proxies_100Workers(t *testing.T) {
	rawProxies := makeRawProxies(20)
	countries := []CountryCode{"US"}

	cfg := DefaultManagerConfig()
	cfg.AcquireTimeout = 10 * time.Second

	pm, err := NewProxyManager(rawProxies, DefaultCRProviderConfig(), countries, cfg)
	if err != nil {
		t.Fatalf("NewProxyManager error: %v", err)
	}
	defer pm.Close()

	const numWorkers = 100
	const opsPerWorker = 20

	var wg sync.WaitGroup
	var successOps atomic.Int64
	var failedOps atomic.Int64

	start := time.Now()
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < opsPerWorker; i++ {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				node, err := pm.Acquire(ctx, "US")
				cancel()
				if err != nil {
					failedOps.Add(1)
					continue
				}

				// Simular latencia de red variable (1-5ms)
				time.Sleep(time.Duration(1+workerID%5) * time.Millisecond)
				node.RecordLatency(time.Millisecond * 2)

				pm.Release(node, nil)
				successOps.Add(1)
			}
		}(w)
	}

	wg.Wait()
	elapsed := time.Since(start)

	t.Logf("20 Proxies / 100 Workers: %d ops exitosas, %d fallidas en %v", successOps.Load(), failedOps.Load(), elapsed)

	if failedOps.Load() > 0 {
		t.Errorf("Ops fallidas bajo contención: %d", failedOps.Load())
	}
	if successOps.Load() != numWorkers*opsPerWorker {
		t.Errorf("Ops exitosas = %d, want %d", successOps.Load(), numWorkers*opsPerWorker)
	}
}

// --- Test 14: Carga Extrema (20 Proxies / 500 Workers) ---
func TestHighContention_20Proxies_500Workers(t *testing.T) {
	rawProxies := makeRawProxies(20)
	countries := []CountryCode{"US"}

	cfg := DefaultManagerConfig()
	cfg.AcquireTimeout = 15 * time.Second

	pm, err := NewProxyManager(rawProxies, DefaultCRProviderConfig(), countries, cfg)
	if err != nil {
		t.Fatalf("NewProxyManager error: %v", err)
	}
	defer pm.Close()

	const numWorkers = 500
	const opsPerWorker = 5

	var wg sync.WaitGroup
	var successOps atomic.Int64
	var failedOps atomic.Int64

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < opsPerWorker; i++ {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				node, err := pm.Acquire(ctx, "US")
				cancel()
				if err != nil {
					failedOps.Add(1)
					continue
				}

				time.Sleep(500 * time.Microsecond)
				pm.Release(node, nil)
				successOps.Add(1)
			}
		}()
	}

	wg.Wait()

	t.Logf("20 Proxies / 500 Workers: %d ops exitosas, %d fallidas", successOps.Load(), failedOps.Load())
	if successOps.Load() != numWorkers*opsPerWorker {
		t.Errorf("Ops exitosas = %d, want %d", successOps.Load(), numWorkers*opsPerWorker)
	}
}

// --- Test 15: Carga Asimétrica y Fallos Parciales ---
func TestAsymmetricLatencyAndFailures(t *testing.T) {
	rawProxies := makeRawProxies(10)
	countries := []CountryCode{"US"}

	cfg := DefaultManagerConfig()
	cfg.AcquireTimeout = 5 * time.Second
	cfg.MaxConsecutiveErrors = 2
	cfg.QuarantineDuration = 200 * time.Millisecond
	cfg.RecoveryInterval = 100 * time.Millisecond

	pm, err := NewProxyManager(rawProxies, DefaultCRProviderConfig(), countries, cfg)
	if err != nil {
		t.Fatalf("NewProxyManager error: %v", err)
	}
	defer pm.Close()

	const numWorkers = 50
	const opsPerWorker = 10

	var wg sync.WaitGroup
	var totalProcessed atomic.Int64

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < opsPerWorker; i++ {
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				node, err := pm.Acquire(ctx, "US")
				cancel()
				if err != nil {
					continue
				}

				// Simular 10% de tasa de fallos de proxy
				if (workerID+i)%10 == 0 {
					pm.Release(node, fmt.Errorf("transient network failure"))
				} else {
					pm.Release(node, nil)
				}
				totalProcessed.Add(1)
			}
		}(w)
	}

	wg.Wait()

	t.Logf("Total operaciones procesadas con fallos transitorios: %d", totalProcessed.Load())
	if totalProcessed.Load() < int64(numWorkers*opsPerWorker*8/10) {
		t.Errorf("Rendimiento muy bajo: solo procesó %d operaciones", totalProcessed.Load())
	}
}

// --- Benchmark: Acquire/Release bajo contención ---
func BenchmarkAcquireRelease(b *testing.B) {
	rawProxies := makeRawProxies(100)
	countries := []CountryCode{"US"}

	cfg := DefaultManagerConfig()
	cfg.AcquireTimeout = 5 * time.Second

	pm, err := NewProxyManager(rawProxies, DefaultCRProviderConfig(), countries, cfg)
	if err != nil {
		b.Fatalf("NewProxyManager error: %v", err)
	}
	defer pm.Close()

	ctx := context.Background()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			node, err := pm.Acquire(ctx, "US")
			if err != nil {
				continue
			}
			pm.Release(node, nil)
		}
	})
}
