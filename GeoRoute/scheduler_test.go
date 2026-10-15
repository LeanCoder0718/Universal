package GeoRoute

import (
	"fmt"
	"sync"
	"testing"
)

// TEST 1: PE = 100, US = 60, XX = 40 -> Expected: PE -> US -> XX
func TestGeoScheduler_LargestGroupFirst_Test1(t *testing.T) {
	lines := make([]string, 0, 200)
	for i := 0; i < 100; i++ {
		lines = append(lines, fmt.Sprintf("movistarplay.pe:user%d@gmail.com:pass%d", i, i))
	}
	for i := 0; i < 60; i++ {
		lines = append(lines, fmt.Sprintf("service.us:user%d@yahoo.com:pass%d", i, i))
	}
	for i := 0; i < 40; i++ {
		lines = append(lines, fmt.Sprintf("unknown_domain.xyz:user%d@xyz.com:pass%d", i, i))
	}

	sched := NewGeoScheduler(nil, DefaultSchedulerConfig())
	reordered := sched.ReorderCombos(lines)

	if len(reordered) != 200 {
		t.Fatalf("expected 200 reordered items, got %d", len(reordered))
	}

	orders := sched.GetProcessingOrder()
	if len(orders) != 3 {
		t.Fatalf("expected 3 groups, got %d", len(orders))
	}

	if orders[0].Country != CountryCode("PE") || orders[0].Total != 100 {
		t.Errorf("pos 1 expected PE(100), got %s(%d)", orders[0].Country, orders[0].Total)
	}
	if orders[1].Country != CountryCode("US") || orders[1].Total != 60 {
		t.Errorf("pos 2 expected US(60), got %s(%d)", orders[1].Country, orders[1].Total)
	}
	if !orders[2].Country.IsUnknown() || orders[2].Total != 40 {
		t.Errorf("pos 3 expected XX(40), got %s(%d)", orders[2].Country, orders[2].Total)
	}
}

// TEST 2: PE = 10, US = 100, XX = 20 -> Expected: US -> XX -> PE
func TestGeoScheduler_LargestGroupFirst_Test2(t *testing.T) {
	lines := make([]string, 0, 130)
	for i := 0; i < 10; i++ {
		lines = append(lines, fmt.Sprintf("movistarplay.pe:user%d@gmail.com:pass%d", i, i))
	}
	for i := 0; i < 100; i++ {
		lines = append(lines, fmt.Sprintf("service.us:user%d@yahoo.com:pass%d", i, i))
	}
	for i := 0; i < 20; i++ {
		lines = append(lines, fmt.Sprintf("unknown.xyz:user%d@xyz.com:pass%d", i, i))
	}

	sched := NewGeoScheduler(nil, DefaultSchedulerConfig())
	_ = sched.ReorderCombos(lines)

	orders := sched.GetProcessingOrder()
	if len(orders) != 3 {
		t.Fatalf("expected 3 groups, got %d", len(orders))
	}

	if orders[0].Country != CountryCode("US") || orders[0].Total != 100 {
		t.Errorf("pos 1 expected US(100), got %s(%d)", orders[0].Country, orders[0].Total)
	}
	if !orders[1].Country.IsUnknown() || orders[1].Total != 20 {
		t.Errorf("pos 2 expected XX(20), got %s(%d)", orders[1].Country, orders[1].Total)
	}
	if orders[2].Country != CountryCode("PE") || orders[2].Total != 10 {
		t.Errorf("pos 3 expected PE(10), got %s(%d)", orders[2].Country, orders[2].Total)
	}
}

// TEST 3: Anti-Starvation test: PE = 1000, US = 1, XX = 1 with FairShare enabled.
// US and XX MUST appear early in the interleaved result rather than waiting at the very end.
func TestGeoScheduler_FairShare_AntiStarvation(t *testing.T) {
	lines := make([]string, 0, 1002)
	for i := 0; i < 1000; i++ {
		lines = append(lines, fmt.Sprintf("movistarplay.pe:pe%d@gmail.com:pass", i))
	}
	lines = append(lines, "service.us:us1@yahoo.com:pass")
	lines = append(lines, "unknown.xyz:xx1@xyz.com:pass")

	cfg := DefaultSchedulerConfig()
	cfg.Mode = ModeFairShare
	cfg.Quantum = 10 // cada 10 de PE, da turno a US y XX

	sched := NewGeoScheduler(nil, cfg)
	reordered := sched.ReorderCombos(lines)

	if len(reordered) != 1002 {
		t.Fatalf("expected 1002 items, got %d", len(reordered))
	}

	// Comprobar que US y XX están en las primeras 20 posiciones
	foundUS := false
	foundXX := false
	for i := 0; i < 20; i++ {
		if reordered[i] == "service.us:us1@yahoo.com:pass" {
			foundUS = true
		}
		if reordered[i] == "unknown.xyz:xx1@xyz.com:pass" {
			foundXX = true
		}
	}

	if !foundUS || !foundXX {
		t.Fatalf("starvation detected: US or XX was not interleaved in the first 20 items (foundUS=%v, foundXX=%v)", foundUS, foundXX)
	}
}

// TEST 4: Todos los grupos iguales: PE = 50, US = 50, ES = 50
func TestGeoScheduler_EqualSizeGroups(t *testing.T) {
	lines := make([]string, 0, 150)
	for i := 0; i < 50; i++ {
		lines = append(lines, fmt.Sprintf("movistarplay.pe:user%d@gmail.com:pass", i))
		lines = append(lines, fmt.Sprintf("service.us:user%d@yahoo.com:pass", i))
		lines = append(lines, fmt.Sprintf("vod.es:user%d@terra.es:pass", i))
	}

	sched := NewGeoScheduler(nil, DefaultSchedulerConfig())
	reordered := sched.ReorderCombos(lines)

	if len(reordered) != 150 {
		t.Fatalf("expected 150 items, got %d", len(reordered))
	}
}

// TEST 5: Solo UNKNOWN
func TestGeoScheduler_OnlyUnknown(t *testing.T) {
	lines := []string{
		"unknown1.xyz:user1@xyz.com:pass",
		"unknown2.xyz:user2@xyz.com:pass",
		"unknown3.xyz:user3@xyz.com:pass",
	}

	sched := NewGeoScheduler(nil, DefaultSchedulerConfig())
	reordered := sched.ReorderCombos(lines)

	if len(reordered) != 3 {
		t.Fatalf("expected 3 items, got %d", len(reordered))
	}

	orders := sched.GetProcessingOrder()
	if len(orders) != 1 || !orders[0].Country.IsUnknown() {
		t.Fatalf("expected 1 XX group, got %v", orders)
	}
}

// TEST 6: Lista vacía y líneas en blanco
func TestGeoScheduler_EmptyAndBlank(t *testing.T) {
	sched := NewGeoScheduler(nil, DefaultSchedulerConfig())
	reordered := sched.ReorderCombos([]string{"   ", "", "\n"})

	if len(reordered) != 0 {
		t.Fatalf("expected 0 items, got %d", len(reordered))
	}

	orders := sched.GetProcessingOrder()
	if len(orders) != 0 {
		t.Fatalf("expected 0 groups, got %d", len(orders))
	}
}

// TEST 7: Custom Priority manual override
func TestGeoScheduler_CustomPriority(t *testing.T) {
	lines := []string{
		"movistarplay.pe:pe1@gmail.com:pass", // PE: 10 items
		"movistarplay.pe:pe2@gmail.com:pass",
		"vod.es:es1@terra.es:pass",      // ES: 1 item (queremos ES = 1)
		"service.us:us1@yahoo.com:pass", // US: 5 items (US = 2)
		"service.us:us2@yahoo.com:pass",
		"service.us:us3@yahoo.com:pass",
		"unknown.xyz:xx1@xyz.com:pass", // XX (XX = 99)
	}

	cfg := DefaultSchedulerConfig()
	cfg.Mode = ModeCustomPriority
	cfg.CustomPriorities = map[CountryCode]int{
		"ES": 1,
		"US": 2,
		"PE": 3,
		"XX": 99,
	}

	sched := NewGeoScheduler(nil, cfg)
	_ = sched.ReorderCombos(lines)

	orders := sched.GetProcessingOrder()
	if len(orders) != 4 {
		t.Fatalf("expected 4 groups, got %d", len(orders))
	}

	if orders[0].Country != "ES" || orders[1].Country != "US" || orders[2].Country != "PE" || !orders[3].Country.IsUnknown() {
		t.Fatalf("unexpected order for custom priority: %v", orders)
	}
}

// TEST 8: Alphabetical Mode
func TestGeoScheduler_Alphabetical(t *testing.T) {
	lines := []string{
		"service.us:us@yahoo.com:pass",
		"movistarplay.pe:pe@gmail.com:pass",
		"vod.es:es@terra.es:pass",
		"claro.br:br@uol.com.br:pass",
		"unknown.xyz:xx@xyz.com:pass",
	}

	cfg := DefaultSchedulerConfig()
	cfg.Mode = ModeAlphabetical

	sched := NewGeoScheduler(nil, cfg)
	_ = sched.ReorderCombos(lines)

	orders := sched.GetProcessingOrder()
	expectedOrder := []CountryCode{"BR", "ES", "PE", "US", "XX"}
	for i, exp := range expectedOrder {
		if orders[i].Country != exp {
			t.Errorf("pos %d expected %s, got %s", i, exp, orders[i].Country)
		}
	}
}

// TEST 9: Concurrent Progress Recording & Race Detector validation
func TestGeoScheduler_ConcurrentRecordProgress(t *testing.T) {
	lines := make([]string, 0, 1000)
	for i := 0; i < 500; i++ {
		lines = append(lines, fmt.Sprintf("movistarplay.pe:pe%d@gmail.com:pass", i))
		lines = append(lines, fmt.Sprintf("service.us:us%d@yahoo.com:pass", i))
	}

	sched := NewGeoScheduler(nil, DefaultSchedulerConfig())
	_ = sched.ReorderCombos(lines)

	var wg sync.WaitGroup
	workers := 20
	linesPerWorker := 50

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			cc := CountryCode("PE")
			if workerID%2 == 1 {
				cc = CountryCode("US")
			}
			for i := 0; i < linesPerWorker; i++ {
				sched.RecordProgress(cc, 1, 0)
			}
		}(w)
	}

	wg.Wait()

	orders := sched.GetProcessingOrder()
	totalCompleted := orders[0].Completed + orders[1].Completed
	if totalCompleted != 1000 {
		t.Fatalf("expected 1000 completed items across groups, got %d", totalCompleted)
	}
}

// TEST 10: Order Preview & Dashboard Rendering
func TestGeoScheduler_DashboardAndPreview(t *testing.T) {
	lines := []string{
		"movistarplay.pe:pe1@gmail.com:pass",
		"movistarplay.pe:pe2@gmail.com:pass",
		"service.us:us1@yahoo.com:pass",
	}

	sched := NewGeoScheduler(nil, DefaultSchedulerConfig())
	_ = sched.ReorderCombos(lines)

	preview := sched.RenderOrderPreview()
	if len(preview) == 0 {
		t.Fatal("expected non-empty preview")
	}

	sched.RecordProgress("PE", 1, 0)
	dashboard := sched.RenderDashboard()
	if len(dashboard) == 0 {
		t.Fatal("expected non-empty dashboard")
	}
}

// BENCHMARKS
func BenchmarkGeoScheduler_10K(b *testing.B) {
	lines := make([]string, 10000)
	for i := 0; i < 10000; i++ {
		switch i % 3 {
		case 0:
			lines[i] = "movistarplay.pe:user@gmail.com:pass"
		case 1:
			lines[i] = "service.us:user@yahoo.com:pass"
		default:
			lines[i] = "unknown.xyz:user@domain.com:pass"
		}
	}
	cfg := DefaultSchedulerConfig()
	sched := NewGeoScheduler(nil, cfg)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = sched.ReorderCombos(lines)
	}
}

func BenchmarkGeoScheduler_100K(b *testing.B) {
	lines := make([]string, 100000)
	for i := 0; i < 100000; i++ {
		switch i % 3 {
		case 0:
			lines[i] = "movistarplay.pe:user@gmail.com:pass"
		case 1:
			lines[i] = "service.us:user@yahoo.com:pass"
		default:
			lines[i] = "unknown.xyz:user@domain.com:pass"
		}
	}
	cfg := DefaultSchedulerConfig()
	sched := NewGeoScheduler(nil, cfg)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = sched.ReorderCombos(lines)
	}
}
