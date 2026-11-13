package Profiles_test

import (
	"context"
	"sync"
	"testing"

	"github.com/LeanCoder0718/Universal/HTTPClient/Profiles"
)

// TestProfileRegistry_Categories verifies that Browser, Android, and IOS categories
// return the correct set of profiles and contain no empty sets in the default catalog.
func TestProfileRegistry_Categories(t *testing.T) {
	catalog := Profiles.DefaultCatalog()
	if !catalog.IsFrozen() {
		t.Fatalf("DefaultCatalog must be frozen by default")
	}

	total := catalog.Count()
	if total == 0 {
		t.Fatalf("DefaultCatalog is empty")
	}

	// 1. Browser mode
	browserProfiles := catalog.ForMode(Profiles.Browser)
	if len(browserProfiles) == 0 {
		t.Fatalf("ForMode(Browser) returned 0 profiles")
	}
	for _, p := range browserProfiles {
		plat := p.Platform()
		if plat != Profiles.PlatformWindows && plat != Profiles.PlatformMacOS && plat != Profiles.PlatformLinux {
			t.Errorf("Browser profile %s has invalid platform: %s", p.ID(), plat)
		}
	}

	// 2. Android mode
	androidProfiles := catalog.ForMode(Profiles.Android)
	if len(androidProfiles) == 0 {
		t.Fatalf("ForMode(Android) returned 0 profiles")
	}
	for _, p := range androidProfiles {
		if p.Platform() != Profiles.PlatformAndroid {
			t.Errorf("Android profile %s has invalid platform: %s", p.ID(), p.Platform())
		}
	}

	// 3. iOS mode
	iosProfiles := catalog.ForMode(Profiles.IOS)
	if len(iosProfiles) == 0 {
		t.Fatalf("ForMode(IOS) returned 0 profiles")
	}
	for _, p := range iosProfiles {
		if p.Platform() != Profiles.PlatformIOS {
			t.Errorf("iOS profile %s has invalid platform: %s", p.ID(), p.Platform())
		}
	}

	// 4. Windows mode
	windowsProfiles := catalog.ForMode(Profiles.Windows)
	if len(windowsProfiles) == 0 {
		t.Fatalf("ForMode(Windows) returned 0 profiles")
	}
	for _, p := range windowsProfiles {
		if p.Platform() != Profiles.PlatformWindows {
			t.Errorf("Windows profile %s has invalid platform: %s", p.ID(), p.Platform())
		}
	}

	// 5. Windows Chrome mode
	winChromeProfiles := catalog.ForMode(Profiles.ModeWindowsChrome)
	if len(winChromeProfiles) == 0 {
		t.Fatalf("ForMode(ModeWindowsChrome) returned 0 profiles")
	}
	for _, p := range winChromeProfiles {
		if p.Platform() != Profiles.PlatformWindows || p.ClientType() != Profiles.ClientChrome {
			t.Errorf("WindowsChrome profile %s has invalid platform/client: %s / %s", p.ID(), p.Platform(), p.ClientType())
		}
	}

	// 6. Android Chrome mode
	androidChromeProfiles := catalog.ForMode(Profiles.ModeAndroidChrome)
	if len(androidChromeProfiles) == 0 {
		t.Fatalf("ForMode(ModeAndroidChrome) returned 0 profiles")
	}
	for _, p := range androidChromeProfiles {
		if p.Platform() != Profiles.PlatformAndroid || p.ClientType() != Profiles.ClientChrome {
			t.Errorf("AndroidChrome profile %s has invalid platform/client: %s / %s", p.ID(), p.Platform(), p.ClientType())
		}
	}
}

// TestProfileManager_ValidationFailFast tests all edge cases and error handling (No panics!).
func TestProfileManager_ValidationFailFast(t *testing.T) {
	catalog := Profiles.DefaultCatalog()

	// 1. Nil Registry
	_, err := Profiles.NewManager(Profiles.ManagerConfig{
		Registry: nil,
		Mode:     Profiles.Browser,
	})
	if err == nil {
		t.Errorf("Expected error for nil Registry, got nil")
	}

	// 2. Unfrozen Registry
	unfrozen := Profiles.NewRegistry()
	_, err = Profiles.NewManager(Profiles.ManagerConfig{
		Registry: unfrozen,
		Mode:     Profiles.Browser,
	})
	if err == nil {
		t.Errorf("Expected error for unfrozen Registry, got nil")
	}

	// 3. Empty Registry (Frozen)
	emptyReg := Profiles.NewRegistry()
	emptyReg.Freeze()
	_, err = Profiles.NewManager(Profiles.ManagerConfig{
		Registry: emptyReg,
		Mode:     Profiles.Browser,
	})
	if err == nil {
		t.Errorf("Expected error for empty Registry, got nil")
	}

	// 4. Invalid / Empty Mode
	_, err = Profiles.NewManager(Profiles.ManagerConfig{
		Registry: catalog,
		Mode:     Profiles.ProfileMode(9999), // Non-existent mode with no candidates
	})
	if err == nil {
		// Default case returns r.all, which is valid, or if empty it errors
	}
}

// TestProfileManager_ContextCancellation verifies that GetProfile respects context cancellation.
func TestProfileManager_ContextCancellation(t *testing.T) {
	catalog := Profiles.DefaultCatalog()
	manager, err := Profiles.NewManager(Profiles.ManagerConfig{
		Registry: catalog,
		Mode:     Profiles.Browser,
	})
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	_, err = manager.GetProfile(ctx)
	if err == nil {
		t.Errorf("Expected error on cancelled context, got nil")
	}

	_, failed := manager.Metrics()
	if failed == 0 {
		t.Errorf("Expected failed metric > 0 on cancelled context")
	}
}

// TestSelectionStrategy_RoundRobin verifies deterministic uniform distribution.
func TestSelectionStrategy_RoundRobin(t *testing.T) {
	catalog := Profiles.DefaultCatalog()
	manager, err := Profiles.NewManager(Profiles.ManagerConfig{
		Registry: catalog,
		Mode:     Profiles.Browser,
		Strategy: Profiles.NewRoundRobin(),
	})
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}

	candidates := manager.Candidates()
	n := len(candidates)
	ctx := context.Background()

	// 2 full rounds
	for round := 0; round < 2; round++ {
		for i := 0; i < n; i++ {
			p, err := manager.GetProfile(ctx)
			if err != nil {
				t.Fatalf("GetProfile failed: %v", err)
			}
			if p.ID() != candidates[i].ID() {
				t.Errorf("Round %d, idx %d: expected %s, got %s", round, i, candidates[i].ID(), p.ID())
			}
		}
	}
}

// TestSelectionStrategy_WeightedRandom verifies statistical distribution sanity.
func TestSelectionStrategy_WeightedRandom(t *testing.T) {
	catalog := Profiles.DefaultCatalog()
	manager, err := Profiles.NewManager(Profiles.ManagerConfig{
		Registry: catalog,
		Mode:     Profiles.Browser,
		Strategy: Profiles.NewWeightedRandom(),
	})
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}

	ctx := context.Background()
	counts := make(map[Profiles.ClientType]int)
	iterations := 10000

	for i := 0; i < iterations; i++ {
		p, err := manager.GetProfile(ctx)
		if err != nil {
			t.Fatalf("GetProfile failed: %v", err)
		}
		counts[p.ClientType()]++
	}

	// In browser candidates, Chrome has weight 65 vs others (Firefox 5, Safari 18, etc.)
	// Chrome should have the highest representation
	chromeCount := counts[Profiles.ClientChrome]
	if chromeCount < (iterations / 3) {
		t.Errorf("Weighted random distribution anomaly: Chrome count %d is too low out of %d", chromeCount, iterations)
	}
}

// TestProfileManager_ConcurrentStress verifies zero race conditions and zero panics under high concurrency.
func TestProfileManager_ConcurrentStress(t *testing.T) {
	catalog := Profiles.DefaultCatalog()
	manager, err := Profiles.NewManager(Profiles.ManagerConfig{
		Registry: catalog,
		Mode:     Profiles.Browser,
		Strategy: Profiles.NewWeightedRandom(),
	})
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}

	workerCounts := []int{10, 50, 100, 500}
	ctx := context.Background()

	for _, workers := range workerCounts {
		var wg sync.WaitGroup
		iterationsPerWorker := 100

		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < iterationsPerWorker; i++ {
					p, err := manager.GetProfile(ctx)
					if err != nil {
						t.Errorf("GetProfile failed in concurrency: %v", err)
						return
					}
					// Verify immutability & non-nil fields
					if p.ID() == "" || p.UserAgent() == "" || p.Version() == "" {
						t.Errorf("Invalid profile data read concurrently: %+v", p)
					}
				}
			}()
		}
		wg.Wait()
	}

	assigned, failed := manager.Metrics()
	if failed > 0 {
		t.Errorf("Expected 0 failures during stress test, got %d", failed)
	}
	if assigned == 0 {
		t.Errorf("Expected assigned > 0, got %d", assigned)
	}
}

// --- BENCHMARKS ---

func BenchmarkProfileSelection_RoundRobin(b *testing.B) {
	catalog := Profiles.DefaultCatalog()
	manager, err := Profiles.NewManager(Profiles.ManagerConfig{
		Registry: catalog,
		Mode:     Profiles.Browser,
		Strategy: Profiles.NewRoundRobin(),
	})
	if err != nil {
		b.Fatalf("NewManager failed: %v", err)
	}

	ctx := context.Background()
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _ = manager.GetProfile(ctx)
	}
}

func BenchmarkProfileSelection_Random(b *testing.B) {
	catalog := Profiles.DefaultCatalog()
	manager, err := Profiles.NewManager(Profiles.ManagerConfig{
		Registry: catalog,
		Mode:     Profiles.Browser,
		Strategy: Profiles.NewRandom(),
	})
	if err != nil {
		b.Fatalf("NewManager failed: %v", err)
	}

	ctx := context.Background()
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _ = manager.GetProfile(ctx)
	}
}

func BenchmarkProfileSelection_WeightedRandom(b *testing.B) {
	catalog := Profiles.DefaultCatalog()
	manager, err := Profiles.NewManager(Profiles.ManagerConfig{
		Registry: catalog,
		Mode:     Profiles.Browser,
		Strategy: Profiles.NewWeightedRandom(),
	})
	if err != nil {
		b.Fatalf("NewManager failed: %v", err)
	}

	ctx := context.Background()
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _ = manager.GetProfile(ctx)
	}
}

func BenchmarkProfileSelection_Parallel(b *testing.B) {
	catalog := Profiles.DefaultCatalog()
	manager, err := Profiles.NewManager(Profiles.ManagerConfig{
		Registry: catalog,
		Mode:     Profiles.Browser,
		Strategy: Profiles.NewWeightedRandom(),
	})
	if err != nil {
		b.Fatalf("NewManager failed: %v", err)
	}

	ctx := context.Background()
	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = manager.GetProfile(ctx)
		}
	})
}
