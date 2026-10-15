package HTTPClient

import (
	"testing"
	"time"
)

func TestSessionHandle_HealthCheck(t *testing.T) {
	h := NewSessionHandle(nil)
	cfg := LifecycleConfig{
		MaxRequestsPerSession: 10,
		MaxSessionAge:         500 * time.Millisecond,
		MaxConsecutiveErrors:  3,
	}

	if !h.IsHealthy(cfg) {
		t.Errorf("new session should be healthy")
	}

	// Exceed max requests
	h.RequestCount.Store(10)
	if h.IsHealthy(cfg) {
		t.Errorf("session should be unhealthy after max requests")
	}
	h.RequestCount.Store(5)

	// Exceed consecutive errors
	h.ConsecutiveErrs.Store(3)
	if h.IsHealthy(cfg) {
		t.Errorf("session should be unhealthy after max consecutive errors")
	}
	h.ConsecutiveErrs.Store(0)

	// Retired session
	h.IsRetired.Store(true)
	if h.IsHealthy(cfg) {
		t.Errorf("retired session should be unhealthy")
	}
}

func TestSessionHandle_PreWarmedAtomic(t *testing.T) {
	h := NewSessionHandle(nil)

	if !h.PreWarmed.CompareAndSwap(false, true) {
		t.Errorf("first CAS on PreWarmed should succeed")
	}
	if h.PreWarmed.CompareAndSwap(false, true) {
		t.Errorf("second CAS on PreWarmed should fail (already true)")
	}
}
