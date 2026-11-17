package HTTPClient

import (
	"testing"

	"github.com/LeanCoder0718/Universal/HTTPClient/Retry"
)

func TestRetryStatusCodes_EngineClassification(t *testing.T) {
	// 1. Configuración por defecto: 400 es considerado ClassPermanent (no reintentable)
	defaultEng := Retry.New(Retry.RetryConfig{}, nil)
	decDefault := defaultEng.Decide(1, Retry.Signal{
		StatusCode: 400,
	})
	if decDefault.Class != Retry.ClassPermanent {
		t.Fatalf("Por defecto, 400 debió ser ClassPermanent, obtenido: %v", decDefault.Class)
	}
	if decDefault.Retry {
		t.Fatalf("Por defecto, 400 no debió reintentar")
	}

	// 2. Con RetryStatusCodes personalizado: [400, 418]
	customEng := Retry.New(Retry.RetryConfig{
		MaxAttempts:      3,
		RetryStatusCodes: []int{400, 418},
	}, nil)

	decCustom := customEng.Decide(1, Retry.Signal{
		StatusCode: 400,
	})
	if decCustom.Class != Retry.ClassBlocked {
		t.Fatalf("Con RetryStatusCodes, 400 debió clasificarse como ClassBlocked, obtenido: %v", decCustom.Class)
	}
	if !decCustom.Retry {
		t.Fatalf("Con RetryStatusCodes, 400 debió tener Retry = true")
	}
	if decCustom.Action != Retry.ActionRotateProxy {
		t.Fatalf("Con RetryStatusCodes, acción debió ser ActionRotateProxy, obtenido: %v", decCustom.Action)
	}

	// Comprobar método IsRetryableStatus
	if !customEng.IsRetryableStatus(400) {
		t.Errorf("IsRetryableStatus(400) debió ser true")
	}
	if !customEng.IsRetryableStatus(418) {
		t.Errorf("IsRetryableStatus(418) debió ser true")
	}
	if !customEng.IsRetryableStatus(403) {
		t.Errorf("IsRetryableStatus(403) debió ser true (código estándar)")
	}
	if customEng.IsRetryableStatus(404) {
		t.Errorf("IsRetryableStatus(404) debió ser false")
	}
}

func TestSession_IsRetryableStatus(t *testing.T) {
	cfg := Default()
	cfg.Retry.RetryStatusCodes = []int{400}
	s, err := NewSession(cfg)
	if err != nil {
		t.Fatalf("Error creando session: %v", err)
	}
	defer s.Close()

	if !s.IsRetryableStatus(400) {
		t.Errorf("s.IsRetryableStatus(400) debió ser true al estar configurado")
	}
	if !s.IsRetryableStatus(403) {
		t.Errorf("s.IsRetryableStatus(403) debió ser true (código estándar)")
	}
	if s.IsRetryableStatus(404) {
		t.Errorf("s.IsRetryableStatus(404) debió ser false")
	}
}

func TestResult_HasStatusCode(t *testing.T) {
	res := &Result{
		Response: &Response{
			StatusCode: 400,
		},
	}
	if !res.HasStatusCode(400, 403) {
		t.Errorf("HasStatusCode debió retornar true para 400")
	}
	if res.HasStatusCode(404, 500) {
		t.Errorf("HasStatusCode debió retornar false para 404, 500")
	}
}
