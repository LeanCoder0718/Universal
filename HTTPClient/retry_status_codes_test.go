package HTTPClient

import (
	"context"
	"net/http"
	"sync/atomic"
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

func TestRetry_Telmex500_BusinessLogic_NoRetry(t *testing.T) {
	eng := Retry.New(Retry.RetryConfig{MaxAttempts: 3}, nil)
	body := []byte(`{"code":"02","message":"Alguno de los datos que ingresaste es incorrecto; por favor, verifícalos e intenta de nuevo."}`)

	// Aunque el status sea 500, el body contiene código 02 de datos incorrectos -> BusinessLogic
	dec := eng.Decide(1, Retry.Signal{
		StatusCode: 500,
		BodySample: body,
	})

	if dec.Class != Retry.ClassBusinessLogic {
		t.Fatalf("Esperado ClassBusinessLogic para HTTP 500 con code 02, obtenido: %v", dec.Class)
	}
	if dec.Retry {
		t.Fatalf("HTTP 500 con code 02 no debe reintentar (Retry debió ser false)")
	}
}

func TestSession_NoRetry_Status500_ReturnsResultNoError(t *testing.T) {
	cfg := Default()
	cfg.Retry.MaxAttempts = 3

	var callCount atomic.Int32
	mockFact := &trackingMockFactory{
		handler: func(w http.ResponseWriter, r *http.Request) {
			callCount.Add(1)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"code":"02","message":"Alguno de los datos que ingresaste es incorrecto"}`))
		},
	}

	session, err := NewSession(cfg, WithTransportFactory(mockFact))
	if err != nil {
		t.Fatalf("error creando sesión: %v", err)
	}
	defer session.Close()

	spec := Spec{
		Method:  "POST",
		URL:     "https://appsso.telmex.com/api/v2.3/sso/login",
		NoRetry: true,
	}

	res, doErr := session.Do(context.Background(), spec)
	if res != nil {
		defer res.Release()
	}

	if doErr != nil {
		t.Fatalf("Con NoRetry: true y respuesta válida (500), Do no debió retornar error, obtenido: %v", doErr)
	}
	if res == nil || res.StatusCode != 500 {
		t.Fatalf("Se esperaba respuesta con StatusCode 500")
	}
	if calls := callCount.Load(); calls != 1 {
		t.Fatalf("Esperado exactamente 1 intento, realizados: %d", calls)
	}
}
