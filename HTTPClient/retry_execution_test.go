package HTTPClient

import (
	"context"
	"net/http"
	"sync/atomic"
	"testing"
)

// TestSession_PostMaxRetriesRespected verifica que una petición POST con MaxRetries = 3
// ante errores 429 (RateLimit) ejecute exactamente 4 intentos (1 original + 3 reintentos)
// de forma automática, sin requerir ningún flag de idempotencia.
func TestSession_PostMaxRetriesRespected(t *testing.T) {
	var callCount atomic.Int32
	mockFact := &trackingMockFactory{
		handler: func(w http.ResponseWriter, r *http.Request) {
			callCount.Add(1)
			// Simular que el servidor responde 429 Too Many Requests
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"error": "rate_limit_exceeded"}`))
		},
	}

	cfg := Default()
	cfg.Retry.MaxAttempts = 1

	sess, err := NewSession(cfg, WithTransportFactory(mockFact))
	if err != nil {
		t.Fatalf("NewSession falló: %v", err)
	}

	// 1. POST hereda o configura MaxRetries: 3 sin ningún flag -> ejecuta 4 intentos
	callCount.Store(0)
	res, _ := sess.Do(context.Background(), Spec{
		Method:     "POST",
		URL:        "https://api.example.com/login",
		MaxRetries: 3,
	})

	if callCount.Load() != 4 {
		t.Errorf("POST debió reintentar automáticamente 4 veces (1 + 3), pero ejecutó: %d", callCount.Load())
	}
	if res == nil || res.StatusCode != http.StatusTooManyRequests {
		t.Errorf("Respuesta esperada 429, obtenida: %v", res)
	}

	// 2. Con NoRetry: true -> ejecuta exactamente 1 intento
	callCount.Store(0)
	_, _ = sess.Do(context.Background(), Spec{
		Method:     "POST",
		URL:        "https://api.example.com/login",
		MaxRetries: 3,
		NoRetry:    true,
	})
	if callCount.Load() != 1 {
		t.Errorf("POST con NoRetry:true debió ejecutar exactamente 1 intento, pero ejecutó: %d", callCount.Load())
	}
}

// TestSession_PostMaxAttemptsRespected verifica que MaxAttempts = 3 en POST ejecute exactamente 3 intentos automáticamente ante 503.
func TestSession_PostMaxAttemptsRespected(t *testing.T) {
	var callCount atomic.Int32
	mockFact := &trackingMockFactory{
		handler: func(w http.ResponseWriter, r *http.Request) {
			callCount.Add(1)
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"error": "overloaded"}`))
		},
	}

	cfg := Default()
	cfg.Retry.MaxAttempts = 1

	sess, err := NewSession(cfg, WithTransportFactory(mockFact))
	if err != nil {
		t.Fatalf("NewSession falló: %v", err)
	}

	// POST sin ningún flag ejecuta exactamente 3 intentos ante 503
	callCount.Store(0)
	res, _ := sess.Do(context.Background(), Spec{
		Method:      "POST",
		URL:         "https://api.example.com/checkout",
		MaxAttempts: 3,
	})

	if callCount.Load() != 3 {
		t.Errorf("Se esperaban exactamente 3 intentos con MaxAttempts = 3, pero se ejecutaron: %d", callCount.Load())
	}
	if res == nil || res.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("Respuesta esperada 503, obtenida: %v", res)
	}
}
