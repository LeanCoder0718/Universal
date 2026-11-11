package HTTPClient

import (
	"context"
	"net/http"
	"sync/atomic"
	"testing"
)

// TestSession_PostMaxRetriesRespected verifica que una petición POST con Idempotent: true y MaxRetries = 3
// ante errores 429 (RateLimit) o 503 (ServerOverloaded) ejecute exactamente 4 intentos
// (1 intento original + 3 reintentos) en lugar de abortar tras el intento 1.
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
	// Simulamos el caso donde la sesión fue iniciada con MaxAttempts = 1 (como hace QuickStart)
	cfg.Retry.MaxAttempts = 1

	sess, err := NewSession(cfg, WithTransportFactory(mockFact))
	if err != nil {
		t.Fatalf("NewSession falló: %v", err)
	}

	// 1. Sin Idempotent: true (default RFC 9110) -> NO se reintenta (1 solo intento)
	callCount.Store(0)
	_, _ = sess.Do(context.Background(), Spec{
		Method:     "POST",
		URL:        "https://api.example.com/login",
		MaxRetries: 3,
	})
	if callCount.Load() != 1 {
		t.Errorf("POST sin Idempotent:true debió abortar al intento 1 por seguridad RFC 9110, ejecutó: %d", callCount.Load())
	}

	// 2. Con Idempotent: true -> SÍ ejecuta exactamente 4 intentos (1 original + 3 reintentos)
	callCount.Store(0)
	res, _ := sess.Do(context.Background(), Spec{
		Method:     "POST",
		URL:        "https://api.example.com/login",
		MaxRetries: 3,
		Idempotent: true,
	})

	if callCount.Load() != 4 {
		t.Errorf("Se esperaban exactamente 4 intentos (1 original + 3 reintentos), pero se ejecutaron: %d", callCount.Load())
	}
	if res == nil || res.StatusCode != http.StatusTooManyRequests {
		t.Errorf("Respuesta esperada 429, obtenida: %v", res)
	}
}

// TestSession_PostMaxAttemptsRespected verifica que MaxAttempts = 3 con Idempotent: true ejecute exactamente 3 intentos.
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
	cfg.Retry.MaxAttempts = 1 // Sesión base en 1

	sess, err := NewSession(cfg, WithTransportFactory(mockFact))
	if err != nil {
		t.Fatalf("NewSession falló: %v", err)
	}

	// 1. Sin Idempotent: true -> 1 solo intento
	callCount.Store(0)
	_, _ = sess.Do(context.Background(), Spec{
		Method:      "POST",
		URL:         "https://api.example.com/checkout",
		MaxAttempts: 3,
	})
	if callCount.Load() != 1 {
		t.Errorf("POST sin Idempotent:true ante 503 debió abortar al intento 1, ejecutó: %d", callCount.Load())
	}

	// 2. Con Idempotent: true -> 3 intentos
	callCount.Store(0)
	res, _ := sess.Do(context.Background(), Spec{
		Method:      "POST",
		URL:         "https://api.example.com/checkout",
		MaxAttempts: 3,
		Idempotent:  true,
	})

	if callCount.Load() != 3 {
		t.Errorf("Se esperaban exactamente 3 intentos con MaxAttempts = 3 e Idempotent:true, pero se ejecutaron: %d", callCount.Load())
	}
	if res == nil || res.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("Respuesta esperada 503, obtenida: %v", res)
	}
}
