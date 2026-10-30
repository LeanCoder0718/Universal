package HTTPClient

import (
	"context"
	"strings"
	"testing"

	"github.com/LeanCoder0718/Universal/GeoRoute"
)

// TestSession_GeoRouteProxyTransitionToGeneric valida que SetGeoCountry conmute a proxies
// regionales (ej. ES, PE) y limpie correctamente el username al retornar a genérico ("XX" o "").
func TestSession_GeoRouteProxyTransitionToGeneric(t *testing.T) {
	pool := newTestProxyPool(t, 1) // 10.0.0.1:8080:user1:pass1
	cfg := Default()
	sess, err := NewSession(cfg,
		WithProxyPool(pool),
		WithTransportFactory(&testLoopbackFactory{}),
	)
	if err != nil {
		t.Fatalf("error creando sesión: %v", err)
	}
	defer sess.Close()

	// 1. Configurar país ES
	if err := sess.SetGeoCountry("ES"); err != nil {
		t.Fatalf("SetGeoCountry(ES) error: %v", err)
	}
	displayES := sess.ProxyDisplay()
	if !strings.Contains(strings.ToLower(displayES), "es") {
		t.Errorf("ProxyDisplay = %q, expected country tag ES", displayES)
	}

	// 2. Conmutar a PE
	if err := sess.SetGeoCountry("PE"); err != nil {
		t.Fatalf("SetGeoCountry(PE) error: %v", err)
	}
	displayPE := sess.ProxyDisplay()
	if !strings.Contains(strings.ToLower(displayPE), "pe") {
		t.Errorf("ProxyDisplay = %q, expected country tag PE", displayPE)
	}

	// 3. Conmutar a genérico con "XX" -> debe retornar al proxy general base (user1)
	if err := sess.SetGeoCountry("XX"); err != nil {
		t.Fatalf("SetGeoCountry(XX) error: %v", err)
	}
	displayXX := sess.ProxyDisplay()
	if displayXX != "user1" {
		t.Errorf("ProxyDisplay = %q, want clean base user 'user1' on XX", displayXX)
	}

	// 4. Conmutar a genérico con "" -> debe permanecer en el proxy general base (user1)
	if err := sess.SetGeoCountry(""); err != nil {
		t.Fatalf("SetGeoCountry('') error: %v", err)
	}
	displayEmpty := sess.ProxyDisplay()
	if displayEmpty != "user1" {
		t.Errorf("ProxyDisplay = %q, want clean base user 'user1' on empty country", displayEmpty)
	}
}

// TestSession_DoWithSpecCountry valida que Spec.Country active la conmutación automática en Do()
func TestSession_DoWithSpecCountry(t *testing.T) {
	pool := newTestProxyPool(t, 1)
	cfg := Default()
	sess, err := NewSession(cfg,
		WithProxyPool(pool),
		WithTransportFactory(&testLoopbackFactory{}),
	)
	if err != nil {
		t.Fatalf("error creando sesión: %v", err)
	}
	defer sess.Close()

	// Ejecutar Do con Spec.Country = "ES"
	specES := Spec{
		Method:  "GET",
		URL:     "http://example.com/check",
		Country: "ES",
	}
	res, err := sess.Do(context.Background(), specES)
	if err != nil {
		t.Fatalf("Do(specES) error: %v", err)
	}
	res.Release()

	displayES := sess.ProxyDisplay()
	if !strings.Contains(strings.ToLower(displayES), "es") {
		t.Errorf("ProxyDisplay = %q, want regional tag ES", displayES)
	}

	// Ejecutar Do con Spec.Country = "XX" (retorno a genérico)
	specXX := Spec{
		Method:  "GET",
		URL:     "http://example.com/check",
		Country: "XX",
	}
	res2, err := sess.Do(context.Background(), specXX)
	if err != nil {
		t.Fatalf("Do(specXX) error: %v", err)
	}
	res2.Release()

	displayXX := sess.ProxyDisplay()
	if displayXX != "user1" {
		t.Errorf("ProxyDisplay = %q, want clean base user 'user1' after XX", displayXX)
	}
}

// TestSession_DoWithContextCountry valida la Opción B: que session.Do extraiga el país automáticamente del context.Context
func TestSession_DoWithContextCountry(t *testing.T) {
	pool := newTestProxyPool(t, 1)
	cfg := Default()
	sess, err := NewSession(cfg,
		WithProxyPool(pool),
		WithTransportFactory(&testLoopbackFactory{}),
	)
	if err != nil {
		t.Fatalf("error creando sesión: %v", err)
	}
	defer sess.Close()

	// 1. Petición con WithCountry(ctx, "ES") y Spec SIN campo Country
	ctxES := WithCountry(context.Background(), "ES")
	specPlain := Spec{
		Method: "GET",
		URL:    "http://example.com/check",
	}
	res1, err := sess.Do(ctxES, specPlain)
	if err != nil {
		t.Fatalf("Do(ctxES) error: %v", err)
	}
	res1.Release()

	displayES := sess.ProxyDisplay()
	if !strings.Contains(strings.ToLower(displayES), "es") {
		t.Errorf("ProxyDisplay = %q, want country tag ES from context", displayES)
	}

	// 2. Siguiente petición con WithCountry(ctx, "XX") y Spec SIN campo Country
	ctxXX := WithCountry(context.Background(), "XX")
	res2, err := sess.Do(ctxXX, specPlain)
	if err != nil {
		t.Fatalf("Do(ctxXX) error: %v", err)
	}
	res2.Release()

	displayXX := sess.ProxyDisplay()
	if displayXX != "user1" {
		t.Errorf("ProxyDisplay = %q, want clean base user 'user1' from context XX", displayXX)
	}
}

// TestSession_Option4_PreservesTaggedProxyExactly valida que con GeoRoute.ActiveApplyGeo = false (Opción 4),
// una proxy cargada con tag explícito (ej: rzP71UVQeRda-country-mx) mantenga su tag intacto en todas las peticiones.
func TestSession_Option4_PreservesTaggedProxyExactly(t *testing.T) {
	orig := GeoRoute.ActiveApplyGeo
	defer func() { GeoRoute.ActiveApplyGeo = orig }()
	GeoRoute.ActiveApplyGeo = false

	rawProxy := "unlimited.proxiware.com:1337:rzP71UVQeRda-country-mx:PEpvpiSMsj77VsFV"
	pool, err := NewPool([]string{rawProxy}, ProxyConfig{Enabled: true, Mode: ModeRotating})
	if err != nil {
		t.Fatalf("error creando pool: %v", err)
	}

	cfg := Default()
	sess, err := NewSession(cfg,
		WithProxyPool(pool),
		WithTransportFactory(&testLoopbackFactory{}),
	)
	if err != nil {
		t.Fatalf("error creando sesión: %v", err)
	}
	defer sess.Close()

	// 1. Verificar que al inicializar la sesión el proxy tenga su tag intacto
	if sess.ProxyDisplay() != "rzP71UVQeRda-country-mx" {
		t.Errorf("ProxyDisplay want rzP71UVQeRda-country-mx, got %q", sess.ProxyDisplay())
	}

	// 2. Intentar SetGeoCountry con cualquier país ("ES", "XX", etc.) -> NO debe mutar
	if err := sess.SetGeoCountry("ES"); err != nil {
		t.Fatalf("SetGeoCountry error: %v", err)
	}
	if sess.ProxyDisplay() != "rzP71UVQeRda-country-mx" {
		t.Errorf("ProxyDisplay after SetGeoCountry(ES) want rzP71UVQeRda-country-mx, got %q", sess.ProxyDisplay())
	}

	// 3. Petición Do con Spec.Country = "XX" -> NO debe eliminar el tag -country-mx
	specXX := Spec{
		Method:  "GET",
		URL:     "http://example.com/check",
		Country: "XX",
	}
	res, err := sess.Do(context.Background(), specXX)
	if err != nil {
		t.Fatalf("Do error: %v", err)
	}
	res.Release()

	if sess.ProxyDisplay() != "rzP71UVQeRda-country-mx" {
		t.Errorf("ProxyDisplay after Do(specXX) want rzP71UVQeRda-country-mx, got %q", sess.ProxyDisplay())
	}
}
