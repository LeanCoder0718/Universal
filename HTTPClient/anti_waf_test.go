package HTTPClient

import (
	"testing"
	"time"

	retry "github.com/LeanCoder0718/Universal/HTTPClient/Retry"
	fhttp "github.com/bogdanfinn/fhttp"
)

// TestAntiWAF_PseudoHeaderOrder verifica que ChromeLayoutSet y FirefoxLayoutSet tengan
// el orden wire exacto de pseudo-cabeceras HTTP/2 nativo de cada navegador en peticiones XHR/JSON.
func TestAntiWAF_PseudoHeaderOrder(t *testing.T) {
	chromeXHR := ChromeLayoutSet.Get(KindXHR)
	if len(chromeXHR.Slots) < 4 {
		t.Fatalf("Chrome XHR layout tiene menos de 4 slots: %d", len(chromeXHR.Slots))
	}

	// Chrome HTTP/2 pseudo-headers: :method, :authority, :scheme, :path
	expectedChrome := []string{":method", ":authority", ":scheme", ":path"}
	for i, exp := range expectedChrome {
		if chromeXHR.Slots[i].Name != exp {
			t.Errorf("slot %d de Chrome XHR esperado %q, got %q (inconsistencia grave de huella)", i, exp, chromeXHR.Slots[i].Name)
		}
	}

	// Firefox HTTP/2 pseudo-headers: :method, :path, :authority, :scheme
	firefoxXHR := FirefoxLayoutSet.Get(KindXHR)
	if len(firefoxXHR.Slots) < 4 {
		t.Fatalf("Firefox XHR layout tiene menos de 4 slots: %d", len(firefoxXHR.Slots))
	}
	expectedFirefox := []string{":method", ":path", ":authority", ":scheme"}
	for i, exp := range expectedFirefox {
		if firefoxXHR.Slots[i].Name != exp {
			t.Errorf("slot %d de Firefox XHR esperado %q, got %q (inconsistencia grave de huella)", i, exp, firefoxXHR.Slots[i].Name)
		}
	}
}

// TestAntiWAF_ChallengeDetection_Status200 verifica que un challenge anti-bot (Cloudflare, Turnstile)
// retornado con código HTTP 200 NO sea tratado erróneamente como ClassSuccess.
func TestAntiWAF_ChallengeDetection_Status200(t *testing.T) {
	classifier := retry.NewDefaultClassifier()

	// 1. HTTP 200 con pantalla "Just a moment..." de Cloudflare
	cfSample := []byte(`<!DOCTYPE html><html><head><title>Just a moment...</title></head><body><div class="challenge-platform">Verifying you are human</div></body></html>`)
	classCF := classifier.Classify(retry.Signal{
		StatusCode:   200,
		Method:       "GET",
		BodySample:   cfSample,
		BodyComplete: true,
	})
	if classCF != retry.ClassChallenge {
		t.Errorf("esperado ClassChallenge para pantalla Cloudflare en 200, got %v", classCF)
	}

	// 2. HTTP 200 legítimo (JSON de API o HTML normal sin challenge)
	legitSample := []byte(`{"code":"00","description":"Éxito","data":{"balance":1500}}`)
	classLegit := classifier.Classify(retry.Signal{
		StatusCode:   200,
		Method:       "POST",
		BodySample:   legitSample,
		BodyComplete: true,
	})
	if classLegit != retry.ClassSuccess {
		t.Errorf("esperado ClassSuccess para respuesta legítima 200, got %v", classLegit)
	}
}

// TestAntiWAF_ChallengeDetection_Status503 verifica que un 503 con challenge anti-bot
// no se trate como simple sobrecarga del servidor (que reintentaría en la misma conexión),
// sino como ClassChallenge (que fuerza rotación de proxy/identidad).
func TestAntiWAF_ChallengeDetection_Status503(t *testing.T) {
	classifier := retry.NewDefaultClassifier()

	// 503 con Just a moment
	cf503 := []byte(`<html><body><span id="challenge-platform">Checking browser...</span></body></html>`)
	class := classifier.Classify(retry.Signal{
		StatusCode: 503,
		Method:     "GET",
		BodySample: cf503,
	})
	if class != retry.ClassChallenge {
		t.Errorf("esperado ClassChallenge para 503 con challenge anti-bot, got %v", class)
	}

	// 503 normal de sobrecarga (sin challenge keywords)
	overload503 := []byte(`Service Temporarily Unavailable. Backend server is overloaded.`)
	classOverload := classifier.Classify(retry.Signal{
		StatusCode: 503,
		Method:     "GET",
		BodySample: overload503,
	})
	if classOverload != retry.ClassServerOverloaded {
		t.Errorf("esperado ClassServerOverloaded para 503 normal, got %v", classOverload)
	}
}

// TestAntiWAF_HeaderDetection verifica que cabeceras específicas de WAF (Cloudflare, AWS WAF, DataDome)
// sean detectadas incluso antes de inspeccionar el cuerpo.
func TestAntiWAF_HeaderDetection(t *testing.T) {
	classifier := retry.NewDefaultClassifier()

	// 1. Header Cloudflare cf-mitigated: challenge
	cfHeaders := map[string][]string{
		"cf-mitigated": {"challenge"},
		"server":       {"cloudflare"},
	}
	classCF := classifier.Classify(retry.Signal{
		StatusCode: 403,
		Method:     "POST",
		Headers:    cfHeaders,
	})
	if classCF != retry.ClassChallenge {
		t.Errorf("esperado ClassChallenge por cabecera cf-mitigated: challenge, got %v", classCF)
	}

	// 2. Header AWS WAF x-amzn-waf-action: block
	awsHeaders := map[string][]string{
		"x-amzn-waf-action": {"block"},
	}
	classAWS := classifier.Classify(retry.Signal{
		StatusCode: 403,
		Method:     "GET",
		Headers:    awsHeaders,
	})
	if classAWS != retry.ClassBlocked {
		t.Errorf("esperado ClassBlocked por cabecera x-amzn-waf-action: block, got %v", classAWS)
	}

	// 3. Header DataDome x-datadome: protected
	ddHeaders := map[string][]string{
		"x-datadome": {"protected"},
	}
	classDD := classifier.Classify(retry.Signal{
		StatusCode: 200,
		Method:     "GET",
		Headers:    ddHeaders,
	})
	if classDD != retry.ClassChallenge {
		t.Errorf("esperado ClassChallenge por cabecera x-datadome: protected, got %v", classDD)
	}
}

// TestAntiWAF_AdaptiveEscalation verifica que un bloqueo persistente escale
// de ActionRotateProxy (intento 1) a ActionRebuildIdentity (intento 2+).
func TestAntiWAF_AdaptiveEscalation(t *testing.T) {
	classifier := retry.NewDefaultClassifier()
	engine := retry.New(retry.RetryConfig{
		MaxAttempts: 3,
		WaitMin:     10 * time.Millisecond,
		WaitMax:     50 * time.Millisecond,
	}, classifier)

	blockSignal := retry.Signal{
		StatusCode: 403,
		Method:     "GET",
		BodySample: []byte(`<title>Access Denied</title>`),
	}

	// Intento 1: rota de proxy (quizá la IP estaba quemada)
	dec1 := engine.Decide(1, blockSignal)
	if !dec1.Retry {
		t.Fatalf("intento 1 debe reintentar")
	}
	if dec1.Action != retry.ActionRotateProxy {
		t.Errorf("intento 1 esperado ActionRotateProxy, got %v", dec1.Action)
	}

	// Intento 2 con nuevo proxy vuelve a fallar con WAF: escalar a reconstruir identidad TLS/headers completa
	dec2 := engine.Decide(2, blockSignal)
	if !dec2.Retry {
		t.Fatalf("intento 2 debe reintentar")
	}
	if dec2.Action != retry.ActionRebuildIdentity {
		t.Errorf("intento 2+ esperado ActionRebuildIdentity (escalamiento de huella), got %v", dec2.Action)
	}
}

// TestAntiWAF_ResponseMethods verifica los helpers IsBlocked() e IsChallenge() en Response y Result.
func TestAntiWAF_ResponseMethods(t *testing.T) {
	// 1. Response con Turnstile de Cloudflare
	respChallenge := &Response{
		StatusCode: 200,
		BodyBytes:  []byte(`<html><body><div id="cf-turnstile">Please verify you are human</div></body></html>`),
	}
	if !respChallenge.IsChallenge() {
		t.Errorf("IsChallenge() debería retornar true para Turnstile")
	}

	resChallenge := &Result{Response: respChallenge}
	if !resChallenge.IsChallenge() {
		t.Errorf("Result.IsChallenge() debería retornar true")
	}

	// 2. Response 403 de bloqueo
	respBlocked := &Response{
		StatusCode: 403,
		BodyBytes:  []byte(`403 Forbidden - Request blocked by security rule`),
	}
	if !respBlocked.IsBlocked() {
		t.Errorf("IsBlocked() debería retornar true para 403")
	}

	resBlocked := &Result{Response: respBlocked}
	if !resBlocked.IsBlocked() {
		t.Errorf("Result.IsBlocked() debería retornar true")
	}

	// 3. Response normal 200
	respNormal := &Response{
		StatusCode: 200,
		BodyBytes:  []byte(`{"status":"ok"}`),
		Headers:    fhttp.Header{"Content-Type": []string{"application/json"}},
	}
	if respNormal.IsChallenge() || respNormal.IsBlocked() {
		t.Errorf("respuesta normal no debe ser marcada como challenge ni blocked")
	}
}

