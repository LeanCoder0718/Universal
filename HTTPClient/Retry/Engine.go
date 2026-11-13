// Package retry — motor de reintentos desacoplado.
package Retry

import (
	"context"
	"errors"
	"math"
	"math/rand/v2"
	"net"
	"strings"
	"time"
)

type RetryConfig struct {
	MaxAttempts  int
	MaxRetries   int // Si MaxRetries > 0 y MaxAttempts == 0, equivale a MaxAttempts = MaxRetries + 1
	WaitMin      time.Duration
	WaitMax      time.Duration
	JitterFactor float64
}

type ErrorClass int

const (
	ClassNone ErrorClass = iota
	ClassSuccess
	ClassTransientNetwork
	ClassTLSFingerprint
	ClassRateLimited
	ClassServerOverloaded
	ClassBlocked
	ClassBusinessLogic
	ClassPermanent
	ClassCanceled  // caller canceló: NUNCA reintentar
	ClassChallenge // desafío o challenge anti-bot (Cloudflare Turnstile, Just a moment, Datadome, etc.)
)

type Action int

const (
	ActionNone Action = iota
	ActionRotateProxy
	ActionRebuildIdentity
	ActionReconnect
	ActionWaitSameConn
	ActionWaitSameProxy
)

// Signal es agnóstico de transporte.
type Signal struct {
	StatusCode    int
	NetErr        error
	Headers       map[string][]string // Cabeceras de respuesta para detección de WAF y mitigación
	BodySample    []byte
	Method        string
	MaxAttempts   int // Intentos máximos autorizados para esta petición específica (override por request)
	BodyComplete  bool
	BodyTruncated bool
	RetryAfter    time.Duration
}

type Decision struct {
	Class  ErrorClass
	Action Action
	Retry  bool
	Wait   time.Duration
	Reason string
}

type Classifier interface {
	Classify(s Signal) ErrorClass
}

type DefaultClassifier struct {
	ChallengeKeywords []string
	WAFKeywords       []string
	NetworkStatusSet  map[int]bool
}

func NewDefaultClassifier() *DefaultClassifier {
	return &DefaultClassifier{
		ChallengeKeywords: []string{
			"challenge-platform",
			"_cf_chl",
			"Just a moment",
			"Attention Required",
			"Enable JavaScript",
			"Verifying you are human",
			"Verify you are human",
			"turnstile",
			"pardon the interruption",
			"Pardon Our Interruption",
			"geo.captcha-delivery.com",
			"datadome.co/captcha",
			"Incapsula incident",
			"radware",
			"rdwr",
			"_event_transid",
		},
		WAFKeywords: []string{
			"Too Many Requests",
			"Request blocked",
			"CloudFront",
			`code":"throttled`,
			"Access Denied",
			"COUNTRY_BLOCKED",
			"country blocked",
			"Forbidden",
			"forbidden",
			"Transaction ID:",
		},
		NetworkStatusSet: map[int]bool{500: true, 502: true, 504: true},
	}
}

func isChallengeHeader(headers map[string][]string) bool {
	if len(headers) == 0 {
		return false
	}
	for k, vv := range headers {
		kLower := strings.ToLower(k)
		for _, v := range vv {
			vLower := strings.ToLower(v)
			if kLower == "cf-mitigated" && (strings.Contains(vLower, "challenge") || strings.Contains(vLower, "captcha")) {
				return true
			}
			if kLower == "cf-chl-bypass" {
				return true
			}
			if kLower == "x-amzn-waf-action" && (strings.Contains(vLower, "challenge") || strings.Contains(vLower, "captcha")) {
				return true
			}
			if kLower == "x-datadome" && (strings.Contains(vLower, "protected") || strings.Contains(vLower, "captcha") || strings.Contains(vLower, "block")) {
				return true
			}
		}
	}
	return false
}

func isWAFBlockedHeader(headers map[string][]string) bool {
	if len(headers) == 0 {
		return false
	}
	for k, vv := range headers {
		kLower := strings.ToLower(k)
		for _, v := range vv {
			vLower := strings.ToLower(v)
			if kLower == "x-amzn-waf-action" && strings.Contains(vLower, "block") {
				return true
			}
		}
	}
	return false
}

func (d *DefaultClassifier) Classify(s Signal) ErrorClass {
	if s.NetErr != nil {
		if errors.Is(s.NetErr, context.Canceled) {
			return ClassCanceled
		}
		if errors.Is(s.NetErr, context.DeadlineExceeded) {
			return ClassTransientNetwork
		}
		var ne net.Error
		if errors.As(s.NetErr, &ne) && ne.Timeout() {
			return ClassTransientNetwork
		}
		errStr := strings.ToLower(s.NetErr.Error())

		// Discriminar fallos de gateway/proxy de fallos de red y huella TLS
		isProxyErr := strings.Contains(errStr, "proxy") ||
			strings.Contains(errStr, "socks") ||
			strings.Contains(errStr, "gateway") ||
			strings.Contains(errStr, "tunnel") ||
			strings.Contains(errStr, "connectex")

		if isProxyErr {
			// El proxy falló o cerró el túnel -> rotar proxy (ActionRotateProxy)
			return ClassBlocked
		}

		// Fallo de negociación TLS estrictamente con el servidor de destino
		if strings.Contains(errStr, "tls:") ||
			strings.Contains(errStr, "handshake") ||
			strings.Contains(errStr, "x509:") ||
			strings.Contains(errStr, "certificate required") ||
			strings.Contains(errStr, "unrecognized name") {
			return ClassTLSFingerprint
		}

		// Errores de transporte estándar (EOF, connection reset, broken pipe, connection refused, timeout).
		// Son transitorios de red: rotan proxy pero NUNCA borran cookies ni escalan a RebuildIdentity.
		return ClassTransientNetwork
	}

	// 1. Detección por cabeceras de respuesta (WAF / Anti-Bot)
	if isChallengeHeader(s.Headers) {
		return ClassChallenge
	}
	if isWAFBlockedHeader(s.Headers) {
		return ClassBlocked
	}

	// 2. Detección de pantallas de desafío anti-bot en el cuerpo (incluso si el código es 200 o 503)
	if containsAny(s.BodySample, d.ChallengeKeywords) {
		// Para respuestas 200 OK, prevenir falsos positivos con frases genéricas (ej. "<noscript>Enable JavaScript</noscript>")
		// exigiendo que contenga una firma técnica anti-bot o que no sea un 200 OK ordinario.
		isGenericKeywordOnly := false
		if s.StatusCode >= 200 && s.StatusCode < 300 {
			hasSpecificAntiBot := containsAny(s.BodySample, []string{
				"challenge-platform", "_cf_chl", "turnstile",  "radware", "rdwr",
				"verifying you are human", "verify you are human", "attention required", "_event_transid",
			})
			if !hasSpecificAntiBot {
				isGenericKeywordOnly = true
			}
		}
		if !isGenericKeywordOnly {
			return ClassChallenge
		}
	}

	// 3. Body incompleto nunca es éxito, aunque el status sea 2xx.
	if s.StatusCode >= 200 && s.StatusCode < 400 {
		if s.BodyTruncated || !s.BodyComplete {
			return ClassTransientNetwork
		}
		return ClassSuccess
	}

	switch {
	case s.StatusCode == 429:
		return ClassRateLimited
	case s.StatusCode == 503:
		return ClassServerOverloaded
	case s.StatusCode == 451:
		return ClassBlocked
	case s.StatusCode == 403:
		if len(s.BodySample) == 0 || containsAny(s.BodySample, d.WAFKeywords) || containsAny(s.BodySample, []string{"<html", "<!doctype html"}) {
			return ClassBlocked
		}
		return ClassBusinessLogic
	case s.StatusCode == 500, s.StatusCode == 502, s.StatusCode == 504:
		return ClassTransientNetwork
	case containsAny(s.BodySample, []string{"COUNTRY_BLOCKED", "country blocked"}):
		return ClassBlocked
	case d.NetworkStatusSet[s.StatusCode]:
		return ClassTransientNetwork
	case s.StatusCode == 400, s.StatusCode == 401, s.StatusCode == 404, s.StatusCode == 405, s.StatusCode == 422:
		return ClassPermanent
	default:
		if s.StatusCode == 0 {
			return ClassTransientNetwork
		}
		return ClassPermanent
	}
}

func containsAny(body []byte, keywords []string) bool {
	if len(body) == 0 {
		return false
	}
	chunk := body
	const scanLimit = 4096
	if len(chunk) > scanLimit {
		chunk = chunk[:scanLimit]
	}
	lower := strings.ToLower(string(chunk))
	for _, kw := range keywords {
		if strings.Contains(lower, strings.ToLower(kw)) {
			return true
		}
	}
	return false
}

type Engine struct {
	cfg        RetryConfig
	classifier Classifier
}

func New(cfg RetryConfig, classifier Classifier) *Engine {
	if classifier == nil {
		classifier = NewDefaultClassifier()
	}
	return &Engine{
		cfg:        cfg,
		classifier: classifier,
	}
}

func (e *Engine) Decide(attempt int, s Signal) Decision {
	class := e.classifier.Classify(s)

	if class == ClassSuccess {
		return Decision{Class: class, Action: ActionNone, Retry: false, Reason: "success"}
	}
	if class == ClassCanceled {
		return Decision{Class: class, Action: ActionNone, Retry: false, Reason: "context canceled"}
	}
	if class == ClassPermanent || class == ClassBusinessLogic {
		return Decision{Class: class, Action: ActionNone, Retry: false, Reason: reasonFor(class)}
	}
	maxAttempts := s.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = e.cfg.MaxAttempts
		if maxAttempts <= 0 && e.cfg.MaxRetries > 0 {
			maxAttempts = e.cfg.MaxRetries + 1
		}
	}
	if maxAttempts > 0 && attempt >= maxAttempts {
		return Decision{Class: class, Action: ActionNone, Retry: false, Reason: "max attempts exhausted"}
	}

	action := actionFor(class)

	// Escalamiento adaptativo de bloqueo: si en el intento 1 rotamos de proxy pero el bloqueo/challenge
	// persiste en intento >= 2, escalar a ActionRebuildIdentity para regenerar completamente la huella
	// (nuevo perfil TLS, nuevo UA, nuevos headers y jar limpio).
	if (class == ClassBlocked || class == ClassChallenge) && attempt >= 2 {
		action = ActionRebuildIdentity
	}

	wait := e.backoff(attempt, class)

	// Respetar Retry-After si el servidor lo especificó
	if s.RetryAfter > 0 {
		if s.RetryAfter <= 60*time.Second {
			if s.RetryAfter > wait {
				wait = s.RetryAfter
			}
		} else {
			// Si el servidor exige esperar un tiempo excesivo (ej. horas/días),
			// no bloquear el worker: forzar cambio de proxy inmediato para continuar
			action = ActionRotateProxy
		}
	}

	return Decision{Class: class, Action: action, Retry: true, Wait: wait, Reason: reasonFor(class)}
}

func actionFor(class ErrorClass) Action {
	switch class {
	case ClassTLSFingerprint:
		return ActionRebuildIdentity
	case ClassBlocked, ClassTransientNetwork, ClassChallenge:
		return ActionRotateProxy // Fallos de red, proxy o bloqueo rotan inmediatamente el proxy
	case ClassRateLimited, ClassServerOverloaded:
		return ActionWaitSameConn
	default:
		return ActionNone
	}
}

func reasonFor(class ErrorClass) string {
	switch class {
	case ClassTLSFingerprint:
		return "tls/handshake error -> rebuild identity"
	case ClassBlocked:
		return "WAF/CDN block -> rotate proxy"
	case ClassChallenge:
		return "anti-bot challenge detected -> rotate proxy"
	case ClassBusinessLogic:
		return "business-logic error, not a bot signal"
	case ClassRateLimited:
		return "rate limited (429)"
	case ClassServerOverloaded:
		return "server overloaded (503)"
	case ClassTransientNetwork:
		return "transient network error -> rotate proxy"
	case ClassPermanent:
		return "permanent error, no retry"
	default:
		return "unclassified"
	}
}

func (e *Engine) backoff(attempt int, class ErrorClass) time.Duration {
	base := float64(e.cfg.WaitMin)
	maxWait := float64(e.cfg.WaitMax)
	if base <= 0 {
		return 0
	}

	wait := base * math.Pow(2, float64(attempt-1))
	if class == ClassRateLimited || class == ClassServerOverloaded {
		wait *= 1.5
	}
	if wait > maxWait {
		wait = maxWait
	}

	r := rand.Float64()

	jitter := wait * e.cfg.JitterFactor * (2*r - 1)
	result := wait + jitter
	if result < 0 {
		result = base
	}
	return time.Duration(result)
}
