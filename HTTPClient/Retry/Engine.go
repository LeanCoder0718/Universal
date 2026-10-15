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
	ClassCanceled // caller canceló: NUNCA reintentar
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
	BodySample    []byte
	Method        string
	Idempotent    bool // true = el caller autoriza retry aunque no sea GET/HEAD/...
	BodyComplete  bool
	BodyTruncated bool
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
	WAFKeywords      []string
	NetworkStatusSet map[int]bool
}

func NewDefaultClassifier() *DefaultClassifier {
	return &DefaultClassifier{
		WAFKeywords: []string{
			"Too Many Requests", "Request blocked", "CloudFront",
			`code":"throttled`, "Access Denied", "pardon the interruption",
			"Enable JavaScript", "challenge-platform", "Just a moment",
			"Attention Required", "_cf_chl", "Incapsula incident",
		},
		NetworkStatusSet: map[int]bool{500: true, 502: true, 504: true},
	}
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
		// Evitar false-positive por body/HTML que mencione "certificate"
		if strings.Contains(errStr, "tls:") ||
			strings.Contains(errStr, "handshake") ||
			strings.Contains(errStr, "x509:") ||
			strings.Contains(errStr, "certificate required") {
			return ClassTLSFingerprint
		}
		return ClassTransientNetwork
	}

	// Body incompleto nunca es éxito, aunque el status sea 2xx.
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
	case s.StatusCode == 403:
		if containsAny(s.BodySample, d.WAFKeywords) {
			return ClassBlocked
		}
		return ClassBusinessLogic
	case d.NetworkStatusSet[s.StatusCode]:
		return ClassTransientNetwork
	case s.StatusCode == 401, s.StatusCode == 400, s.StatusCode == 404:
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
	if e.cfg.MaxAttempts > 0 && attempt >= e.cfg.MaxAttempts {
		return Decision{Class: class, Action: ActionNone, Retry: false, Reason: "max attempts exhausted"}
	}

	// No reintentar métodos no idempotentes salvo autorización explícita.
	if !s.Idempotent && !isIdempotentMethod(s.Method) {
		// Excepción controlada: TLS fingerprint sí puede reconstruir identidad
		// solo si el caller marcó Idempotent o es safe method.
		return Decision{
			Class:  class,
			Action: ActionNone,
			Retry:  false,
			Reason: "non-idempotent method, retry disabled",
		}
	}

	action := actionFor(class)
	wait := e.backoff(attempt, class)
	return Decision{Class: class, Action: action, Retry: true, Wait: wait, Reason: reasonFor(class)}
}

func isIdempotentMethod(m string) bool {
	switch strings.ToUpper(m) {
	case "GET", "HEAD", "OPTIONS", "PUT", "DELETE", "TRACE":
		return true
	default:
		return false
	}
}

func actionFor(class ErrorClass) Action {
	switch class {
	case ClassTLSFingerprint:
		return ActionRebuildIdentity
	case ClassBlocked, ClassTransientNetwork:
		return ActionReconnect
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
		return "WAF/CDN block -> reconnect (same gateway, local retry only)"
	case ClassBusinessLogic:
		return "business-logic error, not a bot signal"
	case ClassRateLimited:
		return "rate limited (429)"
	case ClassServerOverloaded:
		return "server overloaded (503)"
	case ClassTransientNetwork:
		return "transient network error -> reconnect local"
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
