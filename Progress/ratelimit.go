package progress

import (
	"context"
	"golang.org/x/time/rate"
)

// RateLimiter define la abstracción para limitar el ritmo de peticiones por segundo (RPS).
type RateLimiter interface {
	Wait(ctx context.Context) error
}

// TokenBucketLimiter implementa limitación de tasa basada en Token Bucket (golang.org/x/time/rate).
// A diferencia de TickerRateLimiter (un solo canal compartido), Token Bucket distribuye tokens
// de forma justa entre N workers concurrentes sin serialización.
type TokenBucketLimiter struct {
	limiter *rate.Limiter
}

// NewTokenBucketLimiter crea un limitador de tasa basado en Token Bucket.
// rps: peticiones permitidas por segundo.
// burst: ráfaga máxima permitida (tokens acumulados). Si burst <= 0, se usa rps como burst.
func NewTokenBucketLimiter(rps int, burst int) *TokenBucketLimiter {
	if rps <= 0 {
		return nil
	}
	if burst <= 0 {
		burst = rps
	}
	return &TokenBucketLimiter{
		limiter: rate.NewLimiter(rate.Limit(rps), burst),
	}
}

// Wait bloquea hasta que un token esté disponible o el contexto sea cancelado.
func (t *TokenBucketLimiter) Wait(ctx context.Context) error {
	if t == nil || t.limiter == nil {
		return nil
	}
	return t.limiter.Wait(ctx)
}

// SetRate actualiza la tasa de tokens por segundo en caliente.
func (t *TokenBucketLimiter) SetRate(rps int) {
	if t != nil && t.limiter != nil && rps > 0 {
		t.limiter.SetLimit(rate.Limit(rps))
	}
}

// SetBurst actualiza la ráfaga máxima en caliente.
func (t *TokenBucketLimiter) SetBurst(burst int) {
	if t != nil && t.limiter != nil && burst > 0 {
		t.limiter.SetBurst(burst)
	}
}

// NewTickerRateLimiter crea un TokenBucketLimiter para compatibilidad con código existente.
// Deprecated: Usar NewTokenBucketLimiter directamente.
func NewTickerRateLimiter(rps int) *TokenBucketLimiter {
	return NewTokenBucketLimiter(rps, rps)
}

// NoOpRateLimiter es un limitador de tasa nulo que no aplica ningún retraso.
type NoOpRateLimiter struct{}

func (n *NoOpRateLimiter) Wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}
