package GeoRoute

import (
	"context"
)

type contextKey string

const countryContextKey contextKey = "georoute_country_context_key"

// WithCountry inyecta un código de país en el contexto de ejecución.
func WithCountry(ctx context.Context, country string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, countryContextKey, country)
}

// CountryFromContext extrae el código de país presente en el contexto.
// Si no existe, retorna una cadena vacía.
func CountryFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if val, ok := ctx.Value(countryContextKey).(string); ok {
		return val
	}
	return ""
}
