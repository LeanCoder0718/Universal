package progress

import (
	"context"

	"github.com/LeanCoder0718/Universal/GeoRoute"
)

// Line representa una línea individual leída del archivo con su metadato y payload.
type Line struct {
	ID      uint64      // Identificador secuencial único (1-indexed)
	Content []byte      // Payload binario/texto de la línea (excluyendo \r y \n)
	Country string      // Código de país detectado por GeoRoute (ej. "ES", "PE", "US", "XX")
	State   interface{} // Preserva el estado del procesado para reanudación por etapas en reintentos
}

// Context genera un context.Context asociado a la línea que transporta su código de país
// detectado ("ES", "PE", "US", "XX"). Permite que los clientes HTTP adapten automáticamente
// los proxies regionales sin necesidad de especificar el país manualmente en cada petición.
// Si se provee un contexto padre (ej: con timeout o cancelación global), lo preserva y decora.
func (l *Line) Context(parent ...context.Context) context.Context {
	ctx := context.Background()
	if len(parent) > 0 && parent[0] != nil {
		ctx = parent[0]
	}
	country := l.Country
	if country == "" && len(l.Content) > 0 {
		country = string(GeoRoute.NewCountryDetector(1000).Detect(string(l.Content)))
	}
	if country == "" {
		country = "XX"
	}
	return GeoRoute.WithCountry(ctx, country)
}

// Chunk representa un bloque contiguo de N líneas asignado a un worker.
type Chunk struct {
	SequenceID  uint64 // ID de secuencia del chunk (1-indexed)
	StartOffset int64  // Offset en bytes de inicio en el archivo original
	EndOffset   int64  // Offset en bytes de fin en el archivo original
	Lines       []Line // Colección de líneas contenidas en el bloque
}
