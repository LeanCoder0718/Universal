package progress

import (
	"context"
)

// LineResult representa el resultado del procesamiento de una línea individual.
type LineResult struct {
	LineID  uint64
	Line    Line // Línea original preservada para trazabilidad y reintentos por rondas
	Success bool
	Output  []byte
	Err     error
}

// ChunkResult representa el resultado consolidado del procesamiento de un bloque contiguo de líneas.
type ChunkResult struct {
	SequenceID uint64
	Results    []LineResult
}

// ProcessFunc es la función inyectable de procesamiento por línea.
type ProcessFunc func(line *Line) ([]byte, error)

// SinkInterface define la abstracción consumidora del resultQueue.
type SinkInterface interface {
	Run(ctx context.Context, resultQueue <-chan *ChunkResult) (Stats, error)
}

// ResultHandler define la función manejadora de salida por chunk procesado.
type ResultHandler func(res *ChunkResult) error

// LineCollectorFunc define la firma para recolectar líneas fallidas para la siguiente ronda.
type LineCollectorFunc func(line Line, err error)

// WriterSink consume los ChunkResult del canal de salida de forma desacoplada.
type WriterSink struct {
	handler   ResultHandler
	collector LineCollectorFunc
}

// NewWriterSink crea un nuevo WriterSink.
func NewWriterSink(h ResultHandler) *WriterSink {
	return &WriterSink{handler: h}
}

// SetCollector asigna una función opcional para recolectar líneas fallidas por ronda.
func (s *WriterSink) SetCollector(c LineCollectorFunc) {
	s.collector = c
}

// Run procesa en bucle todos los resultados emitidos hacia resultQueue.
func (s *WriterSink) Run(ctx context.Context, resultQueue <-chan *ChunkResult) (Stats, error) {
	var stats Stats
	for {
		select {
		case <-ctx.Done():
			return stats, ctx.Err()
		case res, ok := <-resultQueue:
			if !ok {
				return stats, nil
			}
			if res != nil {
				stats.TotalChunks++
				isRetry := (res.SequenceID == 0)

				if !isRetry {
					stats.TotalLines += uint64(len(res.Results))
				}

				for _, lr := range res.Results {
					if lr.Success {
						stats.SuccessLines++
						if isRetry && stats.FailedLines > 0 {
							stats.FailedLines--
						}
					} else {
						if !isRetry {
							stats.FailedLines++
						}
						if s.collector != nil {
							s.collector(lr.Line, lr.Err)
						}
					}
				}
				if s.handler != nil {
					if err := s.handler(res); err != nil {
						return stats, err
					}
				}

				// Liberar referencias de memoria de líneas y outputs para permitir que el GC
				// recupere memoria inmediatamente en lugar de retener miles de buffers.
				for i := range res.Results {
					res.Results[i].Line.Content = nil
					res.Results[i].Output = nil
				}
				res.Results = nil
			}
		}
	}
}

// Stats contiene el resumen cuantitativo acumulado del pipeline al finalizar la ejecución.
type Stats struct {
	TotalChunks  uint64
	TotalLines   uint64
	SuccessLines uint64
	FailedLines  uint64
}
