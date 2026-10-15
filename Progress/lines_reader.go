package progress

import (
	"io"
)

// LinesReader administra la lectura por bloques (chunks) a partir de una lista []Line existente.
// Preserva los Line.ID originales e inmutables para trazabilidad exacta en rondas de reintento Multi-Pass.
type LinesReader struct {
	lines     []Line
	pool      *ChunkPool
	sched     *AdaptiveScheduler
	chunkSize int
	currIdx   int
	currSeqID uint64
}

// NewLinesReader crea un nuevo LinesReader para una lista dada de Line.
func NewLinesReader(lines []Line, opts ReaderOptions) *LinesReader {
	if opts.ChunkSize <= 0 {
		opts.ChunkSize = 1000
	}
	if opts.ChunkPool == nil {
		opts.ChunkPool = NewChunkPool(opts.ChunkSize)
	}

	return &LinesReader{
		lines:     lines,
		pool:      opts.ChunkPool,
		sched:     opts.Scheduler,
		chunkSize: opts.ChunkSize,
	}
}

// NextChunk lee el siguiente bloque contiguo de líneas y lo encapsula en un *Chunk.
func (r *LinesReader) NextChunk() (*Chunk, error) {
	if r.currIdx >= len(r.lines) {
		return nil, io.EOF
	}

	targetChunkSize := r.chunkSize
	if r.sched != nil {
		remaining := len(r.lines) - r.currIdx
		targetChunkSize = r.sched.CurrentChunkSize(remaining)
	}

	chk := r.pool.Get()
	r.currSeqID++
	chk.SequenceID = r.currSeqID
	chk.StartOffset = int64(r.currIdx)

	linesRead := 0
	for linesRead < targetChunkSize && r.currIdx < len(r.lines) {
		line := r.lines[r.currIdx]
		r.currIdx++

		chk.Lines = append(chk.Lines, line)
		linesRead++
	}

	chk.EndOffset = int64(r.currIdx)

	if len(chk.Lines) == 0 {
		r.pool.Put(chk)
		return nil, io.EOF
	}

	return chk, nil
}

// TotalLines retorna la cantidad de líneas contenidas en el lector.
func (r *LinesReader) TotalLines() int {
	return len(r.lines)
}

// Close no requiere liberar recursos de archivo para LinesReader.
func (r *LinesReader) Close() error {
	return nil
}
