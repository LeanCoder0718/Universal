package progress

import (
	"io"
)

// SliceReader administra la lectura por bloques (chunks) a partir de una lista []string en memoria.
type SliceReader struct {
	lines      []string
	pool       *ChunkPool
	sched      *AdaptiveScheduler
	chunkSize  int
	currIdx    int
	currLineID uint64
	currSeqID  uint64
}

// NewSliceReader retorna una instancia de SliceReader lista para streaming de chunks.
func NewSliceReader(lines []string, opts ReaderOptions) *SliceReader {
	if opts.ChunkSize <= 0 {
		opts.ChunkSize = 1000
	}
	if opts.ChunkPool == nil {
		opts.ChunkPool = NewChunkPool(opts.ChunkSize)
	}

	return &SliceReader{
		lines:     lines,
		pool:      opts.ChunkPool,
		sched:     opts.Scheduler,
		chunkSize: opts.ChunkSize,
	}
}

// NextChunk lee el siguiente bloque contiguo de líneas desde el slice y lo encapsula en un *Chunk.
func (r *SliceReader) NextChunk() (*Chunk, error) {
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
		lineStr := r.lines[r.currIdx]
		r.currIdx++
		r.currLineID++

		chk.Lines = append(chk.Lines, Line{
			ID:      r.currLineID,
			Content: []byte(lineStr),
		})
		linesRead++
	}

	chk.EndOffset = int64(r.currIdx)

	if len(chk.Lines) == 0 {
		r.pool.Put(chk)
		return nil, io.EOF
	}

	return chk, nil
}

// Close no requiere liberar recursos de archivo para SliceReader.
func (r *SliceReader) Close() error {
	return nil
}
