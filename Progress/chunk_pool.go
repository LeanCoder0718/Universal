package progress

import (
	"sync"
)

// ChunkPool administra la reutilización de objetos *Chunk mediante sync.Pool
// para minimizar las asignaciones de memoria en el heap y la presión sobre el GC.
type ChunkPool struct {
	pool sync.Pool
}

// NewChunkPool crea una instancia de ChunkPool configurada con la capacidad inicial por defecto.
func NewChunkPool(defaultCapacity int) *ChunkPool {
	if defaultCapacity <= 0 {
		defaultCapacity = 1000
	}
	return &ChunkPool{
		pool: sync.Pool{
			New: func() interface{} {
				return &Chunk{
					Lines: make([]Line, 0, defaultCapacity),
				}
			},
		},
	}
}

// Get obtiene un *Chunk limpio del pool o asigna uno nuevo si el pool está vacío.
func (cp *ChunkPool) Get() *Chunk {
	chk := cp.pool.Get().(*Chunk)
	chk.SequenceID = 0
	chk.StartOffset = 0
	chk.EndOffset = 0
	chk.Lines = chk.Lines[:0]
	return chk
}

// Put limpia los campos del chunk y lo devuelve al pool para su reutilización.
func (cp *ChunkPool) Put(chk *Chunk) {
	if chk == nil {
		return
	}

	for i := range chk.Lines {
		chk.Lines[i].Content = nil
		chk.Lines[i].State = nil
	}

	chk.Lines = chk.Lines[:0]
	cp.pool.Put(chk)
}
