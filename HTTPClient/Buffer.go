package HTTPClient

import (
	"bytes"
	"sync"
)

var bufferPool = sync.Pool{
	New: func() any {
		buf := new(bytes.Buffer)
		buf.Grow(32 * 1024)
		return buf
	},
}

// GetBuffer obtiene un *bytes.Buffer limpio del pool.
func GetBuffer() *bytes.Buffer {
	buf := bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	return buf
}

// PutBuffer retorna el buffer al pool. No debe usarse el buffer después
// de llamar a esta función (misma regla que sync.Pool en general).
func PutBuffer(buf *bytes.Buffer) {
	if buf == nil {
		return
	}
	// Evita que buffers grandes (ej. respuesta anómala) queden reteniendo
	// memoria RAM en el pool de manera indefinida. El 99% de las peticiones
	// entrarán en 128KB.
	if buf.Cap() > 128*1024 {
		return
	}
	bufferPool.Put(buf)
}
