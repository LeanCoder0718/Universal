package progress

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
)

// ReaderOptions contiene la configuración requerida para inicializar FileReader.
type ReaderOptions struct {
	BufferSize int                // Tamaño en bytes del buffer de lectura bufio (default 256KB)
	ChunkSize  int                // Tamaño inicial de líneas por chunk
	ChunkPool  *ChunkPool         // Pool sync.Pool para reciclar asignaciones de chunks
	Scheduler  *AdaptiveScheduler // Programador adaptativo opcional
}

// FileReader administra la lectura de archivos .txt mediante bufio.Reader optimizado en streaming.
type FileReader struct {
	file         *os.File
	bufReader    *bufio.Reader
	pool         *ChunkPool
	sched        *AdaptiveScheduler
	chunkSize    int
	currLineID   uint64
	currSeqID    uint64
	currOffset   int64
	bytesReadAcc int64
	fileSize     int64  // Tamaño total del archivo para estimar items restantes
	totalLines   uint64 // Líneas leídas hasta ahora
	lineBuf      []byte // Buffer reutilizable para readLineBytes (evita allocs por línea)
}

// NewFileReader abre el archivo especificado y retorna un FileReader listo para streaming.
func NewFileReader(filePath string, opts ReaderOptions) (*FileReader, error) {
	if opts.BufferSize <= 0 {
		opts.BufferSize = 256 * 1024
	}
	if opts.ChunkSize <= 0 {
		opts.ChunkSize = 1000
	}
	if opts.ChunkPool == nil {
		opts.ChunkPool = NewChunkPool(opts.ChunkSize)
	}

	f, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("error al abrir archivo fuente: %w", err)
	}

	// Obtener tamaño del archivo para estimación de items restantes
	var fileSize int64
	if info, statErr := f.Stat(); statErr == nil {
		fileSize = info.Size()
	}

	return &FileReader{
		file:      f,
		bufReader: bufio.NewReaderSize(f, opts.BufferSize),
		pool:      opts.ChunkPool,
		sched:     opts.Scheduler,
		chunkSize: opts.ChunkSize,
		fileSize:  fileSize,
		lineBuf:   make([]byte, 0, 512),
	}, nil
}

// SeekToOffset reposiciona el cursor del archivo y del buffer al offset especificado en bytes.
func (r *FileReader) SeekToOffset(offset int64) error {
	if offset < 0 {
		return fmt.Errorf("offset invalido: %d", offset)
	}
	_, err := r.file.Seek(offset, io.SeekStart)
	if err != nil {
		return fmt.Errorf("error haciendo Seek en archivo fuente: %w", err)
	}
	r.bufReader.Reset(r.file)
	r.currOffset = offset
	r.bytesReadAcc = offset
	return nil
}

// NextChunk lee el siguiente bloque contiguo de líneas y lo encapsula en un *Chunk.
func (r *FileReader) NextChunk() (*Chunk, error) {
	targetChunkSize := r.chunkSize
	if r.sched != nil {
		// Estimar items restantes para habilitar tapering anti-straggler.
		// Usamos bytes restantes / promedio de bytes por línea.
		remaining := 0
		if r.totalLines > 0 && r.fileSize > 0 {
			avgBytesPerLine := float64(r.bytesReadAcc) / float64(r.totalLines)
			if avgBytesPerLine > 0 {
				remainingBytes := r.fileSize - r.bytesReadAcc
				if remainingBytes > 0 {
					remaining = int(float64(remainingBytes) / avgBytesPerLine)
				}
			}
		}
		targetChunkSize = r.sched.CurrentChunkSize(remaining)
	}

	chk := r.pool.Get()
	r.currSeqID++
	chk.SequenceID = r.currSeqID
	chk.StartOffset = r.bytesReadAcc

	linesRead := 0

	for linesRead < targetChunkSize {
		lineBytes, isEOF, err := r.readLineBytes()
		if err != nil && err != io.EOF {
			r.pool.Put(chk)
			return nil, err
		}

		trimmed := bytes.TrimSpace(lineBytes)
		if len(trimmed) > 0 {
			r.currLineID++
			r.totalLines++
			copiedContent := make([]byte, len(trimmed))
			copy(copiedContent, trimmed)

			chk.Lines = append(chk.Lines, Line{
				ID:      r.currLineID,
				Content: copiedContent,
			})
			linesRead++
		}

		if isEOF {
			break
		}
	}

	chk.EndOffset = r.bytesReadAcc

	if len(chk.Lines) == 0 {
		r.pool.Put(chk)
		return nil, io.EOF
	}

	return chk, nil
}

// readLineBytes lee una línea completa reutilizando el buffer interno para minimizar allocations.
func (r *FileReader) readLineBytes() ([]byte, bool, error) {
	// Reutilizar el buffer de línea — evita crear un nuevo slice por cada línea.
	r.lineBuf = r.lineBuf[:0]
	isEOF := false

	for {
		slice, err := r.bufReader.ReadSlice('\n')
		n := len(slice)
		r.bytesReadAcc += int64(n)
		r.lineBuf = append(r.lineBuf, slice...)

		if err == nil {
			break
		}

		if err == bufio.ErrBufferFull {
			continue
		}

		if err == io.EOF {
			isEOF = true
			break
		}

		return nil, false, err
	}

	n := len(r.lineBuf)
	if n > 0 && r.lineBuf[n-1] == '\n' {
		r.lineBuf = r.lineBuf[:n-1]
		n--
	}
	if n > 0 && r.lineBuf[n-1] == '\r' {
		r.lineBuf = r.lineBuf[:n-1]
	}

	return r.lineBuf, isEOF, nil
}

// CurrentOffset retorna el offset acumulado en bytes alcanzado en la lectura del archivo.
func (r *FileReader) CurrentOffset() int64 {
	return r.bytesReadAcc
}

// Close cierra el archivo subyacente.
func (r *FileReader) Close() error {
	if r.file != nil {
		return r.file.Close()
	}
	return nil
}
