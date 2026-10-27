package progress

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
)

// WorkerPoolInterface define la abstracción para iniciar el worker pool primario y escalarlo en caliente.
type WorkerPoolInterface interface {
	NumWorkers() int
	Start(ctx context.Context, chunkQueue <-chan *Chunk, resultQueue chan<- *ChunkResult, wg *sync.WaitGroup)
	AddWorkers(delta int) int
	RemoveWorkers(delta int) int
	CurrentWorkers() int
}

// RetryPoolInterface define la abstracción para iniciar, drenar y cerrar el pool de reintentos desacoplado.
type RetryPoolInterface interface {
	Start(ctx context.Context, resultQueue chan<- *ChunkResult, wg *sync.WaitGroup)
	DrainQueue(w *bufio.Writer) error
	Close()
}

// ChunkReader define la interfaz para lectores de chunks de datos (archivos o slices en memoria).
type ChunkReader interface {
	NextChunk() (*Chunk, error)
	Close() error
}

// Pipeline coordina las 3 etapas del procesamiento masivo con soporte de control P/C/G, reintentos y métricas.
type Pipeline struct {
	reader     ChunkReader
	pool       WorkerPoolInterface
	sink       SinkInterface
	numW       int
	checkpoint *Tracker
	plane      *Plane
	retryPool  RetryPoolInterface
	collector  *Collector
	chunkQueue chan *Chunk

	// Multi-Pass / Rondas de reintento en lotes
	maxRetries        int
	retryClassifier   IsRetryableError
	onRoundTransition func(round int, failedCount int)
	failedRetryLines  []Line
	failedRetryMu     sync.Mutex
	lastFailedSpool   string
}

// NewPipeline crea una nueva instancia de Pipeline.
func NewPipeline(reader ChunkReader, pool WorkerPoolInterface, sink SinkInterface) *Pipeline {
	return &Pipeline{
		reader: reader,
		pool:   pool,
		sink:   sink,
		numW:   pool.NumWorkers(),
	}
}

// PipelineBuilder proporciona una API fluent para construir un Pipeline
// eliminando el boilerplate de múltiples llamadas Set*().
//
// Ejemplo de uso:
//
//	pipe := progress.Build(reader, pool, sink).
//	    WithCheckpoint(tracker).
//	    WithControlPlane(plane).
//	    WithMaxRetries(2).
//	    OnRoundTransition(func(round int, failedCount int) { ... }).
//	    Pipeline()
type PipelineBuilder struct {
	pipe *Pipeline
}

// Build inicia la construcción fluent de un Pipeline.
func Build(reader ChunkReader, pool WorkerPoolInterface, sink SinkInterface) *PipelineBuilder {
	return &PipelineBuilder{
		pipe: NewPipeline(reader, pool, sink),
	}
}

// WithCheckpoint asigna el tracker de checkpoint.
func (b *PipelineBuilder) WithCheckpoint(cp *Tracker) *PipelineBuilder {
	b.pipe.checkpoint = cp
	return b
}

// WithControlPlane asigna el plano de control P/C/G.
func (b *PipelineBuilder) WithControlPlane(plane *Plane) *PipelineBuilder {
	b.pipe.plane = plane
	return b
}

// WithRetryPool asigna el subsistema de reintentos desacoplado.
func (b *PipelineBuilder) WithRetryPool(rp RetryPoolInterface) *PipelineBuilder {
	b.pipe.retryPool = rp
	return b
}

// WithCollector asigna el recolector de métricas.
func (b *PipelineBuilder) WithCollector(col *Collector) *PipelineBuilder {
	b.pipe.collector = col
	return b
}

// WithMaxRetries define el número máximo de rondas de reintento en lotes.
// 0 = sin reintentos (pasada única). 1 = 1 ronda de reintento con todos los workers para los fallos, etc.
func (b *PipelineBuilder) WithMaxRetries(retries int) *PipelineBuilder {
	b.pipe.maxRetries = retries
	return b
}

// WithRetryClassifier asigna el clasificador para determinar qué errores ameritan reintento.
func (b *PipelineBuilder) WithRetryClassifier(fn IsRetryableError) *PipelineBuilder {
	b.pipe.retryClassifier = fn
	return b
}

// OnRoundTransition asigna un callback notificador invocado antes de iniciar cada ronda de reintento.
func (b *PipelineBuilder) OnRoundTransition(fn func(round int, failedCount int)) *PipelineBuilder {
	b.pipe.onRoundTransition = fn
	return b
}

// Pipeline retorna el Pipeline completamente configurado.
func (b *PipelineBuilder) Pipeline() *Pipeline {
	return b.pipe
}

// SetCheckpoint asigna un tracker de checkpoint al pipeline.
func (p *Pipeline) SetCheckpoint(cp *Tracker) {
	p.checkpoint = cp
}

// SetControlPlane asigna el plano de control P/C/G al pipeline.
func (p *Pipeline) SetControlPlane(plane *Plane) {
	p.plane = plane
}

// SetRetryPool asigna el subsistema de reintentos asíncronos desacoplado.
func (p *Pipeline) SetRetryPool(rp RetryPoolInterface) {
	p.retryPool = rp
}

// SetCollector asigna el recolector de métricas atómicas en tiempo real.
func (p *Pipeline) SetCollector(col *Collector) {
	p.collector = col
}

// SetMaxRetries configura el número máximo de rondas de reintento en lotes.
func (p *Pipeline) SetMaxRetries(retries int) {
	p.maxRetries = retries
}

// SetRetryClassifier configura el clasificador de errores para reintento por rondas.
func (p *Pipeline) SetRetryClassifier(fn IsRetryableError) {
	p.retryClassifier = fn
}

// SetOnRoundTransition asigna un callback para notificar la transición de rondas.
func (p *Pipeline) SetOnRoundTransition(fn func(round int, failedCount int)) {
	p.onRoundTransition = fn
}

// AddWorkers incrementa la cantidad de workers concurrentes en caliente.
func (p *Pipeline) AddWorkers(delta int) int {
	if p.pool != nil {
		return p.pool.AddWorkers(delta)
	}
	return p.numW
}

// RemoveWorkers decrementa la cantidad de workers concurrentes en caliente.
func (p *Pipeline) RemoveWorkers(delta int) int {
	if p.pool != nil {
		return p.pool.RemoveWorkers(delta)
	}
	return p.numW
}

// CurrentWorkers retorna el total de workers activos en el pool.
func (p *Pipeline) CurrentWorkers() int {
	if p.pool != nil {
		return p.pool.CurrentWorkers()
	}
	return p.numW
}

// Pause pausa el pipeline en caliente.
func (p *Pipeline) Pause() {
	if p.plane != nil {
		p.plane.Pause()
	}
}

// Resume reanuda el pipeline en caliente.
func (p *Pipeline) Resume() {
	if p.plane != nil {
		p.plane.Resume()
	}
}

// IsPaused indica si el pipeline está pausado en caliente.
func (p *Pipeline) IsPaused() bool {
	if p.plane != nil {
		return p.plane.IsPaused()
	}
	return false
}

// ChunkQueue retorna la cola de prefetch de chunks actual.
func (p *Pipeline) ChunkQueue() <-chan *Chunk {
	return p.chunkQueue
}

// SavePending guarda de forma 100% exacta todas las líneas pendientes en caliente.
// Incluye tanto la cola activa de chunks como las líneas esperando encoladas para rondas posteriores.
func (p *Pipeline) SavePending(sourceFilePath string, outputPath string) (string, error) {
	if p.plane == nil {
		return "", fmt.Errorf("control plane no configurado")
	}

	var drainRetry func(w *bufio.Writer) error
	if p.retryPool != nil {
		drainRetry = p.retryPool.DrainQueue
	}

	drainCombined := func(w *bufio.Writer) error {
		if dp, ok := p.pool.(interface{ DrainPendingLines(w *bufio.Writer) error }); ok {
			if err := dp.DrainPendingLines(w); err != nil {
				return err
			}
		}
		if drainRetry != nil {
			if err := drainRetry(w); err != nil {
				return err
			}
		}
		p.failedRetryMu.Lock()
		spoolFile := p.lastFailedSpool
		for _, fl := range p.failedRetryLines {
			if _, err := w.Write(fl.Content); err != nil {
				p.failedRetryMu.Unlock()
				return err
			}
			if _, err := w.WriteString("\n"); err != nil {
				p.failedRetryMu.Unlock()
				return err
			}
		}
		p.failedRetryMu.Unlock()

		if spoolFile != "" {
			f, err := os.Open(spoolFile)
			if err == nil {
				r := bufio.NewReaderSize(f, 64*1024)
				var hdr [12]byte
				for {
					if _, err := io.ReadFull(r, hdr[:]); err != nil {
						break
					}
					cLen := binary.LittleEndian.Uint32(hdr[8:12])
					content := make([]byte, cLen)
					if cLen > 0 {
						if _, err := io.ReadFull(r, content); err != nil {
							break
						}
					}
					if _, err := w.Write(content); err != nil {
						f.Close()
						return err
					}
					if _, err := w.WriteString("\n"); err != nil {
						f.Close()
						return err
					}
				}
				f.Close()
			}
		}
		return nil
	}

	return p.plane.Save(sourceFilePath, outputPath, p.chunkQueue, drainCombined)
}

// DrainFailedLines drena todas las líneas fallidas de la última ronda en streaming Zero-RAM
// directamente desde el archivo temporal de spool y elimina el archivo al terminar.
func (p *Pipeline) DrainFailedLines(fn func(Line)) {
	p.failedRetryMu.Lock()
	spoolFile := p.lastFailedSpool
	p.lastFailedSpool = ""
	p.failedRetryLines = nil
	p.failedRetryMu.Unlock()

	if spoolFile == "" {
		return
	}
	defer os.Remove(spoolFile)

	f, err := os.Open(spoolFile)
	if err != nil {
		return
	}
	defer f.Close()

	r := bufio.NewReaderSize(f, 256*1024)
	var hdr [12]byte
	for {
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			break
		}
		lineID := binary.LittleEndian.Uint64(hdr[0:8])
		cLen := binary.LittleEndian.Uint32(hdr[8:12])
		content := make([]byte, cLen)
		if cLen > 0 {
			if _, err := io.ReadFull(r, content); err != nil {
				break
			}
		}
		if fn != nil {
			fn(Line{ID: lineID, Content: content})
		}
	}
}

// GetFailedLines retorna una copia de las líneas que no pudieron procesarse tras agotar todas las rondas de reintento.
// Para archivos masivos (>100k líneas), prefiera DrainFailedLines para preservar memoria O(1).
func (p *Pipeline) GetFailedLines() []Line {
	var list []Line
	p.DrainFailedLines(func(l Line) {
		list = append(list, l)
	})
	return list
}

type spoolFileWriter struct {
	file   *os.File
	writer *bufio.Writer
	mu     sync.Mutex
	count  int64
}

func newSpoolFileWriter() (*spoolFileWriter, error) {
	tmp, err := os.CreateTemp("", "rabbit_spool_*.tmp")
	if err != nil {
		return nil, err
	}
	return &spoolFileWriter{
		file:   tmp,
		writer: bufio.NewWriterSize(tmp, 256*1024),
	}, nil
}

func (w *spoolFileWriter) WriteLine(line Line) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	var hdr [12]byte
	binary.LittleEndian.PutUint64(hdr[0:8], line.ID)
	binary.LittleEndian.PutUint32(hdr[8:12], uint32(len(line.Content)))

	if _, err := w.writer.Write(hdr[:]); err != nil {
		return err
	}
	if len(line.Content) > 0 {
		if _, err := w.writer.Write(line.Content); err != nil {
			return err
		}
	}
	w.count++
	return nil
}

func (w *spoolFileWriter) CloseAndReader(opts ReaderOptions) (*spoolFileReader, error) {
	if err := w.writer.Flush(); err != nil {
		w.file.Close()
		os.Remove(w.file.Name())
		return nil, err
	}
	path := w.file.Name()
	w.file.Close()

	return newSpoolFileReader(path, w.count, opts)
}

func (w *spoolFileWriter) Abort() {
	if w.file != nil {
		path := w.file.Name()
		w.file.Close()
		os.Remove(path)
	}
}

type spoolFileReader struct {
	path       string
	file       *os.File
	reader     *bufio.Reader
	pool       *ChunkPool
	sched      *AdaptiveScheduler
	chunkSize  int
	totalLines int64
	readLines  int64
	currSeqID  uint64
	closed     bool
}

func newSpoolFileReader(path string, totalLines int64, opts ReaderOptions) (*spoolFileReader, error) {
	if opts.ChunkSize <= 0 {
		opts.ChunkSize = 1000
	}
	if opts.ChunkPool == nil {
		opts.ChunkPool = NewChunkPool(opts.ChunkSize)
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return &spoolFileReader{
		path:       path,
		file:       f,
		reader:     bufio.NewReaderSize(f, 256*1024),
		pool:       opts.ChunkPool,
		sched:      opts.Scheduler,
		chunkSize:  opts.ChunkSize,
		totalLines: totalLines,
	}, nil
}

func (r *spoolFileReader) NextChunk() (*Chunk, error) {
	if r.readLines >= r.totalLines {
		return nil, io.EOF
	}

	targetSize := r.chunkSize
	if r.sched != nil {
		rem := int(r.totalLines - r.readLines)
		targetSize = r.sched.CurrentChunkSize(rem)
	}

	chk := r.pool.Get()
	r.currSeqID++
	chk.SequenceID = r.currSeqID
	chk.StartOffset = r.readLines

	var hdr [12]byte
	for len(chk.Lines) < targetSize && r.readLines < r.totalLines {
		if _, err := io.ReadFull(r.reader, hdr[:]); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return nil, err
		}

		lineID := binary.LittleEndian.Uint64(hdr[0:8])
		cLen := binary.LittleEndian.Uint32(hdr[8:12])

		content := make([]byte, cLen)
		if cLen > 0 {
			if _, err := io.ReadFull(r.reader, content); err != nil {
				return nil, err
			}
		}

		chk.Lines = append(chk.Lines, Line{
			ID:      lineID,
			Content: content,
		})
		r.readLines++
	}

	if len(chk.Lines) == 0 {
		return nil, io.EOF
	}
	return chk, nil
}

func (r *spoolFileReader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	var err error
	if r.file != nil {
		err = r.file.Close()
		os.Remove(r.path)
	}
	return err
}

// Run ejecuta el pipeline completo en streaming con soporte nativo de Multi-Pass Round Retries y Spooling Zero-RAM.
func (p *Pipeline) Run(ctx context.Context) (Stats, error) {
	defer p.reader.Close()

	if p.collector != nil {
		go p.collector.Run(ctx)
	}

	var totalStats Stats
	var initialTotalLines uint64

	currentReader := p.reader
	maxRetries := p.maxRetries
	if maxRetries < 0 {
		maxRetries = 0
	}

	var activeSpoolReader *spoolFileReader
	defer func() {
		if activeSpoolReader != nil {
			activeSpoolReader.Close()
		}
	}()

	for round := 0; round <= maxRetries; round++ {
		// Preparar spool writer para capturar fallos de esta ronda con O(1) RAM
		spoolWriter, sErr := newSpoolFileWriter()
		if sErr != nil {
			return totalStats, fmt.Errorf("pipeline: error inicializando spool de reintentos: %w", sErr)
		}

		collectFail := func(line Line, err error) {
			if p.retryClassifier == nil || p.retryClassifier(err) {
				if spoolWriter != nil {
					_ = spoolWriter.WriteLine(line)
				}
			}
		}

		roundStats, passErr := p.runPass(ctx, currentReader, collectFail)
		if round == 0 {
			initialTotalLines = roundStats.TotalLines
			totalStats.TotalLines = roundStats.TotalLines
			totalStats.SuccessLines = roundStats.SuccessLines
			totalStats.TotalChunks = roundStats.TotalChunks
		} else {
			totalStats.SuccessLines += roundStats.SuccessLines
			totalStats.TotalChunks += roundStats.TotalChunks
		}

		if passErr != nil && passErr != context.Canceled {
			if spoolWriter != nil {
				spoolWriter.Abort()
			}
			return totalStats, passErr
		}
		if ctx.Err() != nil {
			if spoolWriter != nil {
				spoolWriter.Abort()
			}
			break
		}

		// Si agotamos las rondas de reintento
		if round == maxRetries {
			if spoolWriter != nil {
				_ = spoolWriter.writer.Flush()
				p.failedRetryMu.Lock()
				if spoolWriter.count > 0 {
					p.lastFailedSpool = spoolWriter.file.Name()
					spoolWriter.file.Close()
				} else {
					spoolWriter.Abort()
				}
				p.failedRetryMu.Unlock()
			}
			break
		}

		// Rondas intermedias: preparar para la siguiente ronda si hubo fallos
		_ = spoolWriter.writer.Flush()
		pendingCount := spoolWriter.count

		if pendingCount == 0 {
			spoolWriter.Abort()
			break
		}

		if p.onRoundTransition != nil {
			p.onRoundTransition(round+1, int(pendingCount))
		}

		// Chunks pequeños para que los N workers tomen trabajo de inmediato en la ronda de reintento
		chunkSz := 1
		if pendingCount > int64(p.numW*5) {
			chunkSz = int(pendingCount) / (p.numW * 2)
			if chunkSz > 10 {
				chunkSz = 10
			}
		}

		if activeSpoolReader != nil {
			activeSpoolReader.Close()
			activeSpoolReader = nil
		}

		spoolR, rErr := spoolWriter.CloseAndReader(ReaderOptions{
			ChunkSize: chunkSz,
		})
		if rErr != nil {
			return totalStats, fmt.Errorf("pipeline: error abriendo reader de spool de reintentos: %w", rErr)
		}
		activeSpoolReader = spoolR
		currentReader = activeSpoolReader
	}

	if initialTotalLines >= totalStats.SuccessLines {
		totalStats.FailedLines = initialTotalLines - totalStats.SuccessLines
	} else {
		totalStats.FailedLines = 0
	}

	return totalStats, nil
}

// runPass ejecuta una pasada individual del pipeline sobre el reader especificado.
func (p *Pipeline) runPass(ctx context.Context, reader ChunkReader, onLineFail func(line Line, err error)) (Stats, error) {
	numWorkers := p.numW
	if numWorkers <= 0 {
		numWorkers = 1
	}

	// Buffer sizing inteligente con cap para evitar explosión de RAM a 1k-5k workers.
	chunkBufSize := numWorkers * 4
	if chunkBufSize > 2048 {
		chunkBufSize = 2048
	}
	resultBufSize := numWorkers * 4
	if resultBufSize > 4096 {
		resultBufSize = 4096
	}

	p.chunkQueue = make(chan *Chunk, chunkBufSize)
	rawResultQueue := make(chan *ChunkResult, resultBufSize)
	sinkResultQueue := make(chan *ChunkResult, resultBufSize)

	var wgPrimary sync.WaitGroup
	var wgRetry sync.WaitGroup

	p.pool.Start(ctx, p.chunkQueue, rawResultQueue, &wgPrimary)

	if p.retryPool != nil {
		p.retryPool.Start(ctx, rawResultQueue, &wgRetry)
	}

	// Interceptor que captura fallos de línea para la siguiente ronda sin alterar el Sink
	go func() {
		defer close(sinkResultQueue)
		for res := range rawResultQueue {
			if res != nil {
				for _, lr := range res.Results {
					if !lr.Success && onLineFail != nil {
						// Si retryPool está activo, este gestiona los reintentos en caliente
						// evitando duplicar tareas en el spool de Multi-Pass.
						if p.retryPool == nil {
							onLineFail(lr.Line, lr.Err)
						}
					}
				}
			}
			select {
			case sinkResultQueue <- res:
			case <-ctx.Done():
				return
			}
		}
	}()

	go func() {
		wgPrimary.Wait()

		if p.retryPool != nil {
			p.retryPool.Close()
			wgRetry.Wait()
		}

		close(rawResultQueue)
	}()

	errChan := make(chan error, 1)
	go func() {
		defer close(p.chunkQueue)
		for {
			if p.plane != nil {
				if err := p.plane.WaitIfPaused(ctx); err != nil {
					errChan <- err
					return
				}
			}

			chk, err := reader.NextChunk()
			if err != nil {
				if err == io.EOF {
					errChan <- nil
					return
				}
				errChan <- err
				return
			}

			if p.checkpoint != nil {
				p.checkpoint.SetReaderOffset(chk.EndOffset)
			}
			if p.collector != nil {
				p.collector.IncrChunksCreated()
			}

			select {
			case <-ctx.Done():
				errChan <- ctx.Err()
				return
			case p.chunkQueue <- chk:
			}
		}
	}()

	stats, sinkErr := p.sink.Run(ctx, sinkResultQueue)

	dispatchErr := <-errChan
	if dispatchErr != nil {
		return stats, dispatchErr
	}

	return stats, sinkErr
}
