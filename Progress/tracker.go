package progress

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ComboFileName string
	ComboFilePath string
)

// WorkerProgress almacena el progreso de procesamiento de un worker individual.
type WorkerProgress struct {
	WorkerID    int
	SequenceID  uint64
	LineIndex   int    // Índice dentro del chunk actual (0-indexed)
	CurrentLine Line   // Línea actual en proceso
	ActiveChunk *Chunk // Chunk completo que tiene asignado el worker
}

const trackerShardCount = 64

type trackerShard struct {
	mu       sync.RWMutex
	progress map[int]*WorkerProgress
}

// Tracker administra el estado de avance global y por worker para checkpoints.
// Utiliza sharding de 64 particiones para permitir 5.000+ workers concurrentes
// sin contención de locks en el hot path.
type Tracker struct {
	readerOffset atomic.Int64
	shards       [trackerShardCount]trackerShard
}

// NewTracker crea una instancia de Tracker inicializando sus shards concurrentes.
func NewTracker() *Tracker {
	t := &Tracker{}
	for i := 0; i < trackerShardCount; i++ {
		t.shards[i].progress = make(map[int]*WorkerProgress)
	}
	return t
}

// SetReaderOffset actualiza el offset en bytes alcanzado por el reader de forma atómica.
func (t *Tracker) SetReaderOffset(offset int64) {
	t.readerOffset.Store(offset)
}

// GetReaderOffset retorna el offset actual del reader de forma atómica y lock-free.
func (t *Tracker) GetReaderOffset() int64 {
	return t.readerOffset.Load()
}

func (t *Tracker) getShard(workerID int) *trackerShard {
	u := uint(workerID)
	return &t.shards[u%trackerShardCount]
}

// UpdateWorkerProgress actualiza la posición actual de un worker procesando un chunk (modo coarse).
func (t *Tracker) UpdateWorkerProgress(workerID int, chk *Chunk, lineIndex int) {
	shard := t.getShard(workerID)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	wp, ok := shard.progress[workerID]
	if !ok {
		wp = &WorkerProgress{WorkerID: workerID}
		shard.progress[workerID] = wp
	}

	wp.ActiveChunk = chk
	if chk != nil {
		wp.SequenceID = chk.SequenceID
		wp.LineIndex = lineIndex
		if lineIndex < len(chk.Lines) {
			wp.CurrentLine = chk.Lines[lineIndex]
		}
	} else {
		wp.SequenceID = 0
		wp.LineIndex = 0
	}
}

// UpdateWorkerLine actualiza la línea individual que está siendo procesada por un worker en modo despacho fino.
func (t *Tracker) UpdateWorkerLine(workerID int, line Line) {
	shard := t.getShard(workerID)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	wp, ok := shard.progress[workerID]
	if !ok {
		wp = &WorkerProgress{WorkerID: workerID}
		shard.progress[workerID] = wp
	}
	wp.ActiveChunk = nil
	wp.CurrentLine = line
	wp.LineIndex = 0
	wp.SequenceID = 0
}

// ClearWorkerProgress marca que un worker terminó su tarea actual.
func (t *Tracker) ClearWorkerProgress(workerID int) {
	shard := t.getShard(workerID)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	if wp, ok := shard.progress[workerID]; ok {
		wp.ActiveChunk = nil
		wp.CurrentLine = Line{}
		wp.LineIndex = 0
		wp.SequenceID = 0
	}
}

type progressWriter struct {
	target     io.Writer
	totalBytes int64
	written    int64
	lastReport time.Time
	totalLines uint64
}

func (pw *progressWriter) Write(p []byte) (int, error) {
	n, err := pw.target.Write(p)
	pw.written += int64(n)

	for _, b := range p[:n] {
		if b == '\n' {
			pw.totalLines++
		}
	}

	if time.Since(pw.lastReport) > 50*time.Millisecond || pw.written >= pw.totalBytes {
		pw.render()
		pw.lastReport = time.Now()
	}

	return n, err
}

func (pw *progressWriter) render() {
	total := pw.totalBytes
	if total <= 0 {
		total = 1
	}

	pct := float64(pw.written) / float64(total) * 100.0
	if pct > 100.0 {
		pct = 100.0
	}

	const barWidth = 30
	filled := int((pct / 100.0) * float64(barWidth))
	if filled > barWidth {
		filled = barWidth
	}

	bar := ""
	for i := 0; i < filled; i++ {
		bar += "█"
	}
	for i := filled; i < barWidth; i++ {
		bar += "░"
	}

	currMB := float64(pw.written) / (1024 * 1024)
	totMB := float64(total) / (1024 * 1024)

	fmt.Printf("\r\033[1;33m[GUARDANDO EN CALIENTE G]\033[0m \033[1;36m[%s]\033[0m \033[1;32m%5.1f%%\033[0m (%.2f MB / %.2f MB | %d líneas)",
		bar, pct, currMB, totMB, pw.totalLines)
}

// SavePendingSnapshot genera un archivo `pending_<timestamp>.txt` guardando exactamente las líneas pendientes.
func (t *Tracker) SavePendingSnapshot(
	sourceFilePath string,
	outputPath string,
	chunkQueue <-chan *Chunk,
	drainRetryQueue func(w *bufio.Writer) error,
) (string, error) {
	fmt.Println("\n\033[1;33m=========================================================================\033[0m")
	fmt.Println("\033[1;33m [!] INICIANDO GUARDADO DE LÍNEAS PENDIENTES EN CALIENTE...\033[0m")
	fmt.Println("\033[1;33m=========================================================================\033[0m")

	if outputPath == "" {
		dateFolder := fmt.Sprintf("Remaining[%s]", time.Now().Format("02-01"))
		dir := filepath.Join("Combos", dateFolder)
		if err := os.MkdirAll(dir, 0755); err != nil {
			dir = "."
		}

		baseName := "combos"
		if ComboFileName != "" {
			baseName = ComboFileName
		}
		timeStr := time.Now().Format("15h04m05s")
		outputPath = filepath.Join(dir, fmt.Sprintf("%s_%s.txt", baseName, timeStr))
	} else {
		dir := filepath.Dir(outputPath)
		if dir != "" && dir != "." {
			_ = os.MkdirAll(dir, 0755)
		}
	}

	var srcFileSize int64
	if sourceFilePath != "" {
		if srcInfo, err := os.Stat(sourceFilePath); err == nil {
			srcFileSize = srcInfo.Size()
		}
	}

	var estimatedInMemoryBytes int64
	for i := 0; i < trackerShardCount; i++ {
		shard := &t.shards[i]
		shard.mu.RLock()
		for _, wp := range shard.progress {
			if wp.ActiveChunk != nil && wp.LineIndex < len(wp.ActiveChunk.Lines) {
				for _, line := range wp.ActiveChunk.Lines[wp.LineIndex:] {
					estimatedInMemoryBytes += int64(len(line.Content) + 1)
				}
			} else if len(wp.CurrentLine.Content) > 0 {
				estimatedInMemoryBytes += int64(len(wp.CurrentLine.Content) + 1)
			}
		}
		shard.mu.RUnlock()
	}

	remainingSrcBytes := int64(0)
	if sourceFilePath != "" {
		remainingSrcBytes = srcFileSize - t.readerOffset.Load()
		if remainingSrcBytes < 0 {
			remainingSrcBytes = 0
		}
	}

	totalEstimatedBytes := estimatedInMemoryBytes + remainingSrcBytes
	if totalEstimatedBytes <= 0 {
		totalEstimatedBytes = 1
	}

	outFile, err := os.Create(outputPath)
	if err != nil {
		return "", fmt.Errorf("error al crear archivo de pendientes: %w", err)
	}
	defer outFile.Close()

	rawBufWriter := bufio.NewWriterSize(outFile, 256*1024)

	progWriter := &progressWriter{
		target:     rawBufWriter,
		totalBytes: totalEstimatedBytes,
		lastReport: time.Now(),
	}

	bufWriter := bufio.NewWriterSize(progWriter, 64*1024)

	// Volcado ordenado de líneas activas por worker:
	// - Si el worker tiene ActiveChunk (modo coarse), escribe las líneas restantes del bloque.
	// - Si el worker tiene CurrentLine (modo fine-grained), escribe ONLY su línea en ejecución.
	// De esta forma se elimina la duplicación de chunks compartidos entre workers.
	for i := 0; i < trackerShardCount; i++ {
		shard := &t.shards[i]
		shard.mu.RLock()
		for _, wp := range shard.progress {
			if wp.ActiveChunk != nil && wp.LineIndex < len(wp.ActiveChunk.Lines) {
				remainingLines := wp.ActiveChunk.Lines[wp.LineIndex:]
				for _, line := range remainingLines {
					if _, err := bufWriter.Write(line.Content); err != nil {
						shard.mu.RUnlock()
						return "", err
					}
					if _, err := bufWriter.WriteString("\n"); err != nil {
						shard.mu.RUnlock()
						return "", err
					}
				}
			} else if len(wp.CurrentLine.Content) > 0 {
				if _, err := bufWriter.Write(wp.CurrentLine.Content); err != nil {
					shard.mu.RUnlock()
					return "", err
				}
				if _, err := bufWriter.WriteString("\n"); err != nil {
					shard.mu.RUnlock()
					return "", err
				}
			}
		}
		shard.mu.RUnlock()
	}

	if chunkQueue != nil {
		for {
			select {
			case chk, ok := <-chunkQueue:
				if !ok || chk == nil {
					goto doneQueue
				}
				for _, line := range chk.Lines {
					if _, err := bufWriter.Write(line.Content); err != nil {
						return "", err
					}
					if _, err := bufWriter.WriteString("\n"); err != nil {
						return "", err
					}
				}
			default:
				goto doneQueue
			}
		}
	doneQueue:
	}

	if drainRetryQueue != nil {
		if err := drainRetryQueue(bufWriter); err != nil {
			return "", err
		}
	}

	if sourceFilePath != "" {
		srcFile, err := os.Open(sourceFilePath)
		if err != nil {
			return "", fmt.Errorf("error al abrir archivo fuente para guardar pendientes: %w", err)
		}
		defer srcFile.Close()

		offset := t.readerOffset.Load()
		if offset > 0 {
			if _, err := srcFile.Seek(offset, io.SeekStart); err != nil {
				return "", fmt.Errorf("error haciendo Seek en archivo fuente: %w", err)
			}
		}

		if _, err := io.Copy(bufWriter, srcFile); err != nil {
			return "", fmt.Errorf("error copiando remanente de archivo fuente: %w", err)
		}
	}

	if err := bufWriter.Flush(); err != nil {
		return "", fmt.Errorf("error haciendo Flush en buffer de progreso: %w", err)
	}

	if err := rawBufWriter.Flush(); err != nil {
		return "", fmt.Errorf("error haciendo Flush final: %w", err)
	}

	if err := outFile.Sync(); err != nil {
		return "", fmt.Errorf("error haciendo Sync en archivo de pendientes: %w", err)
	}

	progWriter.written = progWriter.totalBytes
	progWriter.render()

	fmt.Printf("\n\033[1;32m[✓ GUARDADO COMPLETADO EXITOSAMENTE]\033[0m\n")
	fmt.Printf(" \033[1;37m[>] Archivo Generado : %s\033[0m\n", outputPath)
	fmt.Printf(" \033[1;37m[>] Líneas Guardadas : %d líneas\033[0m\n", progWriter.totalLines)
	fmt.Printf(" \033[1;37m[>] Peso Total       : %.2f MB\033[0m\n", float64(progWriter.written)/(1024*1024))
	fmt.Printf("\033[1;33m=========================================================================\033[0m\n\n")

	return outputPath, nil
}
