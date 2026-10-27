package progress

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LeanCoder0718/Universal/GeoRoute"
)

// TestPauseAndResume prueba la capacidad de pausar y continuar el pipeline en caliente
func TestPauseAndResume(t *testing.T) {
	tracker := NewTracker()
	plane := NewPlane(tracker)

	if plane.IsPaused() {
		t.Error("Plane should not be paused initially")
	}

	plane.Pause()
	if !plane.IsPaused() {
		t.Error("Plane should be paused after Pause()")
	}

	plane.Resume()
	if plane.IsPaused() {
		t.Error("Plane should not be paused after Resume()")
	}
}

// TestSavePendingSnapshot prueba el guardado en caliente de las líneas restantes
func TestSavePendingSnapshot(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "remaining_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	tracker := NewTracker()
	plane := NewPlane(tracker)

	outFile := filepath.Join(tmpDir, "remaining_output.txt")
	chunkQueue := make(chan *Chunk, 5)

	chk := &Chunk{
		SequenceID: 1,
		Lines: []Line{
			{ID: 1, Content: []byte("user1@domain.com:pass1")},
			{ID: 2, Content: []byte("user2@domain.com:pass2")},
			{ID: 3, Content: []byte("user3@domain.com:pass3")},
		},
	}
	chunkQueue <- chk

	savedPath, err := plane.Save("", outFile, chunkQueue, nil)
	if err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	if savedPath != outFile {
		t.Errorf("savedPath = %s, want %s", savedPath, outFile)
	}

	data, err := os.ReadFile(outFile)
	if err != nil {
		t.Fatalf("Failed to read saved file: %v", err)
	}

	expected := "user1@domain.com:pass1\nuser2@domain.com:pass2\nuser3@domain.com:pass3\n"
	if string(data) != expected {
		t.Errorf("Saved content = %q, want %q", string(data), expected)
	}
}

// TestPipelineDynamicWorkerScaling prueba el escalado de workers en caliente
func TestPipelineDynamicWorkerScaling(t *testing.T) {
	lines := make([]string, 100)
	for i := 0; i < 100; i++ {
		lines[i] = "test line"
	}

	reader := NewSliceReader(lines, ReaderOptions{ChunkSize: 5})
	var processedCount atomic.Uint64

	processFn := func(line *Line) ([]byte, error) {
		processedCount.Add(1)
		time.Sleep(2 * time.Millisecond)
		return []byte("ok"), nil
	}

	tracker := NewTracker()
	plane := NewPlane(tracker)
	wOpts := WorkerOptions{
		NumWorkers:   5,
		ControlPlane: plane,
		Checkpoint:   tracker,
	}
	workerPool := NewWorkerPool(wOpts, processFn)
	sink := NewWriterSink(func(res *ChunkResult) error { return nil })

	pipe := NewPipeline(reader, workerPool, sink)
	pipe.SetControlPlane(plane)
	pipe.SetCheckpoint(tracker)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go func() {
		time.Sleep(20 * time.Millisecond)
		pipe.AddWorkers(5)
		time.Sleep(20 * time.Millisecond)
		pipe.RemoveWorkers(2)
	}()

	stats, err := pipe.Run(ctx)
	if err != nil && err != context.Canceled {
		t.Fatalf("Pipeline run failed: %v", err)
	}

	if stats.TotalLines != 100 {
		t.Errorf("Processed lines = %d, want 100", stats.TotalLines)
	}
}

// TestPipelineBuilder prueba la construcción fluida con PipelineBuilder
func TestPipelineBuilder(t *testing.T) {
	lines := []string{"a:1", "b:2", "c:3"}
	reader := NewSliceReader(lines, ReaderOptions{ChunkSize: 2})
	tracker := NewTracker()
	plane := NewPlane(tracker)
	collector := NewCollector(nil, 100*time.Millisecond, nil)

	wOpts := WorkerOptions{
		NumWorkers:   2,
		ControlPlane: plane,
		Checkpoint:   tracker,
		Collector:    collector,
	}
	workerPool := NewWorkerPool(wOpts, func(line *Line) ([]byte, error) {
		return []byte("ok"), nil
	})
	sink := NewWriterSink(func(res *ChunkResult) error { return nil })

	pipe := Build(reader, workerPool, sink).
		WithCheckpoint(tracker).
		WithControlPlane(plane).
		WithCollector(collector).
		Pipeline()

	if pipe == nil {
		t.Fatal("Pipeline from builder is nil")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	stats, err := pipe.Run(ctx)
	if err != nil {
		t.Fatalf("Pipeline run failed: %v", err)
	}

	if stats.TotalLines != 3 {
		t.Errorf("TotalLines = %d, want 3", stats.TotalLines)
	}
	if stats.SuccessLines != 3 {
		t.Errorf("SuccessLines = %d, want 3", stats.SuccessLines)
	}
}

// TestTokenBucketLimiter prueba el limitador de tasa Token Bucket
func TestTokenBucketLimiter(t *testing.T) {
	limiter := NewTokenBucketLimiter(100, 10)
	if limiter == nil {
		t.Fatal("TokenBucketLimiter is nil")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	// Consumir 5 tokens sin bloqueo
	for i := 0; i < 5; i++ {
		if err := limiter.Wait(ctx); err != nil {
			t.Fatalf("Wait failed: %v", err)
		}
	}
}

// TestDeadLetterSinkFIFOEviction prueba la evicción FIFO cuando se supera maxEntries
func TestDeadLetterSinkFIFOEviction(t *testing.T) {
	dlx := NewDeadLetterSink(nil)
	dlx.SetMaxEntries(10)

	for i := 1; i <= 15; i++ {
		dlx.Record(RecordItem{
			Line:    Line{ID: uint64(i), Content: []byte("fail")},
			Reason:  "test",
			Attempt: 1,
		})
	}

	if dlx.Count() > 10 {
		t.Errorf("Count() = %d, want <= 10", dlx.Count())
	}
	if dlx.Evicted() == 0 {
		t.Error("Evicted() should be > 0")
	}
}

// TestCollectorLatencyAndLPS prueba el histograma de latencia y throughput
func TestCollectorLatencyAndLPS(t *testing.T) {
	col := NewCollector(nil, 100*time.Millisecond, nil)

	col.RecordLatency(5 * time.Millisecond)    // bucket 0 (<10ms)
	col.RecordLatency(20 * time.Millisecond)   // bucket 1 (10-25ms)
	col.RecordLatency(80 * time.Millisecond)   // bucket 3 (50-100ms)
	col.RecordLatency(6000 * time.Millisecond) // bucket 9 (>5000ms)

	summary := col.LatencySummary()
	if summary.TotalSamples != 4 {
		t.Errorf("TotalSamples = %d, want 4", summary.TotalSamples)
	}
	if summary.P50Ms <= 0 {
		t.Errorf("P50Ms = %f, want > 0", summary.P50Ms)
	}

	col.IncrProcessedLines(50)
	if col.TotalProcessedLines() != 50 {
		t.Errorf("TotalProcessedLines() = %d, want 50", col.TotalProcessedLines())
	}
}

// TestPipelineMultiPassRetries verifica que las líneas que fallan en la ronda 0
// son reintentadas exitosamente en la ronda 1 por los mismos workers principales.
func TestPipelineMultiPassRetries(t *testing.T) {
	lines := []string{
		"line1", "line2", "line3", "line4", "line5",
		"line6", "line7", "line8", "line9", "line10",
	}

	// Mapa para simular que las líneas pares fallan en el intento 1 pero triunfan en el intento 2
	var attempts sync.Map

	processFn := func(line *Line) ([]byte, error) {
		val, _ := attempts.LoadOrStore(line.ID, 0)
		att := val.(int) + 1
		attempts.Store(line.ID, att)

		// Las líneas con ID par fallan en el intento 1
		if line.ID%2 == 0 && att == 1 {
			return nil, os.ErrDeadlineExceeded // error de red simulado
		}
		return []byte("success"), nil
	}

	reader := NewSliceReader(lines, ReaderOptions{ChunkSize: 2})
	tracker := NewTracker()
	plane := NewPlane(tracker)

	wOpts := WorkerOptions{
		NumWorkers:   4,
		ControlPlane: plane,
		Checkpoint:   tracker,
	}
	workerPool := NewWorkerPool(wOpts, processFn)
	sink := NewWriterSink(func(res *ChunkResult) error { return nil })

	var roundTransitions atomic.Int32
	var lastTransitionCount atomic.Int32

	pipe := Build(reader, workerPool, sink).
		WithCheckpoint(tracker).
		WithControlPlane(plane).
		WithMaxRetries(2).
		OnRoundTransition(func(round int, failedCount int) {
			roundTransitions.Add(1)
			lastTransitionCount.Store(int32(failedCount))
		}).
		Pipeline()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stats, err := pipe.Run(ctx)
	if err != nil {
		t.Fatalf("Pipeline.Run falló: %v", err)
	}

	// 10 líneas en total
	if stats.TotalLines != 10 {
		t.Errorf("TotalLines = %d, want 10", stats.TotalLines)
	}

	// Las 10 deben terminar con éxito (5 en R0 + 5 recuperadas en R1)
	if stats.SuccessLines != 10 {
		t.Errorf("SuccessLines = %d, want 10", stats.SuccessLines)
	}

	// 0 fallos finales
	if stats.FailedLines != 0 {
		t.Errorf("FailedLines = %d, want 0", stats.FailedLines)
	}

	// Hubo exactamente 1 transición de ronda (de R0 a R1) con 5 fallos
	if roundTransitions.Load() != 1 {
		t.Errorf("roundTransitions = %d, want 1", roundTransitions.Load())
	}
	if lastTransitionCount.Load() != 5 {
		t.Errorf("lastTransitionCount = %d, want 5", lastTransitionCount.Load())
	}
}

// TestPipelineMultiPassMaxRetriesExceeded verifica que se respete el techo de MaxRetries
func TestPipelineMultiPassMaxRetriesExceeded(t *testing.T) {
	lines := []string{"fail1", "fail2", "fail3"}

	// Todas las líneas fallan siempre
	processFn := func(line *Line) ([]byte, error) {
		return nil, os.ErrPermission
	}

	reader := NewSliceReader(lines, ReaderOptions{ChunkSize: 1})
	workerPool := NewWorkerPool(WorkerOptions{NumWorkers: 2}, processFn)
	sink := NewWriterSink(func(res *ChunkResult) error { return nil })

	var transitions atomic.Int32

	pipe := Build(reader, workerPool, sink).
		WithMaxRetries(2). // R0 (original) + R1 (reintento 1) + R2 (reintento 2)
		OnRoundTransition(func(round int, failedCount int) {
			transitions.Add(1)
		}).
		Pipeline()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stats, err := pipe.Run(ctx)
	if err != nil {
		t.Fatalf("Pipeline.Run falló: %v", err)
	}

	if stats.TotalLines != 3 {
		t.Errorf("TotalLines = %d, want 3", stats.TotalLines)
	}
	if stats.SuccessLines != 0 {
		t.Errorf("SuccessLines = %d, want 0", stats.SuccessLines)
	}
	if stats.FailedLines != 3 {
		t.Errorf("FailedLines = %d, want 3", stats.FailedLines)
	}

	// Deben haber ocurrido 2 transiciones (a R1 y a R2)
	if transitions.Load() != 2 {
		t.Errorf("transitions = %d, want 2", transitions.Load())
	}
}

// TestProcessFacadeWithSlice valida la API de alto nivel Process con un slice []string sin configuración manual.
func TestProcessFacadeWithSlice(t *testing.T) {
	lines := []string{"combo1@test.com:pass1", "combo2@test.com:pass2", "combo3@test.com:pass3"}
	var processed atomic.Uint64

	processFn := func(line *Line) ([]byte, error) {
		processed.Add(1)
		return []byte("success"), nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stats, err := Process(ctx, lines, processFn,
		WithWorkers(3),
		WithAutoTune(false),
		WithKeyboard(false),
		WithMaxRetries(0),
	)
	if err != nil {
		t.Fatalf("Process falló: %v", err)
	}

	if stats.TotalLines != 3 {
		t.Errorf("TotalLines = %d, want 3", stats.TotalLines)
	}
	if stats.SuccessLines != 3 {
		t.Errorf("SuccessLines = %d, want 3", stats.SuccessLines)
	}
	if processed.Load() != 3 {
		t.Errorf("processed count = %d, want 3", processed.Load())
	}
}

// TestProcessFacadeWithFile valida la API de alto nivel Process con un archivo temporal en streaming.
func TestProcessFacadeWithFile(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "facade_test_*.txt")
	if err != nil {
		t.Fatalf("error creando archivo temporal: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	content := "user1:pass1\nuser2:pass2\nuser3:pass3\nuser4:pass4\n"
	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("error escribiendo archivo: %v", err)
	}
	tmpFile.Close()

	var count atomic.Uint64
	fn := func(line *Line) ([]byte, error) {
		count.Add(1)
		return []byte("ok"), nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stats, err := Process(ctx, tmpFile.Name(), fn,
		WithWorkers(4),
		WithAutoTune(false),
		WithKeyboard(false),
		WithMaxRetries(0),
	)
	if err != nil {
		t.Fatalf("Process con archivo falló: %v", err)
	}

	if stats.TotalLines != 4 {
		t.Errorf("TotalLines = %d, want 4", stats.TotalLines)
	}
	if stats.SuccessLines != 4 {
		t.Errorf("SuccessLines = %d, want 4", stats.SuccessLines)
	}
	if count.Load() != 4 {
		t.Errorf("count = %d, want 4", count.Load())
	}
}

// TestFineGrainedParallelismNoConvoy verifica que una línea lenta no bloquee a las demás (Fase 2).
func TestFineGrainedParallelismNoConvoy(t *testing.T) {
	// 5 líneas en un solo chunk. La línea 0 es lenta (100ms), las otras 4 son instantáneas (1ms).
	lines := []string{"slow", "fast1", "fast2", "fast3", "fast4"}
	var fastCompleted atomic.Uint64

	fn := func(line *Line) ([]byte, error) {
		if string(line.Content) == "slow" {
			time.Sleep(100 * time.Millisecond)
		} else {
			fastCompleted.Add(1)
		}
		return []byte("done"), nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	start := time.Now()
	stats, err := Process(ctx, lines, fn,
		WithWorkers(5),
		WithChunkSize(5), // Las 5 líneas en un solo chunk
		WithAutoTune(false),
		WithKeyboard(false),
		WithMaxRetries(0),
	)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("Process falló: %v", err)
	}

	if stats.TotalLines != 5 {
		t.Errorf("TotalLines = %d, want 5", stats.TotalLines)
	}

	// Si las líneas se procesan en paralelo real, las 4 rápidas no tuvieron que esperar 100ms cada una secuencialmente.
	// El tiempo total debe ser cercano a los 100ms de la línea lenta, nunca 400-500ms.
	if elapsed > 300*time.Millisecond {
		t.Errorf("Tiempo transcurrido %v demasiado alto, hubo bloqueo secuencial", elapsed)
	}
}

// TestProgress_GeoRouteCountryGroupingAndOrder valida la agrupación secuencial por país (ES > PE > US > XX)
// y que cada Line entregada a processFn porte su CountryCode exacto.
func TestProgress_GeoRouteCountryGroupingAndOrder(t *testing.T) {
	// Mezclar intencionalmente el orden de entrada
	input := []string{
		"generic1@domain.com:pass", // XX
		"peru1@domain.pe:pass",     // PE
		"spain1@domain.es:pass",    // ES
		"usa1@domain.us:pass",      // US
		"spain2@domain.es:pass",    // ES
		"peru2@domain.pe:pass",     // PE
		"spain3@domain.es:pass",    // ES
		"spain4@domain.es:pass",    // ES
		"usa2@domain.us:pass",      // US
		"spain5@domain.es:pass",    // ES
		"peru3@domain.pe:pass",     // PE
	}
	// Conteo esperado:
	// ES: 5 líneas (mayor volumen -> procesado 1°)
	// PE: 3 líneas (segundo volumen -> procesado 2°)
	// US: 2 líneas (tercer volumen -> procesado 3°)
	// XX: 1 línea  (genérico -> procesado al final)

	var mu sync.Mutex
	var receivedCountries []string

	fn := func(line *Line) ([]byte, error) {
		mu.Lock()
		receivedCountries = append(receivedCountries, line.Country)
		mu.Unlock()
		return []byte("ok"), nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stats, err := Process(ctx, input, fn,
		WithWorkers(1), // 1 worker secuencial para validar el orden exacto de entrega de la cola
		WithChunkSize(2),
		WithAutoTune(false),
		WithKeyboard(false),
		WithMaxRetries(0),
		WithGeoSorting(true),
	)
	if err != nil {
		t.Fatalf("Process error: %v", err)
	}

	if stats.TotalLines != uint64(len(input)) {
		t.Fatalf("TotalLines = %d, want %d", stats.TotalLines, len(input))
	}

	// Validar que las primeras 5 sean ES
	for i := 0; i < 5; i++ {
		if receivedCountries[i] != "ES" {
			t.Errorf("pos %d: country = %s, want ES", i, receivedCountries[i])
		}
	}
	// Las siguientes 3 deben ser PE
	for i := 5; i < 8; i++ {
		if receivedCountries[i] != "PE" {
			t.Errorf("pos %d: country = %s, want PE", i, receivedCountries[i])
		}
	}
	// Las siguientes 2 deben ser US
	for i := 8; i < 10; i++ {
		if receivedCountries[i] != "US" {
			t.Errorf("pos %d: country = %s, want US", i, receivedCountries[i])
		}
	}
	// La última debe ser XX (genérico)
	if receivedCountries[10] != "XX" {
		t.Errorf("pos 10: country = %s, want XX", receivedCountries[10])
	}
}

// TestLineContextAutoDetection valida que line.Context() transporte el país en el context.Context
func TestLineContextAutoDetection(t *testing.T) {
	lineES := Line{Content: []byte("user@domain.es:pass"), Country: "ES"}
	ctxES := lineES.Context()
	if GeoRoute.CountryFromContext(ctxES) != "ES" {
		t.Errorf("expected ES, got %s", GeoRoute.CountryFromContext(ctxES))
	}

	lineUndetected := Line{Content: []byte("user@domain.pe:pass")} // Sin country explícito -> auto-detecta del content
	ctxPE := lineUndetected.Context()
	if GeoRoute.CountryFromContext(ctxPE) != "PE" {
		t.Errorf("expected PE, got %s", GeoRoute.CountryFromContext(ctxPE))
	}
}

