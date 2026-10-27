package progress

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/LeanCoder0718/Universal/GeoRoute"
)

// ProcessOption define una opción funcional para personalizar la ejecución de Process.
type ProcessOption func(*processConfig)

type processConfig struct {
	numWorkers        int
	workload          WorkloadType
	chunkSize         int
	maxRetries        int
	rateLimitRPS      int
	rateLimitBurst    int
	enableAutoTune    bool
	enableKeyboard    bool
	enableGeoSorting  bool
	geoSchedulingMode GeoRoute.SchedulingMode
	metricsCounter    MetricsCounterInterface
	metricsCollector  *Collector
	tracker           *Tracker
	controlPlane      *Plane
	chunkPool         *ChunkPool
	resultHandler     ResultHandler
	lineHandler       func(lr LineResult) error
	retryClassifier   IsRetryableError
	onRoundTransition func(round int, failedCount int)
	onProgressUpdate  func(stats Stats, lps uint64, workers int)
	rampUpDelay       time.Duration
	keyboardCb        KeyboardActionCallback
}

// WithGeoSorting configura la agrupación y ordenamiento secuencial por país (default true con ModeLargestFirst).
// Agrupa los combos procesando primero el país de mayor volumen (ej. ES -> PE -> US -> XX) y cambiando
// el proxy regional dinámicamente o usando el proxy general por defecto al llegar al grupo genérico.
func WithGeoSorting(enabled bool, mode ...GeoRoute.SchedulingMode) ProcessOption {
	return func(c *processConfig) {
		c.enableGeoSorting = enabled
		if len(mode) > 0 {
			c.geoSchedulingMode = mode[0]
		}
	}
}

// WithWorkers define manualmente el número de workers (0 = auto-calibrar según carga y CPU).
func WithWorkers(n int) ProcessOption {
	return func(c *processConfig) { c.numWorkers = n }
}

// WithWorkload define el perfil de carga de trabajo: WorkloadCPU o WorkloadIO (default WorkloadIO).
func WithWorkload(w WorkloadType) ProcessOption {
	return func(c *processConfig) { c.workload = w }
}

// WithChunkSize define el tamaño en líneas de cada chunk de lectura (default 500).
func WithChunkSize(sz int) ProcessOption {
	return func(c *processConfig) { c.chunkSize = sz }
}

// WithMaxRetries define el número de rondas de reintento en lotes (default 1 para mitigar fallos de red).
func WithMaxRetries(r int) ProcessOption {
	return func(c *processConfig) { c.maxRetries = r }
}

// WithRateLimit configura limitación de tasa por segundo (RPS) mediante Token Bucket.
func WithRateLimit(rps int, burst ...int) ProcessOption {
	return func(c *processConfig) {
		c.rateLimitRPS = rps
		if len(burst) > 0 {
			c.rateLimitBurst = burst[0]
		}
	}
}

// WithAutoTune habilita o deshabilita la auto-regulación dinámica de workers en caliente (default true).
func WithAutoTune(enabled bool) ProcessOption {
	return func(c *processConfig) { c.enableAutoTune = enabled }
}

// WithKeyboard habilita o deshabilita los atajos de teclado en Windows (P/C/G/S/+/-) (default true).
func WithKeyboard(enabled bool, cb ...KeyboardActionCallback) ProcessOption {
	return func(c *processConfig) {
		c.enableKeyboard = enabled
		if len(cb) > 0 {
			c.keyboardCb = cb[0]
		}
	}
}

// WithMetricsCounter vincula un contador de métricas externo como RABCOUNT.
func WithMetricsCounter(counter MetricsCounterInterface) ProcessOption {
	return func(c *processConfig) { c.metricsCounter = counter }
}

// WithResultHandler asigna un manejador invocado por cada bloque de resultados emitido.
func WithResultHandler(h ResultHandler) ProcessOption {
	return func(c *processConfig) { c.resultHandler = h }
}

// WithLineResultHandler asigna un callback invocado por cada línea individual procesada.
func WithLineResultHandler(h func(lr LineResult) error) ProcessOption {
	return func(c *processConfig) { c.lineHandler = h }
}

// WithOnRoundTransition asigna un callback invocado al transicionar entre rondas de reintento.
func WithOnRoundTransition(fn func(round int, failedCount int)) ProcessOption {
	return func(c *processConfig) { c.onRoundTransition = fn }
}

// WithProgressUpdate asigna un callback periódico para recibir estadísticas en tiempo real.
func WithProgressUpdate(fn func(stats Stats, lps uint64, workers int)) ProcessOption {
	return func(c *processConfig) { c.onProgressUpdate = fn }
}

// WithRetryClassifier asigna el clasificador de errores para reintentos.
func WithRetryClassifier(fn IsRetryableError) ProcessOption {
	return func(c *processConfig) { c.retryClassifier = fn }
}

// WithRampUpDelay asigna el intervalo de rampa progresiva para levantar workers (default 1ms).
func WithRampUpDelay(d time.Duration) ProcessOption {
	return func(c *processConfig) { c.rampUpDelay = d }
}

func defaultProcessConfig() *processConfig {
	return &processConfig{
		numWorkers:        0,
		workload:          WorkloadIO,
		chunkSize:         500,
		maxRetries:        1,
		enableAutoTune:    true,
		enableKeyboard:    true,
		enableGeoSorting:  true,
		geoSchedulingMode: GeoRoute.ModeLargestFirst,
		rampUpDelay:       1 * time.Millisecond,
	}
}

// autoTuneInitialWorkers calcula un número balanceado de workers iniciales según CPU y perfil.
func autoTuneInitialWorkers(workload WorkloadType) int {
	numCPU := runtime.NumCPU()
	if workload == WorkloadCPU {
		return numCPU
	}

	if numCPU <= 2 {
		return 60
	} else if numCPU <= 4 {
		return 120
	} else if numCPU <= 8 {
		return 200
	}
	return 300
}

type countryGroup struct {
	cc    GeoRoute.CountryCode
	items []string
}

func sortCombosByCountry(combos []string, mode GeoRoute.SchedulingMode) ([]string, []string) {
	if len(combos) == 0 {
		return nil, nil
	}
	detector := GeoRoute.NewCountryDetector(50000)
	groupsMap := make(map[GeoRoute.CountryCode][]string, 16)

	for _, line := range combos {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		cc := detector.Detect(trimmed)
		if cc == "" {
			cc = GeoRoute.Unknown
		}
		groupsMap[cc] = append(groupsMap[cc], trimmed)
	}

	groups := make([]countryGroup, 0, len(groupsMap))
	for cc, items := range groupsMap {
		groups = append(groups, countryGroup{cc: cc, items: items})
	}

	sort.SliceStable(groups, func(i, j int) bool {
		isUnkI := groups[i].cc.IsUnknown() || groups[i].cc == "XX"
		isUnkJ := groups[j].cc.IsUnknown() || groups[j].cc == "XX"
		if isUnkI != isUnkJ {
			return !isUnkI
		}
		if mode == GeoRoute.ModeSmallestFirst {
			if len(groups[i].items) != len(groups[j].items) {
				return len(groups[i].items) < len(groups[j].items)
			}
		} else if mode == GeoRoute.ModeAlphabetical {
			return groups[i].cc < groups[j].cc
		} else {
			if len(groups[i].items) != len(groups[j].items) {
				return len(groups[i].items) > len(groups[j].items)
			}
		}
		return groups[i].cc < groups[j].cc
	})

	totalCount := 0
	for _, grp := range groups {
		totalCount += len(grp.items)
	}

	orderedCombos := make([]string, 0, totalCount)
	orderedCountries := make([]string, 0, totalCount)

	for _, grp := range groups {
		ccStr := string(grp.cc)
		if grp.cc.IsUnknown() {
			ccStr = "XX"
		}
		for _, item := range grp.items {
			orderedCombos = append(orderedCombos, item)
			orderedCountries = append(orderedCountries, ccStr)
		}
	}

	return orderedCombos, orderedCountries
}

func sortLinesByCountry(lines []Line, mode GeoRoute.SchedulingMode) []Line {
	if len(lines) <= 1 {
		return lines
	}
	detector := GeoRoute.NewCountryDetector(50000)
	type lineGroup struct {
		cc    GeoRoute.CountryCode
		items []Line
	}
	groupsMap := make(map[GeoRoute.CountryCode][]Line, 16)

	for _, l := range lines {
		raw := string(l.Content)
		var detectedCC GeoRoute.CountryCode
		if l.Country != "" {
			detectedCC = GeoRoute.CountryCode(l.Country)
		} else {
			detectedCC = detector.Detect(raw)
			if detectedCC == "" {
				detectedCC = GeoRoute.Unknown
			}
		}
		l.Country = string(detectedCC)
		if detectedCC.IsUnknown() {
			l.Country = "XX"
		}
		groupsMap[detectedCC] = append(groupsMap[detectedCC], l)
	}

	groups := make([]lineGroup, 0, len(groupsMap))
	for cc, items := range groupsMap {
		groups = append(groups, lineGroup{cc: cc, items: items})
	}

	sort.SliceStable(groups, func(i, j int) bool {
		isUnkI := groups[i].cc.IsUnknown() || groups[i].cc == "XX"
		isUnkJ := groups[j].cc.IsUnknown() || groups[j].cc == "XX"
		if isUnkI != isUnkJ {
			return !isUnkI
		}
		if mode == GeoRoute.ModeSmallestFirst {
			if len(groups[i].items) != len(groups[j].items) {
				return len(groups[i].items) < len(groups[j].items)
			}
		} else if mode == GeoRoute.ModeAlphabetical {
			return groups[i].cc < groups[j].cc
		} else {
			if len(groups[i].items) != len(groups[j].items) {
				return len(groups[i].items) > len(groups[j].items)
			}
		}
		return groups[i].cc < groups[j].cc
	})

	out := make([]Line, 0, len(lines))
	for _, grp := range groups {
		out = append(out, grp.items...)
	}
	return out
}

func readAllLinesFromFile(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var lines []string
	scanner := bufio.NewScanner(f)
	buf := make([]byte, 1024*1024)
	scanner.Buffer(buf, 10*1024*1024)
	for scanner.Scan() {
		text := strings.TrimSpace(scanner.Text())
		if text != "" {
			lines = append(lines, text)
		}
	}
	return lines, scanner.Err()
}

// resolveReader transforma cualquier origen soportado en un ChunkReader optimizado.
func resolveReader(source interface{}, chunkSize int, pool *ChunkPool, enableGeo bool, mode GeoRoute.SchedulingMode) (ChunkReader, int64, error) {
	if chunkSize <= 0 {
		chunkSize = 500
	}

	rOpts := ReaderOptions{
		BufferSize: 256 * 1024,
		ChunkSize:  chunkSize,
		ChunkPool:  pool,
	}

	switch s := source.(type) {
	case string:
		fi, err := os.Stat(s)
		if err != nil {
			return nil, 0, fmt.Errorf("progress: error accediendo al archivo fuente: %w", err)
		}
		if fi.IsDir() {
			return nil, 0, fmt.Errorf("progress: la ruta es un directorio: %s", s)
		}

		if enableGeo && fi.Size() < 250*1024*1024 {
			lines, err := readAllLinesFromFile(s)
			if err == nil {
				orderedCombos, orderedCountries := sortCombosByCountry(lines, mode)
				return NewSliceReaderWithCountries(orderedCombos, orderedCountries, rOpts), int64(len(orderedCombos)), nil
			}
		}

		r, err := NewFileReader(s, rOpts)
		if err != nil {
			return nil, 0, err
		}
		return r, fi.Size(), nil

	case []string:
		if enableGeo {
			orderedCombos, orderedCountries := sortCombosByCountry(s, mode)
			return NewSliceReaderWithCountries(orderedCombos, orderedCountries, rOpts), int64(len(orderedCombos)), nil
		}
		return NewSliceReader(s, rOpts), int64(len(s)), nil

	case []Line:
		if enableGeo {
			orderedLines := sortLinesByCountry(s, mode)
			return NewLinesReader(orderedLines, rOpts), int64(len(orderedLines)), nil
		}
		return NewLinesReader(s, rOpts), int64(len(s)), nil

	case ChunkReader:
		return s, 0, nil

	default:
		return nil, 0, fmt.Errorf("progress: tipo de origen no soportado: %T (esperado string, []string, []Line o ChunkReader)", source)
	}
}

// startAutoTuner ajusta dinámicamente la concurrencia en caliente según latencias y rendimiento de red.
func startAutoTuner(ctx context.Context, pipe *Pipeline, col *Collector, maxWorkers int) {
	if pipe == nil || col == nil {
		return
	}
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()

		var lastProcessed uint64
		var consecutiveGoodSamples int

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if pipe.IsPaused() {
					continue
				}

				current := pipe.CurrentWorkers()
				processed := col.TotalProcessedLines()
				delta := processed - lastProcessed
				lastProcessed = processed

				if delta > 0 {
					lat := col.LatencySummary()

					// Si la latencia P95 se dispara a más de 3 segundos, reducir workers para descongestionar
					if lat.P95Ms > 3000 && current > 20 {
						pipe.RemoveWorkers(5)
						consecutiveGoodSamples = 0
					} else if lat.P90Ms < 600 && current < maxWorkers {
						// Si la latencia es baja y estable, escalar suavemente
						consecutiveGoodSamples++
						if consecutiveGoodSamples >= 3 {
							pipe.AddWorkers(5)
							consecutiveGoodSamples = 0
						}
					}
				}
			}
		}
	}()
}

// Process ejecuta el procesamiento masivo de combos/líneas con paralelismo real en Go,
// auto-escalado inteligente, sincronización libre de carreras y cero configuración obligatoria.
// El consumidor solo necesita proporcionar el origen de datos y la función de procesamiento processFn.
func Process(ctx context.Context, source interface{}, fn ProcessFunc, opts ...ProcessOption) (Stats, error) {
	cfg := defaultProcessConfig()
	for _, opt := range opts {
		opt(cfg)
	}

	if cfg.numWorkers <= 0 {
		cfg.numWorkers = autoTuneInitialWorkers(cfg.workload)
	}

	chunkPool := NewChunkPool(cfg.chunkSize)
	reader, _, err := resolveReader(source, cfg.chunkSize, chunkPool, cfg.enableGeoSorting, cfg.geoSchedulingMode)
	if err != nil {
		return Stats{}, err
	}

	tracker := NewTracker()
	plane := NewPlane(tracker)
	collector := NewCollector(cfg.metricsCounter, 500*time.Millisecond, nil)

	var limiter RateLimiter
	if cfg.rateLimitRPS > 0 {
		burst := cfg.rateLimitBurst
		if burst <= 0 {
			burst = cfg.rateLimitRPS
		}
		limiter = NewTokenBucketLimiter(cfg.rateLimitRPS, burst)
	}

	workerOpts := WorkerOptions{
		NumWorkers:   cfg.numWorkers,
		Workload:     cfg.workload,
		ForceWorkers: true,
		ControlPlane: plane,
		Checkpoint:   tracker,
		Collector:    collector,
		ChunkPool:    chunkPool,
		RateLimiter:  limiter,
		RampUpDelay:  cfg.rampUpDelay,
	}

	workerPool := NewWorkerPool(workerOpts, fn)

	sinkHandler := cfg.resultHandler
	if cfg.lineHandler != nil {
		orig := sinkHandler
		sinkHandler = func(res *ChunkResult) error {
			if orig != nil {
				if err := orig(res); err != nil {
					return err
				}
			}
			for _, lr := range res.Results {
				if err := cfg.lineHandler(lr); err != nil {
					return err
				}
			}
			return nil
		}
	}

	sink := NewWriterSink(sinkHandler)

	builder := Build(reader, workerPool, sink).
		WithCheckpoint(tracker).
		WithControlPlane(plane).
		WithCollector(collector).
		WithMaxRetries(cfg.maxRetries)

	if cfg.retryClassifier != nil {
		builder.WithRetryClassifier(cfg.retryClassifier)
	}
	if cfg.onRoundTransition != nil {
		builder.OnRoundTransition(cfg.onRoundTransition)
	}

	pipe := builder.Pipeline()

	// Iniciar listener de teclado en segundo plano si está habilitado
	if cfg.enableKeyboard {
		pipe.StartKeyboardListener(ctx, cfg.keyboardCb)
	}

	// Iniciar auto-tuner dinámico en segundo plano si está habilitado
	if cfg.enableAutoTune {
		maxCap := cfg.numWorkers * 3
		if maxCap > 1000 {
			maxCap = 1000
		}
		startAutoTuner(ctx, pipe, collector, maxCap)
	}

	// Notificador periódico de progreso si fue registrado
	if cfg.onProgressUpdate != nil {
		go func() {
			ticker := time.NewTicker(500 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					cfg.onProgressUpdate(
						Stats{
							TotalLines:   collector.TotalProcessedLines(),
							SuccessLines: collector.TotalProcessedLines(),
						},
						collector.LinesPerSecond(),
						pipe.CurrentWorkers(),
					)
				}
			}
		}()
	}

	return pipe.Run(ctx)
}
