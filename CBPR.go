package Universal

import (
	"github.com/LeanCoder0718/Universal/GeoRoute"
	"github.com/LeanCoder0718/Universal/Utils"
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/sqweek/dialog"
)

const numShards = 1024

type shard struct {
	sync.Mutex
	m             map[string]GeoRoute.CountryCode
	countryCounts map[GeoRoute.CountryCode]int
}

var (
	Red        = Utils.Red
	ColorLogo  = Utils.ColorLogo
	OrangeCo   = Utils.OrangeCo
	WhiteColor = Utils.WhiteColor
	LimeGreen  = Utils.LimeGreen
	Cyan       = Utils.Cyan
)


func findRemainingComboFiles() []string {
	var files []string
	pattern := filepath.Join("Combos", "Remaining*", "*.txt")
	matches, _ := filepath.Glob(pattern)
	for _, m := range matches {
		if fi, err := os.Stat(m); err == nil && !fi.IsDir() && fi.Size() > 0 {
			files = append(files, m)
		}
	}
	combosPattern := filepath.Join("Combos", "*.txt")
	combosMatches, _ := filepath.Glob(combosPattern)
	for _, m := range combosMatches {
		if fi, err := os.Stat(m); err == nil && !fi.IsDir() && fi.Size() > 0 {
			files = append(files, m)
		}
	}

	sort.Slice(files, func(i, j int) bool {
		fi, errI := os.Stat(files[i])
		fj, errJ := os.Stat(files[j])
		if errI == nil && errJ == nil {
			return fi.ModTime().After(fj.ModTime())
		}
		return files[i] > files[j]
	})
	return files
}

var updateLeanCoderOnce sync.Once

// UpdateLeanCoderProjects comprueba y actualiza automáticamente los repositorios Git
// de LeanCoder (github.com/LeanCoder0718) antes de iniciar el programa.
// Se ejecuta una única vez por sesión y no detiene el programa si no hay conexión o git.
func UpdateLeanCoderProjects() {
	updateLeanCoderOnce.Do(func() {
		gitPath, err := exec.LookPath("git")
		if err != nil || gitPath == "" {
			return
		}

		var candidateDirs []string
		seen := make(map[string]bool)

		addCandidate := func(p string) {
			if p == "" {
				return
			}
			abs, err := filepath.Abs(p)
			if err != nil {
				return
			}
			abs = filepath.Clean(abs)
			if seen[abs] {
				return
			}
			seen[abs] = true

			gitTarget := filepath.Join(abs, ".git")
			if _, err := os.Stat(gitTarget); err == nil {
				candidateDirs = append(candidateDirs, abs)
			}
		}

		// Candidatos en directorio de trabajo y Universal
		addCandidate(".")
		addCandidate("Universal")
		addCandidate(filepath.Join("..", "Universal"))

		if exePath, err := os.Executable(); err == nil {
			exeDir := filepath.Dir(exePath)
			addCandidate(exeDir)
			addCandidate(filepath.Join(exeDir, "Universal"))
		}

		// Subcarpetas del directorio actual
		if entries, err := os.ReadDir("."); err == nil {
			for _, e := range entries {
				if e.IsDir() && !strings.HasPrefix(e.Name(), ".") {
					addCandidate(e.Name())
					addCandidate(filepath.Join(e.Name(), "Universal"))
				}
			}
		}

		// Carpetas hermanas en el directorio padre
		if entries, err := os.ReadDir(".."); err == nil {
			for _, e := range entries {
				if e.IsDir() && !strings.HasPrefix(e.Name(), ".") {
					addCandidate(filepath.Join("..", e.Name()))
					addCandidate(filepath.Join("..", e.Name(), "Universal"))
				}
			}
		}

		var reposToUpdate []string
		for _, dir := range candidateDirs {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			cmd := exec.CommandContext(ctx, "git", "-C", dir, "remote", "get-url", "origin")
			out, err := cmd.Output()
			cancel()
			if err == nil {
				remoteURL := strings.ToLower(string(out))
				if strings.Contains(remoteURL, "leancoder") {
					reposToUpdate = append(reposToUpdate, dir)
				}
			}
		}

		if len(reposToUpdate) == 0 {
			return
		}

		fmt.Println(Red(" → "), WhiteColor("VERIFICANDO ACTUALIZACIONES DE LEANCODER..."))
		for _, dir := range reposToUpdate {
			repoName := filepath.Base(dir)
			if strings.EqualFold(repoName, "Universal") {
				parent := filepath.Base(filepath.Dir(dir))
				if parent != "" && parent != "." && parent != "/" && parent != "\\" {
					repoName = parent + "/" + repoName
				}
			}

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			cmd := exec.CommandContext(ctx, "git", "-C", dir, "pull", "--ff-only")
			out, err := cmd.CombinedOutput()
			cancel()

			if err == nil {
				outStr := strings.ToLower(string(out))
				if strings.Contains(outStr, "already up to date") || strings.Contains(outStr, "actualizado") {
					fmt.Printf("   %s %s: %s\n", LimeGreen("✔"), repoName, WhiteColor("Al día"))
				} else {
					fmt.Printf("   %s %s: %s\n", LimeGreen("✔"), repoName, LimeGreen("¡Actualizado con éxito!"))
				}
			} else {
				fmt.Printf("   %s %s: %s\n", OrangeCo("⚠"), repoName, WhiteColor("No se pudo conectar (continuando...)"))
			}
		}
		fmt.Println()
	})
}

func OpenCombo() (string, int64, error) {

	UpdateLeanCoderProjects()

	remainingFiles := findRemainingComboFiles()
	var filePath string

	if len(remainingFiles) > 0 {
		fmt.Println(Red(" → "), WhiteColor("COMBOS REMAINING"))
		fmt.Println()
		fmt.Printf(" [%s] %s\n", Red(0), WhiteColor("Cargar un nuevo archivo de combo (Selección manual)"))
		for i, f := range remainingFiles {
			fileName := filepath.Base(f)
			fmt.Printf(" [%s] %s\n", Red(i+1), WhiteColor(fileName))
		}
		fmt.Println()

		sel := Utils.ReadInt(Red(" → ")+WhiteColor("Selecciona una opción [0 para nuevo]: "), 0)
		if sel > 0 && sel <= len(remainingFiles) {
			filePath = remainingFiles[sel-1]
		}
	}

	if filePath == "" {
		fmt.Print(Red(" → "), WhiteColor("Combo Archivo seleccionado: "))
		var err error
		filePath, err = dialog.File().Filter("Text Files", "txt").Title("Selecciona tu archivo de combos").Load()
		if err != nil {
			return "", 0, err
		}
	}

	return LoadComboFromFile(filePath)
}

// FastCountLines cuenta las líneas de un archivo de cualquier tamaño en streaming por bloques de 1MB a velocidad de disco (Zero-RAM).
func FastCountLines(filePath string) (int64, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	buf := make([]byte, 1024*1024) // 1 MB buffer
	var count int64
	var totalBytes int64
	var lastByte byte = '\n'

	for {
		n, err := file.Read(buf)
		if n > 0 {
			totalBytes += int64(n)
			count += int64(bytes.Count(buf[:n], []byte{'\n'}))
			lastByte = buf[n-1]
		}
		if err != nil {
			if err == io.EOF {
				break
			}
			return count, err
		}
	}
	if totalBytes > 0 && lastByte != '\n' {
		count++
	}
	return count, nil
}

// sampleCountryDetection realiza un muestreo ultra rápido sin cargar el archivo a RAM para detectar el país predominante.
func sampleCountryDetection(filePath string, maxSample int) (map[GeoRoute.CountryCode]int, GeoRoute.CountryCode) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, GeoRoute.Unknown
	}
	defer file.Close()

	detector := GeoRoute.NewCountryDetector(10000)
	countryCounts := make(map[GeoRoute.CountryCode]int)
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)

	sampled := 0
	for scanner.Scan() && sampled < maxSample {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		sampled++

		// Formato ULP o normal
		if lastColon := strings.LastIndexByte(line, ':'); lastColon > 0 {
			if prevColon := strings.LastIndexByte(line[:lastColon], ':'); prevColon > 0 {
				rec, dErr := detector.DetectNormalized(line, GeoRoute.FormatULP, GeoRoute.PolicyPreferService)
				if dErr == nil && !rec.Country.IsUnknown() {
					countryCounts[rec.Country]++
					continue
				}
			}
		}

		cc := detector.Detect(line)
		countryCounts[cc]++
	}

	var topCountry GeoRoute.CountryCode
	var maxCount int
	for cc, count := range countryCounts {
		if !cc.IsUnknown() && count > maxCount {
			maxCount = count
			topCountry = cc
		}
	}

	return countryCounts, topCountry
}

// LoadComboFromFile indexa y prepara el archivo para procesamiento en streaming de alta escala (50M+ líneas, Zero-RAM).
func LoadComboFromFile(filePath string) (string, int64, error) {
	fmt.Println(WhiteColor(filePath))
	Utils.ComboFilePath = filePath

	base := filepath.Base(filePath)
	ext := filepath.Ext(base)
	nameWithoutExt := strings.TrimSuffix(base, ext)

	if idx := strings.LastIndex(nameWithoutExt, "_"); idx > 0 && len(nameWithoutExt)-idx == 10 {
		nameWithoutExt = nameWithoutExt[:idx]
	}
	Utils.ComboFileName = nameWithoutExt

	start := time.Now()
	fmt.Print(Red(" → "), WhiteColor("Indexando combo masivo (Streaming Zero-RAM)... "))

	totalLines, err := FastCountLines(filePath)
	if err != nil {
		return "", 0, fmt.Errorf("error analizando archivo: %w", err)
	}

	duration := time.Since(start)
	fmt.Println(LimeGreen("Completado ✔"))
	fmt.Println()
	fmt.Println(Red(" →"), WhiteColor("Total de líneas detectadas:"), LimeGreen(humanize.Comma(totalLines)))
	fmt.Println(Red(" →"), WhiteColor("Modo de memoria:"), Cyan("STREAMING DIRECTO (Zero-RAM Overhead)"))
	fmt.Println(Red(" →"), WhiteColor("Tiempo de indexación:"), OrangeCo(duration))

	// Muestreo rápido de país para telemetría
	countryCounts, topCountry := sampleCountryDetection(filePath, 20000)
	if len(countryCounts) > 0 {
		RenderCountryBreakdown(countryCounts, 20000)
		if !topCountry.IsUnknown() {
			Utils.DetectedCountry = string(topCountry)
			fmt.Printf("\n %s %s %s\n", LimeGreen("✓"), WhiteColor("País predominante detectado:"), LimeGreen(string(topCountry)))
		}
	}

	Utils.ComboLoaded = totalLines
	time.Sleep(1 * time.Second)
	return filePath, totalLines, nil
}

// RenderCountryBreakdown muestra una tabla con el conteo de cada país/TLD detectado en el combo (solo códigos y contadores).
func RenderCountryBreakdown(countryCounts map[GeoRoute.CountryCode]int, totalLines int) {
	if len(countryCounts) == 0 {
		return
	}

	type countryItem struct {
		code  GeoRoute.CountryCode
		count int
	}

	var items []countryItem
	var totalIdentified int
	var genericCount int

	for cc, count := range countryCounts {
		if cc.IsUnknown() {
			genericCount += count
		} else {
			totalIdentified += count
		}
		items = append(items, countryItem{code: cc, count: count})
	}

	sort.Slice(items, func(i, j int) bool {
		if items[i].code.IsUnknown() != items[j].code.IsUnknown() {
			return !items[i].code.IsUnknown()
		}
		return items[i].count > items[j].count
	})

	fmt.Println()
	fmt.Println(ColorLogo("┌──────────────────────────────────────┐"))
	fmt.Println(WhiteColor("            COUNTRY DETECT            "))
	fmt.Println(ColorLogo("└──────────────────────────────────────┘"))

	tldNames := map[GeoRoute.CountryCode]string{
		"PE": "PE (.pe / .com.pe)",
		"DE": "DE (.de)",
		"US": "US (.us / .net)",
		"ES": "ES (.es / .com.es)",
		"MX": "MX (.mx / .com.mx)",
		"CL": "CL (.cl / .com.cl)",
		"AR": "AR (.ar / .com.ar)",
		"BR": "BR (.br / .com.br)",
		"CO": "CO (.co / .com.co)",
		"GB": "GB (.uk / .co.uk)",
		"FR": "FR (.fr)",
		"IT": "IT (.it)",
		"PT": "PT (.pt)",
		"NL": "NL (.nl)",
		"RU": "RU (.ru)",
		"UA": "UA (.ua)",
		"CA": "CA (.ca)",
		"AU": "AU (.au)",
		"TR": "TR (.tr)",
		"EC": "EC (.ec)",
		"UY": "UY (.uy)",
		"VE": "VE (.ve)",
		"BO": "BO (.bo)",
		"PY": "PY (.py)",
		"CR": "CR (.cr)",
		"PA": "PA (.pa)",
		"GT": "GT (.gt)",
		"DO": "DO (.do)",
		"XX": "XX (Genérico .com)",
	}

	maxDisplay := 15
	displayed := 0
	for _, it := range items {
		if displayed >= maxDisplay && it.code != "XX" {
			continue
		}
		label, ok := tldNames[it.code]
		if !ok {
			if it.code.IsUnknown() {
				label = "XX (Genérico .com/.net)"
			} else {
				label = fmt.Sprintf("%s (.%s)", it.code, strings.ToLower(string(it.code)))
			}
		}

		fmt.Printf(" %-34s  %-24s \n",
			LimeGreen(label),
			WhiteColor(humanize.Comma(int64(it.count))),
		)
		displayed++
	}

	fmt.Println(ColorLogo("┌──────────────────────────────────────┐"))
	fmt.Printf("%s: %-10s  - %s: %-15s \n",
		WhiteColor(" TOTAL"),
		LimeGreen(humanize.Comma(int64(totalIdentified))),
		WhiteColor("GENERIC (XX)"),
		OrangeCo(humanize.Comma(int64(genericCount))),
	)
	fmt.Println(ColorLogo("└──────────────────────────────────────┘"))
}

func ExtractUniqueDomains(lines []string) []string {
	start := time.Now()

	domainSet := make(map[string]struct{})
	seenLines := make(map[string]struct{})

	result := make([]string, 0)

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		// Evitar líneas duplicadas exactas
		if _, exists := seenLines[line]; exists {
			continue
		}
		seenLines[line] = struct{}{}

		email := strings.SplitN(line, ":", 2)[0]
		emailParts := strings.SplitN(email, "@", 2)
		if len(emailParts) != 2 {
			continue
		}

		domain := strings.ToLower(emailParts[1])

		if _, exists := domainSet[domain]; !exists {
			domainSet[domain] = struct{}{}
			result = append(result, domain)
		}
	}

	duration := time.Since(start)

	fmt.Println(Red(" ────────────────────────────"))
	fmt.Println(Red(" →"), WhiteColor("Dominios únicos:"), LimeGreen(humanize.Comma(int64(len(result)))))
	fmt.Println(Red(" →"), WhiteColor("Tiempo transcurrido:"), OrangeCo(duration))

	time.Sleep(2 * time.Second)

	return result
}

func OpenProxy(proxyType string) ([]string, error) {

	if proxyType != "off" {
		fmt.Print(Red(" → "), WhiteColor("Proxy Archivo seleccionado: "))
		// Bucle que se repetirá hasta que se seleccione un archivo
		var proxyList []string
		var rutaArchivo string
		var err error
		for {
			rutaArchivo, err = dialog.File().Filter("Archivos de texto", "txt").Title("PROXIES").Load()
			if err == nil {
				break // Salir del bucle si se selecciona un archivo correctamente
			}
		}
		fmt.Print(WhiteColor(rutaArchivo))

		rawBytes, err := os.ReadFile(rutaArchivo)
		if err != nil {
			fmt.Printf("no se pudo abrir el archivo: %v\n", err)
			return nil, err
		}
		rawBytes = Utils.NormalizeTextBytes(rawBytes)
		// Leer todas las líneas del archivo y almacenarlas en un slice
		scanner := bufio.NewScanner(bytes.NewReader(rawBytes))
		for scanner.Scan() {
			pLine := strings.TrimSpace(scanner.Text())
			if pLine != "" {
				proxyList = append(proxyList, pLine)
			}
		}
		// Imprimir texto con colores

		fmt.Println()
		fmt.Println(Red(" →"), WhiteColor("Total de proxys procesadas:"), LimeGreen(humanize.Comma(int64(len(proxyList)))))

		// Adaptación Regional con GeoRoute
		if len(proxyList) > 0 {
			targetCountry := Utils.DetectedCountry
			fmt.Println()
			fmt.Println(ColorLogo("┌─────────────────────────────────────────────────────────────┐"))
			fmt.Println(ColorLogo("│          CONFIGURACIÓN REGIONAL DE PROXIES (GeoRoute)       │"))
			fmt.Println(ColorLogo("└─────────────────────────────────────────────────────────────┘"))
			if targetCountry != "" {
				fmt.Printf(" %s %s %s\n\n", Red("→"), WhiteColor("País detectado en el combo:"), LimeGreen(targetCountry))
			}
			fmt.Println(WhiteColor(" ¿Cómo deseas configurar la adaptación regional de tus proxies?"))
			fmt.Println()
			fmt.Printf("  [%s] %s\n", Red("1"), WhiteColor("Auto-detectar formato de la lista cargada"))
			fmt.Printf("  [%s] %s\n", Red("2"), WhiteColor("Pegar un proxy de ejemplo (ej: resident...-country-al:...)"))
			fmt.Printf("  [%s] %s\n", Red("3"), WhiteColor("Usar plantilla estándar (__cr.{country})"))
			fmt.Printf("  [%s] %s\n", Red("4"), WhiteColor("No modificar proxies (usar tal como están)"))
			fmt.Println()

			choice := Utils.ReadInt(Red(" → ")+WhiteColor("Selecciona una opción [1]: "), 1)

			var providerCfg GeoRoute.ProxyProviderConfig
			var applyGeo bool = true

			switch choice {
			case 1:
				if cfg, ok := GeoRoute.DetectProviderFromProxies(proxyList); ok {
					providerCfg = cfg
					fmt.Printf("\n %s Formato regional detectado: %s\n", LimeGreen("[GeoRoute]"), OrangeCo(cfg.RegionTemplate))
				} else {
					fmt.Printf("\n %s No se detectó patrón automático en la lista.\n", Red("[GeoRoute]"))
					example := Utils.ReadString(fmt.Sprintf(" %s Pega tu proxy de ejemplo: ", Red("→")))
					if p, err := GeoRoute.ParseProxy(example); err == nil {
						if cfg, ok := GeoRoute.DetectProviderFromProxy(p); ok {
							providerCfg = cfg
						} else {
							providerCfg = GeoRoute.BrightDataProviderConfig()
						}
					} else {
						providerCfg = GeoRoute.BrightDataProviderConfig()
					}
				}
			case 2:
				example := Utils.ReadString(fmt.Sprintf("\n %s Pega tu proxy de ejemplo con tu formato (ej: resident...-country-al:...): ", Red("→")))
				if p, err := GeoRoute.ParseProxy(example); err == nil {
					if cfg, ok := GeoRoute.DetectProviderFromProxy(p); ok {
						providerCfg = cfg
						fmt.Printf(" %s Formato extraído del ejemplo: %s\n", LimeGreen("[GeoRoute]"), OrangeCo(cfg.RegionTemplate))
					} else {
						providerCfg = GeoRoute.BrightDataProviderConfig()
					}
				} else {
					providerCfg = GeoRoute.BrightDataProviderConfig()
				}
			case 3:
				providerCfg = GeoRoute.DefaultCRProviderConfig()
			case 4:
				applyGeo = false
			default:
				if cfg, ok := GeoRoute.DetectProviderFromProxies(proxyList); ok {
					providerCfg = cfg
				} else {
					providerCfg = GeoRoute.BrightDataProviderConfig()
				}
			}

			GeoRoute.ActiveProviderConfig = providerCfg
			GeoRoute.ActiveApplyGeo = applyGeo

			if applyGeo {
				fmt.Println()
				fmt.Printf(" %s %s %s (%s)\n",
					LimeGreen("[GeoRoute]"),
					WhiteColor("Asignación geográfica por cuenta activa con plantilla:"),
					LimeGreen(providerCfg.RegionTemplate),
					OrangeCo(fmt.Sprintf("%d proxies base cargados", len(proxyList))),
				)
			} else {
				fmt.Println()
				fmt.Printf(" %s %s (%s)\n",
					LimeGreen("[GeoRoute]"),
					WhiteColor("Modo sin adaptación regional (proxies directos)"),
					OrangeCo(fmt.Sprintf("%d proxies cargados", len(proxyList))),
				)
			}
		}

		time.Sleep(2 * time.Second)

		return proxyList, nil
	}
	return nil, nil
}

// FUNCIONES EXTRAS

func UIStart() (string, int64, []string, int, string) {
	Threader := Utils.ReadInt(fmt.Sprintf("%s %s", Red(" →"), "¿Cuantos Hilos o Taks?: "), 1)

	proxyType := SelectProxy()

	comboFilePath, totalLines, err := OpenCombo()
	if err != nil {
		fmt.Println(err)
	}
	ProxyList, err2 := OpenProxy(proxyType)
	if err2 != nil {
		fmt.Println(err2)
	}

	return comboFilePath, totalLines, ProxyList, Threader, proxyType
}

// UIStartSlice inicia la interfaz seleccionando proxies y cargando el combo completo como slice []string en memoria.
func UIStartSlice() ([]string, []string, int, string) {
	Threader := Utils.ReadInt(fmt.Sprintf("%s %s", Red(" →"), "¿Cuantos Hilos o Taks?: "), 1)

	proxyType := SelectProxy()
	var ComboList []string

	ComboList, err := OpenComboSlice()
	if err != nil {
		fmt.Println(err)
	}
	ProxyList, err2 := OpenProxy(proxyType)
	if err2 != nil {
		fmt.Println(err2)
	}

	return ComboList, ProxyList, Threader, proxyType
}

// OpenComboSlice abre el diálogo de combos y carga el archivo completo en memoria como []string.
func OpenComboSlice() ([]string, error) {

	UpdateLeanCoderProjects()

	remainingFiles := findRemainingComboFiles()
	var filePath string

	if len(remainingFiles) > 0 {
		fmt.Println(Red(" → "), WhiteColor("COMBOS REMAINING"))
		fmt.Println()
		fmt.Printf(" [%s] %s\n", Red(0), WhiteColor("Cargar un nuevo archivo de combo (Selección manual)"))
		for i, f := range remainingFiles {
			fileName := filepath.Base(f)
			fmt.Printf(" [%s] %s\n", Red(i+1), WhiteColor(fileName))
		}
		fmt.Println()

		sel := Utils.ReadInt(Red(" → ")+WhiteColor("Selecciona una opción [0 para nuevo]: "), 0)
		if sel > 0 && sel <= len(remainingFiles) {
			filePath = remainingFiles[sel-1]
		}
	}

	if filePath == "" {
		fmt.Print(Red(" → "), WhiteColor("Combo Archivo seleccionado: "))
		var err error
		filePath, err = dialog.File().Filter("Text Files", "txt").Title("Selecciona tu archivo de combos").Load()
		if err != nil {
			return nil, err
		}
	}

	return LoadComboSliceFromFile(filePath)
}

// LoadComboSliceFromFile procesa, deduplica concurrentemente y clasifica geográficamente un archivo de combos dado.
func LoadComboSliceFromFile(filePath string) ([]string, error) {
	fmt.Println(WhiteColor(filePath))
	Utils.ComboFilePath = filePath

	base := filepath.Base(filePath)
	ext := filepath.Ext(base)
	nameWithoutExt := strings.TrimSuffix(base, ext)

	if idx := strings.LastIndex(nameWithoutExt, "_"); idx > 0 && len(nameWithoutExt)-idx == 10 {
		nameWithoutExt = nameWithoutExt[:idx]
	}
	Utils.ComboFileName = nameWithoutExt

	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	data = Utils.NormalizeTextBytes(data)

	var shards [numShards]shard
	for i := 0; i < numShards; i++ {
		shards[i].m = make(map[string]GeoRoute.CountryCode)
		shards[i].countryCounts = make(map[GeoRoute.CountryCode]int)
	}

	workers := runtime.NumCPU()
	if workers < 4 {
		workers = 4
	}

	var duplicateCount atomic.Uint64
	var totalProcessed atomic.Uint64

	start := time.Now()

	// Dividir el buffer completo en N chunks contiguos alineados a \n
	totalBytes := len(data)
	chunkSize := totalBytes / workers

	var wg sync.WaitGroup

	for w := 0; w < workers; w++ {
		startIdx := w * chunkSize
		endIdx := (w + 1) * chunkSize
		if w == workers-1 {
			endIdx = totalBytes
		} else {
			for endIdx < totalBytes && data[endIdx] != '\n' {
				endIdx++
			}
			if endIdx < totalBytes {
				endIdx++
			}
		}

		if w > 0 {
			for startIdx < totalBytes && data[startIdx-1] != '\n' {
				startIdx++
			}
		}

		if startIdx >= endIdx {
			continue
		}

		chunkData := data[startIdx:endIdx]

		wg.Add(1)
		go func(chunk []byte) {
			defer wg.Done()
			detector := GeoRoute.NewCountryDetector(50000)

			idx := 0
			n := len(chunk)
			var localProcessed uint64
			var localDup uint64

			for idx < n {
				lineStart := idx
				for idx < n && chunk[idx] != '\n' && chunk[idx] != '\r' {
					idx++
				}
				lineBytes := chunk[lineStart:idx]

				for idx < n && (chunk[idx] == '\n' || chunk[idx] == '\r') {
					idx++
				}

				if len(lineBytes) == 0 {
					continue
				}

				localProcessed++

				var cleanBytes []byte
				var country GeoRoute.CountryCode

				lastColon := bytes.LastIndexByte(lineBytes, ':')
				if lastColon > 0 {
					prevColon := bytes.LastIndexByte(lineBytes[:lastColon], ':')
					if prevColon > 0 {
						userBytes := lineBytes[prevColon+1 : lastColon]
						passBytes := lineBytes[lastColon+1:]
						if len(userBytes) > 0 && len(passBytes) > 0 {
							cleanBytes = lineBytes[prevColon+1:]
							rawStr := string(lineBytes)
							rec, dErr := detector.DetectNormalized(rawStr, GeoRoute.FormatULP, GeoRoute.PolicyPreferService)
							if dErr == nil && !rec.Country.IsUnknown() {
								country = rec.Country
							} else {
								country = GeoRoute.Unknown
							}
						} else {
							cleanBytes = lineBytes
							country = detector.Detect(string(lineBytes))
						}
					} else {
						cleanBytes = lineBytes
						country = detector.Detect(string(lineBytes))
					}
				} else {
					cleanBytes = lineBytes
					country = GeoRoute.Unknown
				}

				hash := uint32(2166136261)
				for k := 0; k < len(cleanBytes); k++ {
					hash ^= uint32(cleanBytes[k])
					hash *= 16777619
				}
				sIdx := hash % numShards

				shards[sIdx].Lock()
				if _, exists := shards[sIdx].m[string(cleanBytes)]; exists {
					localDup++
				} else {
					strKey := string(cleanBytes)
					shards[sIdx].m[strKey] = country
					shards[sIdx].countryCounts[country]++
				}
				shards[sIdx].Unlock()
			}

			totalProcessed.Add(localProcessed)
			duplicateCount.Add(localDup)
		}(chunkData)
	}

	wg.Wait()

	totalUnique := 0
	countryCounts := make(map[GeoRoute.CountryCode]int)
	for i := 0; i < numShards; i++ {
		totalUnique += len(shards[i].m)
		for cc, count := range shards[i].countryCounts {
			countryCounts[cc] += count
		}
	}

	result := make([]string, 0, totalUnique)
	for i := 0; i < numShards; i++ {
		for line := range shards[i].m {
			result = append(result, line)
		}
	}

	duration := time.Since(start)
	fmt.Println()
	fmt.Println(Red(" →"), WhiteColor("Total de líneas:"), humanize.Comma(int64(totalProcessed.Load())))
	fmt.Println(Red(" →"), WhiteColor("Líneas únicas:"), LimeGreen(humanize.Comma(int64(totalUnique))))
	fmt.Println(Red(" →"), WhiteColor("Líneas duplicadas o vacías:"), Red(humanize.Comma(int64(duplicateCount.Load()))))
	fmt.Println(Red(" →"), WhiteColor("Tiempo transcurrido:"), OrangeCo(duration))

	if len(countryCounts) > 0 {
		RenderCountryBreakdown(countryCounts, len(result))

		var topCountry GeoRoute.CountryCode
		var maxCount int
		for cc, count := range countryCounts {
			if !cc.IsUnknown() && count > maxCount {
				maxCount = count
				topCountry = cc
			}
		}
		if !topCountry.IsUnknown() {
			Utils.DetectedCountry = string(topCountry)
			fmt.Printf("\n %s %s %s (%s)\n", LimeGreen("✓"), WhiteColor("País predominante detectado:"), LimeGreen(string(topCountry)), OrangeCo(fmt.Sprintf("%d Lines", maxCount)))
		}
	}
	Utils.ComboLoaded = int64(len(result))
	time.Sleep(2 * time.Second)
	return result, nil
}

func SelectProxy() string {
	fmt.Println(Red(" →"), "¿Deseas usar proxy?")
	fmt.Println()
	fmt.Print(" [", Red("1"), "]", " HTTP\n")
	fmt.Print(" [", Red("2"), "]", " SOCKS5\n")
	fmt.Print(" [", Red("3"), "]", " OFF\n")
	fmt.Println()

	response_string := Utils.ReadString(Red(" → ") + WhiteColor("Opcion [1/2/3, Default: 1]: "))
	if response_string == "" {
		response_string = "1"
	}

	switch response_string {
	case "1":
		return "http"
	case "2":
		return "socks5"
	case "3":
		return "off"
	default:
		fmt.Println()
		fmt.Print(Red("Opción no válida."))
		time.Sleep(2 * time.Second)
		return SelectProxy()
	}
}
