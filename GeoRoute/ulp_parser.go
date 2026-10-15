package GeoRoute

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
)

// InputFormat define el tipo de formato de los registros de entrada.
type InputFormat int

const (
	FormatAuto InputFormat = iota
	FormatULP
	FormatCombo
)

func (f InputFormat) String() string {
	switch f {
	case FormatULP:
		return "ULP"
	case FormatCombo:
		return "Combo"
	default:
		return "Auto"
	}
}

var (
	ErrEmptyLine     = errors.New("georoute/parser: línea vacía")
	ErrMalformedLine = errors.New("georoute/parser: línea malformada sin delimitadores suficientes")
)

// ULPParser proporciona métodos optimizados para el parsing y análisis de registros ULP y Combos.
type ULPParser struct{}

// NewULPParser crea una nueva instancia del parser ULP.
func NewULPParser() *ULPParser {
	return &ULPParser{}
}

// DetectFormat analiza una muestra de líneas para determinar automáticamente si el archivo es ULP o Combo.
func (p *ULPParser) DetectFormat(samples []string) InputFormat {
	if len(samples) == 0 {
		return FormatCombo
	}

	ulpScore := 0
	comboScore := 0

	for _, line := range samples {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		parts := strings.Split(line, ":")
		if len(parts) >= 3 {
			// Si tiene 3+ partes y la primera parte parece dominio o URL
			first := strings.ToLower(parts[0])
			if strings.Contains(first, ".") || strings.HasPrefix(first, "http") || strings.Contains(first, "/") {
				ulpScore++
			} else {
				comboScore++
			}
		} else if len(parts) == 2 {
			// user:pass estándar
			comboScore++
		}
	}

	if ulpScore > comboScore {
		return FormatULP
	}
	return FormatCombo
}

// ParseULPLine procesa una línea individual y extrae la estructura ULPRecord conservando el Raw intacto.
func (p *ULPParser) ParseULPLine(rawLine string) (ULPRecord, error) {
	trimmed := strings.TrimSpace(rawLine)
	if trimmed == "" {
		return ULPRecord{Raw: rawLine}, ErrEmptyLine
	}

	// Un registro ULP estándar contiene al menos 2 delimitadores ':' (Service:User:Password)
	// Pero puede haber variaciones (URL con http://, puertos :8080, o passwords con ':')
	parts := strings.Split(trimmed, ":")
	if len(parts) < 2 {
		return ULPRecord{Raw: rawLine}, ErrMalformedLine
	}

	var service, user, pass string

	if len(parts) == 2 {
		// Caso Combo (User:Pass) ingresado en parser ULP
		user = parts[0]
		pass = parts[1]
	} else if len(parts) == 3 {
		// Formato ULP estándar: Service:User:Password
		service = parts[0]
		user = parts[1]
		pass = parts[2]
	} else {
		// 4 o más partes (3+ colons)
		// 1. Verificar si la primera parte es http/https y la segunda comienza con //
		if (strings.HasPrefix(parts[0], "http") || strings.HasPrefix(parts[0], "https")) && strings.HasPrefix(parts[1], "//") {
			service = parts[0] + ":" + parts[1]
			user = parts[2]
			pass = strings.Join(parts[3:], ":")
		} else if strings.Contains(parts[0], ".") && isPortNumber(parts[1]) && strings.Contains(parts[1], "/") {
			// Caso host:port/path:user:pass
			service = parts[0] + ":" + parts[1]
			user = parts[2]
			pass = strings.Join(parts[3:], ":")
		} else {
			// Formato estándar con password que contiene ':'
			service = parts[0]
			user = parts[1]
			pass = strings.Join(parts[2:], ":")
		}
	}

	return ULPRecord{
		Raw:      rawLine,
		Service:  service,
		Email:    user,
		Password: pass,
		Country:  Unknown,
		Source:   CountrySourceUnknown,
	}, nil
}

// ExtractHost normaliza una URL o campo de servicio para extraer exclusivamente el hostname limpio.
func ExtractHost(service string) string {
	raw := strings.TrimSpace(service)
	if raw == "" {
		return ""
	}

	// 1. Quitar esquema (http://, https://, ftp://, etc.)
	if idx := strings.Index(raw, "://"); idx != -1 {
		raw = raw[idx+3:]
	}

	// 2. Quitar rutas (/path, /login, etc.) y query params (?id=1, #hash)
	if idx := strings.IndexAny(raw, "/?#"); idx != -1 {
		raw = raw[:idx]
	}

	// 3. Quitar puerto si existe (:8080)
	if idx := strings.Index(raw, ":"); idx != -1 {
		raw = raw[:idx]
	}

	// 4. Limpiar posibles caracteres sobrantes
	raw = strings.Trim(raw, " .[]")

	return strings.ToLower(raw)
}

// ExtractEmailDomain extrae el dominio limpio de un email o identificador.
func ExtractEmailDomain(email string) string {
	email = strings.TrimSpace(strings.ToLower(email))
	idx := strings.LastIndex(email, "@")
	if idx == -1 || idx == len(email)-1 {
		return ""
	}
	domain := email[idx+1:]
	// Quitar posibles caracteres de cierre o espacios
	domain = strings.Trim(domain, " <>()[]:;,")
	return domain
}

func isPortNumber(s string) bool {
	if idx := strings.Index(s, "/"); idx != -1 {
		s = s[:idx]
	}
	if len(s) == 0 || len(s) > 5 {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// ParseStream procesa un flujo io.Reader en paralelo utilizando un bounded worker pool y backpressure controlada.
func (p *ULPParser) ParseStream(r io.Reader, workers int, onRecord func(ULPRecord, error)) {
	if workers <= 0 {
		workers = runtime.NumCPU()
		if workers < 2 {
			workers = 2
		}
	}

	type lineJob struct {
		raw string
	}

	jobs := make(chan lineJob, workers*256)
	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				rec, err := p.ParseULPLine(job.raw)
				onRecord(rec, err)
			}
		}()
	}

	scanner := bufio.NewScanner(r)
	buf := make([]byte, 1024*1024)
	scanner.Buffer(buf, 10*1024*1024) // hasta 10MB por línea

	for scanner.Scan() {
		jobs <- lineJob{raw: scanner.Text()}
	}

	close(jobs)
	wg.Wait()
}

// ParseULPBytesZeroCopy extrae los campos de una línea ULP directamente a nivel de bytes sin alocaciones de memoria en el heap.
func ParseULPBytesZeroCopy(line []byte) (service, user, pass []byte, ok bool) {
	// Trim de saltos de línea y espacios en extremos
	for len(line) > 0 && (line[len(line)-1] == '\r' || line[len(line)-1] == '\n' || line[len(line)-1] == ' ') {
		line = line[:len(line)-1]
	}
	for len(line) > 0 && line[0] == ' ' {
		line = line[1:]
	}
	if len(line) == 0 {
		return nil, nil, nil, false
	}

	idx1 := bytes.IndexByte(line, ':')
	if idx1 == -1 {
		return nil, nil, nil, false
	}

	idx2 := bytes.IndexByte(line[idx1+1:], ':')
	if idx2 == -1 {
		// Formato Combo simple (user:pass)
		return nil, line[:idx1], line[idx1+1:], true
	}
	idx2 += idx1 + 1

	// Manejo de URLs con esquema http:// o https://
	if bytes.HasPrefix(line, []byte("http")) && bytes.HasPrefix(line[idx1+1:], []byte("//")) {
		idx3 := bytes.IndexByte(line[idx2+1:], ':')
		if idx3 != -1 {
			idx3 += idx2 + 1
			return line[:idx2], line[idx2+1 : idx3], line[idx3+1:], true
		}
	}

	return line[:idx1], line[idx1+1 : idx2], line[idx2+1:], true
}

// ExtractHostBytes extrae el hostname limpio de un slice de bytes de servicio sin alocar memoria.
func ExtractHostBytes(service []byte) []byte {
	if len(service) == 0 {
		return nil
	}
	// Quitar esquema ://
	if idx := bytes.Index(service, []byte("://")); idx != -1 {
		service = service[idx+3:]
	}
	// Quitar ruta /, query ?, hash # o puerto :
	for i, b := range service {
		if b == '/' || b == '?' || b == '#' || b == ':' {
			service = service[:i]
			break
		}
	}
	return service
}

// ProcessFileParallelBlocks divide cualquier archivo masivo (1M a 50M+ líneas) en bloques contiguos paralelos
// alineados a saltos de línea \n, procesando gigabytes por segundo con cero alocaciones en el hot-path.
func (p *ULPParser) ProcessFileParallelBlocks(filePath string, workers int, onRecord func(service, user, pass, raw []byte)) (int64, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return 0, err
	}
	totalBytes := stat.Size()
	if totalBytes == 0 {
		return 0, nil
	}

	if workers <= 0 {
		workers = runtime.NumCPU()
		if workers < 2 {
			workers = 2
		}
	}

	chunkSize := totalBytes / int64(workers)
	var totalProcessed atomic.Int64
	var wg sync.WaitGroup

	for w := 0; w < workers; w++ {
		startOffset := int64(w) * chunkSize
		endOffset := int64(w+1) * chunkSize
		if w == workers-1 {
			endOffset = totalBytes
		}

		wg.Add(1)
		go func(startOff, endOff int64) {
			defer wg.Done()

			f, err := os.Open(filePath)
			if err != nil {
				return
			}
			defer f.Close()

			bufSize := endOff - startOff + 65536
			buf := make([]byte, bufSize)

			readStart := startOff
			if startOff > 0 {
				readStart = startOff - 1
			}

			n, err := f.ReadAt(buf, readStart)
			if err != nil && err.Error() != "EOF" && n == 0 {
				return
			}
			data := buf[:n]

			// Alinear inicio con el primer \n si no somos el primer bloque
			if startOff > 0 {
				nl := bytes.IndexByte(data, '\n')
				if nl == -1 {
					return
				}
				data = data[nl+1:]
			}

			bytesToProcess := endOff - startOff
			var count int64

			for len(data) > 0 && bytesToProcess > 0 {
				nl := bytes.IndexByte(data, '\n')
				var line []byte
				if nl == -1 {
					line = data
					data = nil
				} else {
					line = data[:nl]
					data = data[nl+1:]
					bytesToProcess -= int64(nl + 1)
				}

				svc, user, pass, ok := ParseULPBytesZeroCopy(line)
				if ok {
					count++
					onRecord(svc, user, pass, line)
				}
			}

			totalProcessed.Add(count)
		}(startOffset, endOffset)
	}

	wg.Wait()
	return totalProcessed.Load(), nil
}
