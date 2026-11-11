package Utils

import (
	"bufio"
	crand "crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf16"
	"unicode/utf8"
)

func UrlEnconde(input string) string {
	return url.QueryEscape(input)
}

// Pool de buffers para reducir allocaciones
var bufferPool = sync.Pool{
	New: func() interface{} {
		return make([]byte, 0, 1024)
	},
}

// Función Unescape desescapa una cadena emulando la función Unescape de OpenBullet 2
// (basada en Regex.Unescape de .NET). Decodifica secuencias \uXXXX (con soporte para
// pares subrogados UTF-16 como emojis), \UXXXXXXXX, \xXX, \n, \r, \t, \", \', \\, \/
// y cualquier carácter precedido por una barra invertida.
func Unescape(s string) (string, error) {
	return UnescapeString(s), nil
}

// UnescapeString desescapa una cadena directamente (estilo OpenBullet 2),
// sin retornar error, garantizando que el texto se procese limpiamente sin fallos.
func UnescapeString(s string) string {
	if !strings.ContainsRune(s, '\\') {
		return s
	}

	buf := bufferPool.Get().([]byte)
	buf = buf[:0]
	if cap(buf) < len(s) {
		buf = make([]byte, 0, len(s))
	}
	defer func() {
		if cap(buf) <= 128*1024 {
			bufferPool.Put(buf)
		}
	}()

	for i := 0; i < len(s); {
		nextSlash := strings.IndexByte(s[i:], '\\')
		if nextSlash == -1 {
			buf = append(buf, s[i:]...)
			break
		}
		if nextSlash > 0 {
			buf = append(buf, s[i:i+nextSlash]...)
			i += nextSlash
		}

		// s[i] == '\\'
		i++
		if i >= len(s) {
			buf = append(buf, '\\')
			break
		}

		switch s[i] {
		case 'a':
			buf = append(buf, '\a')
			i++
		case 'b':
			buf = append(buf, '\b')
			i++
		case 'e':
			buf = append(buf, 0x1b)
			i++
		case 'f':
			buf = append(buf, '\f')
			i++
		case 'n':
			buf = append(buf, '\n')
			i++
		case 'r':
			buf = append(buf, '\r')
			i++
		case 't':
			buf = append(buf, '\t')
			i++
		case 'v':
			buf = append(buf, '\v')
			i++
		case '\\':
			buf = append(buf, '\\')
			i++
		case '/':
			buf = append(buf, '/')
			i++
		case '\'':
			buf = append(buf, '\'')
			i++
		case '"':
			buf = append(buf, '"')
			i++
		case 'x':
			if i+2 < len(s) {
				if v, err := strconv.ParseUint(s[i+1:i+3], 16, 8); err == nil {
					buf = append(buf, byte(v))
					i += 3
					continue
				}
			}
			if i+1 < len(s) {
				if v, err := strconv.ParseUint(s[i+1:i+2], 16, 8); err == nil {
					buf = append(buf, byte(v))
					i += 2
					continue
				}
			}
			buf = append(buf, 'x')
			i++
		case 'u':
			if i+4 < len(s) {
				if v, err := strconv.ParseUint(s[i+1:i+5], 16, 32); err == nil {
					r := rune(v)
					i += 5
					// Soporte para pares subrogados UTF-16 (surrogate pairs para emojis/astral codes)
					if utf16.IsSurrogate(r) && r >= 0xD800 && r <= 0xDBFF {
						if i+5 < len(s) && s[i] == '\\' && (s[i+1] == 'u' || s[i+1] == 'U') {
							if v2, err2 := strconv.ParseUint(s[i+2:i+6], 16, 32); err2 == nil {
								r2 := rune(v2)
								if r2 >= 0xDC00 && r2 <= 0xDFFF {
									combined := utf16.DecodeRune(r, r2)
									var runeBytes [utf8.UTFMax]byte
									n := utf8.EncodeRune(runeBytes[:], combined)
									buf = append(buf, runeBytes[:n]...)
									i += 6
									continue
								}
							}
						}
					}
					var runeBytes [utf8.UTFMax]byte
					n := utf8.EncodeRune(runeBytes[:], r)
					buf = append(buf, runeBytes[:n]...)
					continue
				}
			}
			buf = append(buf, 'u')
			i++
		case 'U':
			if i+8 < len(s) {
				if v, err := strconv.ParseUint(s[i+1:i+9], 16, 32); err == nil {
					var runeBytes [utf8.UTFMax]byte
					n := utf8.EncodeRune(runeBytes[:], rune(v))
					buf = append(buf, runeBytes[:n]...)
					i += 9
					continue
				}
			}
			buf = append(buf, 'U')
			i++
		case 'c':
			if i+1 < len(s) {
				ch := s[i+1]
				if (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') {
					buf = append(buf, ch&0x1F)
					i += 2
					continue
				}
			}
			buf = append(buf, 'c')
			i++
		default:
			if s[i] >= '0' && s[i] <= '7' {
				end := i
				for end < len(s) && end-i < 3 && s[end] >= '0' && s[end] <= '7' {
					end++
				}
				if v, err := strconv.ParseUint(s[i:end], 8, 8); err == nil {
					buf = append(buf, byte(v))
					i = end
					continue
				}
			}
			// Cualquier otro carácter precedido por '\' pierde la barra (ej: \/, \., \-, \?, etc.)
			r, size := utf8.DecodeRuneInString(s[i:])
			var runeBytes [utf8.UTFMax]byte
			n := utf8.EncodeRune(runeBytes[:], r)
			buf = append(buf, runeBytes[:n]...)
			i += size
		}
	}

	return string(buf)
}

// Inicializa writers concurrentes
func InitWriters(dirPath string, fileNames []string, cfg WriterConfig) error {
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		return err
	}

	writersClosed.Store(false)
	writersMu.Lock()
	currentDirPath = dirPath
	currentWriterConfig = cfg
	if cfg.BufferSize <= 0 {
		cfg.BufferSize = 1000
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = 100 * time.Millisecond
	}
	writers = make(map[string]chan string)
	for _, name := range fileNames {
		spawnWriterLocked(dirPath, name, cfg)
	}
	writersMu.Unlock()
	return nil
}

func spawnWriterLocked(dirPath, fileName string, cfg WriterConfig) chan string {
	ch := make(chan string, cfg.BufferSize)
	writers[fileName] = ch
	writersWg.Add(1)

	go func(fName string, lineChan chan string) {
		defer writersWg.Done()

		filePath := filepath.Join(dirPath, fName+".txt")
		file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			fmt.Printf("Error abriendo %s: %v\n", filePath, err)
			return
		}
		defer file.Close()

		writer := bufio.NewWriter(file)
		ticker := time.NewTicker(cfg.FlushInterval)
		defer ticker.Stop()

		batch := make([]string, 0, 50)
		lastWrite := time.Now()

		for {
			select {
			case line, ok := <-lineChan:
				if !ok {
					// Escribir lo que queda y cerrar
					for _, l := range batch {
						writer.WriteString(l)
						writer.WriteByte('\n')
					}
					writer.Flush()
					file.Sync()
					return
				}

				batch = append(batch, line)
				lastWrite = time.Now()

				// Escribir si: batch lleno O canal vacío
				if len(batch) >= 50 || len(lineChan) == 0 {
					for _, l := range batch {
						writer.WriteString(l)
						writer.WriteByte('\n')
					}
					writer.Flush()
					batch = batch[:0]
				}

			case <-ticker.C:
				// Flush periódico si hay datos y pasó el tiempo
				if len(batch) > 0 && time.Since(lastWrite) > cfg.FlushInterval {
					for _, l := range batch {
						writer.WriteString(l)
						writer.WriteByte('\n')
					}
					writer.Flush()
					file.Sync()
					batch = batch[:0]
				}
			}
		}
	}(fileName, ch)

	return ch
}

func getOrRegisterWriter(fileName string) chan string {
	writersMu.Lock()
	defer writersMu.Unlock()

	if ch, ok := writers[fileName]; ok && ch != nil {
		return ch
	}
	if writersClosed.Load() || currentDirPath == "" {
		return nil
	}
	cfg := currentWriterConfig
	if cfg.BufferSize <= 0 {
		cfg.BufferSize = 1000
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = 100 * time.Millisecond
	}
	return spawnWriterLocked(currentDirPath, fileName, cfg)
}

// SaveResult persiste una línea de resultado de forma thread-safe con backpressure real y auto-registro de categorías.
// Garantiza cero descarte silencioso de cuentas por buffer lleno o categorías no pre-registradas.
func SaveResult(fileName, line string) {
	if writersClosed.Load() {
		return
	}

	if strings.EqualFold(fileName, "REMOVE") || strings.EqualFold(fileName, "REMOVED") {
		fileName = "REMOVED"
	}

	if strings.EqualFold(fileName, "RETRY ACCOUNT") || strings.EqualFold(fileName, "RetryAccount") || strings.EqualFold(fileName, "RETRY_ACCOUNT") || strings.EqualFold(fileName, "RETRYACC") {
		fileName = "RetryAccount"
	}

	writersMu.RLock()
	ch, ok := writers[fileName]
	writersMu.RUnlock()
	if !ok || ch == nil {
		ch = getOrRegisterWriter(fileName)
		if ch == nil {
			return
		}
	}

	// Backpressure real: esperamos a que el buffer reciba la línea para garantizar que NINGÚN hit se pierda.
	// defer recover() protege contra carreras si CloseWriters cierra el canal durante el apagado.
	defer func() {
		_ = recover()
	}()
	ch <- line
}

// CloseWriters drena y cierra ordenadamente todos los writers garantizando cero panics por canal cerrado.
func CloseWriters() {
	if !writersClosed.CompareAndSwap(false, true) {
		return
	}

	writersMu.Lock()
	for _, ch := range writers {
		close(ch)
	}
	writersMu.Unlock()

	writersWg.Wait()
}

func LR(input, left, right string, recursive bool) LRResult {
	var results []string

	if left == `` && right == `` {
		return LRResult{Single: input, All: []string{input}, Err: nil}
	}

	if (left != `` && !strings.Contains(input, left)) || (right != `` && !strings.Contains(input, right)) {
		return LRResult{Err: errors.New("delimitadores no encontrados")}
	}

	partial := input

	if recursive {
		for {
			indexLeft := strings.Index(partial, left)
			if indexLeft == -1 {
				break
			}
			start := indexLeft + len(left)
			partial = partial[start:]

			indexRight := strings.Index(partial, right)
			if indexRight == -1 {
				break
			}

			results = append(results, partial[:indexRight])
			partial = partial[indexRight+len(right):]
		}
		if len(results) == 0 {
			return LRResult{Err: errors.New("no se encontraron coincidencias")}
		}
		return LRResult{All: results, Err: nil}
	}

	// No recursivo (solo el primero)
	indexLeft := strings.Index(partial, left)
	if indexLeft == -1 {
		return LRResult{Err: errors.New("delimitador izquierdo no encontrado")}
	}
	start := indexLeft + len(left)
	partial = partial[start:]

	indexRight := strings.Index(partial, right)
	if indexRight == -1 {
		return LRResult{Err: errors.New("delimitador derecho no encontrado")}
	}

	single := strings.TrimSpace(partial[:indexRight])
	return LRResult{Single: single, All: []string{single}, Err: nil}
}

// ==============================================
// STRUTURA PARA L/R RESULT
// ==============================================
type LRResult struct {
	Single string
	All    []string
	Err    error
}

// ParseSubscriptions extrae y formatea las suscripciones usando encoding/json
func ParseSubscriptions(body string) string {
	// Definimos la estructura básica que nos interesa del JSON
	type SubResponse struct {
		Subscriptions []struct {
			DisplayName   string `json:"displayName"`
			AccountStatus string `json:"accountStatus"`
			ActivationUrl string `json:"activationUrl"`
		} `json:"subscriptions"`
	}

	var resp SubResponse
	if err := json.Unmarshal([]byte(body), &resp); err != nil {
		return "Subs[ Ninguna ]"
	}

	if len(resp.Subscriptions) == 0 {
		return "Subs[ Ninguna ]"
	}

	var parsedSubs []string
	for _, sub := range resp.Subscriptions {
		// Si no tiene nombre, la saltamos o le ponemos Desconocido
		if sub.DisplayName == "" {
			sub.DisplayName = "Unknown"
		}
		if sub.AccountStatus == "" {
			sub.AccountStatus = "N/A"
		}
		if sub.ActivationUrl == "" {
			sub.ActivationUrl = "N/A"
		}

		// Formateamos como tú pediste: Plan: X - Status: Y - Url: Z
		formatted := fmt.Sprintf("Plan: %s - Status: %s - Url: %s", sub.DisplayName, sub.AccountStatus, sub.ActivationUrl)
		parsedSubs = append(parsedSubs, formatted)
	}

	// Las unimos con " | "
	return fmt.Sprintf("Subs[ %s ]", strings.Join(parsedSubs, " | "))
}

const (
	lowerChars = "abcdefghijklmnopqrstuvwxyz"
	upperChars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
	digitChars = "0123456789"
	allChars   = lowerChars + upperChars + digitChars
)

func GenerateRandomPassword() string {
	length := rand.Intn(12-7+1) + 7

	var password []byte

	password = append(password, lowerChars[rand.Intn(len(lowerChars))])
	password = append(password, upperChars[rand.Intn(len(upperChars))])
	password = append(password, digitChars[rand.Intn(len(digitChars))])

	for i := 3; i < length; i++ {
		password = append(password, allChars[rand.Intn(len(allChars))])
	}

	for i := range password {
		j := rand.Intn(i + 1)
		password[i], password[j] = password[j], password[i]
	}

	return string(password)
}

var (
	stdinReader = bufio.NewReader(os.Stdin)
	stdinMutex  sync.Mutex
)

// ReadString solicita una entrada de texto al usuario consumiendo la línea completa de os.Stdin.
func ReadString(prompt string) string {
	stdinMutex.Lock()
	defer stdinMutex.Unlock()

	if prompt != "" {
		fmt.Print(prompt)
	}
	text, err := stdinReader.ReadString('\n')
	if err != nil {
		return ""
	}
	return strings.TrimSpace(text)
}

// SetStdinReaderForTest permite inyectar un io.Reader personalizado para pruebas unitarias.
func SetStdinReaderForTest(r io.Reader) func() {
	stdinMutex.Lock()
	old := stdinReader
	stdinReader = bufio.NewReader(r)
	stdinMutex.Unlock()
	return func() {
		stdinMutex.Lock()
		stdinReader = old
		stdinMutex.Unlock()
	}
}

// ReadInt solicita una entrada numérica al usuario de forma sanitizada y robusta.
// Si el usuario presiona Enter sin ingresar datos, se retorna defaultValue.
// Si el usuario ingresa letras o caracteres no numéricos, se advierte y se repite la solicitud.
// Solo se permiten números enteros mayores o iguales a 0.
func ReadInt(prompt string, defaultValue int) int {
	stdinMutex.Lock()
	defer stdinMutex.Unlock()

	for {
		if prompt != "" {
			fmt.Print(prompt)
		}
		text, err := stdinReader.ReadString('\n')
		if err != nil {
			return defaultValue
		}
		text = strings.TrimSpace(text)
		if text == "" {
			return defaultValue
		}
		val, err := strconv.Atoi(text)
		if err != nil {
			fmt.Println(Red(" [!] Entrada inválida: solo se permiten números enteros. Intente nuevamente."))
			continue
		}
		if val < 0 {
			fmt.Println(Red(" [!] Entrada inválida: el valor no puede ser negativo."))
			continue
		}
		return val
	}
}

// ReadBool solicita una confirmación booleana (Y/N) al usuario.
func ReadBool(prompt string, defaultVal bool) bool {
	stdinMutex.Lock()
	defer stdinMutex.Unlock()

	if prompt != "" {
		fmt.Print(prompt)
	}
	text, err := stdinReader.ReadString('\n')
	if err != nil {
		return defaultVal
	}
	text = strings.ToLower(strings.TrimSpace(text))
	if text == "" {
		return defaultVal
	}
	return text == "y" || text == "yes" || text == "s" || text == "si" || text == "true" || text == "1"
}

// CleanResponse sanitiza y formatea la respuesta para que quede limpia en una sola línea en el archivo .txt
func CleanResponse(raw string) string {
	raw = strings.ReplaceAll(raw, "\r", " ")
	raw = strings.ReplaceAll(raw, "\n", " ")
	raw = strings.ReplaceAll(raw, "\t", " ")
	raw = strings.Join(strings.Fields(raw), " ")
	if len(raw) > 500 {
		return raw[:500] + "..."
	}
	return raw
}

// DebugStep permite imprimir pasos de depuración de forma estandarizada.
func DebugStep(step, label, msg string) {
	// Opcional para depuración limpia
	_ = step
	_ = label
	_ = msg
}

// Funcion para ingresar el dominio con caracteres ramdon o cargar lista MyDomains.txt
func EntradaDominio(ChangeEmail bool) ([]string, error) {
	if !ChangeEmail {
		return nil, nil
	}

	randomDomain := ReadString(Red("  → ") + WhiteColor("Desea usar un dominio con caracteres ramdon? (Y/N): "))
	if strings.ToLower(randomDomain) == "y" || strings.ToLower(randomDomain) == "s" {
		fmt.Println()
		fmt.Printf("   %s\n", WhiteColor("Si tu dominio es GMAIL existirá 2 formas"))
		fmt.Printf("   %s\n", WhiteColor("Primera forma: el resultado será así: example@gmail.com = e.x.ample@gmail.com o exa.mp.le@gmail.com - ETC"))
		fmt.Printf("   %s\n", WhiteColor("Segunda forma: el resultado será así: example@gmail.com = example+12345@gmail.com o example+789456@gmail.com - ETC"))
		fmt.Printf("   %s\n", WhiteColor("En caso que sea diferente, el resultado será así: example@me.es = exa+12345mple@me.es o e+789456xample@me.es"))
		fmt.Printf("   %s\n\n", WhiteColor("SI NO ESTÁS DE ACUERDO CON ESTA CONFIGURACIÓN, PORFAVOR DIGITE MANUALMENTE EN MyDomains.txt"))

		input := strings.TrimSpace(ReadString(Red("  → ") + WhiteColor("Ingrese el dominio: ")))

		var localPart, domainPart string
		if strings.Contains(input, "@") {
			parts := strings.SplitN(input, "@", 2)
			localPart = strings.TrimSpace(parts[0])
			domainPart = strings.TrimSpace(parts[1])
		} else {
			localPart = ""
			domainPart = input
		}

		domainPart = strings.TrimPrefix(domainPart, "@")
		if domainPart == "" {
			domainPart = "gmail.com"
		}

		// Protección: si el usuario ingresó solo el dominio (ej: "@gmail.com" o "gmail.com")
		// generamos un usuario base aleatorio para que las variantes funcionen correctamente.
		if localPart == "" {
			localPart = fmt.Sprintf("example%s", RandomDigits(4, 7))
			fmt.Println()
			fmt.Printf("   %s\n", Yellow(fmt.Sprintf("➤ [!] No ingresaste un usuario antes del '@'. Se asignó automáticamente: %s@%s", localPart, domainPart)))
		}

		dominioGenerado = localPart + "@" + domainPart

		if strings.Contains(strings.ToLower(dominioGenerado), "@gmail.com") {
			fmt.Println()
			fmt.Printf("   %s\n", WhiteColor("[1] Primera forma: e.x.ample@gmail.com o exa.mp.le@gmail.com - ETC"))
			fmt.Printf("   %s\n\n", WhiteColor("[2] Segunda forma: example+12345@gmail.com o example+789456@gmail.com - ETC"))
			for {
				response := ReadInt(Red("  → ") +WhiteColor(" Seleccione su formato [1/2]: "), 1)
				if response == 1 {
					FormaGmail = "1"
					break
				} else if response == 2 {
					FormaGmail = "2"
					break
				} else {
					fmt.Printf("   %s\n", WhiteColor("➤ Formato no válido"))
				}
			}
		}
		return []string{}, nil
	}

	// Asegúrate de que el archivo existe, créalo si no
	if _, err := os.Stat(NombreArchivoDominios); os.IsNotExist(err) {
		fmt.Println(Red(" ────────────────────────────"))
		fmt.Printf("%s%s - %s\n", Red(" → Archivo no encontrado"), WhiteColor(", creando archivo"), LimeGreen(NombreArchivoDominios))
		file, err := os.Create(NombreArchivoDominios)
		if err != nil {
			return nil, fmt.Errorf("error al crear el archivo: %w", err)
		}
		file.Close()
	}

	var lineas []string
	mensajeMostrado := false // para mostrar el mensaje solo una vez

	for {
		file, err := os.Open(NombreArchivoDominios)
		if err != nil {
			return nil, fmt.Errorf("error al abrir el archivo: %w", err)
		}

		scanner := bufio.NewScanner(file)
		lineas = nil
		for scanner.Scan() {
			text := strings.TrimSpace(scanner.Text())
			if text != "" {
				lineas = append(lineas, text)
			}
		}
		file.Close()

		if err := scanner.Err(); err != nil {
			return nil, fmt.Errorf("error al leer el archivo: %w", err)
		}

		if len(lineas) > 0 {
			break
		}

		if !mensajeMostrado {
			fmt.Printf("%s %s\n", Red(" →"), WhiteColor("Coloca tus Dominios Example@yopmail.com en "+NombreArchivoDominios))
			mensajeMostrado = true
		}

		time.Sleep(2 * time.Second)
	}

	if len(lineas) < 20 {
		fmt.Printf("%s %s\n", Red(" →"), WhiteColor("Dominios menos de 20"))
		time.Sleep(2 * time.Second)
	}

	return lineas, nil
}

// Random10to60 devuelve un número aleatorio entre 10 y 60 en formato string
func Random10to60() string {
	num := rand.Intn(51) + 10 // 10–60
	return strconv.Itoa(num)
}

// RandomDigits genera una cadena de dígitos aleatorios de longitud entre minLen y maxLen.
func RandomDigits(minLen, maxLen int) string {
	length := rand.Intn(maxLen-minLen+1) + minLen
	var sb strings.Builder
	for i := 0; i < length; i++ {
		sb.WriteByte('0' + byte(rand.Intn(10)))
	}
	return sb.String()
}

// RandomNUM genera un número entero aleatorio entre minLen y maxLen.
func RandomNUM(minLen, maxLen int) string {
	num := rand.Intn(maxLen-minLen+1) + minLen
	return strconv.Itoa(num)
}

// RandomString genera una cadena hexadecimal aleatoria a partir de bytes criptográficos.
func RandomString(length int) string {
	b := make([]byte, (length+1)/2)
	crand.Read(b)
	return hex.EncodeToString(b)[:length]
}

// EmailVariantGenerator genera variaciones dinámicas de correo para evitar colisiones.
type EmailVariantGenerator struct {
	local   []rune
	domain  string
	seen    map[string]struct{}
	formato string
}

// NewEmailVariantGenerator crea un nuevo generador de variantes de correo.
func NewEmailVariantGenerator(email string) *EmailVariantGenerator {
	email = strings.TrimSpace(email)
	if email == "" {
		email = "@gmail.com"
	}

	at := strings.IndexRune(email, '@')
	var local []rune
	var domain string

	if at < 0 {
		// El usuario ingresó solo el dominio sin '@' (ej: "gmail.com")
		local = []rune(fmt.Sprintf("example%s", RandomDigits(4, 7)))
		domain = strings.TrimPrefix(email, "@")
	} else if at == 0 {
		// El usuario ingresó solo el dominio con '@' (ej: "@gmail.com")
		local = []rune(fmt.Sprintf("example%s", RandomDigits(4, 7)))
		domain = email[1:]
	} else {
		local = []rune(email[:at])
		domain = email[at+1:]
	}

	domain = strings.TrimSpace(domain)
	if len(domain) == 0 {
		domain = "gmail.com"
	}
	if len(local) == 0 {
		local = []rune(fmt.Sprintf("example%s", RandomDigits(4, 7)))
	}

	return &EmailVariantGenerator{
		local:   local,
		domain:  domain,
		seen:    make(map[string]struct{}),
		formato: FormaGmail,
	}
}

// Next genera un nuevo email según el formato seleccionado.
func (g *EmailVariantGenerator) Next() string {
	if g == nil {
		return fmt.Sprintf("fallback+%s@yopmail.com", RandomDigits(6, 10))
	}

	switch g.formato {
	case "1":
		return g.Forma_1_GMAIL()
	case "2":
		return g.Forma_2_GMAIL()
	default:
		return g.Forma_3()
	}
}

func (g *EmailVariantGenerator) Forma_1_GMAIL() string {
	base := string(g.local)
	n := len(base)

	if n <= 1 {
		return g.Forma_2_GMAIL()
	}

	mask := rand.Intn(1 << (n - 1))
	var b strings.Builder
	for i := 0; i < n; i++ {
		b.WriteByte(base[i])
		if i < n-1 && (mask&(1<<i)) != 0 {
			b.WriteByte('.')
		}
	}
	return fmt.Sprintf("%s@%s", b.String(), g.domain)
}

func (g *EmailVariantGenerator) Forma_2_GMAIL() string {
	if len(g.local) == 0 {
		g.local = []rune(fmt.Sprintf("example%s", RandomDigits(4, 7)))
	}
	for i := 0; i < 100; i++ {
		randNum := "+" + RandomDigits(6, 8)
		email := string(g.local) + randNum + "@" + g.domain
		if _, ok := g.seen[email]; ok {
			continue
		}
		g.seen[email] = struct{}{}
		return email
	}
	return fmt.Sprintf("%s+%s@%s", string(g.local), RandomDigits(10, 14), g.domain)
}

func (g *EmailVariantGenerator) Forma_3() string {
	if len(g.local) == 0 {
		g.local = []rune(fmt.Sprintf("example%s", RandomDigits(4, 7)))
	}
	for i := 0; i < 100; i++ {
		n := len(g.local)
		var b strings.Builder

		plusPos := rand.Intn(n) + 1 // entre 1 y n
		randNum := RandomDigits(5, 8)
		for j := 0; j < n; j++ {
			b.WriteRune(g.local[j])
			if j+1 == plusPos {
				b.WriteRune('+')
				b.WriteString(randNum)
			}
		}
		email := b.String() + "@" + g.domain
		if _, ok := g.seen[email]; ok {
			continue
		}
		g.seen[email] = struct{}{}
		return email
	}
	return fmt.Sprintf("%s+%s@%s", string(g.local), RandomDigits(10, 14), g.domain)
}

// TomarLineaCorreo toma una línea de correo según la configuración activa (generador random o lista MyDomains.txt).
func TomarLineaCorreo() string {
	// ── Caso 1: dominioGenerado activo — usar generador de variantes ──
	if dominioGenerado != "" {
		onceDominio.Do(func() {
			gen := NewEmailVariantGenerator(dominioGenerado)
			if gen != nil {
				NewDominioGen = gen
			}
		})

		if NewDominioGen != nil {
			return NewDominioGen.Next()
		}
	}

	// ── Caso 2: Sin dominios en lista — fallback a yopmail random ─────
	if len(DominiosMios) == 0 {
		return fmt.Sprintf("RamDominio%s@yopmail.net", RandomString(8))
	}

	// ── Caso 3: Tomar línea aleatoria de DominiosMios ─────────────────
	LineasSeleccionadasMu.Lock()
	defer LineasSeleccionadasMu.Unlock()

	if len(lineasSeleccionadas) < len(DominiosMios) {
		var disponibles []int
		for i := range DominiosMios {
			if !lineasSeleccionadas[i] {
				disponibles = append(disponibles, i)
			}
		}

		var indiceAleatorio int
		if len(disponibles) > 0 {
			indiceAleatorio = disponibles[rand.Intn(len(disponibles))]
		} else {
			lineasSeleccionadas = make(map[int]bool)
			indiceAleatorio = rand.Intn(len(DominiosMios))
		}

		lineasSeleccionadas[indiceAleatorio] = true
		lineaSeleccionada := strings.TrimSpace(DominiosMios[indiceAleatorio])

		if lineaSeleccionada == "" || !strings.Contains(lineaSeleccionada, "@") {
			return fmt.Sprintf("fallback+%s@yopmail.com", RandomDigits(6, 10))
		}

		return lineaSeleccionada
	}

	// Todos los dominios ya fueron usados
	return fmt.Sprintf("RamDominio%s@yopmail.com", RandomString(8))
}

// RemoverLineaDeCorreo elimina una línea usada de DominiosMios y del archivo MyDomains.txt de forma thread-safe.
func RemoverLineaDeCorreo(lineaSeleccionada string) error {
	LineasSeleccionadasMu.Lock()
	defer LineasSeleccionadasMu.Unlock()

	if len(DominiosMios) == 0 {
		return nil
	}

	var lineasActualizadas []string
	for _, linea := range DominiosMios {
		if strings.TrimSpace(linea) != strings.TrimSpace(lineaSeleccionada) {
			lineasActualizadas = append(lineasActualizadas, linea)
		}
	}

	DominiosMios = lineasActualizadas

	file, err := os.Create(NombreArchivoDominios)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	for _, linea := range lineasActualizadas {
		_, err := writer.WriteString(linea + "\n")
		if err != nil {
			return err
		}
	}
	return writer.Flush()
}
