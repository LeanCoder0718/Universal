// Package response modela la respuesta HTTP de forma tipada, reutilizando
// buffers (internal/pool) para reducir presión de GC. Mantiene la API
// pública compatible en espíritu con Response.go original (IsSuccess,
// JSONUnmarshal, etc.) pero SIN mezclar parsing de cookies (eso ahora es
// responsabilidad exclusiva de cookies.Jar) ni lógica de retry (ahora en
// retry.Engine) — el Response original tenía métodos que tocaban ambas
// preocupaciones indirectamente vía RetryReason.
package HTTPClient

import (
	"bytes"
	"compress/gzip"
	"compress/zlib"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/LeanCoder0718/Universal/Utils"
	"github.com/andybalholm/brotli"
	fhttp "github.com/bogdanfinn/fhttp"
)

// Response es el resultado final de UN intento de request, ya sin
// conocimiento de cuántos reintentos hubo (eso lo agrega el caller vía
// Result si lo necesita) — Single Responsibility real.
type Response struct {
	StatusCode    int
	Proto         string // ej: "HTTP/2.0"
	Headers       fhttp.Header
	BodyBytes     []byte
	ContentLength int64
	Truncated     bool
	Complete      bool
	Request       *fhttp.Request
	Duration      time.Duration
	Err           error

	compressed   bool
	originalSize int64
	processOnce  sync.Once
	poolBuf      *bytes.Buffer
}

// Result envuelve Response con metadata de orquestación (intentos, proxy
// usado). Separar Result de Response deja claro que "cuántos intentos"
// es un concern de workers/retry, no de la respuesta HTTP en sí misma.
type Result struct {
	*Response
	Attempts int
	ProxyURL string
	Err      error
}

// Release devuelve el buffer subyacente al sync.Pool (Zero-Copy optimization).
// Es imperativo llamarlo vía defer o al final del procesamiento de Result.
func (res *Result) Release() {
	if res != nil && res.Response != nil {
		res.Response.Release()
	}
}

// PreserveBodyForError recicla el buffer al pool pero conserva una copia segura
// desacoplada de los bytes en BodyBytes para el llamador, evitando memory leaks y data races.
func (res *Result) PreserveBodyForError() {
	if res != nil && res.Response != nil {
		res.Response.PreserveBodyForError()
	}
}

// Release devuelve el buffer interno al pool para evitar memory leaks de buffers y aliviar al GC.
func (r *Response) Release() {
	if r != nil && r.poolBuf != nil {
		PutBuffer(r.poolBuf)
		r.poolBuf = nil
		r.BodyBytes = nil
	}
}

// PreserveBodyForError devuelve el buffer al pool pero conserva una copia segura
// desacoplada de los bytes en r.BodyBytes, evitando data races con el pool y
// permitiendo al llamador inspeccionar cuerpos truncados o de error de forma segura.
func (r *Response) PreserveBodyForError() {
	if r != nil && r.poolBuf != nil {
		if len(r.BodyBytes) > 0 {
			safeCopy := make([]byte, len(r.BodyBytes))
			copy(safeCopy, r.BodyBytes)
			r.BodyBytes = safeCopy
		} else {
			r.BodyBytes = nil
		}
		PutBuffer(r.poolBuf)
		r.poolBuf = nil
	}
}

func (r *Response) IsSuccess() bool       { return r.StatusCode >= 200 && r.StatusCode < 300 }
func (r *Response) IsRedirect() bool      { return r.StatusCode >= 300 && r.StatusCode < 400 }
func (r *Response) IsClientError() bool   { return r.StatusCode >= 400 && r.StatusCode < 500 }
func (r *Response) IsServerError() bool   { return r.StatusCode >= 500 }
func (r *Response) IsRateLimited() bool   { return r.StatusCode == 429 }
func (r *Response) IsForbidden() bool     { return r.StatusCode == 403 }

// IsBlocked retorna true si la respuesta fue bloqueada por WAF, CDN o proxy (403, 451 o firmas en headers/body).
func (r *Response) IsBlocked() bool {
	if r == nil {
		return false
	}
	if r.StatusCode == 403 || r.StatusCode == 451 {
		return true
	}
	return isBlockedResponse(r.Headers, r.BodyBytes)
}

// IsChallenge retorna true si la respuesta contiene un desafío o pantalla anti-bot (Cloudflare, Datadome, Turnstile, etc.).
func (r *Response) IsChallenge() bool {
	if r == nil {
		return false
	}
	return isChallengeResponse(r.Headers, r.BodyBytes)
}

// IsBlocked retorna true si el resultado fue bloqueado por WAF o CDN.
func (res *Result) IsBlocked() bool {
	if res == nil || res.Response == nil {
		return false
	}
	return res.Response.IsBlocked()
}

// IsChallenge retorna true si el resultado contiene un desafío anti-bot.
func (res *Result) IsChallenge() bool {
	if res == nil || res.Response == nil {
		return false
	}
	return res.Response.IsChallenge()
}

func isBlockedResponse(headers fhttp.Header, body []byte) bool {
	if headers != nil {
		for k, vv := range headers {
			if strings.EqualFold(k, "x-amzn-waf-action") {
				for _, v := range vv {
					if strings.Contains(strings.ToLower(v), "block") {
						return true
					}
				}
			}
		}
	}
	if len(body) == 0 {
		return false
	}
	chunk := body
	if len(chunk) > 4096 {
		chunk = chunk[:4096]
	}
	lower := strings.ToLower(string(chunk))
	wafKeywords := []string{
		"request blocked",
		"access denied",
		"country_blocked",
		"country blocked",
		`code":"throttled`,
	}
	for _, kw := range wafKeywords {
		if strings.Contains(lower, kw) {
			return true
		}
	}
	return false
}

func isChallengeResponse(headers fhttp.Header, body []byte) bool {
	if headers != nil {
		for k, vv := range headers {
			kLower := strings.ToLower(k)
			for _, v := range vv {
				vLower := strings.ToLower(v)
				if kLower == "cf-mitigated" && (strings.Contains(vLower, "challenge") || strings.Contains(vLower, "captcha")) {
					return true
				}
				if kLower == "cf-chl-bypass" {
					return true
				}
				if kLower == "x-amzn-waf-action" && (strings.Contains(vLower, "challenge") || strings.Contains(vLower, "captcha")) {
					return true
				}
				if kLower == "x-datadome" && (strings.Contains(vLower, "protected") || strings.Contains(vLower, "captcha")) {
					return true
				}
			}
		}
	}
	if len(body) == 0 {
		return false
	}
	chunk := body
	if len(chunk) > 4096 {
		chunk = chunk[:4096]
	}
	lower := strings.ToLower(string(chunk))
	challengeKeywords := []string{
		"challenge-platform",
		"_cf_chl",
		"just a moment",
		"attention required",
		"verifying you are human",
		"verify you are human",
		"turnstile",
		"pardon our interruption",
		"pardon the interruption",
		"datadome",
		"enable javascript and cookies to continue",
	}
	for _, kw := range challengeKeywords {
		if strings.Contains(lower, kw) {
			return true
		}
	}
	return false
}
func (r *Response) GetStatusText() string { return http.StatusText(r.StatusCode) }

func (r *Response) String() string { return string(r.BodyBytes) }
func (r *Response) Bytes() []byte  { return r.BodyBytes }

// Unescape retorna el cuerpo (Body) desescapando secuencias de escape
// idéntico a la función Unescape de OpenBullet 2 (\uXXXX, \/, \", \', \\, \n, emojis, etc.).
func (r *Response) Unescape() string {
	if r == nil || len(r.BodyBytes) == 0 {
		return ""
	}
	return Utils.UnescapeString(string(r.BodyBytes))
}

// Unescape retorna el cuerpo del resultado HTTP desescapado idéntico a OpenBullet 2.
func (res *Result) Unescape() string {
	if res == nil || res.Response == nil {
		return ""
	}
	return res.Response.Unescape()
}

func (r *Response) HeaderValue(name string) string {
	if r.Headers == nil {
		return ""
	}
	if v := r.Headers.Get(name); v != "" {
		return v
	}
	for k, vv := range r.Headers {
		if strings.EqualFold(k, name) && len(vv) > 0 {
			return vv[0]
		}
	}
	return ""
}

func (r *Response) ContentType() string     { return r.HeaderValue("Content-Type") }
func (r *Response) ContentEncoding() string { return r.HeaderValue("Content-Encoding") }

// RetryAfter extrae y resuelve la duración solicitada por el servidor en la cabecera 'Retry-After'.
func (r *Response) RetryAfter() time.Duration {
	if r == nil {
		return 0
	}
	return ParseRetryAfter(r.HeaderValue("Retry-After"))
}

// ParseRetryAfter parsea el valor de la cabecera 'Retry-After' en segundos enteros
// (ej. "120") o en formato HTTP-Date (RFC 1123, RFC 850). Retorna 0 si es inválido o vacío.
func ParseRetryAfter(val string) time.Duration {
	val = strings.TrimSpace(val)
	if val == "" {
		return 0
	}
	if secs, err := strconv.Atoi(val); err == nil && secs >= 0 {
		return time.Duration(secs) * time.Second
	}
	if t, err := http.ParseTime(val); err == nil {
		d := time.Until(t)
		if d > 0 {
			return d
		}
	}
	return 0
}

func (r *Response) IsJSON() bool {
	return strings.Contains(strings.ToLower(r.ContentType()), "application/json")
}

func (r *Response) JSONUnmarshal(dest any) error {
	if len(r.BodyBytes) == 0 {
		return ErrEmptyResponseBody
	}
	return json.Unmarshal(r.BodyBytes, dest)
}

// ContainsAny escanea el body por cualquiera de los textos dados. Usado
// por retry.Classifier a través de un Signal (no directamente acá) para
// mantener response desacoplado de retry.
func (r *Response) ContainsAny(texts ...string) bool {
	if len(r.BodyBytes) == 0 {
		return false
	}
	for _, t := range texts {
		if bytes.Contains(r.BodyBytes, []byte(t)) {
			return true
		}
	}
	return false
}

// DecompressIfNeeded descomprime el body según Content-Encoding, usando
// buffers del pool interno para minimizar allocations en el hot path.
// Idempotente vía sync.Once (igual semántica que el original).
func (r *Response) DecompressIfNeeded() error {
	if r == nil || len(r.BodyBytes) == 0 {
		return nil
	}
	var err error
	r.processOnce.Do(func() {
		err = r.decompress()
	})
	return err
}

func (r *Response) decompress() error {
	if len(r.BodyBytes) == 0 {
		return nil
	}
	encoding := strings.ToLower(r.ContentEncoding())
	if encoding == "" {
		return nil
	}

	var reader io.Reader
	switch {
	case strings.Contains(encoding, "gzip"):
		if len(r.BodyBytes) < 2 || r.BodyBytes[0] != 0x1f || r.BodyBytes[1] != 0x8b {
			return nil // ya descomprimido por el transporte
		}
		gz, err := gzip.NewReader(bytes.NewReader(r.BodyBytes))
		if err != nil {
			return fmt.Errorf("response: gzip: %w", err)
		}
		defer gz.Close()
		reader = gz
	case strings.Contains(encoding, "br"):
		reader = brotli.NewReader(bytes.NewReader(r.BodyBytes))
	case strings.Contains(encoding, "deflate"):
		zr, err := zlib.NewReader(bytes.NewReader(r.BodyBytes))
		if err != nil {
			return fmt.Errorf("response: deflate: %w", err)
		}
		defer zr.Close()
		reader = zr
	default:
		return nil
	}

	r.originalSize = int64(len(r.BodyBytes))
	buf := GetBuffer()
	success := false
	defer func() {
		if !success {
			PutBuffer(buf)
		}
	}()

	if _, err := io.Copy(buf, io.LimitReader(reader, 10*1024*1024)); err != nil {
		return fmt.Errorf("response: descomprimiendo: %w", err)
	}

	// Swap buffer para evitar memory copy destructivo
	if r.poolBuf != nil {
		PutBuffer(r.poolBuf)
	}
	r.poolBuf = buf
	r.BodyBytes = buf.Bytes()
	r.compressed = true
	success = true
	return nil
}

// --- QoL Utilities (Ported from HTTPClient_TLS) ---

func (r *Response) JSONPretty() (string, error) {
	if len(r.BodyBytes) == 0 {
		return "", fmt.Errorf("response body is empty")
	}
	var jsonData interface{}
	if err := json.Unmarshal(r.BodyBytes, &jsonData); err != nil {
		return "", fmt.Errorf("invalid JSON: %w", err)
	}
	prettyJSON, err := json.MarshalIndent(jsonData, "", "  ")
	if err != nil {
		return "", fmt.Errorf("error formatting JSON: %w", err)
	}
	return string(prettyJSON), nil
}

func (r *Response) GetURLRedirect() string {
	if r.Headers != nil {
		loc := r.Headers.Get("Location")
		if loc == "" {
			loc = r.Headers.Get("location")
		}
		if loc != "" {
			if strings.HasPrefix(loc, "/") {
				if r.Request != nil && r.Request.URL != nil {
					scheme := r.Request.URL.Scheme
					if scheme == "" {
						scheme = "https"
					}
					return scheme + "://" + r.Request.URL.Host + loc
				}
				return loc
			}
			return loc
		}
	}
	if r.Request != nil && r.Request.URL != nil {
		return r.Request.URL.String()
	}
	return ""
}

func (r *Response) GetHeaderValue(name string) string {
	if r.Headers != nil {
		return r.Headers.Get(name)
	}
	return ""
}

func (r *Response) GetHeaderValues(name string) []string {
	if r.Headers == nil {
		return nil
	}
	values := r.Headers.Values(name)
	if len(values) == 0 {
		return nil
	}
	result := make([]string, len(values))
	copy(result, values)
	return result
}

func (r *Response) HasHeader(name string) bool {
	return r.Headers != nil && r.Headers.Get(name) != ""
}

func (r *Response) GetServer() string          { return r.GetHeaderValue("Server") }
func (r *Response) GetContentType() string     { return r.GetHeaderValue("Content-Type") }
func (r *Response) GetContentEncoding() string { return r.GetHeaderValue("Content-Encoding") }

func (r *Response) IsHTML() bool {
	return strings.Contains(strings.ToLower(r.GetContentType()), "text/html")
}

func (r *Response) IsXML() bool {
	ct := strings.ToLower(r.GetContentType())
	return strings.Contains(ct, "application/xml") || strings.Contains(ct, "text/xml")
}

func (r *Response) ContainsText(text string) bool {
	if len(r.BodyBytes) == 0 {
		return false
	}
	return bytes.Contains(r.BodyBytes, []byte(text))
}

func (r *Response) ContainsTextIgnoreCase(text string) bool {
	if len(r.BodyBytes) == 0 || text == "" {
		return false
	}
	return containsBytesIgnoreCase(r.BodyBytes, text)
}

// containsBytesIgnoreCase realiza búsqueda case-insensitive sin alocar copias de memoria completas.
func containsBytesIgnoreCase(b []byte, sub string) bool {
	if len(sub) == 0 {
		return true
	}
	if len(b) < len(sub) {
		return false
	}
	subLower := strings.ToLower(sub)
	firstLower := subLower[0]
	firstUpper := firstLower
	if firstLower >= 'a' && firstLower <= 'z' {
		firstUpper = firstLower - 32
	}

	n := len(subLower)
	maxI := len(b) - n
	for i := 0; i <= maxI; i++ {
		c := b[i]
		if c == firstLower || c == firstUpper {
			match := true
			for j := 1; j < n; j++ {
				cb := b[i+j]
				if cb >= 'A' && cb <= 'Z' {
					cb += 32
				}
				if cb != subLower[j] {
					match = false
					break
				}
			}
			if match {
				return true
			}
		}
	}
	return false
}

func (r *Response) ContainsAnyText(texts ...string) bool {
	if len(r.BodyBytes) == 0 {
		return false
	}
	for _, text := range texts {
		if bytes.Contains(r.BodyBytes, []byte(text)) {
			return true
		}
	}
	return false
}

func (r *Response) ContainsAllText(texts ...string) bool {
	if len(r.BodyBytes) == 0 {
		return false
	}
	for _, text := range texts {
		if !bytes.Contains(r.BodyBytes, []byte(text)) {
			return false
		}
	}
	return true
}

func (r *Response) CountOccurrences(text string) int {
	if len(r.BodyBytes) == 0 || text == "" {
		return 0
	}
	return bytes.Count(r.BodyBytes, []byte(text))
}

func (r *Response) GetSize() int {
	return len(r.BodyBytes)
}

func (r *Response) SaveToFile(filename string) error {
	if len(r.BodyBytes) == 0 {
		return fmt.Errorf("response body is empty")
	}
	if filename == "" {
		return fmt.Errorf("filename cannot be empty")
	}
	return os.WriteFile(filename, r.BodyBytes, 0644)
}

func (r *Response) GetCookie(name string) *fhttp.Cookie {
	if r.Request != nil {
		for _, c := range r.Request.Cookies() {
			if strings.EqualFold(c.Name, name) {
				return c
			}
		}
	}
	// También buscar en los Set-Cookie de la respuesta por si acaba de llegar
	if r.Headers != nil {
		for _, c := range ParseSetCookies(r.Headers) {
			if strings.EqualFold(c.Name, name) {
				return c
			}
		}
	}
	return nil
}

// parseSingleSetCookie delega en ParseSetCookie de Cookies.go
func parseSingleSetCookie(sc string) *fhttp.Cookie {
	return ParseSetCookie(sc)
}

func (r *Response) GetCookieValue(name string) string {
	if cookie := r.GetCookie(name); cookie != nil {
		return cookie.Value
	}
	return ""
}

func (r *Response) HasCookie(name string) bool {
	return r.GetCookie(name) != nil
}

func (r *Response) HeadersToString() string {
	var sb strings.Builder
	if r.Headers != nil {
		for k, v := range r.Headers {
			for _, val := range v {
				sb.WriteString(k)
				sb.WriteString(": ")
				sb.WriteString(val)
				sb.WriteString("\n")
			}
		}
	}
	return sb.String()
}

func (r *Response) GetAllCookiesFromSetCookie() []*fhttp.Cookie {
	if r.Headers == nil {
		return nil
	}
	return ParseSetCookies(r.Headers)
}

func (r *Response) CookiesToString() string {
	var sb strings.Builder
	var allCookies []*fhttp.Cookie
	if r.Request != nil {
		allCookies = append(allCookies, r.Request.Cookies()...)
	}
	allCookies = append(allCookies, r.GetAllCookiesFromSetCookie()...)

	seen := make(map[string]bool)
	first := true

	for _, c := range allCookies {
		name := strings.ToLower(c.Name)
		if seen[name] {
			continue
		}
		seen[name] = true

		if !first {
			sb.WriteString("; ")
		}
		sb.WriteString(c.Name)
		sb.WriteString("=")
		sb.WriteString(c.Value)
		first = false
	}
	return sb.String()
}

func (r *Response) SentCookiesToString() string {
	if r.Request != nil {
		cookieHeader := r.Request.Header.Get("Cookie")
		if cookieHeader != "" {
			return cookieHeader
		}
		cookies := r.Request.Cookies()
		if len(cookies) > 0 {
			var sb strings.Builder
			for i, c := range cookies {
				if i > 0 {
					sb.WriteString("; ")
				}
				sb.WriteString(c.Name)
				sb.WriteString("=")
				sb.WriteString(c.Value)
			}
			return sb.String()
		}
	}
	return ""
}

func (r *Response) CookiesToMap() map[string]string {
	cookieMap := make(map[string]string)

	if r.Request != nil {
		for _, c := range r.Request.Cookies() {
			cookieMap[c.Name] = c.Value
		}
	}
	for _, c := range r.GetAllCookiesFromSetCookie() {
		cookieMap[c.Name] = c.Value
	}
	return cookieMap
}

func (r *Response) Debug() string {
	var sb strings.Builder
	sb.Grow(2048)
	sb.WriteString("=== RESPONSE DEBUG INFO ===\n")
	sb.WriteString(fmt.Sprintf("  Status:   %d %s\n", r.StatusCode, http.StatusText(r.StatusCode)))
	sb.WriteString(fmt.Sprintf("  Duration: %v\n", r.Duration))
	size := r.GetSize()
	sb.WriteString(fmt.Sprintf("  Size:     %d bytes\n", size))

	if ct := r.GetContentType(); ct != "" {
		sb.WriteString(fmt.Sprintf("  Content-Type: %s\n", ct))
	}
	if server := r.GetServer(); server != "" {
		sb.WriteString(fmt.Sprintf("  Server:   %s\n", server))
	}
	if r.Request != nil && r.Request.URL != nil {
		sb.WriteString(fmt.Sprintf("  Request URL:    %s\n", r.Request.URL.String()))
		sb.WriteString(fmt.Sprintf("  Request Method: %s\n", r.Request.Method))
	}
	sb.WriteString("=== HEADERS ===\n")
	sb.WriteString(r.HeadersToString())

	cookiesStr := r.CookiesToString()
	if cookiesStr != "" {
		sb.WriteString("=== COOKIES ===\n")
		sb.WriteString(cookiesStr)
		sb.WriteString("\n")
	}
	return sb.String()
}

// FromRaw construye un Response a partir de una *fhttp.Response cruda,
// aplicando el límite de body configurado (maxBodyBytes) para evitar
// tragar páginas HTML de error gigantes de proxies residenciales.
func FromRaw(raw *fhttp.Response, req *fhttp.Request, start time.Time, maxBody int64) (*Response, error) {
	if raw == nil {
		return &Response{
			Err:      ErrNilResponse,
			Request:  req,
			Duration: time.Since(start),
			Complete: false,
		}, ErrNilResponse
	}
	defer raw.Body.Close()

	if maxBody <= 0 {
		maxBody = 4 * 1024 * 1024
	}

	resp := &Response{
		StatusCode:    raw.StatusCode,
		Headers:       raw.Header.Clone(),
		Request:       req,
		Duration:      time.Since(start),
		ContentLength: raw.ContentLength,
	}

	buf := GetBuffer()

	// maxBody+1 detecta truncamiento sin leer el body entero a RAM si es enorme
	limited := io.LimitReader(raw.Body, maxBody+1)
	n, err := buf.ReadFrom(limited)
	if err != nil {
		resp.Err = err
		resp.Complete = false
		drainBodyIfNeeded(raw, 0)

		// Guardamos el buffer incompleto en poolBuf para que el caller pueda liberarlo
		resp.poolBuf = buf
		resp.BodyBytes = buf.Bytes()
		return resp, err
	}

	resp.poolBuf = buf

	if n > maxBody {
		resp.Truncated = true
		resp.Complete = false
		rawBytes := buf.Bytes()
		if int64(len(rawBytes)) > maxBody {
			rawBytes = rawBytes[:maxBody]
		}
		resp.BodyBytes = rawBytes

		// Drenado inteligente: salva Keep-Alive en HTTP/1.1 si el sobrante es <= 256KB;
		// en HTTP/2 no drena nada (RST_STREAM instantáneo).
		drainBodyIfNeeded(raw, raw.ContentLength-maxBody)

		err = fmt.Errorf("response: %w (%d bytes)", ErrBodyExceedsLimit, maxBody)
		resp.Err = err
		return resp, err
	}

	resp.BodyBytes = buf.Bytes()
	resp.Complete = true
	return resp, nil
}

// drainBodyIfNeeded aplica una estrategia de drenado inteligente:
// 1. En HTTP/2 (raw.ProtoMajor >= 2), NO drenar. raw.Body.Close() envía un frame
//    RST_STREAM que cancela el stream sin descartar la conexión multiplexada subyacente.
// 2. En HTTP/1.1, solo intenta drenar hasta 256KB adicionales para preservar Keep-Alive.
//    Si la respuesta remota es masiva (ej. 50MB) o desconocida, se detiene tras 256KB
//    y cierra la conexión TCP, evitando tragar megabytes de datos innecesarios.
func drainBodyIfNeeded(raw *fhttp.Response, remainingBytes int64) {
	if raw == nil || raw.Body == nil {
		return
	}
	if raw.ProtoMajor >= 2 {
		return
	}

	const maxDrainWindow = 256 * 1024
	if remainingBytes > maxDrainWindow {
		return
	}

	limitedDrain := io.LimitReader(raw.Body, maxDrainWindow)
	_, _ = io.Copy(io.Discard, limitedDrain)
}
