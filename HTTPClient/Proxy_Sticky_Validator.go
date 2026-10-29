package HTTPClient

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/fatih/color"
	"github.com/google/uuid"
	"golang.org/x/net/proxy"
)

// StickyStatus representa el resultado de la validación del soporte sticky de una proxy.
type StickyStatus string

const (
	// StickyOK: Misma IP con la misma session y distinta IP con otra session.
	StickyOK StickyStatus = "StickyOK"
	// StickyIgnored: Acepta el formato -session-<uuid> pero la IP cambia siempre (no hay sticky).
	StickyIgnored StickyStatus = "StickyIgnored"
	// StickyRejected: Error de autenticación o conexión (407, 400, EOF, etc.) con el sufijo.
	StickyRejected StickyStatus = "StickyRejected"
	// Unreachable: La proxy no responde (timeout, DNS o conexión rechazada tanto con como sin sufijo).
	Unreachable StickyStatus = "Unreachable"
)

// StickyMode representa el modo de operación asignado a la proxy.
type StickyMode string

const (
	StickyModeSticky   StickyMode = "sticky"
	StickyModeRotating StickyMode = "rotating"
	StickyModeDisabled StickyMode = "disabled"
)

// StickyResult contiene el diagnóstico completo de la validación de una proxy.
type StickyResult struct {
	Entry           Entry        `json:"entry"`
	Raw             string       `json:"raw"`
	Status          StickyStatus `json:"status"`
	Mode            StickyMode   `json:"mode"`
	StickySupported bool         `json:"sticky_supported"`
	ObservedIPs     []string     `json:"observed_ips"`
	ErrorReason     string       `json:"error_reason,omitempty"`
	TestedAt        time.Time    `json:"tested_at"`
}

// StickyValidatorConfig define las opciones para la validación de sticky sessions.
type StickyValidatorConfig struct {
	EndpointURL    string        `json:"endpoint_url"`    // Endpoint de detección de IP (default: https://api.ipify.org)
	RequestTimeout time.Duration `json:"request_timeout"` // Timeout por petición individual (default: 10s)
	MaxRetries     int           `json:"max_retries"`     // Reintentos máximos por petición transitoria (default: 2)
	ProxyType      Type          `json:"proxy_type"`      // Tipo de proxy (http, socks5, etc.)
	Verbose        bool          `json:"verbose"`
}

// DefaultStickyValidatorConfig devuelve la configuración por defecto recomendada.
func DefaultStickyValidatorConfig() StickyValidatorConfig {
	return StickyValidatorConfig{
		EndpointURL:    "https://api.ipify.org",
		RequestTimeout: 10 * time.Second,
		MaxRetries:     2,
		ProxyType:      TypeHTTP,
		Verbose:        false,
	}
}

// StickyPoolConfig define las opciones para la validación concurrente de proxies.
type StickyPoolConfig struct {
	Workers      int                   `json:"workers"` // Número de workers en paralelo (default: 10)
	ValidatorCfg StickyValidatorConfig `json:"validator_cfg"`
}

// ValidateStickySupport ejecuta el diagnóstico para determinar si una Entry soporta sticky sessions.
// Reutiliza FormatProxyUserWithSession para armar el usuario sticky.
func ValidateStickySupport(ctx context.Context, entry Entry, customCfg ...StickyValidatorConfig) (StickyResult, error) {
	cfg := DefaultStickyValidatorConfig()
	if len(customCfg) > 0 {
		cfg = customCfg[0]
	}
	if cfg.RequestTimeout <= 0 {
		cfg.RequestTimeout = 10 * time.Second
	}
	if cfg.MaxRetries < 0 {
		cfg.MaxRetries = 2
	}
	if cfg.EndpointURL == "" {
		cfg.EndpointURL = "https://api.ipify.org"
	}

	rawString := entry.String(cfg.ProxyType)

	// Helper para reintentar sin sufijo en caso de fallo con sufijo
	handleSuffixFailure := func(causeErr error) (StickyResult, error) {
		plainIP, errPlain := fetchStickyIPWithRetry(ctx, entry, entry.User, cfg)
		if errPlain == nil {
			// Funciona perfectamente sin sufijo -> Rechazó el sufijo sticky
			res := StickyResult{
				Entry:           entry,
				Raw:             rawString,
				Status:          StickyRejected,
				Mode:            StickyModeRotating,
				StickySupported: false,
				ObservedIPs:     []string{plainIP},
				ErrorReason:     fmt.Sprintf("Rechazó sufijo session (%v), pero responde sin sufijo (IP: %s)", causeErr, plainIP),
				TestedAt:        time.Now(),
			}
			LogStickyResult(res)
			return res, nil
		}

		// Falló tanto con sufijo como sin sufijo -> Proxy inaccesible o caída
		res := StickyResult{
			Entry:           entry,
			Raw:             rawString,
			Status:          Unreachable,
			Mode:            StickyModeDisabled,
			StickySupported: false,
			ErrorReason:     fmt.Sprintf("Proxy inaccesible: con sufijo (%v), sin sufijo (%v)", causeErr, errPlain),
			TestedAt:        time.Now(),
		}
		LogStickyResult(res)
		return res, nil
	}

	// 1. Generar UUID y construir usuario sticky para la Session 1
	session1 := uuid.NewString()
	user1 := FormatProxyUserWithSession(entry.User, session1)

	// 2. Hacer 3 requests seguidas con la MISMA session
	var session1IPs []string
	for i := 1; i <= 3; i++ {
		if ctx.Err() != nil {
			return StickyResult{}, ctx.Err()
		}

		ip, err := fetchStickyIPWithRetry(ctx, entry, user1, cfg)
		if err != nil {
			return handleSuffixFailure(fmt.Errorf("petición %d con session %s falló: %w", i, session1, err))
		}
		session1IPs = append(session1IPs, ip)
	}

	// Verificar si las 3 IPs son iguales en Session 1
	ip1, ip2, ip3 := session1IPs[0], session1IPs[1], session1IPs[2]
	if ip1 != ip2 || ip2 != ip3 {
		// La proxy aceptó el sufijo pero rotó la IP en cada petición -> StickyIgnored
		res := StickyResult{
			Entry:           entry,
			Raw:             rawString,
			Status:          StickyIgnored,
			Mode:            StickyModeRotating,
			StickySupported: false,
			ObservedIPs:     session1IPs,
			ErrorReason:     fmt.Sprintf("IP cambió con la misma sesión (%s, %s, %s)", ip1, ip2, ip3),
			TestedAt:        time.Now(),
		}
		LogStickyResult(res)
		return res, nil
	}

	// 3. Hacer 1 request con OTRA session nueva
	session2 := uuid.NewString()
	user2 := FormatProxyUserWithSession(entry.User, session2)

	ipNew, err := fetchStickyIPWithRetry(ctx, entry, user2, cfg)
	if err != nil {
		return handleSuffixFailure(fmt.Errorf("petición con nueva session %s falló: %w", session2, err))
	}

	allIPs := append(session1IPs, ipNew)

	// Si la IP con la nueva session es distinta, soporta sticky y rota correctamente
	if ipNew != ip1 {
		res := StickyResult{
			Entry:           entry,
			Raw:             rawString,
			Status:          StickyOK,
			Mode:            StickyModeSticky,
			StickySupported: true,
			ObservedIPs:     allIPs,
			TestedAt:        time.Now(),
		}
		LogStickyResult(res)
		return res, nil
	}

	// La IP no rotó al cambiar de sesión
	res := StickyResult{
		Entry:           entry,
		Raw:             rawString,
		Status:          StickyIgnored,
		Mode:            StickyModeRotating,
		StickySupported: false,
		ObservedIPs:     allIPs,
		ErrorReason:     fmt.Sprintf("La IP no cambió con nueva sesión (%s == %s)", ip1, ipNew),
		TestedAt:        time.Now(),
	}
	LogStickyResult(res)
	return res, nil
}

// ValidateStickySupportRaw valida una proxy a partir de su string crudo (ej: host:port:user:pass o URL).
func ValidateStickySupportRaw(ctx context.Context, rawProxy string, customCfg ...StickyValidatorConfig) (StickyResult, error) {
	entry, ok := parseEntry(rawProxy)
	if !ok {
		res := StickyResult{
			Raw:         rawProxy,
			Status:      Unreachable,
			Mode:        StickyModeDisabled,
			ErrorReason: fmt.Sprintf("no se pudo parsear el proxy: %s", rawProxy),
			TestedAt:    time.Now(),
		}
		return res, fmt.Errorf("error parseando proxy %s", rawProxy)
	}
	return ValidateStickySupport(ctx, entry, customCfg...)
}

// ValidateStickyPool valida concurrentemente un conjunto de Entries usando un worker pool seguro sin data races.
func ValidateStickyPool(ctx context.Context, entries []Entry, customPoolCfg ...StickyPoolConfig) []StickyResult {
	if len(entries) == 0 {
		return nil
	}

	poolCfg := StickyPoolConfig{
		Workers:      10,
		ValidatorCfg: DefaultStickyValidatorConfig(),
	}
	if len(customPoolCfg) > 0 {
		poolCfg = customPoolCfg[0]
	}
	if poolCfg.Workers <= 0 {
		poolCfg.Workers = 10
	}
	if poolCfg.Workers > len(entries) {
		poolCfg.Workers = len(entries)
	}

	type job struct {
		idx   int
		entry Entry
	}

	jobs := make(chan job, len(entries))
	for i, e := range entries {
		jobs <- job{idx: i, entry: e}
	}
	close(jobs)

	results := make([]StickyResult, len(entries))
	var wg sync.WaitGroup

	for w := 0; w < poolCfg.Workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				select {
				case <-ctx.Done():
					results[j.idx] = StickyResult{
						Entry:       j.entry,
						Raw:         j.entry.String(poolCfg.ValidatorCfg.ProxyType),
						Status:      Unreachable,
						Mode:        StickyModeDisabled,
						ErrorReason: "contexto cancelado",
						TestedAt:    time.Now(),
					}
					continue
				default:
				}

				res, _ := ValidateStickySupport(ctx, j.entry, poolCfg.ValidatorCfg)
				results[j.idx] = res
			}
		}()
	}

	wg.Wait()
	return results
}

// ValidateStickyPoolRaw valida concurrentemente proxies en formato string.
func ValidateStickyPoolRaw(ctx context.Context, rawProxies []string, customPoolCfg ...StickyPoolConfig) []StickyResult {
	var entries []Entry
	for _, raw := range rawProxies {
		trimmed := strings.TrimSpace(raw)
		if trimmed == "" {
			continue
		}
		if e, ok := parseEntry(trimmed); ok {
			entries = append(entries, e)
		} else {
			entries = append(entries, Entry{Host: trimmed})
		}
	}
	return ValidateStickyPool(ctx, entries, customPoolCfg...)
}

func fetchStickyIPWithRetry(ctx context.Context, entry Entry, userOverride string, cfg StickyValidatorConfig) (string, error) {
	var lastErr error
	for attempt := 0; attempt <= cfg.MaxRetries; attempt++ {
		if ctx.Err() != nil {
			return "", ctx.Err()
		}

		ip, err := fetchStickyIP(ctx, entry, userOverride, cfg)
		if err == nil {
			return ip, nil
		}
		lastErr = err

		errStr := strings.ToLower(err.Error())
		if strings.Contains(errStr, "407") || strings.Contains(errStr, "400") {
			return "", lastErr
		}

		if attempt < cfg.MaxRetries {
			select {
			case <-ctx.Done():
				return "", ctx.Err()
			case <-time.After(100 * time.Millisecond):
			}
		}
	}
	return "", lastErr
}

func fetchStickyIP(ctx context.Context, entry Entry, userOverride string, cfg StickyValidatorConfig) (string, error) {
	reqCtx, cancel := context.WithTimeout(ctx, cfg.RequestTimeout)
	defer cancel()

	testEntry := entry
	if userOverride != "" {
		testEntry.User = userOverride
	}

	proxyURL, err := testEntry.URL(cfg.ProxyType)
	if err != nil {
		return "", fmt.Errorf("generando URL de proxy: %w", err)
	}

	transport := &http.Transport{
		Proxy: http.ProxyURL(proxyURL),
		DialContext: (&net.Dialer{
			Timeout:   cfg.RequestTimeout,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		DisableKeepAlives:     true,
		TLSHandshakeTimeout:   cfg.RequestTimeout,
		ResponseHeaderTimeout: cfg.RequestTimeout,
	}

	if cfg.ProxyType == TypeSOCKS5 || strings.HasPrefix(string(cfg.ProxyType), "socks") {
		var auth *proxy.Auth
		if testEntry.User != "" || testEntry.Pass != "" {
			auth = &proxy.Auth{
				User:     testEntry.User,
				Password: testEntry.Pass,
			}
		}
		dialer, dialErr := proxy.SOCKS5("tcp", net.JoinHostPort(testEntry.Host, testEntry.Port), auth, proxy.Direct)
		if dialErr != nil {
			return "", fmt.Errorf("creando dialer socks5: %w", dialErr)
		}
		transport.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
			return dialer.Dial(network, addr)
		}
		transport.Proxy = nil
	}

	client := &http.Client{
		Transport: transport,
		Timeout:   cfg.RequestTimeout,
	}
	defer transport.CloseIdleConnections()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, cfg.EndpointURL, nil)
	if err != nil {
		return "", fmt.Errorf("creando request: %w", err)
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")

	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusProxyAuthRequired {
		return "", fmt.Errorf("407 Proxy Authentication Required")
	}
	if resp.StatusCode == http.StatusBadRequest {
		return "", fmt.Errorf("400 Bad Request (proxy rejected session parameter)")
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("status code %d inesperado", resp.StatusCode)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if err != nil {
		return "", fmt.Errorf("leyendo respuesta IP: %w", err)
	}

	ip := strings.TrimSpace(string(body))
	if ip == "" {
		return "", fmt.Errorf("respuesta vacía del endpoint de IP")
	}

	return ip, nil
}

// LogStickyResult formatea el resultado en consola con colores claros.
func LogStickyResult(res StickyResult) {
	key := res.Entry.key()
	if key == ":" || key == "" {
		key = res.Raw
	}

	cyan := color.New(color.FgCyan).SprintFunc()
	green := color.New(color.FgGreen, color.Bold).SprintFunc()
	yellow := color.New(color.FgYellow, color.Bold).SprintFunc()
	red := color.New(color.FgRed, color.Bold).SprintFunc()

	switch res.Status {
	case StickyOK:
		fmt.Printf(" %s [%s] %s | Modo: %s | IPs: %s\n",
			cyan("[STICKY-CHECK]"),
			green(string(res.Status)),
			key,
			green(string(res.Mode)),
			strings.Join(res.ObservedIPs, " -> "))
	case StickyIgnored:
		fmt.Printf(" %s [%s] %s | Modo: %s | IPs: %s | Causa: %s\n",
			cyan("[STICKY-CHECK]"),
			yellow(string(res.Status)),
			key,
			yellow(string(res.Mode)),
			strings.Join(res.ObservedIPs, ", "),
			res.ErrorReason)
	case StickyRejected:
		fmt.Printf(" %s [%s] %s | Modo: %s | Causa: %s\n",
			cyan("[STICKY-CHECK]"),
			red(string(res.Status)),
			key,
			yellow(string(res.Mode)),
			res.ErrorReason)
	case Unreachable:
		fmt.Printf(" %s [%s] %s | Modo: %s | Causa: %s\n",
			cyan("[STICKY-CHECK]"),
			red(string(res.Status)),
			key,
			red(string(res.Mode)),
			res.ErrorReason)
	}
}

// StickyValidationCache gestiona la caché con TTL para evitar re-validaciones.
type StickyValidationCache struct {
	mu       sync.RWMutex
	filePath string
	ttl      time.Duration
	entries  map[string]StickyResult
}

// NewStickyValidationCache crea o inicializa una instancia de caché persistente.
func NewStickyValidationCache(filePath string, ttl time.Duration) *StickyValidationCache {
	if ttl <= 0 {
		ttl = 1 * time.Hour
	}
	cache := &StickyValidationCache{
		filePath: filePath,
		ttl:      ttl,
		entries:  make(map[string]StickyResult),
	}
	if filePath != "" {
		_ = cache.Load()
	}
	return cache
}

func (c *StickyValidationCache) Get(key string) (StickyResult, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	res, found := c.entries[key]
	if !found || time.Since(res.TestedAt) > c.ttl {
		return StickyResult{}, false
	}
	return res, true
}

func (c *StickyValidationCache) Set(res StickyResult) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if res.TestedAt.IsZero() {
		res.TestedAt = time.Now()
	}
	if res.Raw != "" {
		c.entries[res.Raw] = res
	}
	if key := res.Entry.key(); key != "" && key != ":" {
		c.entries[key] = res
	}
}

func (c *StickyValidationCache) Load() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.filePath == "" {
		return nil
	}
	data, err := os.ReadFile(c.filePath)
	if err != nil {
		return err
	}
	var loaded map[string]StickyResult
	if err := json.Unmarshal(data, &loaded); err != nil {
		return err
	}
	for k, v := range loaded {
		c.entries[k] = v
	}
	return nil
}

func (c *StickyValidationCache) Save() error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.filePath == "" {
		return nil
	}
	dir := filepath.Dir(c.filePath)
	if dir != "" && dir != "." {
		_ = os.MkdirAll(dir, 0755)
	}
	data, err := json.MarshalIndent(c.entries, "", "  ")
	if err != nil {
		return err
	}
	tmp := c.filePath + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return err
	}
	return os.Rename(tmp, c.filePath)
}

// ValidateWithCache valida un slice de proxies consultando primero la caché.
func (c *StickyValidationCache) ValidateWithCache(ctx context.Context, rawProxies []string, poolCfg ...StickyPoolConfig) []StickyResult {
	results := make([]StickyResult, len(rawProxies))
	var toValidate []string
	var toValidateIndices []int

	for i, raw := range rawProxies {
		trimmed := strings.TrimSpace(raw)
		if cached, ok := c.Get(trimmed); ok {
			results[i] = cached
			continue
		}
		toValidate = append(toValidate, trimmed)
		toValidateIndices = append(toValidateIndices, i)
	}

	if len(toValidate) > 0 {
		validated := ValidateStickyPoolRaw(ctx, toValidate, poolCfg...)
		for j, res := range validated {
			idx := toValidateIndices[j]
			results[idx] = res
			c.Set(res)
		}
		_ = c.Save()
	}

	return results
}

// StickyProxyConfigEntry representa el formato de persistencia individual para cada proxy.
type StickyProxyConfigEntry struct {
	Raw             string       `json:"raw"`
	Host            string       `json:"host"`
	Port            string       `json:"port"`
	User            string       `json:"user"`
	StickySupported bool         `json:"sticky_supported"`
	Mode            StickyMode   `json:"mode"`
	Status          StickyStatus `json:"status"`
	ObservedIPs     []string     `json:"observed_ips,omitempty"`
	LastError       string       `json:"last_error,omitempty"`
	LastChecked     time.Time    `json:"last_checked"`
}

// StickyConfigFile es el formato del archivo de configuración global de proxies.
type StickyConfigFile struct {
	UpdatedAt     time.Time                         `json:"updated_at"`
	TotalProxies  int                               `json:"total_proxies"`
	StickyCount   int                               `json:"sticky_count"`
	RotatingCount int                               `json:"rotating_count"`
	DisabledCount int                               `json:"disabled_count"`
	Proxies       map[string]StickyProxyConfigEntry `json:"proxies"`
}

// SaveStickyConfigFile guarda la configuración completa de proxies validadas.
func SaveStickyConfigFile(filePath string, results []StickyResult) error {
	if filePath == "" {
		return nil
	}
	dir := filepath.Dir(filePath)
	if dir != "" && dir != "." {
		_ = os.MkdirAll(dir, 0755)
	}

	cfg := StickyConfigFile{
		UpdatedAt: time.Now(),
		Proxies:   make(map[string]StickyProxyConfigEntry),
	}

	for _, res := range results {
		key := res.Raw
		if key == "" {
			key = res.Entry.key()
		}
		entry := StickyProxyConfigEntry{
			Raw:             res.Raw,
			Host:            res.Entry.Host,
			Port:            res.Entry.Port,
			User:            res.Entry.User,
			StickySupported: res.StickySupported,
			Mode:            res.Mode,
			Status:          res.Status,
			ObservedIPs:     res.ObservedIPs,
			LastError:       res.ErrorReason,
			LastChecked:     res.TestedAt,
		}
		cfg.Proxies[key] = entry
		cfg.TotalProxies++

		switch res.Mode {
		case StickyModeSticky:
			cfg.StickyCount++
		case StickyModeRotating:
			cfg.RotatingCount++
		case StickyModeDisabled:
			cfg.DisabledCount++
		}
	}

	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	tmp := filePath + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return err
	}
	return os.Rename(tmp, filePath)
}

// PrepareProxiesForSession es la función orquestadora principal para cualquier proyecto que use Universal.
// 1. Diagnostica automáticamente las proxies con soporte Sticky Session en memoria.
// 2. Filtra proxies caídas (Unreachable).
// 3. Determina si todas las proxies activas soportan Sticky Session (StickyOK).
// 4. Mantiene la validación 100% en memoria sin escribir archivos .json en disco por defecto.
func PrepareProxiesForSession(ctx context.Context, rawProxies []string, proxyType string, customCachePath ...string) (activeProxies []string, useSticky bool) {
	if len(rawProxies) == 0 {
		return rawProxies, false
	}

	var cacheFile string
	if len(customCachePath) > 0 && customCachePath[0] != "" {
		cacheFile = customCachePath[0]
	}

	fmt.Printf("\n %s Iniciando verificación de soporte Sticky Session para %d proxies...\n",
		color.CyanString("[HTTPClient.Sticky]"), len(rawProxies))

	cache := NewStickyValidationCache(cacheFile, 1*time.Hour)
	pType := TypeHTTP
	if strings.HasPrefix(strings.ToLower(proxyType), "socks") {
		pType = TypeSOCKS5
	}

	poolCfg := StickyPoolConfig{
		Workers: 10,
		ValidatorCfg: StickyValidatorConfig{
			EndpointURL:    "https://api.ipify.org",
			RequestTimeout: 10 * time.Second,
			MaxRetries:     2,
			ProxyType:      pType,
		},
	}

	results := cache.ValidateWithCache(ctx, rawProxies, poolCfg)
	if cacheFile != "" {
		configFile := filepath.Join(filepath.Dir(cacheFile), "proxy_config.json")
		_ = SaveStickyConfigFile(configFile, results)
	}

	var stickyCount, rotatingCount, disabledCount int
	var healthy []string

	for _, res := range results {
		raw := res.Raw
		if raw == "" {
			raw = res.Entry.String(pType)
		}
		if res.Mode == StickyModeDisabled {
			disabledCount++
			continue
		}
		healthy = append(healthy, raw)
		if res.Mode == StickyModeSticky {
			stickyCount++
		} else {
			rotatingCount++
		}
	}

	if len(healthy) == 0 {
		fmt.Printf(" %s %s Todas las proxies dieron timeout/error. Usando lista original en modo rotativo.\n",
			color.RedString("[AVISO]"), color.YellowString("Sin proxies válidas."))
		return rawProxies, false
	}

	useSticky = (stickyCount > 0 && rotatingCount == 0)

	fmt.Printf(" %s Diagnóstico: %s StickyOK | %s Rotativo/No-Sticky | %s Inaccesibles\n",
		color.CyanString("[HTTPClient.Sticky]"),
		color.GreenString("%d", stickyCount),
		color.YellowString("%d", rotatingCount),
		color.RedString("%d", disabledCount))

	if useSticky {
		fmt.Printf(" %s %s Todas las proxies verificadas soportan sesión fija.\n\n",
			color.CyanString("[HTTPClient.Sticky]"),
			color.GreenString("✔ Modo Sticky habilitado:"))
	} else {
		fmt.Printf(" %s %s Usando modo rotativo estándar (sin sufijo de sesión).\n\n",
			color.CyanString("[HTTPClient.Sticky]"),
			color.YellowString("ℹ Modo normal:"))
	}

	return healthy, useSticky
}
