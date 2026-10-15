package GeoRoute

import (
	"errors"
	"strings"
	"sync"

	"golang.org/x/net/publicsuffix"
)

// CountryCode representa un código de país ISO 3166-1 alpha-2 en MAYÚSCULAS normalizado.
type CountryCode string

const (
	// Unknown representa un dominio sin país asignable o ambiguo (ej. .com, .net, .org) -> "XX".
	Unknown CountryCode = "XX"
)

// IsUnknown verifica si el código de país es desconocido o vacío.
func (cc CountryCode) IsUnknown() bool {
	return cc == Unknown || cc == "UNKNOWN" || cc == ""
}

var (
	ErrInvalidDomain  = errors.New("georoute: dominio o email vacío/inválido")
	ErrUnknownCountry = errors.New("georoute: no se pudo determinar el país de forma inequívoca")
)

// CountryDetector analiza TLDs, dominios compuestos y Public Suffixes de forma inequívoca y determinista.
type CountryDetector struct {
	cache        map[string]CountryCode
	cacheMu      sync.RWMutex
	maxCacheSize int

	genericTLDs  map[string]struct{}
	compoundSLDs map[string]CountryCode
	ccTLDs       map[string]CountryCode
	parser       *ULPParser
}

// NewCountryDetector inicializa el detector con los catálogos oficiales y una caché acotada.
func NewCountryDetector(maxCacheSize int) *CountryDetector {
	if maxCacheSize <= 0 {
		maxCacheSize = 50000
	}

	d := &CountryDetector{
		cache:        make(map[string]CountryCode, 1024),
		maxCacheSize: maxCacheSize,
		genericTLDs:  make(map[string]struct{}, 128),
		compoundSLDs: make(map[string]CountryCode, 256),
		ccTLDs:       make(map[string]CountryCode, 256),
		parser:       NewULPParser(),
	}

	d.initCatalogs()
	return d
}

func (d *CountryDetector) initCatalogs() {
	// 1. TLDs Genéricos / Internacionales / Ambiguos -> SIEMPRE UNKNOWN ("XX")
	generics := []string{
		"com", "net", "org", "info", "biz", "gov", "edu", "mil", "int",
		"io", "co", "app", "dev", "ai", "me", "online", "site", "tech",
		"store", "xyz", "top", "vip", "club", "live", "pro", "space",
		"icu", "shop", "work", "world", "today", "agency", "email",
		"cloud", "digital", "group", "link", "media", "news", "one",
		"tv", "cc", "fm", "gg", "to", "ws", "mobi", "asia", "cat", "center",
		"company", "cool", "design", "directory", "download", "events",
		"expert", "fit", "fun", "games", "global", "host", "life", "ltd",
		"network", "page", "plus", "press", "pub", "rocks", "run", "services",
		"solutions", "stream", "systems", "team", "tools", "video", "vip",
		"website", "wiki", "win", "zone",
	}
	for _, g := range generics {
		d.genericTLDs[g] = struct{}{}
	}

	// 2. Second-Level Domains Compuestos (.com.es, .com.ar, .co.uk, etc.)
	compounds := map[string]CountryCode{
		// España
		"com.es": "ES", "nom.es": "ES", "org.es": "ES", "gob.es": "ES", "edu.es": "ES",
		// Perú
		"com.pe": "PE", "net.pe": "PE", "org.pe": "PE", "gob.pe": "PE", "edu.pe": "PE", "mil.pe": "PE",
		// Argentina
		"com.ar": "AR", "net.ar": "AR", "org.ar": "AR", "gob.ar": "AR", "edu.ar": "AR", "int.ar": "AR", "tur.ar": "AR",
		// Brasil
		"com.br": "BR", "net.br": "BR", "org.br": "BR", "gov.br": "BR", "edu.br": "BR", "art.br": "BR", "ind.br": "BR", "esp.br": "BR",
		// Colombia
		"com.co": "CO", "net.co": "CO", "org.co": "CO", "gov.co": "CO", "edu.co": "CO",
		// Chile
		"com.cl": "CL", "net.cl": "CL", "org.cl": "CL", "gob.cl": "CL", "edu.cl": "CL",
		// Ecuador
		"com.ec": "EC", "net.ec": "EC", "org.ec": "EC", "gob.ec": "EC", "edu.ec": "EC",
		// Uruguay
		"com.uy": "UY", "net.uy": "UY", "org.uy": "UY", "gub.uy": "UY", "edu.uy": "UY",
		// Venezuela
		"com.ve": "VE", "net.ve": "VE", "org.ve": "VE", "gob.ve": "VE", "edu.ve": "VE",
		// México
		"com.mx": "MX", "net.mx": "MX", "org.mx": "MX", "gob.mx": "MX", "edu.mx": "MX",
		// Reino Unido
		"co.uk": "GB", "org.uk": "GB", "gov.uk": "GB", "ac.uk": "GB", "me.uk": "GB", "net.uk": "GB",
		// Australia / Nueva Zelanda / Asia / África
		"com.au": "AU", "net.au": "AU", "org.au": "AU", "edu.au": "AU", "gov.au": "AU",
		"co.jp": "JP", "ne.jp": "JP", "or.jp": "JP", "ac.jp": "JP", "go.jp": "JP",
		"co.nz": "NZ", "net.nz": "NZ", "org.nz": "NZ",
		"com.py": "PY", "net.py": "PY", "edu.py": "PY", "gov.py": "PY",
		"com.bo": "BO", "net.bo": "BO", "gob.bo": "BO", "edu.bo": "BO",
		"com.pa": "PA", "net.pa": "PA", "gob.pa": "PA", "edu.pa": "PA",
		"com.do": "DO", "net.do": "DO", "gob.do": "DO", "edu.do": "DO",
		"com.gt": "GT", "net.gt": "GT", "gob.gt": "GT", "edu.gt": "GT",
		"com.cr": "CR", "net.cr": "CR", "go.cr": "CR", "ed.cr": "CR",
		"co.za": "ZA", "net.za": "ZA", "org.za": "ZA", "gov.za": "ZA", "edu.za": "ZA",
		"com.tr": "TR", "edu.tr": "TR", "gov.tr": "TR", "org.tr": "TR",
		"com.ua": "UA", "net.ua": "UA", "edu.ua": "UA", "gov.ua": "UA",
		"com.ru": "RU", "net.ru": "RU", "org.ru": "RU", "edu.ru": "RU", "gov.ru": "RU",
		"com.vn": "VN", "edu.vn": "VN", "gov.vn": "VN",
		"ac.ke": "KE", "co.ke": "KE", "go.ke": "KE",
		"com.ng": "NG", "edu.ng": "NG", "gov.ng": "NG",
		"com.pk": "PK", "gov.pk": "PK", "edu.pk": "PK",
		"com.ph": "PH", "edu.ph": "PH", "gov.ph": "PH",
		"com.my": "MY", "edu.my": "MY", "gov.my": "MY",
		"com.sg": "SG", "edu.sg": "SG", "gov.sg": "SG",
		"co.th": "TH", "ac.th": "TH", "go.th": "TH",
		"co.in": "IN", "net.in": "IN", "org.in": "IN", "ac.in": "IN", "edu.in": "IN", "gov.in": "IN",
		"co.id": "ID", "ac.id": "ID", "go.id": "ID",
	}
	for sld, cc := range compounds {
		d.compoundSLDs[sld] = cc
	}

	// 3. Country-Code Top-Level Domains (ccTLDs simples)
	cctlds := map[string]CountryCode{
		// Latinoamérica y Caribe
		"pe": "PE", "ar": "AR", "br": "BR", "es": "ES", "cl": "CL",
		"co": "CO", "ec": "EC", "uy": "UY", "ve": "VE", "mx": "MX",
		"py": "PY", "bo": "BO", "pa": "PA", "cr": "CR", "gt": "GT",
		"hn": "HN", "sv": "SV", "ni": "NI", "cu": "CU", "do": "DO",
		"pr": "PR",
		// Europa
		"uk": "GB", "de": "DE", "fr": "FR", "it": "IT", "pt": "PT",
		"nl": "NL", "be": "BE", "ch": "CH", "at": "AT", "se": "SE",
		"no": "NO", "fi": "FI", "dk": "DK", "pl": "PL", "cz": "CZ",
		"ro": "RO", "gr": "GR", "ie": "IE", "hu": "HU", "ru": "RU",
		"ua": "UA", "bg": "BG", "hr": "HR", "sk": "SK", "si": "SI",
		"ee": "EE", "lv": "LV", "lt": "LT", "lu": "LU", "is": "IS",
		// Norteamérica / Asia / Oceanía / África
		"us": "US", "ca": "CA", "jp": "JP", "kr": "KR", "cn": "CN",
		"in": "IN", "au": "AU", "nz": "NZ", "za": "ZA", "sg": "SG",
		"my": "MY", "ph": "PH", "id": "ID", "th": "TH", "vn": "VN",
		"tr": "TR", "sa": "SA", "ae": "AE", "il": "IL", "eg": "EG",
		"ma": "MA", "dz": "DZ", "tn": "TN", "ng": "NG", "ke": "KE",
		"gh": "GH", "tw": "TW", "hk": "HK", "pk": "PK", "bd": "BD",
		"kz": "KZ", "uz": "UZ", "az": "AZ", "ge": "GE", "by": "BY",
	}
	for tld, cc := range cctlds {
		d.ccTLDs[tld] = cc
	}
}

// Detect recibe una línea, email, cookie o combo completo (ej: "pais.au gsid=...", "user@domain.com.es:pass" o "movistarplay.cl:user@gmail.com:pass")
// y devuelve el CountryCode determinista.
func (d *CountryDetector) Detect(input string) CountryCode {
	input = strings.TrimSpace(input)
	if input == "" {
		return Unknown
	}

	// 1. Detección explícita de tags/prefijos de país en cookies o líneas (ej: "pais.au", "pais:pe", "[AU]", "country.us", "PE | ...")
	if tagCC := d.detectExplicitTag(input); !tagCC.IsUnknown() {
		return tagCC
	}

	// 2. Si contiene al menos 2 colones, parsear como posible ULP
	if strings.Count(input, ":") >= 2 {
		rec, err := d.parser.ParseULPLine(input)
		if err == nil && rec.Service != "" {
			d.DetectULP(&rec, PolicyPreferService)
			if !rec.Country.IsUnknown() {
				return rec.Country
			}
		}
	}

	// 3. Si es combo "email:pass"
	rawInput := input
	if idx := strings.IndexByte(input, ':'); idx > 0 {
		input = input[:idx]
	}

	// 4. Extraer parte del dominio luego del '@'
	atIdx := strings.LastIndexByte(input, '@')
	var domain string
	if atIdx >= 0 {
		domain = input[atIdx+1:]
	} else {
		// Si no tiene '@', evaluar primer token
		fields := strings.Fields(rawInput)
		if len(fields) > 0 {
			domain = fields[0]
		} else {
			domain = input
		}
	}

	return d.DetectDomain(domain)
}

func (d *CountryDetector) detectExplicitTag(input string) CountryCode {
	trimmed := strings.TrimSpace(input)
	lower := strings.ToLower(trimmed)

	// Patrón 0: Búsqueda en campos estructurados tipo clave-valor (ej: "| Country = AR |", "Country: AR", "Pais = AR", "country=[AR]")
	tagLabels := []string{"country", "pais", "geo", "region", "location"}
	for _, label := range tagLabels {
		idx := strings.Index(lower, label)
		for idx != -1 {
			isWordStart := idx == 0 || lower[idx-1] == ' ' || lower[idx-1] == '|' || lower[idx-1] == ';' || lower[idx-1] == '[' || lower[idx-1] == '{' || lower[idx-1] == '('
			if isWordStart {
				afterLabel := lower[idx+len(label):]
				afterLabelTrimmed := strings.TrimLeft(afterLabel, " \t")
				if len(afterLabelTrimmed) > 0 && (afterLabelTrimmed[0] == '=' || afterLabelTrimmed[0] == ':' || afterLabelTrimmed[0] == '.') {
					val := strings.TrimLeft(afterLabelTrimmed[1:], " \t[](){}<>\"'")
					if len(val) >= 2 {
						c0 := val[0]
						c1 := val[1]
						if c0 >= 'a' && c0 <= 'z' && c1 >= 'a' && c1 <= 'z' {
							if len(val) == 2 || val[2] == ' ' || val[2] == '|' || val[2] == ';' || val[2] == ']' || val[2] == '}' || val[2] == ')' || val[2] == '"' || val[2] == '\'' || val[2] == '\t' || val[2] == '\n' || val[2] == '\r' {
								code := val[:2]
								if cc, ok := d.ccTLDs[code]; ok {
									return cc
								}
								return CountryCode(strings.ToUpper(code))
							}
						}
					}
				}
			}
			next := strings.Index(lower[idx+len(label):], label)
			if next == -1 {
				break
			}
			idx += len(label) + next
		}
	}

	// Patrón 1: "pais.au", "pais:au", "pais=au", "pais_au", "country.au", "geo.au", etc.
	prefixes := []string{
		"pais.", "pais:", "pais=", "pais_", "pais ",
		"country.", "country:", "country=", "country_", "country ",
		"geo.", "geo:", "geo=", "geo_", "geo ",
		"region.", "region:", "region=", "region_", "region ",
	}
	for _, p := range prefixes {
		if strings.HasPrefix(lower, p) {
			rest := strings.TrimSpace(lower[len(p):])
			var code []byte
			for i := 0; i < len(rest); i++ {
				if rest[i] >= 'a' && rest[i] <= 'z' {
					code = append(code, rest[i])
				} else {
					break
				}
			}
			if len(code) == 2 {
				cStr := string(code)
				if cc, ok := d.ccTLDs[cStr]; ok {
					return cc
				}
				return CountryCode(strings.ToUpper(cStr))
			}
		}
	}

	// Patrón 2: "[AU]", "(AU)", "{AU}", "<AU>" al inicio
	if len(lower) >= 4 {
		if (lower[0] == '[' && lower[3] == ']') ||
			(lower[0] == '(' && lower[3] == ')') ||
			(lower[0] == '{' && lower[3] == '}') ||
			(lower[0] == '<' && lower[3] == '>') {
			code := lower[1:3]
			if code[0] >= 'a' && code[0] <= 'z' && code[1] >= 'a' && code[1] <= 'z' {
				if cc, ok := d.ccTLDs[code]; ok {
					return cc
				}
				return CountryCode(strings.ToUpper(code))
			}
		}
	}

	// Patrón 3: "AU | ...", "AU: ...", "AU - ...", "AU_..." al inicio
	if len(lower) >= 3 {
		code := lower[:2]
		sep := lower[2]
		if code[0] >= 'a' && code[0] <= 'z' && code[1] >= 'a' && code[1] <= 'z' {
			if sep == '|' || sep == ':' || sep == '-' || sep == '_' || sep == ' ' || sep == '\t' {
				if cc, ok := d.ccTLDs[code]; ok {
					return cc
				}
				return CountryCode(strings.ToUpper(code))
			}
		}
	}

	// Patrón 4: Primer token (ej: "pais.au", ".pe", "au")
	firstToken := strings.Fields(lower)
	if len(firstToken) > 0 {
		tok := firstToken[0]
		tok = strings.Trim(tok, "[],(){}<>;:")
		if strings.HasPrefix(tok, "pais.") || strings.HasPrefix(tok, "country.") || strings.HasPrefix(tok, "geo.") {
			parts := strings.Split(tok, ".")
			if len(parts) >= 2 && len(parts[1]) == 2 {
				code := parts[1]
				if cc, ok := d.ccTLDs[code]; ok {
					return cc
				}
				return CountryCode(strings.ToUpper(code))
			}
		}
		if strings.HasPrefix(tok, ".") && len(tok) == 3 {
			code := tok[1:]
			if cc, ok := d.ccTLDs[code]; ok {
				return cc
			}
		}
	}

	return Unknown
}

// DetectDomain analiza un hostname/dominio y devuelve su CountryCode.
func (d *CountryDetector) DetectDomain(domain string) CountryCode {
	domain = strings.TrimSpace(strings.ToLower(domain))
	if domain == "" {
		return Unknown
	}

	// 1. Revisar caché acotada
	d.cacheMu.RLock()
	if cc, ok := d.cache[domain]; ok {
		d.cacheMu.RUnlock()
		return cc
	}
	d.cacheMu.RUnlock()

	// 2. Clasificación determinista
	cc := d.classifyDomain(domain)

	// 3. Guardar en caché acotada
	d.cacheMu.Lock()
	if len(d.cache) >= d.maxCacheSize {
		// Flush O(1) de caché para evitar memory leak o contención compleja
		d.cache = make(map[string]CountryCode, d.maxCacheSize/2)
	}
	d.cache[domain] = cc
	d.cacheMu.Unlock()

	return cc
}

func (d *CountryDetector) classifyDomain(domain string) CountryCode {
	// Quitar posibles puertos o rutas residuales
	if colonIdx := strings.IndexByte(domain, ':'); colonIdx >= 0 {
		domain = domain[:colonIdx]
	}
	if slashIdx := strings.IndexByte(domain, '/'); slashIdx >= 0 {
		domain = domain[:slashIdx]
	}

	parts := strings.Split(domain, ".")
	numParts := len(parts)
	if numParts < 2 {
		return Unknown
	}

	// A. Verificar compuestos de 2 niveles al final (ej: "com.es", "co.uk", "com.ar")
	if numParts >= 2 {
		lastTwo := parts[numParts-2] + "." + parts[numParts-1]
		if cc, ok := d.compoundSLDs[lastTwo]; ok {
			return cc
		}
	}

	// B. Utilizar Public Suffix oficial para sufijos de múltiples niveles (ej: .ac.uk, .gov.pk)
	suffix, _ := publicsuffix.PublicSuffix(domain)
	if suffix != "" && suffix != domain {
		suffixParts := strings.Split(suffix, ".")
		lastSuffixPart := suffixParts[len(suffixParts)-1]
		if _, isGen := d.genericTLDs[lastSuffixPart]; !isGen {
			if cc, isCc := d.ccTLDs[lastSuffixPart]; isCc {
				return cc
			}
		}
	}

	// C. Verificar TLD final
	tld := parts[numParts-1]

	// Si es genérico (.com, .net, .org, etc.), NO adivinar -> UNKNOWN ("XX")
	if _, isGeneric := d.genericTLDs[tld]; isGeneric {
		return Unknown
	}

	// Si es un ccTLD inequívoco (.es, .pe, .br, etc.)
	if cc, isCcTLD := d.ccTLDs[tld]; isCcTLD {
		return cc
	}

	// Caso no reconocido o ambiguo -> UNKNOWN ("XX")
	return Unknown
}

// DetectService analiza el hostname del servicio y determina el país, fuente y confianza.
func (d *CountryDetector) DetectService(service string) (CountryCode, CountrySource, ConfidenceLevel) {
	host := ExtractHost(service)
	if host == "" {
		return Unknown, CountrySourceUnknown, ConfidenceUnknown
	}

	cc := d.DetectDomain(host)
	if !cc.IsUnknown() {
		return cc, CountrySourceServiceDomain, ConfidenceHigh
	}
	return Unknown, CountrySourceUnknown, ConfidenceUnknown
}

// DetectEmail analiza el dominio del correo y determina el país, fuente y confianza.
func (d *CountryDetector) DetectEmail(email string) (CountryCode, CountrySource, ConfidenceLevel) {
	domain := ExtractEmailDomain(email)
	if domain == "" {
		return Unknown, CountrySourceUnknown, ConfidenceUnknown
	}

	cc := d.DetectDomain(domain)
	if !cc.IsUnknown() {
		return cc, CountrySourceEmailDomain, ConfidenceMedium
	}
	return Unknown, CountrySourceUnknown, ConfidenceUnknown
}

// ResolveConflict resuelve discrepancias entre país de servicio y país de correo según la política indicada.
func (d *CountryDetector) ResolveConflict(svcCountry, emailCountry CountryCode, policy ConflictPolicy) (CountryCode, *CountryConflict, ConfidenceLevel) {
	if svcCountry == emailCountry {
		return svcCountry, nil, ConfidenceHigh
	}

	conflict := &CountryConflict{
		ServiceCountry: svcCountry,
		EmailCountry:   emailCountry,
		PolicyUsed:     policy,
	}

	switch policy {
	case PolicyPreferEmail:
		if !emailCountry.IsUnknown() {
			conflict.ResolvedAs = emailCountry
			return emailCountry, conflict, ConfidenceMedium
		}
		conflict.ResolvedAs = svcCountry
		return svcCountry, conflict, ConfidenceHigh

	case PolicyPreferExplicit:
		// En ausencia de metadato explícito adicional, priorizar servicio
		conflict.ResolvedAs = svcCountry
		return svcCountry, conflict, ConfidenceHigh

	case PolicyMarkUnknown:
		conflict.ResolvedAs = Unknown
		return Unknown, conflict, ConfidenceUnknown

	case PolicyManualReview:
		conflict.ResolvedAs = Unknown
		return Unknown, conflict, ConfidenceLow

	case PolicyPreferService:
		fallthrough
	default:
		if !svcCountry.IsUnknown() {
			conflict.ResolvedAs = svcCountry
			return svcCountry, conflict, ConfidenceHigh
		}
		conflict.ResolvedAs = emailCountry
		return emailCountry, conflict, ConfidenceMedium
	}
}

// DetectULP analiza un ULPRecord, ejecuta la detección multifuente, gestiona conflictos y actualiza el registro in-place.
func (d *CountryDetector) DetectULP(rec *ULPRecord, policy ConflictPolicy) *ULPRecord {
	if rec == nil {
		return nil
	}

	svcCountry, _, _ := d.DetectService(rec.Service)
	emailCountry, _, _ := d.DetectEmail(rec.Email)

	if !svcCountry.IsUnknown() && !emailCountry.IsUnknown() {
		if svcCountry == emailCountry {
			rec.Country = svcCountry
			rec.Source = CountrySourceServiceDomain
			rec.Confidence = ConfidenceHigh
			rec.Conflict = nil
		} else {
			resolved, conflict, confLevel := d.ResolveConflict(svcCountry, emailCountry, policy)
			rec.Country = resolved
			rec.Conflict = conflict
			rec.Confidence = confLevel
			if resolved == svcCountry {
				rec.Source = CountrySourceServiceDomain
			} else if resolved == emailCountry {
				rec.Source = CountrySourceEmailDomain
			} else {
				rec.Source = CountrySourceUnknown
			}
		}
	} else if !svcCountry.IsUnknown() {
		rec.Country = svcCountry
		rec.Source = CountrySourceServiceDomain
		rec.Confidence = ConfidenceHigh
		rec.Conflict = nil
	} else if !emailCountry.IsUnknown() {
		rec.Country = emailCountry
		rec.Source = CountrySourceEmailDomain
		rec.Confidence = ConfidenceMedium
		rec.Conflict = nil
	} else {
		rec.Country = Unknown
		rec.Source = CountrySourceUnknown
		rec.Confidence = ConfidenceUnknown
		rec.Conflict = nil
	}

	return rec
}

// DetectNormalized genera un NormalizedRecord listo para consumo de GeoRouter.
func (d *CountryDetector) DetectNormalized(rawLine string, format InputFormat, policy ConflictPolicy) (NormalizedRecord, error) {
	if format == FormatCombo {
		trimmed := strings.TrimSpace(rawLine)
		if trimmed == "" {
			return NormalizedRecord{Raw: rawLine, Country: Unknown, Source: CountrySourceUnknown, Confidence: ConfidenceUnknown}, ErrEmptyLine
		}
		email := trimmed
		if idx := strings.IndexByte(trimmed, ':'); idx > 0 {
			email = trimmed[:idx]
		}
		cc, src, conf := d.DetectEmail(email)
		return NormalizedRecord{
			Raw:        rawLine,
			Email:      email,
			Country:    cc,
			Source:     src,
			Confidence: conf,
		}, nil
	}

	// Formato ULP o Auto
	ulp, err := d.parser.ParseULPLine(rawLine)
	if err != nil {
		return NormalizedRecord{
			Raw:        rawLine,
			Country:    Unknown,
			Source:     CountrySourceUnknown,
			Confidence: ConfidenceUnknown,
		}, err
	}

	d.DetectULP(&ulp, policy)

	return NormalizedRecord{
		Raw:        ulp.Raw,
		Service:    ulp.Service,
		Email:      ulp.Email,
		Country:    ulp.Country,
		Source:     ulp.Source,
		Confidence: ulp.Confidence,
		Conflict:   ulp.Conflict,
	}, nil
}

var (
	defaultDetector = NewCountryDetector(50000)
)

// ResolveCountryForAccount resuelve el código de país para una cuenta de correo electrónico o usa el defaultFallback.
func ResolveCountryForAccount(email, defaultFallback string) string {
	if email == "" {
		return strings.ToUpper(defaultFallback)
	}
	cc, _, _ := defaultDetector.DetectEmail(email)
	if !cc.IsUnknown() {
		return string(cc)
	}
	if defaultFallback != "" && defaultFallback != "XX" {
		return strings.ToUpper(defaultFallback)
	}
	return ""
}
