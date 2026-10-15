package GeoRoute

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
)

var (
	ErrInvalidProxy        = errors.New("georoute: formato de proxy inválido")
	ErrInvalidRegion       = errors.New("georoute: región de proxy inválida")
	ErrProxyTransformation = errors.New("georoute: error transformando proxy")
	ErrUnsupportedProvider = errors.New("georoute: proveedor de proxy no soportado")
)

// ParsedProxy representa la estructura canónica y descompuesta de un proxy.
type ParsedProxy struct {
	Scheme       string // "http", "https", "socks5" (o vacío si no se especificó)
	Host         string // ej. "gw.example-proxy.com" o "192.168.1.1"
	Port         string // ej. "823"
	Username     string // Username completo original (ej: "user__cr.us" o "user")
	BaseUsername string // Username limpio sin el tag regional (ej: "user")
	Password     string // Password intacta
	Region       string // Código de región actual detectado en el username (ej: "us", en minúsculas)
	Raw          string // Cadena cruda original
}

// ParseProxy descompone estructuralmente una cadena de proxy en sus componentes exactos.
func ParseProxy(raw string) (ParsedProxy, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ParsedProxy{}, fmt.Errorf("%w: cadena vacía", ErrInvalidProxy)
	}

	p := ParsedProxy{Raw: raw}
	working := raw

	// 1. Detectar si viene con prefijo scheme (ej: "http://", "socks5://")
	if schemeIdx := strings.Index(working, "://"); schemeIdx >= 0 {
		p.Scheme = strings.ToLower(working[:schemeIdx])
		working = working[schemeIdx+3:]
	}

	// 2. Comprobar si tiene formato estándar URL (user:pass@host:port)
	if atIdx := strings.LastIndexByte(working, '@'); atIdx >= 0 {
		userInfo := working[:atIdx]
		hostPort := working[atIdx+1:]

		// Parsear user:pass
		if uParts := strings.SplitN(userInfo, ":", 2); len(uParts) == 2 {
			p.Username, _ = url.QueryUnescape(uParts[0])
			p.Password, _ = url.QueryUnescape(uParts[1])
		} else {
			p.Username, _ = url.QueryUnescape(userInfo)
		}

		// Parsear host:port
		host, port, err := net.SplitHostPort(hostPort)
		if err != nil {
			return ParsedProxy{}, fmt.Errorf("%w: host:port inválido tras @: %v", ErrInvalidProxy, err)
		}
		p.Host = host
		p.Port = port
	} else {
		// 3. Formato clásico de lista (host:port:user:pass o host:port)
		parts := strings.Split(working, ":")
		switch len(parts) {
		case 2:
			p.Host = parts[0]
			p.Port = parts[1]
		case 4:
			p.Host = parts[0]
			p.Port = parts[1]
			p.Username = parts[2]
			p.Password = parts[3]
		default:
			return ParsedProxy{}, fmt.Errorf("%w: cantidad inesperada de campos separados por ':' (%d)", ErrInvalidProxy, len(parts))
		}
	}

	// 4. Analizar username para extraer región preexistente y base username
	p.extractRegionFromUsername()

	// 5. Validar integridad básica
	if err := p.Validate(); err != nil {
		return ParsedProxy{}, err
	}

	return p, nil
}

var isoCountrySet = map[string]struct{}{
	"AD": {}, "AE": {}, "AF": {}, "AG": {}, "AI": {}, "AL": {}, "AM": {}, "AO": {}, "AQ": {}, "AR": {},
	"AS": {}, "AT": {}, "AU": {}, "AW": {}, "AX": {}, "AZ": {}, "BA": {}, "BB": {}, "BD": {}, "BE": {},
	"BF": {}, "BG": {}, "BH": {}, "BI": {}, "BJ": {}, "BL": {}, "BM": {}, "BN": {}, "BO": {}, "BQ": {},
	"BR": {}, "BS": {}, "BT": {}, "BV": {}, "BW": {}, "BY": {}, "BZ": {}, "CA": {}, "CC": {}, "CD": {},
	"CF": {}, "CG": {}, "CH": {}, "CI": {}, "CK": {}, "CL": {}, "CM": {}, "CN": {}, "CO": {}, "CR": {},
	"CU": {}, "CV": {}, "CW": {}, "CX": {}, "CY": {}, "CZ": {}, "DE": {}, "DJ": {}, "DK": {}, "DM": {},
	"DO": {}, "DZ": {}, "EC": {}, "EE": {}, "EG": {}, "EH": {}, "ER": {}, "ES": {}, "ET": {}, "FI": {},
	"FJ": {}, "FK": {}, "FM": {}, "FO": {}, "FR": {}, "GA": {}, "GB": {}, "GD": {}, "GE": {}, "GF": {},
	"GG": {}, "GH": {}, "GI": {}, "GL": {}, "GM": {}, "GN": {}, "GP": {}, "GQ": {}, "GR": {}, "GS": {},
	"GT": {}, "GU": {}, "GW": {}, "GY": {}, "HK": {}, "HM": {}, "HN": {}, "HR": {}, "HT": {}, "HU": {},
	"ID": {}, "IE": {}, "IL": {}, "IM": {}, "IN": {}, "IO": {}, "IQ": {}, "IR": {}, "IS": {}, "IT": {},
	"JE": {}, "JM": {}, "JO": {}, "JP": {}, "KE": {}, "KG": {}, "KH": {}, "KI": {}, "KM": {}, "KN": {},
	"KP": {}, "KR": {}, "KW": {}, "KY": {}, "KZ": {}, "LA": {}, "LB": {}, "LC": {}, "LI": {}, "LK": {},
	"LR": {}, "LS": {}, "LT": {}, "LU": {}, "LV": {}, "LY": {}, "MA": {}, "MC": {}, "MD": {}, "ME": {},
	"MF": {}, "MG": {}, "MH": {}, "MK": {}, "ML": {}, "MM": {}, "MN": {}, "MO": {}, "MP": {}, "MQ": {},
	"MR": {}, "MS": {}, "MT": {}, "MU": {}, "MV": {}, "MW": {}, "MX": {}, "MY": {}, "MZ": {}, "NA": {},
	"NC": {}, "NE": {}, "NF": {}, "NG": {}, "NI": {}, "NL": {}, "NO": {}, "NP": {}, "NR": {}, "NU": {},
	"NZ": {}, "OM": {}, "PA": {}, "PE": {}, "PF": {}, "PG": {}, "PH": {}, "PK": {}, "PL": {}, "PM": {},
	"PN": {}, "PR": {}, "PS": {}, "PT": {}, "PW": {}, "PY": {}, "QA": {}, "RE": {}, "RO": {}, "RS": {},
	"RU": {}, "RW": {}, "SA": {}, "SB": {}, "SC": {}, "SD": {}, "SE": {}, "SG": {}, "SH": {}, "SI": {},
	"SJ": {}, "SK": {}, "SL": {}, "SM": {}, "SN": {}, "SO": {}, "SR": {}, "SS": {}, "ST": {}, "SV": {},
	"SX": {}, "SY": {}, "SZ": {}, "TC": {}, "TD": {}, "TF": {}, "TG": {}, "TH": {}, "TJ": {}, "TK": {},
	"TL": {}, "TM": {}, "TN": {}, "TO": {}, "TR": {}, "TT": {}, "TV": {}, "TW": {}, "TZ": {}, "UA": {},
	"UG": {}, "UM": {}, "US": {}, "UY": {}, "UZ": {}, "VA": {}, "VC": {}, "VE": {}, "VG": {}, "VI": {},
	"VN": {}, "VU": {}, "WF": {}, "WS": {}, "YE": {}, "YT": {}, "ZA": {}, "ZM": {}, "ZW": {}, "UK": {},
}

// IsValidISOCountry valida si un código corresponde a un país ISO o comodines permitidos.
func IsValidISOCountry(code string) bool {
	c := strings.ToUpper(strings.TrimSpace(code))
	if c == "ANY" || c == "ALL" {
		return true
	}
	_, ok := isoCountrySet[c]
	return ok
}

// ExtractCountryFromUsername extrae de forma universal cualquier tag de país en el username.
// Detecta prefijos conocidos (-zone-custom-region-, -country-, -cc-, __cr., etc.) y
// delimitadores generales (-, _, ., :, =, /) seguidos de código ISO-2.
func ExtractCountryFromUsername(user string) (detectedRegion string, prefixStr string, suffixStr string, isUpper bool, found bool) {
	if user == "" {
		return "", "", "", false, false
	}

	knownPrefixes := []string{
		"-zone-custom-region-",
		"_zone-custom-region-",
		"-country-",
		"_country-",
		"_country_",
		":country:",
		"-region-",
		"_region-",
		"_region_",
		":region:",
		"-cc-",
		"_cc-",
		"__cr.",
		".cr.",
		"-cr-",
		"_cr_",
		"-loc-",
		"_loc-",
		"-geo-",
		"_geo-",
		"-c-",
		"_c-",
		"-zone-custom-",
		"_zone-custom_",
		"-zone-",
		"_zone_",
	}

	// 1. Probar primero prefijos conocidos estructurados
	for _, p := range knownPrefixes {
		if idx := strings.Index(user, p); idx >= 0 {
			pEnd := idx + len(p)
			tag := user[pEnd:]
			endIdx := len(tag)
			for i, r := range tag {
				if r == '_' || r == '-' || r == ':' || r == ';' || r == '@' {
					endIdx = i
					break
				}
			}
			rawTag := tag[:endIdx]
			if len(rawTag) >= 2 {
				isUp := (rawTag == strings.ToUpper(rawTag))
				return rawTag, user[:pEnd], tag[endIdx:], isUp, true
			}
		}
	}

	// 2. Detección general por delimitador (-, _, ., :, =, /) seguido de ISO de 2 letras
	delims := "-_.:=/"
	for i := 0; i < len(user); i++ {
		ch := user[i]
		if strings.IndexByte(delims, ch) >= 0 {
			if i+3 <= len(user) {
				candidate := user[i+1 : i+3]
				if IsValidISOCountry(candidate) {
					if i+3 == len(user) || strings.IndexByte(delims+";@", user[i+3]) >= 0 {
						isUp := (candidate == strings.ToUpper(candidate))
						return candidate, user[:i+1], user[i+3:], isUp, true
					}
				}
			}
		}
	}

	return "", "", "", false, false
}

// ApplyCountryToUsername adapta el país en un username de proxy preservando el formato y mayúsculas/minúsculas.
func ApplyCountryToUsername(user, countryCode string) string {
	if user == "" || countryCode == "" || countryCode == "XX" || countryCode == "GENERIC" {
		return user
	}

	_, prefixStr, suffixStr, isUpper, found := ExtractCountryFromUsername(user)
	if found {
		targetCC := countryCode
		if isUpper {
			targetCC = strings.ToUpper(countryCode)
		} else {
			targetCC = strings.ToLower(countryCode)
		}
		return prefixStr + targetCC + suffixStr
	}

	// Si no tenía tag regional preexistente pero hay un proveedor configurado
	if ActiveProviderConfig.RegionTemplate != "" {
		targetCC := countryCode
		if ActiveProviderConfig.LowercaseCountry {
			targetCC = strings.ToLower(countryCode)
		} else {
			targetCC = strings.ToUpper(countryCode)
		}
		regionTag := strings.ReplaceAll(ActiveProviderConfig.RegionTemplate, ActiveProviderConfig.CountryPlaceholder, targetCC)
		return user + regionTag
	}

	return user
}

// extractRegionFromUsername detecta y separa cualquier tag regional existente (ej: __cr.us, -zone-custom-region-MX, -country-es, -cc-pe).
func (p *ParsedProxy) extractRegionFromUsername() {
	p.BaseUsername = p.Username
	p.Region = ""

	if p.Username == "" {
		return
	}

	tag, prefixStr, suffixStr, _, found := ExtractCountryFromUsername(p.Username)
	if found {
		p.Region = strings.ToLower(tag)
		p.BaseUsername = stripRegionalPrefix(prefixStr, suffixStr)
	}
}

func stripRegionalPrefix(prefixStr, suffixStr string) string {
	knownPrefixes := []string{
		"-zone-custom-region-",
		"_zone-custom-region-",
		"-country-",
		"_country-",
		"_country_",
		":country:",
		"-region-",
		"_region-",
		"_region_",
		":region:",
		"-cc-",
		"_cc-",
		"__cr.",
		".cr.",
		"-cr-",
		"_cr_",
		"-loc-",
		"_loc-",
		"-geo-",
		"_geo-",
		"-c-",
		"_c-",
		"-zone-custom-",
		"_zone-custom_",
		"-zone-",
		"_zone_",
	}
	for _, p := range knownPrefixes {
		if idx := strings.Index(prefixStr, p); idx >= 0 {
			return prefixStr[:idx] + suffixStr
		}
	}
	if len(prefixStr) > 0 {
		return prefixStr[:len(prefixStr)-1] + suffixStr
	}
	return prefixStr + suffixStr
}

// Validate comprueba que los campos estructurales sean válidos y seguros.
func (p ParsedProxy) Validate() error {
	if strings.TrimSpace(p.Host) == "" {
		return fmt.Errorf("%w: host vacío", ErrInvalidProxy)
	}

	portNum, err := strconv.Atoi(p.Port)
	if err != nil || portNum <= 0 || portNum > 65535 {
		return fmt.Errorf("%w: puerto inválido %q", ErrInvalidProxy, p.Port)
	}

	return nil
}

// String reconstruye el proxy en formato canónico "host:port:user:pass" o "host:port"
func (p ParsedProxy) String() string {
	if p.Username == "" && p.Password == "" {
		return fmt.Sprintf("%s:%s", p.Host, p.Port)
	}
	return fmt.Sprintf("%s:%s:%s:%s", p.Host, p.Port, p.Username, p.Password)
}

// URLString reconstruye la URL completa con scheme (ej. "http://user:pass@host:port")
func (p ParsedProxy) URLString(defaultScheme string) string {
	scheme := p.Scheme
	if scheme == "" {
		scheme = defaultScheme
	}
	if scheme == "" {
		scheme = "http"
	}

	if p.Username == "" && p.Password == "" {
		return fmt.Sprintf("%s://%s:%s", scheme, p.Host, p.Port)
	}
	return fmt.Sprintf("%s://%s:%s@%s:%s", scheme, url.QueryEscape(p.Username), url.QueryEscape(p.Password), p.Host, p.Port)
}
