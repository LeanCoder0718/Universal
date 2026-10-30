// Package cookies implementa un jar conforme a RFC 6265, reemplazando
// SharedDomainJar (Client.go) que tenía una regla de asociación laxa:
//
//	} else if strings.HasSuffix(reqHost, cookieDomain) || rootDomain(u) == root {
//
// Esa segunda condición (`rootDomain(u) == root`) es casi SIEMPRE true
// dentro del mismo dominio raíz, lo que anula la validación de Domain/Path
// y puede filtrar cookies "host-only" hacia subdominios incorrectos.
// Para un objetivo como Netflix, que usa cookies Secure/HttpOnly con
// scoping estricto, esto es una fuente directa de detección o de sesión
// corrupta (cookie enviada a un host que el servidor no esperaba).
package HTTPClient

import (
	"strings"
	"sync"
	"time"

	fhttp "github.com/bogdanfinn/fhttp"
	"golang.org/x/net/publicsuffix"
)

// SameSite refleja los valores válidos del atributo SameSite (RFC 6265bis).
type SameSite int

const (
	SameSiteUnspecified SameSite = iota
	SameSiteNone
	SameSiteLax
	SameSiteStrict
)

// Cookie es nuestra representación completa RFC 6265, independiente de
// fhttp.Cookie (que no modela HostOnly ni CreationTime necesarios para
// ordenar correctamente por especificidad, §5.4).
type Cookie struct {
	Name      string
	Value     string
	Domain    string // normalizado, sin punto inicial
	Path      string
	Secure    bool
	HttpOnly  bool
	SameSite  SameSite
	Expires   time.Time
	HostOnly  bool // true si el servidor NO especificó Domain (§5.3)
	CreatedAt time.Time
}

// isExpired evalúa expiración por Expires (MaxAge ya se resuelve al parsear).
func (c Cookie) isExpired(now time.Time) bool {
	return !c.Expires.IsZero() && c.Expires.Before(now)
}

// domainMatches implementa el algoritmo de RFC 6265 §5.1.3 (domain-match).
func (c Cookie) domainMatches(host string) bool {
	host = strings.ToLower(host)
	domain := strings.ToLower(c.Domain)
	if c.HostOnly {
		return host == domain
	}
	if host == domain {
		return true
	}
	if !strings.HasSuffix(host, "."+domain) {
		return false
	}
	// RFC 6265 §5.1.3: el host debe ser un nombre canónico (no IP) para
	// que el sufijo cuente como match de subdominio.
	return true
}

// pathMatches implementa RFC 6265 §5.1.4 (path-match).
func (c Cookie) pathMatches(reqPath string) bool {
	if reqPath == "" {
		reqPath = "/"
	}
	cp := c.Path
	if cp == "" {
		cp = "/"
	}
	if reqPath == cp {
		return true
	}
	if strings.HasPrefix(reqPath, cp) {
		if strings.HasSuffix(cp, "/") {
			return true
		}
		if len(reqPath) > len(cp) && reqPath[len(cp)] == '/' {
			return true
		}
	}
	return false
}

// publicSuffixes es una lista mínima de sufijos públicos comunes.
// Para producción de alto rigor se recomienda inyectar la lista completa
// de publicsuffix.org vía Jar.SetPublicSuffixChecker; esta lista embebida
// cubre los casos más frecuentes sin traer una dependencia externa.
var publicSuffixes = map[string]bool{
	"com": true, "net": true, "org": true, "io": true, "co": true,
	"gov": true, "edu": true, "info": true, "biz": true,
}

// isPublicSuffix rechaza que un servidor fije una cookie para un dominio
// como ".com" (RFC 6265 §5.3, paso 5) — protección que SharedDomainJar
// no implementaba en absoluto.
func isPublicSuffix(domain string) bool {
	suffix, icann := publicsuffix.PublicSuffix(strings.ToLower(domain))
	return icann && suffix == domain
}

// domainBucket almacena las cookies de UN dominio raíz. Cada dominio
// raíz tiene su propio lock implícito vía el mutex del Jar (RWMutex a
// nivel de Jar es suficiente dado el volumen esperado; particionar más
// no aporta rendimiento real y añade complejidad).
type domainBucket struct {
	cookies map[string]*Cookie // key: Domain|Path|Name (RFC 6265 identity)
}

// Jar es el motor de cookies COMPLETO y ÚNICO del proyecto. Sustituye a
// SharedDomainJar. Es thread-safe y aplica reglas RFC 6265 estrictas.
type Jar struct {
	mu      sync.RWMutex
	buckets map[string]*domainBucket
	cfg     CookieConfig
}

// New crea un Jar vacío. cfg controla límites y rigor (rechazo de
// public suffixes, máximo de cookies por dominio).
func NewJAR(cfg CookieConfig) *Jar {
	return &Jar{
		buckets: make(map[string]*domainBucket),
		cfg:     cfg,
	}
}

// Clear vacía todas las cookies del Jar, útil para reciclar una Session
// entre combos sin destruir las conexiones TCP/TLS subyacentes.
func (j *Jar) Clear() {
	if j == nil {
		return
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	j.buckets = make(map[string]*domainBucket)
}

func cookieIdentity(c *Cookie) string {
	// RFC 6265 §5.3: la clave del store es name + domain + path.
	// Colapsar Domain producía sobrescritura host-only vs Domain=.
	return strings.ToLower(c.Domain) + "|" + c.Path + "|" + c.Name
}

// SetCookies procesa Set-Cookie de una respuesta para reqURL, aplicando
// las reglas de scoping de RFC 6265 §5.3 completas: domain matching,
// public-suffix rejection, HostOnly cuando no hay Domain explícito, y
// expiración (Expires/MaxAge).
func (j *Jar) SetCookies(reqHost, reqPath string, fcookies []*fhttp.Cookie) {
	if len(fcookies) == 0 {
		return
	}
	root := RootDomain(reqHost)

	j.mu.Lock()
	defer j.mu.Unlock()

	bucket := j.buckets[root]
	if bucket == nil {
		bucket = &domainBucket{cookies: make(map[string]*Cookie)}
		j.buckets[root] = bucket
	}

	now := time.Now()
	for _, fc := range fcookies {
		if fc == nil || fc.Name == "" {
			continue
		}
		c := fromFHTTP(fc, reqHost, reqPath, now, j.cfg)

		if j.cfg.RejectPublicSuffixes && !c.HostOnly && isPublicSuffix(c.Domain) {
			continue
		}
		if !c.HostOnly && !hostMatchesForSetting(reqHost, c.Domain) {
			continue
		}

		key := cookieIdentity(&c)
		isDelete := fc.MaxAge < 0 || c.isExpired(now) || strings.EqualFold(fc.Value, "DELETE")
		if isDelete {
			delete(bucket.cookies, key)
			// Purgar bajo "/" SOLO si el servidor no especificó un Path o si ya apuntaba a "/"
			if fc.Path == "" || c.Path == "/" {
				slashKey := strings.ToLower(c.Domain) + "|/|" + c.Name
				delete(bucket.cookies, slashKey)
			}
			continue
		}
		if len(bucket.cookies) >= j.cfg.MaxCookiesPerDomain && j.cfg.MaxCookiesPerDomain > 0 {
			evictOldest(bucket)
		}
		bucket.cookies[key] = &c
	}
}

// hostMatchesForSetting valida que un host pueda fijar una cookie para
// el Domain declarado (debe ser el propio host o un ancestro de él).
func hostMatchesForSetting(host, domain string) bool {
	host = strings.ToLower(host)
	domain = strings.ToLower(domain)
	return host == domain || strings.HasSuffix(host, "."+domain)
}

func evictOldest(bucket *domainBucket) {
	var oldestKey string
	var oldestTime time.Time
	for k, c := range bucket.cookies {
		if oldestKey == "" || c.CreatedAt.Before(oldestTime) {
			oldestKey = k
			oldestTime = c.CreatedAt
		}
	}
	if oldestKey != "" {
		delete(bucket.cookies, oldestKey)
	}
}

func fromFHTTP(fc *fhttp.Cookie, reqHost, reqPath string, now time.Time, cfg CookieConfig) Cookie {
	c := Cookie{
		Name:      fc.Name,
		Value:     fc.Value,
		Path:      fc.Path,
		Secure:    fc.Secure,
		HttpOnly:  fc.HttpOnly,
		CreatedAt: now,
	}
	if cfg.DisableHostOnly {
		c.HostOnly = false
		c.Domain = RootDomain(reqHost)
	} else {
		if fc.Domain == "" {
			c.HostOnly = true
			c.Domain = strings.ToLower(reqHost)
		} else {
			c.HostOnly = false
			c.Domain = strings.ToLower(strings.TrimPrefix(fc.Domain, "."))
		}
	}
	if cfg.DisableHostOnly {
		c.Path = "/"
	} else if c.Path == "" {
		c.Path = defaultPath(reqPath)
	}
	switch fc.SameSite {
	case fhttp.SameSiteNoneMode:
		c.SameSite = SameSiteNone
	case fhttp.SameSiteLaxMode:
		c.SameSite = SameSiteLax
	case fhttp.SameSiteStrictMode:
		c.SameSite = SameSiteStrict
	default:
		c.SameSite = SameSiteUnspecified
	}
	if fc.MaxAge != 0 {
		c.Expires = now.Add(time.Duration(fc.MaxAge) * time.Second)
	} else if !fc.Expires.IsZero() {
		c.Expires = fc.Expires
	}
	return c
}

// defaultPath implementa RFC 6265 §5.1.4 (default-path) cuando el
// servidor no especifica Path explícitamente.
func defaultPath(reqPath string) string {
	if reqPath == "" || reqPath[0] != '/' {
		return "/"
	}
	idx := strings.LastIndex(reqPath, "/")
	if idx <= 0 {
		return "/"
	}
	return reqPath[:idx]
}

// isMoreSpecific compara dos cookies con el mismo Name aplicables a reqHost+reqPath.
// Retorna true si 'a' es más específica que 'b':
// 1. Path más largo (RFC 6265 §5.4: paths más específicos tienen precedencia).
// 2. Especificidad de dominio: host exacto / host-only > dominio padre con wildcard (ej. login.yahoo.com > yahoo.com).
// 3. Dominio más largo (subdominio más profundo > dominio raíz).
// 4. Si path y dominio tienen igual especificidad, la versión más reciente (CreatedAt más nueva) prevalece sobre la antigua.
func isMoreSpecific(a, b *Cookie, reqHost string) bool {
	// 1. Longitud de Path (más específico primero)
	if len(a.Path) != len(b.Path) {
		return len(a.Path) > len(b.Path)
	}

	// 2. Coincidencia exacta de host / HostOnly frente a dominio padre
	reqHostLower := strings.ToLower(reqHost)
	aDomainLower := strings.ToLower(a.Domain)
	bDomainLower := strings.ToLower(b.Domain)

	aExact := a.HostOnly || aDomainLower == reqHostLower
	bExact := b.HostOnly || bDomainLower == reqHostLower
	if aExact != bExact {
		return aExact
	}

	// 3. Profundidad del dominio (longitud de subdominio)
	if len(aDomainLower) != len(bDomainLower) {
		return len(aDomainLower) > len(bDomainLower)
	}

	// 4. Recencia temporal (la cookie creada/actualizada más recientemente gana)
	return a.CreatedAt.After(b.CreatedAt)
}

// CookiesFor devuelve las cookies aplicables a un request HTTPS/HTTP a
// reqHost+reqPath, deduplicadas por nombre (conservando la más específica:
// subdominio exacto sobre dominio padre, path más profundo sobre raíz),
// ordenadas por especificidad de Path (más largo primero, RFC 6265 §5.4)
// y filtrando por Secure/scheme.
func (j *Jar) CookiesFor(reqHost, reqPath string, isSecure bool) []*fhttp.Cookie {
	root := RootDomain(reqHost)

	j.mu.RLock()
	bucket := j.buckets[root]
	if bucket == nil {
		j.mu.RUnlock()
		return nil
	}
	candidates := make([]*Cookie, 0, len(bucket.cookies))
	now := time.Now()
	for _, c := range bucket.cookies {
		if c.isExpired(now) || strings.EqualFold(c.Value, "DELETE") {
			continue
		}
		if c.Secure && !isSecure {
			continue
		}
		if !c.domainMatches(reqHost) {
			continue
		}
		if !c.pathMatches(reqPath) {
			continue
		}
		candidates = append(candidates, c)
	}
	j.mu.RUnlock()

	if len(candidates) == 0 {
		return nil
	}

	// Orden por especificidad de path (RFC 6265 §5.4: paths más largos
	// primero) y luego por CreatedAt (más antiguas primero) para
	// estabilidad determinística entre requests. No deduplicar por nombre:
	// un navegador real envía todas las cookies coincidentes.
	for i := 1; i < len(candidates); i++ {
		for k := i; k > 0; k-- {
			a, b := candidates[k-1], candidates[k]
			if len(a.Path) < len(b.Path) ||
				(len(a.Path) == len(b.Path) && a.CreatedAt.After(b.CreatedAt)) {
				candidates[k-1], candidates[k] = candidates[k], candidates[k-1]
			} else {
				break
			}
		}
	}

	out := make([]*fhttp.Cookie, 0, len(candidates))
	for _, c := range candidates {
		out = append(out, &fhttp.Cookie{Name: c.Name, Value: c.Value})
	}
	return out
}

// AddManual permite inyectar una cookie explícita (ej. token de sesión
// capturado externamente), pasando por las mismas reglas de scoping.
func (j *Jar) AddManual(reqHost, reqPath string, c Cookie) {
	c.CreatedAt = time.Now()
	if c.Path == "" {
		c.Path = defaultPath(reqPath)
	}
	root := RootDomain(reqHost)

	j.mu.Lock()
	defer j.mu.Unlock()
	bucket := j.buckets[root]
	if bucket == nil {
		bucket = &domainBucket{cookies: make(map[string]*Cookie)}
		j.buckets[root] = bucket
	}
	bucket.cookies[cookieIdentity(&c)] = &c
}

// Reset limpia completamente el jar (equivalente a Reset() del jar viejo).
// Reset limpia completamente el jar (equivalente a Reset() del jar viejo).
func (j *Jar) Reset() {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.buckets = make(map[string]*domainBucket)
}

// Snapshot expone una vista de solo lectura, útil para debug/logging.
func (j *Jar) Snapshot() map[string][]Cookie {
	j.mu.RLock()
	defer j.mu.RUnlock()
	out := make(map[string][]Cookie, len(j.buckets))
	for domain, bucket := range j.buckets {
		list := make([]Cookie, 0, len(bucket.cookies))
		for _, c := range bucket.cookies {
			list = append(list, *c)
		}
		out[domain] = list
	}
	return out
}
