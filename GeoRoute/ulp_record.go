package GeoRoute

import "fmt"

// CountrySource identifica el origen exacto desde el cual se determinó el país.
type CountrySource string

const (
	CountrySourceExplicitMetadata CountrySource = "ExplicitMetadata"
	CountrySourceServiceDomain    CountrySource = "ServiceDomain"
	CountrySourceEmailDomain      CountrySource = "EmailDomain"
	CountrySourceOther            CountrySource = "Other"
	CountrySourceUnknown          CountrySource = "Unknown"
)

// ConfidenceLevel cuantifica la certidumbre de la clasificación geográfica.
type ConfidenceLevel string

const (
	ConfidenceHigh    ConfidenceLevel = "HIGH"    // Metadato explícito (1.00) o ccTLD de servicio directo (0.95)
	ConfidenceMedium  ConfidenceLevel = "MEDIUM"  // ccTLD de correo (0.85) o conflicto resuelto por política (0.70)
	ConfidenceLow     ConfidenceLevel = "LOW"     // Heurística de subdominio o patrón secundario (0.50)
	ConfidenceUnknown ConfidenceLevel = "UNKNOWN" // Dominio genérico (.com, .net) o sin indicador geográfico (0.00)
)

// Score devuelve la puntuación numérica normalizada asociada al nivel de confianza.
func (c ConfidenceLevel) Score() float64 {
	switch c {
	case ConfidenceHigh:
		return 0.95
	case ConfidenceMedium:
		return 0.85
	case ConfidenceLow:
		return 0.50
	default:
		return 0.00
	}
}

// ConflictPolicy define la estrategia para resolver discrepancias entre el país del servicio y el país del email.
type ConflictPolicy int

const (
	// PolicyPreferService (Por defecto): El país del servicio prevalece sobre el del email.
	// Justificación técnica: En registros ULP, el dominio del servicio indica el portal/CDN/endpoint
	// regional al que se dirige la petición, siendo determinante para el enrutamiento de proxy.
	PolicyPreferService ConflictPolicy = iota

	// PolicyPreferEmail: El país derivado del dominio del correo prevalece sobre el del servicio.
	PolicyPreferEmail

	// PolicyPreferExplicit: Metadatos explícitos prevalecen sobre cualquier dominio.
	PolicyPreferExplicit

	// PolicyMarkUnknown: Si existe discrepancia entre fuentes válidas, se clasifica como XX (Unknown).
	PolicyMarkUnknown

	// PolicyManualReview: Se marca como conflictivo para revisión o cuarentena.
	PolicyManualReview
)

func (p ConflictPolicy) String() string {
	switch p {
	case PolicyPreferService:
		return "PreferService"
	case PolicyPreferEmail:
		return "PreferEmail"
	case PolicyPreferExplicit:
		return "PreferExplicit"
	case PolicyMarkUnknown:
		return "MarkUnknown"
	case PolicyManualReview:
		return "ManualReview"
	default:
		return "UnknownPolicy"
	}
}

// CountryConflict almacena la información detallada de una discrepancia geográfica.
type CountryConflict struct {
	ServiceCountry CountryCode    `json:"service_country"`
	EmailCountry   CountryCode    `json:"email_country"`
	ResolvedAs     CountryCode    `json:"resolved_as"`
	PolicyUsed     ConflictPolicy `json:"policy_used"`
}

func (cc CountryConflict) String() string {
	return fmt.Sprintf("%s (Service) vs %s (Email) -> Resolved: %s via %s",
		cc.ServiceCountry, cc.EmailCountry, cc.ResolvedAs, cc.PolicyUsed.String())
}

// ULPRecord representa un registro ULP parseado con preservación íntegra del raw original
// y aislamiento estricto de campos sensibles.
type ULPRecord struct {
	Raw        string           // Línea original intacta
	Service    string           // Hostname o URL del servicio
	Email      string           // Email o nombre de usuario
	Password   string           // Tratado como confidencial: no se expone en String() ni logs
	Country    CountryCode      // Código ISO 3166-1 alpha-2 o "XX"
	Source     CountrySource    // Origen de la detección
	Confidence ConfidenceLevel  // Nivel de certidumbre
	Conflict   *CountryConflict // Registro del conflicto si existió discrepancia
}

// String enmascara de manera forzosa el password para prevenir fugas accidentales en logs.
func (r ULPRecord) String() string {
	return fmt.Sprintf("ULPRecord{Service: %q, Email: %q, Password: [REDACTED], Country: %s, Source: %s, Confidence: %s}",
		r.Service, r.Email, r.Country, r.Source, r.Confidence)
}

// GoString previene la exposición del password en formateos %#v.
func (r ULPRecord) GoString() string {
	return r.String()
}

// NormalizedRecord es la estructura intermedia estándar y unificada para todo el flujo de GeoRouter.
type NormalizedRecord struct {
	Raw        string           `json:"raw"`
	Service    string           `json:"service,omitempty"`
	Email      string           `json:"email,omitempty"`
	Country    CountryCode      `json:"country"`
	Source     CountrySource    `json:"source"`
	Confidence ConfidenceLevel  `json:"confidence"`
	Conflict   *CountryConflict `json:"conflict,omitempty"`
}

func (nr NormalizedRecord) String() string {
	return fmt.Sprintf("NormalizedRecord{Service: %q, Email: %q, Country: %s, Source: %s, Confidence: %s}",
		nr.Service, nr.Email, nr.Country, nr.Source, nr.Confidence)
}
