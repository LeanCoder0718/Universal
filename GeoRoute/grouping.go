package GeoRoute

import (
	"strings"
)

// ProcessingOrder define el ordenamiento de los registros tras la clasificación.
type ProcessingOrder int

const (
	// OrderOriginal conserva el orden original de entrada del archivo.
	OrderOriginal ProcessingOrder = iota
	// OrderCountry agrupa todos los registros secuencialmente por país (ej. todos ES, luego BR...).
	OrderCountry
	// OrderCustom agrupa según una lista explícita de prioridad de países provista por el usuario.
	OrderCustom
)

// GroupingEngine clasifica colecciones de registros o líneas individuales.
type GroupingEngine struct {
	detector *CountryDetector
}

// NewGroupingEngine crea una nueva instancia del motor de agrupación.
func NewGroupingEngine(detector *CountryDetector) *GroupingEngine {
	if detector == nil {
		detector = NewCountryDetector(50000)
	}
	return &GroupingEngine{detector: detector}
}

// ClassifySingle clasifica un registro individual sin alocar memoria extra (ideal para hot-path streaming).
func (g *GroupingEngine) ClassifySingle(combo string) CountryCode {
	return g.detector.Detect(combo)
}

// ClassifyBatch realiza una clasificación O(n) en una sola pasada.
// Agrupa los combos en un mapa de slices por código de país.
func (g *GroupingEngine) ClassifyBatch(combos []string) map[CountryCode][]string {
	result := make(map[CountryCode][]string, 16)

	for _, line := range combos {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		cc := g.detector.Detect(line)
		result[cc] = append(result[cc], line)
	}

	return result
}

// ClassifyNormalizedBatch agrupa un slice de NormalizedRecord por CountryCode.
func (g *GroupingEngine) ClassifyNormalizedBatch(records []NormalizedRecord) map[CountryCode][]NormalizedRecord {
	result := make(map[CountryCode][]NormalizedRecord, 16)

	for _, rec := range records {
		cc := rec.Country
		if cc == "" {
			cc = Unknown
		}
		result[cc] = append(result[cc], rec)
	}

	return result
}

// ReorderBatch reorganiza un slice de combos según la estrategia de ordenamiento elegida.
func (g *GroupingEngine) ReorderBatch(combos []string, order ProcessingOrder, customPriority []CountryCode) []string {
	if order == OrderOriginal || len(combos) <= 1 {
		return combos
	}

	cfg := DefaultSchedulerConfig()
	switch order {
	case OrderCountry:
		cfg.Mode = ModeLargestFirst
	case OrderCustom:
		cfg.Mode = ModeCustomPriority
		cfg.CustomPriorities = make(map[CountryCode]int)
		for idx, cc := range customPriority {
			cfg.CustomPriorities[cc] = idx + 1
		}
	default:
		cfg.Mode = ModeLargestFirst
	}

	sched := NewGeoScheduler(g.detector, cfg)
	return sched.ReorderCombos(combos)
}

// ReorderNormalizedBatch reorganiza un slice de NormalizedRecord según la estrategia de ordenamiento elegida.
func (g *GroupingEngine) ReorderNormalizedBatch(records []NormalizedRecord, order ProcessingOrder, customPriority []CountryCode) []NormalizedRecord {
	if order == OrderOriginal || len(records) <= 1 {
		return records
	}

	groups := g.ClassifyNormalizedBatch(records)
	out := make([]NormalizedRecord, 0, len(records))

	if order == OrderCustom && len(customPriority) > 0 {
		visited := make(map[CountryCode]struct{}, len(customPriority))
		for _, cc := range customPriority {
			if items, exists := groups[cc]; exists {
				out = append(out, items...)
				visited[cc] = struct{}{}
			}
		}
		for cc, items := range groups {
			if _, ok := visited[cc]; !ok {
				out = append(out, items...)
			}
		}
		return out
	}

	var unknownItems []NormalizedRecord
	for cc, items := range groups {
		if cc.IsUnknown() {
			unknownItems = append(unknownItems, items...)
			continue
		}
		out = append(out, items...)
	}

	if len(unknownItems) > 0 {
		out = append(out, unknownItems...)
	}

	return out
}
