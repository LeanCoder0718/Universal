package GeoRoute

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

// Metrics gestiona las métricas de rendimiento, distribución geográfica, fuentes y conflictos.
type Metrics struct {
	TotalRecords    atomic.Int64
	Classified      atomic.Int64
	UnknownCount    atomic.Int64
	InvalidCount    atomic.Int64
	ConflictsCount  atomic.Int64
	RegionalProxies atomic.Int64
	GeneralProxies  atomic.Int64
	ProxyErrors     atomic.Int64

	mu                 sync.RWMutex
	byCountry          map[CountryCode]int64
	bySource           map[CountrySource]int64
	byConfidence       map[ConfidenceLevel]int64
	byCountryAndSource map[CountryCode]map[CountrySource]int64
	conflictsBreakdown map[string]int64
}

// NewMetrics inicializa un recolector de métricas atómico y thread-safe.
func NewMetrics() *Metrics {
	return &Metrics{
		byCountry:          make(map[CountryCode]int64, 64),
		bySource:           make(map[CountrySource]int64, 8),
		byConfidence:       make(map[ConfidenceLevel]int64, 8),
		byCountryAndSource: make(map[CountryCode]map[CountrySource]int64, 64),
		conflictsBreakdown: make(map[string]int64, 32),
	}
}

// RecordClassification registra el resultado básico del análisis de un registro (compatibilidad hacia atrás).
func (m *Metrics) RecordClassification(country CountryCode, hasError bool) {
	m.TotalRecords.Add(1)

	if hasError {
		m.InvalidCount.Add(1)
		return
	}

	if country.IsUnknown() {
		m.UnknownCount.Add(1)
	} else {
		m.Classified.Add(1)
	}

	m.mu.Lock()
	m.byCountry[country]++
	m.mu.Unlock()
}

// RecordNormalized registra el resultado completo y estructurado de un NormalizedRecord.
func (m *Metrics) RecordNormalized(rec NormalizedRecord, hasError bool) {
	m.TotalRecords.Add(1)

	if hasError {
		m.InvalidCount.Add(1)
		return
	}

	if rec.Country.IsUnknown() {
		m.UnknownCount.Add(1)
	} else {
		m.Classified.Add(1)
	}

	if rec.Conflict != nil {
		m.ConflictsCount.Add(1)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.byCountry[rec.Country]++
	m.bySource[rec.Source]++
	m.byConfidence[rec.Confidence]++

	if _, ok := m.byCountryAndSource[rec.Country]; !ok {
		m.byCountryAndSource[rec.Country] = make(map[CountrySource]int64, 4)
	}
	m.byCountryAndSource[rec.Country][rec.Source]++

	if rec.Conflict != nil {
		key := fmt.Sprintf("%s vs %s", rec.Conflict.ServiceCountry, rec.Conflict.EmailCountry)
		m.conflictsBreakdown[key]++
	}
}

// RecordProxyAssignment registra el tipo de proxy asignado (regional vs general) o error.
func (m *Metrics) RecordProxyAssignment(country CountryCode, err error) {
	if err != nil {
		m.ProxyErrors.Add(1)
		return
	}

	if country.IsUnknown() {
		m.GeneralProxies.Add(1)
	} else {
		m.RegionalProxies.Add(1)
	}
}

// SourceStat contiene el conteo de una fuente de detección.
type SourceStat struct {
	Source     CountrySource
	Count      int64
	Percentage float64
}

// CountryStat contiene el conteo y detalle de fuentes para un país individual.
type CountryStat struct {
	Country    CountryCode
	Count      int64
	Percentage float64
	Sources    map[CountrySource]int64
}

// ConflictStat contiene el detalle de un tipo de conflicto registrado.
type ConflictStat struct {
	ConflictPair string
	Count        int64
}

// MetricsSnapshot representa una captura inmutable en tiempo real de todas las métricas.
type MetricsSnapshot struct {
	TotalRecords        int64
	Classified          int64
	Unknown             int64
	Invalid             int64
	Conflicts           int64
	RegionalProxies     int64
	GeneralProxies      int64
	ProxyErrors         int64
	ClassificationRate  float64
	UnknownRate         float64
	RegionalProxyRate   float64
	ProxyErrorRate      float64
	CountryDistribution []CountryStat
	SourceDistribution  []SourceStat
	TopConflicts        []ConflictStat
}

// Snapshot genera una captura atómica de todas las métricas activas.
func (m *Metrics) Snapshot() MetricsSnapshot {
	total := m.TotalRecords.Load()
	classified := m.Classified.Load()
	unknown := m.UnknownCount.Load()
	invalid := m.InvalidCount.Load()
	conflicts := m.ConflictsCount.Load()
	regional := m.RegionalProxies.Load()
	general := m.GeneralProxies.Load()
	errors := m.ProxyErrors.Load()

	var classRate, unkRate, regRate, errRate float64
	if total > 0 {
		classRate = (float64(classified) / float64(total)) * 100
		unkRate = (float64(unknown) / float64(total)) * 100
	}

	totalProxies := regional + general + errors
	if totalProxies > 0 {
		regRate = (float64(regional) / float64(totalProxies)) * 100
		errRate = (float64(errors) / float64(totalProxies)) * 100
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	// 1. Distribución de Países
	dist := make([]CountryStat, 0, len(m.byCountry))
	for cc, count := range m.byCountry {
		var pct float64
		if total > 0 {
			pct = (float64(count) / float64(total)) * 100
		}

		sourcesCopy := make(map[CountrySource]int64)
		if srcMap, ok := m.byCountryAndSource[cc]; ok {
			for s, c := range srcMap {
				sourcesCopy[s] = c
			}
		}

		dist = append(dist, CountryStat{
			Country:    cc,
			Count:      count,
			Percentage: pct,
			Sources:    sourcesCopy,
		})
	}
	sort.Slice(dist, func(i, j int) bool {
		return dist[i].Count > dist[j].Count
	})

	// 2. Distribución de Fuentes
	srcDist := make([]SourceStat, 0, len(m.bySource))
	for src, count := range m.bySource {
		var pct float64
		if total > 0 {
			pct = (float64(count) / float64(total)) * 100
		}
		srcDist = append(srcDist, SourceStat{
			Source:     src,
			Count:      count,
			Percentage: pct,
		})
	}
	sort.Slice(srcDist, func(i, j int) bool {
		return srcDist[i].Count > srcDist[j].Count
	})

	// 3. Conflictos principales
	confList := make([]ConflictStat, 0, len(m.conflictsBreakdown))
	for pair, count := range m.conflictsBreakdown {
		confList = append(confList, ConflictStat{
			ConflictPair: pair,
			Count:        count,
		})
	}
	sort.Slice(confList, func(i, j int) bool {
		return confList[i].Count > confList[j].Count
	})

	return MetricsSnapshot{
		TotalRecords:        total,
		Classified:          classified,
		Unknown:             unknown,
		Invalid:             invalid,
		Conflicts:           conflicts,
		RegionalProxies:     regional,
		GeneralProxies:      general,
		ProxyErrors:         errors,
		ClassificationRate:  classRate,
		UnknownRate:         unkRate,
		RegionalProxyRate:   regRate,
		ProxyErrorRate:      errRate,
		CountryDistribution: dist,
		SourceDistribution:  srcDist,
		TopConflicts:        confList,
	}
}

// RenderDashboard genera la vista ASCII estándar para GeoRouter.
func (s MetricsSnapshot) RenderDashboard() string {
	var b strings.Builder

	b.WriteString("┌─────────────────────────────────────────────┐\n")
	b.WriteString("│          GEO ROUTER — ULP ANALYSIS          │\n")
	b.WriteString("├─────────────────────────────────────────────┤\n")
	b.WriteString(fmt.Sprintf("│ TOTAL RECORDS   %-27d │\n", s.TotalRecords))
	b.WriteString(fmt.Sprintf("│ CLASSIFIED      %-27d │\n", s.Classified))
	b.WriteString(fmt.Sprintf("│ UNKNOWN (XX)    %-27d │\n", s.Unknown))
	if s.Invalid > 0 {
		b.WriteString(fmt.Sprintf("│ INVALID         %-27d │\n", s.Invalid))
	}
	if s.Conflicts > 0 {
		b.WriteString(fmt.Sprintf("│ CONFLICTS       %-27d │\n", s.Conflicts))
	}
	b.WriteString("├─────────────────────────────────────────────┤\n")
	b.WriteString("│ COUNTRY DISTRIBUTION                        │\n")
	b.WriteString("│                                             │\n")

	limit := 8
	if len(s.CountryDistribution) < limit {
		limit = len(s.CountryDistribution)
	}

	for i := 0; i < limit; i++ {
		stat := s.CountryDistribution[i]
		barLen := int(stat.Percentage / 5)
		if barLen > 20 {
			barLen = 20
		}
		bar := strings.Repeat("█", barLen)
		b.WriteString(fmt.Sprintf("│ %-5s %-12d %-16s %5.1f%% │\n", stat.Country, stat.Count, bar, stat.Percentage))
	}

	b.WriteString("└─────────────────────────────────────────────┘\n")

	return b.String()
}

// RenderDetailedReport genera un informe ASCII exhaustivo con desglose de fuentes y matriz de conflictos.
func (s MetricsSnapshot) RenderDetailedReport() string {
	var b strings.Builder

	b.WriteString(s.RenderDashboard())
	b.WriteString("\n")

	// Detalle de Fuentes por País
	b.WriteString("DETALLE DE SOURCE POR PAÍS:\n")
	b.WriteString("─────────────────────────────────────────────\n")
	topCountries := 6
	if len(s.CountryDistribution) < topCountries {
		topCountries = len(s.CountryDistribution)
	}

	for i := 0; i < topCountries; i++ {
		cStat := s.CountryDistribution[i]
		b.WriteString(fmt.Sprintf("%s (%d registros):\n", cStat.Country, cStat.Count))
		if len(cStat.Sources) == 0 {
			b.WriteString("    (Sin desglose detallado)\n")
		} else {
			for src, cnt := range cStat.Sources {
				b.WriteString(fmt.Sprintf("    %-18s: %d\n", src, cnt))
			}
		}
	}
	b.WriteString("\n")

	// Resumen de Conflictos
	if len(s.TopConflicts) > 0 {
		b.WriteString("COUNTRY CONFLICTS (TOP):\n")
		b.WriteString("─────────────────────────────────────────────\n")
		topConf := 10
		if len(s.TopConflicts) < topConf {
			topConf = len(s.TopConflicts)
		}
		for i := 0; i < topConf; i++ {
			b.WriteString(fmt.Sprintf("  %-16s %d\n", s.TopConflicts[i].ConflictPair, s.TopConflicts[i].Count))
		}
		b.WriteString("─────────────────────────────────────────────\n")
	}

	return b.String()
}
