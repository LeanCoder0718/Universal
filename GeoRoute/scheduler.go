package GeoRoute

import (
	"container/heap"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// SchedulingMode define el algoritmo o política de ordenamiento de los grupos de país.
type SchedulingMode string

const (
	// ModeLargestFirst procesa primero los grupos con mayor cantidad de registros (PE -> US -> XX).
	ModeLargestFirst SchedulingMode = "LARGEST_GROUP_FIRST"
	// ModeSmallestFirst procesa primero los grupos con menor cantidad de registros.
	ModeSmallestFirst SchedulingMode = "SMALLEST_GROUP_FIRST"
	// ModeOriginalOrder mantiene la secuencia original de los registros sin reordenar.
	ModeOriginalOrder SchedulingMode = "ORIGINAL_ORDER"
	// ModeAlphabetical ordena los grupos alfabéticamente por código de país (BR -> ES -> PE -> US -> XX).
	ModeAlphabetical SchedulingMode = "ALPHABETICAL"
	// ModeCustomPriority utiliza prioridades manuales explícitas asignadas por el usuario.
	ModeCustomPriority SchedulingMode = "CUSTOM_PRIORITY"
	// ModeFairShare distribuye el procesamiento de forma proporcional (Weighted Deficit Round Robin) para evitar starvation.
	ModeFairShare SchedulingMode = "FAIR_SHARE"
	// ModePriorityAging combina prioridad por tamaño con envejecimiento para asegurar que los grupos pequeños avancen.
	ModePriorityAging SchedulingMode = "PRIORITY_AGING"
)

// GroupState representa el ciclo de vida de un grupo de país dentro del scheduler.
type GroupState string

const (
	GroupStatePending   GroupState = "PENDING"
	GroupStateActive    GroupState = "ACTIVE"
	GroupStateCompleted GroupState = "COMPLETED"
	GroupStatePaused    GroupState = "PAUSED"
	GroupStateError     GroupState = "ERROR"
)

// SchedulerConfig contiene todas las opciones de configuración para el GeoScheduler.
type SchedulerConfig struct {
	Mode                SchedulingMode      // Política de scheduling activa
	CustomPriorities    map[CountryCode]int // Prioridades manuales (1 = máxima prioridad)
	UnknownPriority     int                 // Prioridad asignada al grupo UNKNOWN / XX (default: 999)
	EnableFairness      bool                // Activa interleaving proporcional anti-starvation
	EnablePriorityAging bool                // Activa envejecimiento dinámico de colas
	AgingThreshold      int                 // Cantidad de ráfagas antes de elevar la prioridad
	Quantum             int                 // Tamaño del quantum de registros en modo FairShare (default: 50)
	ChunkSize           int                 // Tamaño de chunk emitido hacia los workers (default: 50)
}

// DefaultSchedulerConfig genera la configuración recomendada de fábrica para alta estabilidad y throughput.
func DefaultSchedulerConfig() SchedulerConfig {
	return SchedulerConfig{
		Mode:                ModeLargestFirst,
		CustomPriorities:    make(map[CountryCode]int),
		UnknownPriority:     999,
		EnableFairness:      false,
		EnablePriorityAging: false,
		AgingThreshold:      5,
		Quantum:             50,
		ChunkSize:           50,
	}
}

// GeoGroup representa un grupo de registros clasificados bajo un mismo país.
type GeoGroup struct {
	Country         CountryCode
	TotalItems      int
	PendingItems    atomic.Int64
	ProcessingItems atomic.Int64
	CompletedItems  atomic.Int64
	FailedItems     atomic.Int64
	Priority        int
	CustomPriority  int
	Age             int
	Deficit         int
	State           GroupState
	Items           []string
	ReadIdx         int
	Index           int // Índice interno para el heap container/heap
	mu              sync.RWMutex
}

// GroupSummary proporciona una instantánea del estado de un grupo para reportes y previsualización.
type GroupSummary struct {
	Country    CountryCode `json:"country"`
	Total      int         `json:"total"`
	Pending    int64       `json:"pending"`
	Completed  int64       `json:"completed"`
	Failed     int64       `json:"failed"`
	Priority   int         `json:"priority"`
	State      GroupState  `json:"state"`
	Percentage float64     `json:"percentage"`
}

// GeoScheduler orquesta el encolamiento, priorización y despacho de registros clasificados por país.
type GeoScheduler struct {
	mu            sync.RWMutex
	cfg           SchedulerConfig
	detector      *CountryDetector
	groups        map[CountryCode]*GeoGroup
	groupList     []*GeoGroup
	pq            *groupPriorityQueue
	totalLoaded   int
	processed     atomic.Int64
	activeCountry CountryCode
	startTime     time.Time
}

// NewGeoScheduler crea una nueva instancia del programador inteligente por grupos.
func NewGeoScheduler(detector *CountryDetector, cfg SchedulerConfig) *GeoScheduler {
	if detector == nil {
		detector = NewCountryDetector(50000)
	}
	if cfg.ChunkSize <= 0 {
		cfg.ChunkSize = 50
	}
	if cfg.Quantum <= 0 {
		cfg.Quantum = 50
	}
	if cfg.UnknownPriority <= 0 {
		cfg.UnknownPriority = 999
	}

	return &GeoScheduler{
		cfg:       cfg,
		detector:  detector,
		groups:    make(map[CountryCode]*GeoGroup),
		groupList: make([]*GeoGroup, 0),
		pq:        &groupPriorityQueue{},
	}
}

// LoadLines clasifica un slice completo de registros y construye los grupos internos en O(N).
func (s *GeoScheduler) LoadLines(lines []string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.groups = make(map[CountryCode]*GeoGroup)
	s.groupList = make([]*GeoGroup, 0)
	s.totalLoaded = 0
	s.processed.Store(0)

	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}

		cc := s.detector.Detect(trimmed)
		if cc == "" {
			cc = Unknown
		}

		grp, exists := s.groups[cc]
		if !exists {
			customPrio := s.cfg.CustomPriorities[cc]
			if customPrio <= 0 {
				if cc.IsUnknown() {
					customPrio = s.cfg.UnknownPriority
				} else {
					customPrio = 500
				}
			}

			grp = &GeoGroup{
				Country:        cc,
				CustomPriority: customPrio,
				State:          GroupStatePending,
				Items:          make([]string, 0, 128),
			}
			s.groups[cc] = grp
			s.groupList = append(s.groupList, grp)
		}

		grp.Items = append(grp.Items, trimmed)
		grp.TotalItems++
	}

	for _, grp := range s.groups {
		grp.PendingItems.Store(int64(grp.TotalItems))
		s.totalLoaded += grp.TotalItems
	}

	s.buildScheduleLocked()
}

// buildScheduleLocked calcula las prioridades de los grupos según la política configurada.
func (s *GeoScheduler) buildScheduleLocked() {
	if len(s.groupList) == 0 {
		return
	}

	switch s.cfg.Mode {
	case ModeLargestFirst:
		// Orden descendente por TotalItems; si empatan, conocidos antes que UNKNOWN
		sort.SliceStable(s.groupList, func(i, j int) bool {
			if s.groupList[i].TotalItems != s.groupList[j].TotalItems {
				return s.groupList[i].TotalItems > s.groupList[j].TotalItems
			}
			if s.groupList[i].Country.IsUnknown() != s.groupList[j].Country.IsUnknown() {
				return !s.groupList[i].Country.IsUnknown()
			}
			return s.groupList[i].Country < s.groupList[j].Country
		})

	case ModeSmallestFirst:
		// Orden ascendente por TotalItems
		sort.SliceStable(s.groupList, func(i, j int) bool {
			if s.groupList[i].TotalItems != s.groupList[j].TotalItems {
				return s.groupList[i].TotalItems < s.groupList[j].TotalItems
			}
			return s.groupList[i].Country < s.groupList[j].Country
		})

	case ModeAlphabetical:
		// Orden alfabético por código de país (UNKNOWN / XX al final)
		sort.SliceStable(s.groupList, func(i, j int) bool {
			if s.groupList[i].Country.IsUnknown() != s.groupList[j].Country.IsUnknown() {
				return !s.groupList[i].Country.IsUnknown()
			}
			return s.groupList[i].Country < s.groupList[j].Country
		})

	case ModeCustomPriority:
		// Orden ascendente por CustomPriority (1 es mayor prioridad)
		sort.SliceStable(s.groupList, func(i, j int) bool {
			if s.groupList[i].CustomPriority != s.groupList[j].CustomPriority {
				return s.groupList[i].CustomPriority < s.groupList[j].CustomPriority
			}
			return s.groupList[i].TotalItems > s.groupList[j].TotalItems
		})

	case ModeFairShare, ModePriorityAging:
		// Para FairShare o Aging, orden base por tamaño inicial y pesos proporcionales
		sort.SliceStable(s.groupList, func(i, j int) bool {
			return s.groupList[i].TotalItems > s.groupList[j].TotalItems
		})
	}

	// Asignar número de prioridad de 1 a N
	for idx, grp := range s.groupList {
		grp.Priority = idx + 1
		grp.Deficit = 0
		grp.Age = 0
	}

	// Inicializar la Priority Queue (Heap)
	s.pq = &groupPriorityQueue{}
	for _, grp := range s.groupList {
		heap.Push(s.pq, grp)
	}
}

// ReorderCombos reorganiza la lista plana de combos aplicando la política de scheduling activa.
// No altera el orden interno de las líneas de cada país, solo la secuencia de los grupos.
func (s *GeoScheduler) ReorderCombos(lines []string) []string {
	s.LoadLines(lines)

	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.totalLoaded == 0 {
		return []string{}
	}

	if s.cfg.Mode == ModeOriginalOrder || len(lines) <= 1 || len(s.groupList) <= 1 {
		return lines
	}

	// Si el modo es FairShare, entrelazar los registros según Deficit Round Robin
	if s.cfg.Mode == ModeFairShare || s.cfg.EnableFairness {
		return s.interleaveFairShareLocked()
	}

	// Para LargestFirst, SmallestFirst, CustomPriority, Alphabetical: concatenación secuencial de grupos
	out := make([]string, 0, len(lines))
	for _, grp := range s.groupList {
		out = append(out, grp.Items...)
	}

	return out
}

// interleaveFairShareLocked realiza un entrelazado ponderado de los registros de los países para evitar inanición.
func (s *GeoScheduler) interleaveFairShareLocked() []string {
	out := make([]string, 0, s.totalLoaded)

	// Crear copias de índices de lectura
	cursors := make([]int, len(s.groupList))
	remCount := s.totalLoaded

	quantum := s.cfg.Quantum
	if quantum <= 0 {
		quantum = 50
	}

	for remCount > 0 {
		progressMade := false
		for i, grp := range s.groupList {
			cur := cursors[i]
			if cur >= len(grp.Items) {
				continue
			}

			// Despachar hasta 'quantum' ítems del grupo actual
			end := cur + quantum
			if end > len(grp.Items) {
				end = len(grp.Items)
			}

			chunk := grp.Items[cur:end]
			out = append(out, chunk...)
			cursors[i] = end
			remCount -= len(chunk)
			progressMade = true
		}

		if !progressMade {
			break
		}
	}

	return out
}

// GetProcessingOrder retorna la lista ordenada de grupos con sus estadísticas actuales.
func (s *GeoScheduler) GetProcessingOrder() []GroupSummary {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]GroupSummary, len(s.groupList))
	for i, grp := range s.groupList {
		pending := grp.PendingItems.Load()
		completed := grp.CompletedItems.Load()
		failed := grp.FailedItems.Load()

		var pct float64
		if grp.TotalItems > 0 {
			pct = (float64(completed+failed) / float64(grp.TotalItems)) * 100.0
		}

		result[i] = GroupSummary{
			Country:    grp.Country,
			Total:      grp.TotalItems,
			Pending:    pending,
			Completed:  completed,
			Failed:     failed,
			Priority:   grp.Priority,
			State:      grp.State,
			Percentage: pct,
		}
	}
	return result
}

// RenderOrderPreview genera la tabla de previsualización para la consola antes de comenzar la ejecución.
func (s *GeoScheduler) RenderOrderPreview() string {
	orders := s.GetProcessingOrder()
	if len(orders) == 0 {
		return " [GEO SCHEDULER] No hay grupos registrados.\n"
	}

	var sb strings.Builder
	sb.WriteString("\n ┌────────────────────────────────────────────────────────────┐\n")
	sb.WriteString(fmt.Sprintf(" │  GEO SCHEDULER — ORDER PREVIEW (MODE: %-20s)│\n", s.cfg.Mode))
	sb.WriteString(" ├─────┬───────────┬──────────────┬───────────┬───────────────┤\n")
	sb.WriteString(" │ POS │ PAÍS      │ REGISTROS    │ PRIORIDAD │ POLÍTICA      │\n")
	sb.WriteString(" ├─────┼───────────┼──────────────┼───────────┼───────────────┤\n")

	for i, ord := range orders {
		countryName := string(ord.Country)
		if ord.Country.IsUnknown() {
			countryName = "XX (UNKNOWN)"
		}

		sb.WriteString(fmt.Sprintf(" │ %-3d │ %-9s │ %-12d │ %-9d │ %-13s │\n",
			i+1, countryName, ord.Total, ord.Priority, s.cfg.Mode))
	}
	sb.WriteString(" └─────┴───────────┴──────────────┴───────────┴───────────────┘\n")

	return sb.String()
}

// RenderDashboard genera el panel de control de progreso en tiempo real.
func (s *GeoScheduler) RenderDashboard() string {
	orders := s.GetProcessingOrder()
	if len(orders) == 0 {
		return ""
	}

	s.mu.RLock()
	active := s.activeCountry
	if active == "" && len(orders) > 0 {
		active = orders[0].Country
	}
	s.mu.RUnlock()

	var sb strings.Builder
	sb.WriteString("\n ┌────────────────────────────────────────────────────────────────────────┐\n")
	sb.WriteString(fmt.Sprintf(" │ GEO SCHEDULER DASHBOARD | MODO: %-15s | ACTIVO: %-10s │\n", s.cfg.Mode, active))
	sb.WriteString(" ├─────────┬────────┬───────────┬───────────┬───────────┬─────────────────┤\n")
	sb.WriteString(" │ PAÍS    │ TOTAL  │ LISTOS    │ RESTANTES │ FALLIDOS  │ ESTADO          │\n")
	sb.WriteString(" ├─────────┼────────┼───────────┼───────────┼───────────┼─────────────────┤\n")

	for _, ord := range orders {
		cName := string(ord.Country)
		if ord.Country.IsUnknown() {
			cName = "XX"
		}

		stateStr := string(ord.State)
		if ord.Pending == 0 && ord.Total > 0 {
			stateStr = "COMPLETED"
		} else if ord.Country == active {
			stateStr = "ACTIVE"
		}

		sb.WriteString(fmt.Sprintf(" │ %-7s │ %-6d │ %-9d │ %-9d │ %-9d │ %-15s │\n",
			cName, ord.Total, ord.Completed, ord.Pending, ord.Failed, stateStr))
	}
	sb.WriteString(" └─────────┴────────┴───────────┴───────────┴───────────┴─────────────────┘\n")

	return sb.String()
}

// RecordProgress registra el avance de un país de forma atómica y segura para concurrencia.
func (s *GeoScheduler) RecordProgress(country CountryCode, completed int, failed int) {
	s.mu.RLock()
	grp, exists := s.groups[country]
	s.mu.RUnlock()

	if !exists || grp == nil {
		return
	}

	delta := int64(completed + failed)
	if delta > 0 {
		grp.CompletedItems.Add(int64(completed))
		grp.FailedItems.Add(int64(failed))
		grp.PendingItems.Add(-delta)
		s.processed.Add(delta)

		if grp.PendingItems.Load() <= 0 {
			grp.mu.Lock()
			grp.State = GroupStateCompleted
			grp.mu.Unlock()
		}
	}
}

// SetActiveCountry actualiza el país en procesamiento actual para el dashboard.
func (s *GeoScheduler) SetActiveCountry(cc CountryCode) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.activeCountry = cc
	if grp, ok := s.groups[cc]; ok {
		grp.mu.Lock()
		grp.State = GroupStateActive
		grp.mu.Unlock()
	}
}

// GroupCount retorna el número de grupos clasificados.
func (s *GeoScheduler) GroupCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.groupList)
}

// TotalLines retorna el total de líneas administradas por el scheduler.
func (s *GeoScheduler) TotalLines() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.totalLoaded
}

// ============================================================================
// Priority Queue interna (container/heap) para ordenamiento dinámico
// ============================================================================

type groupPriorityQueue []*GeoGroup

func (pq groupPriorityQueue) Len() int { return len(pq) }

func (pq groupPriorityQueue) Less(i, j int) bool {
	// Menor valor de Priority significa mayor precedencia de despacho
	if pq[i].Priority != pq[j].Priority {
		return pq[i].Priority < pq[j].Priority
	}
	return pq[i].TotalItems > pq[j].TotalItems
}

func (pq groupPriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].Index = i
	pq[j].Index = j
}

func (pq *groupPriorityQueue) Push(x any) {
	n := len(*pq)
	item := x.(*GeoGroup)
	item.Index = n
	*pq = append(*pq, item)
}

func (pq *groupPriorityQueue) Pop() any {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.Index = -1
	*pq = old[0 : n-1]
	return item
}
