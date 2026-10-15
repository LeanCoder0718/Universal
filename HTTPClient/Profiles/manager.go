package Profiles

import (
	"context"
	"fmt"
	"sync/atomic"
)

// Manager es el orquestador principal del sistema de perfiles.
//
// Flujo de vida:
//  1. Se crea con New() y un Registry ya poblado+congelado.
//  2. Las goroutines llaman a GetProfile() concurrentemente.
//  3. GetProfile() selecciona un perfil candidato usando la estrategia
//     configurada y lo retorna. El perfil es inmutable: no hay "release"
//     ni "return to pool" porque no hay estado mutable que limpiar.
//
// Performance:
//   - GetProfile() es O(1) (RoundRobin) o O(N) (WeightedRandom, N pequeño).
//   - Zero allocations en el hot path (no se crea copia del ProfileSpec).
//   - Zero locks en el hot path (Registry está congelado, estrategias son lock-free).
//
// Thread-safety:
//   - Seguro para uso concurrente por 10, 100, 1.000+ goroutines.
//   - No hay global mutable state.
//   - No hay maps compartidos sin sincronización.
//   - No hay punteros reutilizados incorrectamente.
type Manager struct {
	registry   *Registry
	candidates []*ProfileSpec
	strategy   SelectionStrategy
	mode       ProfileMode

	assignedCount atomic.Uint64
	failedCount   atomic.Uint64
}

// ManagerConfig define los parámetros de construcción del Manager.
type ManagerConfig struct {
	Registry *Registry
	Mode     ProfileMode
	Strategy SelectionStrategy // nil = usa la estrategia por defecto para el modo
}

// NewManager crea un Manager validado y listo para uso concurrente.
//
// Precondiciones (Fail-Fast):
//   - El Registry debe estar congelado (Freeze llamado).
//   - Debe haber al menos un perfil candidato para el modo seleccionado.
func NewManager(cfg ManagerConfig) (*Manager, error) {
	if cfg.Registry == nil {
		return nil, fmt.Errorf("profiles: registry es nil")
	}
	if !cfg.Registry.IsFrozen() {
		return nil, fmt.Errorf("profiles: registry debe estar congelado (Freeze) antes de crear el Manager")
	}
	if cfg.Registry.Count() == 0 {
		return nil, fmt.Errorf("profiles: registry vacío, no hay perfiles registrados")
	}

	candidates := cfg.Registry.ForMode(cfg.Mode)
	if len(candidates) == 0 {
		return nil, fmt.Errorf("profiles: no hay perfiles disponibles para el modo %q", cfg.Mode)
	}

	strategy := cfg.Strategy
	if strategy == nil {
		strategy = DefaultStrategyForMode(cfg.Mode)
	}

	return &Manager{
		registry:   cfg.Registry,
		candidates: candidates,
		strategy:   strategy,
		mode:       cfg.Mode,
	}, nil
}

// GetProfile selecciona y retorna un perfil según la estrategia configurada.
//
// Esta función es el HOT PATH del sistema: se llama una vez por cada
// combo/tarea procesada. Por diseño:
//   - No aloca memoria (retorna puntero existente).
//   - No adquiere locks (Registry congelado, estrategia lock-free).
//   - No hace I/O.
//   - Respeta cancelación de contexto.
//
// El ProfileSpec retornado es INMUTABLE. La goroutine puede leerlo
// libremente sin sincronización. Nunca debe modificar sus campos.
func (m *Manager) GetProfile(ctx context.Context) (*ProfileSpec, error) {
	// Verificar cancelación para evitar trabajo innecesario.
	if err := ctx.Err(); err != nil {
		m.failedCount.Add(1)
		return nil, fmt.Errorf("profiles: contexto cancelado: %w", err)
	}

	spec := m.strategy.Select(m.candidates)
	if spec == nil {
		m.failedCount.Add(1)
		return nil, fmt.Errorf("profiles: estrategia retornó nil (sin candidatos para modo %q)", m.mode)
	}

	m.assignedCount.Add(1)
	return spec, nil
}

// Metrics retorna los contadores de perfiles asignados y fallos de selección.
func (m *Manager) Metrics() (assigned, failed uint64) {
	return m.assignedCount.Load(), m.failedCount.Load()
}

// Mode retorna el modo de perfil configurado.
func (m *Manager) Mode() ProfileMode {
	return m.mode
}

// CandidateCount retorna cuántos perfiles están disponibles para la
// estrategia actual.
func (m *Manager) CandidateCount() int {
	return len(m.candidates)
}

// Candidates retorna la lista de perfiles candidatos (solo lectura).
func (m *Manager) Candidates() []*ProfileSpec {
	return m.candidates
}

// Registry retorna el Registry subyacente.
func (m *Manager) Registry() *Registry {
	return m.registry
}
