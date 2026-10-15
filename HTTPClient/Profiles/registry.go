package Profiles

import (
	"fmt"
	"sync"
)

// Registry es el catálogo centralizado de ProfileSpec disponibles.
//
// Diseño: Write-Once, Read-Many (WORM).
//   - Se construye una vez al arranque (Register).
//   - Se lee concurrentemente por N goroutines sin locks (los slices son
//     inmutables después de Freeze).
//
// Esto elimina completamente la contención de locks en el hot path
// (selección de perfil por goroutine).
type Registry struct {
	mu     sync.RWMutex
	frozen bool

	// Índice principal: todos los perfiles registrados.
	all []*ProfileSpec

	// Índices secundarios para búsqueda O(1) por plataforma y tipo.
	byPlatform   map[PlatformType][]*ProfileSpec
	byClient     map[ClientType][]*ProfileSpec
	byPlatClient map[platClientKey][]*ProfileSpec
	byID         map[string]*ProfileSpec
}

type platClientKey struct {
	platform PlatformType
	client   ClientType
}

// NewRegistry crea un Registry vacío listo para registrar perfiles.
func NewRegistry() *Registry {
	return &Registry{
		byPlatform:   make(map[PlatformType][]*ProfileSpec),
		byClient:     make(map[ClientType][]*ProfileSpec),
		byPlatClient: make(map[platClientKey][]*ProfileSpec),
		byID:         make(map[string]*ProfileSpec),
	}
}

// Register añade un ProfileSpec al catálogo. Solo puede llamarse ANTES
// de Freeze(). Después de Freeze(), cualquier intento de Register devuelve
// error (Fail-Fast para prevenir mutación post-inicialización).
//
// Retorna error si:
//   - El registry ya fue congelado.
//   - El perfil es nil.
//   - Ya existe un perfil con el mismo ID.
//   - El perfil no pasa la validación.
func (r *Registry) Register(spec *ProfileSpec) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.frozen {
		return fmt.Errorf("profiles: registry congelado, no se puede registrar %q", spec.ID())
	}
	if spec == nil {
		return fmt.Errorf("profiles: spec es nil")
	}

	// Validación antes de registrar (Fail-Fast)
	if err := Validate(spec); err != nil {
		return fmt.Errorf("profiles: validación fallida para %q: %w", spec.ID(), err)
	}

	if _, exists := r.byID[spec.id]; exists {
		return fmt.Errorf("profiles: perfil duplicado con ID %q", spec.id)
	}

	r.all = append(r.all, spec)
	r.byPlatform[spec.platform] = append(r.byPlatform[spec.platform], spec)
	r.byClient[spec.clientType] = append(r.byClient[spec.clientType], spec)

	key := platClientKey{platform: spec.platform, client: spec.clientType}
	r.byPlatClient[key] = append(r.byPlatClient[key], spec)
	r.byID[spec.id] = spec

	return nil
}

// RegisterAll registra múltiples perfiles de una vez. Se detiene al primer
// error (fail-fast).
func (r *Registry) RegisterAll(specs ...*ProfileSpec) error {
	for _, spec := range specs {
		if err := r.Register(spec); err != nil {
			return err
		}
	}
	return nil
}

// Freeze congela el registry, impidiendo registros futuros. A partir de
// este punto, todos los métodos de lectura (All, ByPlatform, etc.) son
// completamente thread-safe sin locks porque los slices subyacentes
// nunca se modificarán de nuevo.
//
// Diseño: llamar Freeze() en la inicialización, ANTES de lanzar goroutines.
func (r *Registry) Freeze() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.frozen = true
}

// IsFrozen indica si el registry está congelado.
func (r *Registry) IsFrozen() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.frozen
}

// --- Métodos de lectura (thread-safe post-Freeze, sin locks necesarios) ---

// All retorna todos los perfiles registrados. El slice retornado es de
// solo lectura: modificarlo es responsabilidad exclusiva del caller (no
// recomendado, usar append sobre el resultado ROMPE la inmutabilidad).
func (r *Registry) All() []*ProfileSpec {
	return r.all
}

// ByPlatform retorna los perfiles de una plataforma específica.
func (r *Registry) ByPlatform(p PlatformType) []*ProfileSpec {
	return r.byPlatform[p]
}

// ByClient retorna los perfiles de un tipo de cliente específico.
func (r *Registry) ByClient(c ClientType) []*ProfileSpec {
	return r.byClient[c]
}

// ByPlatformAndClient retorna los perfiles que coinciden con plataforma Y tipo.
func (r *Registry) ByPlatformAndClient(p PlatformType, c ClientType) []*ProfileSpec {
	return r.byPlatClient[platClientKey{platform: p, client: c}]
}

// ByID retorna un perfil por su ID único, o nil si no existe.
func (r *Registry) ByID(id string) *ProfileSpec {
	return r.byID[id]
}

// Count retorna la cantidad total de perfiles registrados.
func (r *Registry) Count() int {
	return len(r.all)
}

// ForMode retorna los perfiles candidatos según el ProfileMode seleccionado
// por el usuario. Es el puente entre la selección del usuario y el pool
// de perfiles elegibles.
func (r *Registry) ForMode(mode ProfileMode) []*ProfileSpec {
	switch mode {
	case Browser:
		// Todos los browsers de escritorio (Windows + macOS + Linux)
		totalLen := len(r.byPlatform[PlatformWindows]) + len(r.byPlatform[PlatformMacOS]) + len(r.byPlatform[PlatformLinux])
		result := make([]*ProfileSpec, 0, totalLen)
		result = append(result, r.byPlatform[PlatformWindows]...)
		result = append(result, r.byPlatform[PlatformMacOS]...)
		result = append(result, r.byPlatform[PlatformLinux]...)
		return result
	case Windows:
		return r.ByPlatform(PlatformWindows)
	case WindowsChrome:
		return r.ByPlatformAndClient(PlatformWindows, ClientChrome)
	case Android:
		return r.ByPlatform(PlatformAndroid)
	case IOS:
		return r.ByPlatform(PlatformIOS)
	default:
		return r.all
	}
}
