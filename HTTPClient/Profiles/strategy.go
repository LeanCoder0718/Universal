package Profiles

import (
	"crypto/rand"
	"encoding/binary"
	"sync/atomic"
)

// SelectionStrategy define cómo se elige un perfil del pool de candidatos.
// Cada estrategia es thread-safe para uso concurrente.
type SelectionStrategy interface {
	// Select elige un perfil del slice de candidatos.
	// El slice candidates NUNCA debe modificarse (inmutable post-Freeze).
	Select(candidates []*ProfileSpec) *ProfileSpec
}

// --- RoundRobin Strategy ---

// RoundRobinStrategy distribuye uniformemente los perfiles entre las
// goroutines usando un contador atómico. Es O(1), lock-free y garantiza
// distribución perfectamente equitativa.
//
// Ejemplo con 3 perfiles y 6 goroutines:
//
//	Worker 1 → Profile A
//	Worker 2 → Profile B
//	Worker 3 → Profile C
//	Worker 4 → Profile A
//	Worker 5 → Profile B
//	Worker 6 → Profile C
type RoundRobinStrategy struct {
	counter atomic.Uint64
}

// NewRoundRobin crea una estrategia RoundRobin.
func NewRoundRobin() *RoundRobinStrategy {
	return &RoundRobinStrategy{}
}

func (s *RoundRobinStrategy) Select(candidates []*ProfileSpec) *ProfileSpec {
	n := uint64(len(candidates))
	if n == 0 {
		return nil
	}
	idx := s.counter.Add(1) - 1
	return candidates[idx%n]
}

// --- Random Strategy ---

// RandomStrategy selecciona perfiles aleatoriamente usando un PRNG local
// per-goroutine (splitmix64). No usa math/rand global para evitar la
// contención del mutex interno de math/rand bajo alta concurrencia.
//
// Cada llamada a Select genera un número pseudoaleatorio con calidad
// suficiente para distribución (no criptográfico, pero sin sesgo).
type RandomStrategy struct {
	state atomic.Uint64
}

// NewRandom crea una estrategia Random con una semilla criptográficamente
// segura (crypto/rand).
func NewRandom() *RandomStrategy {
	s := &RandomStrategy{}
	seed := cryptoSeed()
	s.state.Store(seed)
	return s
}

func (s *RandomStrategy) Select(candidates []*ProfileSpec) *ProfileSpec {
	n := uint64(len(candidates))
	if n == 0 {
		return nil
	}
	val := s.next()
	return candidates[val%n]
}

// next genera el siguiente número pseudoaleatorio usando splitmix64.
// Usa atomic Add para ser thread-safe sin mutex (lock-free).
func (s *RandomStrategy) next() uint64 {
	// splitmix64: increment-then-mix, completamente lock-free.
	z := s.state.Add(0x9E3779B97F4A7C15)
	z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9
	z = (z ^ (z >> 27)) * 0x94D049BB133111EB
	return z ^ (z >> 31)
}

// --- Fixed Strategy ---

// FixedStrategy siempre retorna el primer perfil de los candidatos.
// Útil cuando el usuario selecciona un tipo específico y solo hay una
// variante, o para testing determinístico.
type FixedStrategy struct{}

func NewFixed() *FixedStrategy {
	return &FixedStrategy{}
}

func (s *FixedStrategy) Select(candidates []*ProfileSpec) *ProfileSpec {
	if len(candidates) == 0 {
		return nil
	}
	return candidates[0]
}

// --- WeightedRandom Strategy ---

// WeightedRandomStrategy permite asignar pesos a diferentes perfiles.
// Los perfiles con mayor peso se seleccionan con mayor frecuencia.
// Útil para distribuciones realistas donde Chrome domina el mercado.
type WeightedRandomStrategy struct {
	rng *RandomStrategy
}

func NewWeightedRandom() *WeightedRandomStrategy {
	return &WeightedRandomStrategy{rng: NewRandom()}
}

func (s *WeightedRandomStrategy) Select(candidates []*ProfileSpec) *ProfileSpec {
	if len(candidates) == 0 {
		return nil
	}
	// Heurística de pesos basada en market share real:
	// Chrome ~65%, Firefox ~5%, Safari ~18%, Edge ~5%, Brave ~2%, Opera ~2%, OkHTTP ~3%
	totalWeight := 0
	for _, p := range candidates {
		totalWeight += clientWeight(p.clientType)
	}
	if totalWeight == 0 {
		return candidates[0]
	}

	target := int(s.rng.next() % uint64(totalWeight))
	cumulative := 0
	for _, p := range candidates {
		cumulative += clientWeight(p.clientType)
		if target < cumulative {
			return p
		}
	}
	return candidates[len(candidates)-1]
}

// clientWeight asigna un peso relativo basado en la cuota de mercado real.
func clientWeight(c ClientType) int {
	switch c {
	case ClientChrome:
		return 65
	case ClientFirefox:
		return 5
	case ClientSafari, ClientSafariOS:
		return 18
	case ClientEdge:
		return 5
	case ClientBrave:
		return 2
	case ClientOpera:
		return 2
	case ClientOkHTTP:
		return 3
	default:
		return 1
	}
}

// --- Strategy Factory ---

// DefaultStrategyForMode retorna la estrategia más apropiada según el
// modo seleccionado por el usuario.
func DefaultStrategyForMode(mode ProfileMode) SelectionStrategy {
	switch mode {
	case Browser:
		return NewRoundRobin()
	case Android, IOS:
		return NewRoundRobin()
	default:
		return NewRoundRobin()
	}
}

// cryptoSeed genera una semilla de 64 bits criptográficamente segura.
func cryptoSeed() uint64 {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		// Fallback a valor arbitrario si crypto/rand falla (extremadamente raro)
		return 0xDEADBEEFCAFEBABE
	}
	return binary.LittleEndian.Uint64(b[:])
}
