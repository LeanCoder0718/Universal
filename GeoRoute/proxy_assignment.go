package GeoRoute

import (
	"errors"
	"fmt"
	"sync/atomic"
)

var (
	ErrNoProxiesAvailable = errors.New("georoute: no hay proxies configurados en el pool")
)

// ProxyAssignmentEngine administra la asignación y transformación regional de proxies.
type ProxyAssignmentEngine struct {
	baseProxies []ParsedProxy
	provider    ProxyProvider
	rrIndex     atomic.Uint64
}

// NewProxyAssignmentEngine crea una nueva instancia de asignación de proxies.
func NewProxyAssignmentEngine(rawProxies []string, provider ProxyProvider) (*ProxyAssignmentEngine, error) {
	if provider == nil {
		provider = NewTemplateProvider(DefaultCRProviderConfig())
	}

	parsedList := make([]ParsedProxy, 0, len(rawProxies))
	for _, raw := range rawProxies {
		p, err := ParseProxy(raw)
		if err != nil {
			continue // Omitir entradas malformadas sin causar panic
		}
		parsedList = append(parsedList, p)
	}

	return &ProxyAssignmentEngine{
		baseProxies: parsedList,
		provider:    provider,
	}, nil
}

// TotalProxies retorna la cantidad de proxies base válidos en el engine.
func (e *ProxyAssignmentEngine) TotalProxies() int {
	return len(e.baseProxies)
}

// Assign selecciona el siguiente proxy base en Round-Robin y le aplica la transformación para el país dado.
func (e *ProxyAssignmentEngine) Assign(country CountryCode) (string, error) {
	n := len(e.baseProxies)
	if n == 0 {
		return "", ErrNoProxiesAvailable
	}

	// 1. Selección Round-Robin atómica
	idx := int(e.rrIndex.Add(1)-1) % n
	baseProxy := e.baseProxies[idx]

	// 2. Transformar según el país (si country == Unknown se genera proxy general)
	transformed, err := e.provider.Transform(baseProxy, country)
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrProxyTransformation, err)
	}

	return transformed, nil
}
