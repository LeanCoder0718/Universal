package progress

import (
	"github.com/RabbitCoder2023/RABCOUNT"
)

// RABCountAdapter adapta la librería externa RABCOUNT a la interfaz MetricsCounterInterface.
type RABCountAdapter struct {
	counter *RABCOUNT.RABCOUNT
}

// NewRABCountAdapter crea un nuevo adaptador sobre la librería RABCOUNT.
func NewRABCountAdapter() *RABCountAdapter {
	return &RABCountAdapter{
		counter: RABCOUNT.NewRABCOUNT(),
	}
}

// RawCounter retorna el objeto *RABCOUNT.RABCOUNT interno.
func (r *RABCountAdapter) RawCounter() *RABCOUNT.RABCOUNT {
	return r.counter
}

// Add incrementa el contador por N unidades.
func (r *RABCountAdapter) Add(n uint64) {
	if r.counter != nil {
		r.counter.Incr(int64(n))
	}
}

// CPM retorna el CPM (Conexiones/Líneas por Minuto) actual.
func (r *RABCountAdapter) CPM() uint64 {
	if r.counter != nil {
		cpm := r.counter.CPM()
		if cpm < 0 {
			return 0
		}
		return uint64(cpm)
	}
	return 0
}

// MaxCPM retorna el CPM máximo histórico registrado.
func (r *RABCountAdapter) MaxCPM() uint64 {
	if r.counter != nil {
		maxCpm := r.counter.MaxCPM()
		if maxCpm < 0 {
			return 0
		}
		return uint64(maxCpm)
	}
	return 0
}

// ResetCPM reinicia el cálculo de CPM.
func (r *RABCountAdapter) ResetCPM() {}
