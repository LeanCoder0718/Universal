package Utils

import (
	"fmt"
	"math"
	"sync/atomic"
	"time"

	"github.com/RabbitCoder2023/RABCOUNT"
)

// Contador concurrente
func Contador(rc *RABCOUNT.RABCOUNT, combo, estado string, variable *int64) {
	atomic.AddInt64(&CheckCounter, 1)
	if rc != nil {
		rc.Incr(1)
	}
	if variable != nil {
		atomic.AddInt64(variable, 1)
	}

	SaveResult(estado, combo)
}

// ProgressManager calcula el progreso real y tiempo estimado restante (Time Left).
type ProgressManager struct {
	rc        *RABCOUNT.RABCOUNT
	total     int64
	startTime time.Time
	endTime   *time.Time
}

func NewProgressManager(r *RABCOUNT.RABCOUNT, total int64) *ProgressManager {
	if total <= 0 {
		total = ComboLoaded
	}
	return &ProgressManager{rc: r, total: total}
}

func (pm *ProgressManager) Start() {
	pm.startTime = time.Now()
	pm.endTime = nil
}

func (pm *ProgressManager) Stop() {
	t := time.Now()
	pm.endTime = &t
}

func (pm *ProgressManager) getTotal() int64 {
	if pm.total > 0 {
		return pm.total
	}
	return atomic.LoadInt64(&ComboLoaded)
}

// Progress retorna el porcentaje de avance entre 0.0 y 1.0 (0% a 100%).
func (pm *ProgressManager) Progress() float64 {
	total := pm.getTotal()
	if total <= 0 {
		return 0.0
	}
	checked := atomic.LoadInt64(&CheckCounter)
	ratio := float64(checked) / float64(total)
	return math.Min(math.Max(ratio, 0.0), 1.0)
}

func (pm *ProgressManager) Elapsed() time.Duration {
	if pm.endTime != nil {
		return pm.endTime.Sub(pm.startTime)
	}
	if pm.startTime.IsZero() {
		return 0
	}
	return time.Since(pm.startTime)
}

// Remaining calcula con precisión el tiempo restante (Time Left) según el avance real y la velocidad.
func (pm *ProgressManager) Remaining() time.Duration {
	total := pm.getTotal()
	checked := atomic.LoadInt64(&CheckCounter)
	if total <= 0 || checked >= total {
		return 0
	}

	remainingItems := total - checked
	if remainingItems <= 0 {
		return 0
	}

	var cpm float64
	if pm.rc != nil {
		cpm = float64(pm.rc.CPM())
	}

	// Si el CPM móvil instantáneo aún no está disponible o es inestable,
	// calcular con la velocidad promedio desde el inicio de la ejecución.
	elapsed := pm.Elapsed()
	if cpm <= 10 && elapsed.Seconds() > 1 && checked > 0 {
		cpm = (float64(checked) / elapsed.Seconds()) * 60.0
	}

	if cpm <= 0 {
		return 0
	}

	minutes := float64(remainingItems) / cpm
	seconds := minutes * 60.0

	if math.IsNaN(seconds) || math.IsInf(seconds, 0) || seconds <= 0 {
		return 0
	}

	// Límite superior razonable (máximo 99 horas)
	if seconds > 356400 {
		seconds = 356400
	}

	return time.Duration(seconds * float64(time.Second))
}

func (pm *ProgressManager) ETA() time.Time {
	rem := pm.Remaining()
	if rem <= 0 {
		return time.Time{}
	}
	return time.Now().Add(rem)
}

// FormatDuration formatea una duración en formato legible HH:MM:SS.
func FormatDuration(d time.Duration) string {
	if d <= 0 {
		return "00:00:00"
	}
	secs := int64(d.Seconds())
	h := secs / 3600
	m := (secs % 3600) / 60
	s := secs % 60
	return fmt.Sprintf("%02d:%02d:%02d", h, m, s)
}
