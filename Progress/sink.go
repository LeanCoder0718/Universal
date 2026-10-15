package progress

import (
	"fmt"
	"io"
	"sync"
)

// RecordItem representa un elemento registrado en el Dead-Letter Sink.
type RecordItem struct {
	Line        Line
	Attempt     int
	MaxAttempts int
	LastError   error
	Reason      string
}

// DeadLetterSink recibe y audita las tareas que han fallado definitivamente o han sido descartadas.
// Tiene un límite máximo de entradas para evitar crecimiento infinito de RAM en ejecuciones largas.
type DeadLetterSink struct {
	mu         sync.Mutex
	entries    []RecordItem
	maxEntries int
	writer     io.Writer
	onRecord   func(item RecordItem)
	evicted    uint64
}

// DefaultMaxDLXEntries es el límite por defecto de entradas en el DeadLetterSink.
const DefaultMaxDLXEntries = 10_000

// NewDeadLetterSink crea una instancia de DeadLetterSink con un escritor opcional.
func NewDeadLetterSink(writer io.Writer) *DeadLetterSink {
	return &DeadLetterSink{
		entries:    make([]RecordItem, 0, 128),
		maxEntries: DefaultMaxDLXEntries,
		writer:     writer,
	}
}

// NewDeadLetterSinkWithHandler crea una instancia de DeadLetterSink con un escritor y un callback handler opcional.
func NewDeadLetterSinkWithHandler(writer io.Writer, handler func(item RecordItem)) *DeadLetterSink {
	return &DeadLetterSink{
		entries:    make([]RecordItem, 0, 128),
		maxEntries: DefaultMaxDLXEntries,
		writer:     writer,
		onRecord:   handler,
	}
}

// SetMaxEntries configura el límite máximo de entradas almacenadas.
func (d *DeadLetterSink) SetMaxEntries(max int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if max > 0 {
		d.maxEntries = max
	}
}

// Record registra un fallo en el Dead-Letter Sink de forma thread-safe.
// Si se excede maxEntries, los registros más antiguos se descartan (FIFO eviction).
func (d *DeadLetterSink) Record(item RecordItem) {
	d.mu.Lock()
	if d.maxEntries > 0 && len(d.entries) >= d.maxEntries {
		// Evicción FIFO: descartar el 10% más antiguo de una vez para amortizar el costo
		evictCount := d.maxEntries / 10
		if evictCount < 1 {
			evictCount = 1
		}
		copy(d.entries, d.entries[evictCount:])
		d.entries = d.entries[:len(d.entries)-evictCount]
		d.evicted += uint64(evictCount)
	}
	d.entries = append(d.entries, item)
	w := d.writer
	cb := d.onRecord
	d.mu.Unlock()

	if w != nil {
		fmt.Fprintf(w, "[DLX] LineID=%d Attempt=%d/%d Reason=%s Err=%v Line=%s\n",
			item.Line.ID, item.Attempt, item.MaxAttempts, item.Reason, item.LastError, string(item.Line.Content))
	}
	if cb != nil {
		cb(item)
	}
}

// Count retorna la cantidad total de registros almacenados en el Dead-Letter Sink.
func (d *DeadLetterSink) Count() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.entries)
}

// Evicted retorna la cantidad de registros que han sido descartados por evicción.
func (d *DeadLetterSink) Evicted() uint64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.evicted
}

// Entries retorna una copia de los registros capturados.
func (d *DeadLetterSink) Entries() []RecordItem {
	d.mu.Lock()
	defer d.mu.Unlock()
	copied := make([]RecordItem, len(d.entries))
	copy(copied, d.entries)
	return copied
}
