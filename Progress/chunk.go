package progress

// Line representa una línea individual leída del archivo con su metadato y payload.
type Line struct {
	ID      uint64      // Identificador secuencial único (1-indexed)
	Content []byte      // Payload binario/texto de la línea (excluyendo \r y \n)
	State   interface{} // Preserva el estado del procesado para reanudación por etapas en reintentos
}

// Chunk representa un bloque contiguo de N líneas asignado a un worker.
type Chunk struct {
	SequenceID  uint64 // ID de secuencia del chunk (1-indexed)
	StartOffset int64  // Offset en bytes de inicio en el archivo original
	EndOffset   int64  // Offset en bytes de fin en el archivo original
	Lines       []Line // Colección de líneas contenidas en el bloque
}
