package progress

// KeyboardActionCallback define una función opcional para notificar a la UI o logs
// cuando el usuario presiona una tecla de control en caliente.
type KeyboardActionCallback func(action string, currentWorkers int)
