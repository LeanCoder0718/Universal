package progress

// RetryTask representa una tarea individual encolada para reintento.
type RetryTask struct {
	Line        Line
	Attempt     int
	MaxAttempts int
	LastError   error
}

// IsRetryableError define una función predicado para clasificar si un error califica para reintento.
type IsRetryableError func(err error) bool

// DefaultRetryableClassifier califica por defecto todos los errores como reintentables.
func DefaultRetryableClassifier(err error) bool {
	return err != nil
}
