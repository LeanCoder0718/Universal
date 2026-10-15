package HTTPClient

import (
	"errors"
	"fmt"
)

// Errores centinela (Sentinel Errors) estándar para el subsistema HTTPClient.
// Permiten a los llamadores identificar de forma tipada la causa exacta del error
// usando `errors.Is(err, HTTPClient.Err...)`.
var (
	// ErrPoolClosed indica que se intentó usar un WorkerPool ya cerrado.
	ErrPoolClosed = errors.New("httpclient: pool is closed")

	// ErrSessionClosed indica que se intentó usar una Session no inicializada o cerrada.
	ErrSessionClosed = errors.New("httpclient: session is closed or uninitialized")

	// ErrMaxRetriesExceeded indica que se agotaron todos los reintentos configurados sin éxito.
	ErrMaxRetriesExceeded = errors.New("httpclient: max retry attempts exceeded")

	// ErrBackpressureTimeout indica que el contexto expiró o fue cancelado esperando un slot de concurrencia.
	ErrBackpressureTimeout = errors.New("httpclient: backpressure timeout waiting for concurrency slot")

	// ErrEmptyProxyPool indica que no hay proxies válidos disponibles en la lista provista.
	ErrEmptyProxyPool = errors.New("httpclient: proxy pool is empty or has no valid entries")

	// ErrNilProfileManager indica que el ProfileManager proporcionado es nil.
	ErrNilProfileManager = errors.New("httpclient: profile manager is nil")

	// ErrInvalidURL indica que la URL proporcionada no tiene un formato válido.
	ErrInvalidURL = errors.New("httpclient: invalid URL")

	// ErrEmptyResponseBody indica que el cuerpo de la respuesta HTTP está vacío.
	ErrEmptyResponseBody = errors.New("httpclient: empty response body")

	// ErrBodyExceedsLimit indica que el tamaño del cuerpo de la respuesta excedió el límite máximo configurado.
	ErrBodyExceedsLimit = errors.New("httpclient: response body exceeds configured size limit")

	// ErrRateLimitExceeded indica que se alcanzó el límite de tasa de peticiones por segundo.
	ErrRateLimitExceeded = errors.New("httpclient: rate limit exceeded")

	// ErrNilResponse indica que el resultado o la respuesta retornada es nil.
	ErrNilResponse = errors.New("httpclient: nil response")

	// ErrNoProxiesAvailable indica que no hay proxies saludables o disponibles según la política de asignación.
	ErrNoProxiesAvailable = errors.New("httpclient: no proxies available in pool")
)

// RequestError enriquece un error con el contexto detallado de la petición (intento, proxy, URL).
type RequestError struct {
	URL      string
	Method   string
	Attempt  int
	ProxyURL string
	Err      error
}

func (e *RequestError) Error() string {
	if e.ProxyURL != "" {
		return fmt.Sprintf("httpclient [%s %s] attempt %d (proxy %s): %v", e.Method, e.URL, e.Attempt, e.ProxyURL, e.Err)
	}
	return fmt.Sprintf("httpclient [%s %s] attempt %d: %v", e.Method, e.URL, e.Attempt, e.Err)
}

func (e *RequestError) Unwrap() error {
	return e.Err
}
