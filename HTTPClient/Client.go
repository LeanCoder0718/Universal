package HTTPClient

import (
	"context"
)

// Get es un método de conveniencia para realizar una petición GET como navegación principal.
func (s *Session) Get(ctx context.Context, url string, headers map[string]string) (*Result, error) {
	return s.Do(ctx, Spec{
		Method: "GET",
		URL:    url,
		Kind:   KindDocument,
		Extra:  headers,
	})
}

// Post es un método de conveniencia para realizar una petición POST como navegación principal.
func (s *Session) Post(ctx context.Context, url string, body []byte, headers map[string]string) (*Result, error) {
	return s.Do(ctx, Spec{
		Method: "POST",
		URL:    url,
		Kind:   KindDocument,
		Body:   body,
		Extra:  headers,
	})
}

// GetXHR es un método de conveniencia para peticiones GET tipo API/Ajax (CORS).
func (s *Session) GetXHR(ctx context.Context, url string, headers map[string]string) (*Result, error) {
	return s.Do(ctx, Spec{
		Method: "GET",
		URL:    url,
		Kind:   KindXHR,
		Extra:  headers,
	})
}

// PostXHR es un método de conveniencia para peticiones POST tipo API/Ajax (CORS),
// ideal para enviar JSON.
func (s *Session) PostXHR(ctx context.Context, url string, body []byte, headers map[string]string) (*Result, error) {
	return s.Do(ctx, Spec{
		Method:      "POST",
		URL:         url,
		Kind:        KindXHR,
		Body:        body,
		ContentType: "application/json",
		Extra:       headers,
	})
}
