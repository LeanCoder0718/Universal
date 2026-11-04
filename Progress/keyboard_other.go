//go:build !windows

package progress

import "context"

// StartKeyboardListener en plataformas no-Windows es un no-op seguro para entornos sin msvcrt.dll.
func (p *Pipeline) StartKeyboardListener(ctx context.Context, callbacks ...KeyboardActionCallback) {
	// No-op en Linux/macOS
}

