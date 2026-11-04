//go:build windows

package progress

import (
	"context"
	"syscall"
	"time"
)

// StartKeyboardListener inicia la escucha no-bloqueante en una goroutine dedicada en Windows.
func (p *Pipeline) StartKeyboardListener(ctx context.Context, callbacks ...KeyboardActionCallback) {
	var cb KeyboardActionCallback
	if len(callbacks) > 0 && callbacks[0] != nil {
		cb = callbacks[0]
	}

	go func() {
		msvcrt := syscall.NewLazyDLL("msvcrt.dll")
		procKbhit := msvcrt.NewProc("_kbhit")
		procGetch := msvcrt.NewProc("_getch")

		ticker := time.NewTicker(30 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				r, _, _ := procKbhit.Call()
				if r == 0 {
					continue
				}

				chCall, _, _ := procGetch.Call()
				if chCall == 0 || chCall == 0xE0 {
					procGetch.Call()
					continue
				}

				ch := byte(chCall)
				switch ch {
				case 'p', 'P':
					p.Pause()
					if cb != nil {
						cb("PAUSE", p.CurrentWorkers())
					}
				case 'c', 'C':
					p.Resume()
					if cb != nil {
						cb("RESUME", p.CurrentWorkers())
					}
				case 's', 'S', 'g', 'G':
					_, _ = p.SavePending("", "")
					if cb != nil {
						cb("SAVE", p.CurrentWorkers())
					}
				case '+', '=':
					newW := p.AddWorkers(1)
					if cb != nil {
						cb("SCALE_UP", newW)
					}
				case '-', '_':
					newW := p.RemoveWorkers(1)
					if cb != nil {
						cb("SCALE_DOWN", newW)
					}
				}
			}
		}
	}()
}

