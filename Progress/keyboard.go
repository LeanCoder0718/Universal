package progress

import (
	"context"
	"syscall"
	"time"
)

// KeyboardActionCallback define una función opcional para notificar a la UI o logs
// cuando el usuario presiona una tecla de control en caliente.
type KeyboardActionCallback func(action string, currentWorkers int)

// StartKeyboardListener inicia la escucha no-bloqueante en una goroutine dedicada para:
//   - 'p' / 'P': Pausar pipeline en caliente (WaitIfPaused).
//   - 'c' / 'C': Continuar / Reanudar pipeline en caliente.
//   - 's' / 'S': Guardar pendientes a disco (SavePending) de forma segura.
//   - '+' / '=': Incrementar concurrencia (+10 workers en caliente).
//   - '-' / '_': Reducir concurrencia (-10 workers en caliente de forma suave).
//
// StartKeyboardListener inicia la escucha no-bloqueante en una goroutine dedicada.
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
