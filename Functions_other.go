//go:build !windows

package Universal

import (
	"fmt"
	"os"
	"os/exec"
)

// ClearConsole limpia la terminal en sistemas POSIX/Unix.
func ClearConsole() {
	cmd := exec.Command("clear")
	cmd.Stdout = os.Stdout
	if err := cmd.Run(); err != nil {
		fmt.Print("\033[H\033[2J")
	}
}

// ConsoleTitle actualiza el título de la terminal en sistemas POSIX/Unix vía secuencias ANSI.
func ConsoleTitle(title string) (int, error) {
	fmt.Printf("\033]0;%s\007", title)
	return 0, nil
}

