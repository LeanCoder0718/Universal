//go:build !windows

package Utils

import (
	"fmt"
)

// ConsoleTitle actualiza el título de la terminal en sistemas POSIX/Unix vía secuencias ANSI.
func ConsoleTitle(title string) (int, error) {
	fmt.Printf("\033]0;%s\007", title)
	return 0, nil
}

