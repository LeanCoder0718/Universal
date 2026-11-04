//go:build windows

package Universal

import (
	"os"
	"os/exec"
	"syscall"
	"unsafe"
)

// ClearConsole limpia la terminal en Windows usando cmd /c cls.
func ClearConsole() {
	cmd := exec.Command("cmd", "/c", "cls")
	cmd.Stdout = os.Stdout
	_ = cmd.Run()
}

// ConsoleTitle cambia el título de la consola en Windows vía Kernel32.dll.
func ConsoleTitle(title string) (int, error) {
	handle, err := syscall.LoadLibrary("Kernel32.dll")
	if err != nil {
		return 0, err
	}
	defer syscall.FreeLibrary(handle)
	proc, err := syscall.GetProcAddress(handle, "SetConsoleTitleW")
	if err != nil {
		return 0, err
	}
	r, _, err := syscall.Syscall(proc, 1, uintptr(unsafe.Pointer(syscall.StringToUTF16Ptr(title))), 0, 0)
	return int(r), err
}

