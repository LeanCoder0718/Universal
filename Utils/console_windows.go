//go:build windows

package Utils

import (
	"syscall"
	"unsafe"
)

// ConsoleTitle actualiza el título de la consola de Windows.
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
	r, _, err := syscall.SyscallN(proc, uintptr(unsafe.Pointer(syscall.StringToUTF16Ptr(title))))
	return int(r), err
}

