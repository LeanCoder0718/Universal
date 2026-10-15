package Universal

import (
	"RABBIT/Universal/Utils"
	"os"
	"os/exec"
	"syscall"
	"unsafe"
)

func ClearConsole() {
	var cmd *exec.Cmd
	cmd = exec.Command("cmd", "/c", "cls")

	cmd.Stdout = os.Stdout
	cmd.Run()
}

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

func ParteFinal() {
	Utils.ReadString(Utils.Red("\n\nPresiona Enter para salir..."))
}
