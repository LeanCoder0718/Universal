package Universal

import (
	"github.com/LeanCoder0718/Universal/Utils"
)

func ParteFinal() {
	Utils.ReadString(Utils.Red("\n\nPresiona Enter para salir..."))
}

// Unescape desescapa una cadena emulando la función Unescape de OpenBullet 2
// (\uXXXX, \/, \", \', \\, \n, emojis UTF-16, etc.).
func Unescape(s string) string {
	return Utils.UnescapeString(s)
}
