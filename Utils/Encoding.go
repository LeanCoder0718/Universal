package Utils

import (
	"bytes"
	"unicode/utf16"
	"unicode/utf8"
)

// NormalizeTextBytes detecta y normaliza automáticamente la codificación de un archivo de texto,
// convirtiendo UTF-16LE, UTF-16BE y UTF-8 con BOM a UTF-8 estándar limpio.
// Además, elimina cualquier byte nulo (\x00) suelto que corrompa la lectura de cadenas.
func NormalizeTextBytes(data []byte) []byte {
	if len(data) == 0 {
		return data
	}

	// 1. Detección por BOM (Byte Order Mark)
	// UTF-8 con BOM: 0xEF, 0xBB, 0xBF
	if len(data) >= 3 && data[0] == 0xEF && data[1] == 0xBB && data[2] == 0xBF {
		return sanitizeNullBytes(data[3:])
	}

	// UTF-16LE con BOM: 0xFF, 0xFE
	if len(data) >= 2 && data[0] == 0xFF && data[1] == 0xFE {
		return decodeUTF16LE(data[2:])
	}

	// UTF-16BE con BOM: 0xFE, 0xFF
	if len(data) >= 2 && data[0] == 0xFE && data[1] == 0xFF {
		return decodeUTF16BE(data[2:])
	}

	// 2. Heurística para UTF-16 sin BOM (común en salidas de PowerShell / Windows)
	// En texto ASCII/UTF-16 (como emails y passwords):
	// - UTF-16LE: casi todos los bytes en posiciones impares (1, 3, 5...) son 0x00.
	// - UTF-16BE: casi todos los bytes en posiciones pares (0, 2, 4...) son 0x00.
	sampleSize := len(data)
	if sampleSize > 1024 {
		sampleSize = 1024
	}
	if sampleSize >= 4 {
		evenZeros := 0
		oddZeros := 0
		totalPairs := sampleSize / 2

		for i := 0; i < totalPairs; i++ {
			if data[2*i] == 0x00 {
				evenZeros++
			}
			if data[2*i+1] == 0x00 {
				oddZeros++
			}
		}

		// Si más del 60% de las posiciones impares son 0x00 y las pares no lo son (< 20%), es UTF-16LE
		if oddZeros > (totalPairs*6)/10 && evenZeros < (totalPairs*2)/10 {
			return decodeUTF16LE(data)
		}
		// Si más del 60% de las posiciones pares son 0x00 y las impares no lo son (< 20%), es UTF-16BE
		if evenZeros > (totalPairs*6)/10 && oddZeros < (totalPairs*2)/10 {
			return decodeUTF16BE(data)
		}
	}

	// 3. Ya es UTF-8 o ASCII: sanitizar cualquier byte nulo suelto
	return sanitizeNullBytes(data)
}

func decodeUTF16LE(b []byte) []byte {
	if len(b)%2 != 0 {
		b = b[:len(b)-1]
	}
	u16 := make([]uint16, len(b)/2)
	for i := 0; i < len(u16); i++ {
		u16[i] = uint16(b[2*i]) | (uint16(b[2*i+1]) << 8)
	}
	runes := utf16.Decode(u16)

	var buf bytes.Buffer
	buf.Grow(len(runes))
	utf8Buf := make([]byte, 4)
	for _, r := range runes {
		if r == 0 || r == 0xFFFD {
			continue
		}
		n := utf8.EncodeRune(utf8Buf, r)
		buf.Write(utf8Buf[:n])
	}
	return buf.Bytes()
}

func decodeUTF16BE(b []byte) []byte {
	if len(b)%2 != 0 {
		b = b[:len(b)-1]
	}
	u16 := make([]uint16, len(b)/2)
	for i := 0; i < len(u16); i++ {
		u16[i] = (uint16(b[2*i]) << 8) | uint16(b[2*i+1])
	}
	runes := utf16.Decode(u16)

	var buf bytes.Buffer
	buf.Grow(len(runes))
	utf8Buf := make([]byte, 4)
	for _, r := range runes {
		if r == 0 || r == 0xFFFD {
			continue
		}
		n := utf8.EncodeRune(utf8Buf, r)
		buf.Write(utf8Buf[:n])
	}
	return buf.Bytes()
}

func sanitizeNullBytes(b []byte) []byte {
	if !bytes.Contains(b, []byte{0x00}) {
		return b
	}
	return bytes.ReplaceAll(b, []byte{0x00}, nil)
}
