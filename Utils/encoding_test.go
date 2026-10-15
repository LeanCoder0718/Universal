package Utils

import (
	"bytes"
	"testing"
	"unicode/utf16"
)

func TestNormalizeTextBytes_UTF8Clean(t *testing.T) {
	input := []byte("test@example.com:password123\r\nanother@domain.com:secret")
	out := NormalizeTextBytes(input)
	if !bytes.Equal(out, input) {
		t.Fatalf("expected unchanged UTF-8, got %q", string(out))
	}
}

func TestNormalizeTextBytes_UTF8BOM(t *testing.T) {
	bom := []byte{0xEF, 0xBB, 0xBF}
	content := []byte("test@gmail.com:pass")
	out := NormalizeTextBytes(append(bom, content...))
	if !bytes.Equal(out, content) {
		t.Fatalf("expected stripped BOM, got %q", string(out))
	}
}

func TestNormalizeTextBytes_UTF16LE_WithBOM(t *testing.T) {
	text := "user@aol.com:mypassword"
	runes := []rune(text)
	u16 := utf16.Encode(runes)
	var leBytes []byte
	leBytes = append(leBytes, 0xFF, 0xFE) // BOM
	for _, v := range u16 {
		leBytes = append(leBytes, byte(v), byte(v>>8))
	}

	out := NormalizeTextBytes(leBytes)
	if string(out) != text {
		t.Fatalf("expected %q, got %q", text, string(out))
	}
}

func TestNormalizeTextBytes_UTF16LE_WithoutBOM(t *testing.T) {
	text := "user@gmail.com:pass123\r\ntest@yahoo.com:pass456"
	runes := []rune(text)
	u16 := utf16.Encode(runes)
	var leBytes []byte
	for _, v := range u16 {
		leBytes = append(leBytes, byte(v), byte(v>>8))
	}

	out := NormalizeTextBytes(leBytes)
	if string(out) != text {
		t.Fatalf("expected %q, got %q", text, string(out))
	}
}

func TestNormalizeTextBytes_ResidualNullBytes(t *testing.T) {
	input := []byte("\x00user\x00@hotmail.com\x00:pass\x00")
	expected := "user@hotmail.com:pass"
	out := NormalizeTextBytes(input)
	if string(out) != expected {
		t.Fatalf("expected %q, got %q", expected, string(out))
	}
}
