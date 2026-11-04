package Utils

import (
	"testing"
)

func TestUnescape_UserExactJSON(t *testing.T) {
	input := `{"code":"00","description":"\u00c9xito","phone":"ZPG8k5pDqrHTuBJ43w114kVpDIjPsxywr2puau5ef4wQkD\/7c6k=","products":[{"product":"PFYcFClJOao0ddWYKRyyUtPmre1SbzoJw+N5cO+33Bi9","status":"mucyZG28Rdz7sQRTxjOrOMFuJTNvglCyyPQ1iiw=","psi":"GlCninXGwPxcD16LrHZGkN1fg1tG5FjCzLUoCMuGBf6TnG2AEKrEt7uZA921PcEcVp\/Mk8GDqELG724oIY9MNw==","sku":"7xPQnR+iSQ\/p6jW59NIy6bOL1vXmOw3UqxLcTKPUKyKu","virtualNumber":"RAryhsOGPSAe0L9eeveEu2\/whDlA7bNqp8CMb\/ebxMrpMothKj0=","contractFolio":"mHZ0\/JwMcVYfM6LMGdp955\/Ag7JzfAKrVXBasvw3CohaM5oYz\/eYohV\/PQ==","email":""},{"product":"n3nJZo8k0w2YiqanVMXK\/US7wd2m6iwgsPXFLo9VJv21","status":"XlY+6tsdlFDIlld8+TGCq+jNjSvwCWjx577KqWA=","psi":"","sku":"","virtualNumber":"","contractFolio":"LrnEfIdtvCa61tUzlmSUv8gq5gzUTbAfObmWrjpG28D6sXhvuZIk1DX9zw==","email":""},{"product":"z1Xaxh3R+6Zh80Cv0H0t9qVya6EDGG8n4\/G9Q3KvSPLl","status":"50r1IwZkcksviEgIK5i1vGV3Us9diKKqX6lptk4=","psi":"","sku":"","virtualNumber":"","contractFolio":"7i7YLQD2JNJShdHrjrkmMab2\/Ep+xj2fTUoXZOdt1xFZYtzoKk7HQmgvgw==","email":""}]}`

	expected := `{"code":"00","description":"Éxito","phone":"ZPG8k5pDqrHTuBJ43w114kVpDIjPsxywr2puau5ef4wQkD/7c6k=","products":[{"product":"PFYcFClJOao0ddWYKRyyUtPmre1SbzoJw+N5cO+33Bi9","status":"mucyZG28Rdz7sQRTxjOrOMFuJTNvglCyyPQ1iiw=","psi":"GlCninXGwPxcD16LrHZGkN1fg1tG5FjCzLUoCMuGBf6TnG2AEKrEt7uZA921PcEcVp/Mk8GDqELG724oIY9MNw==","sku":"7xPQnR+iSQ/p6jW59NIy6bOL1vXmOw3UqxLcTKPUKyKu","virtualNumber":"RAryhsOGPSAe0L9eeveEu2/whDlA7bNqp8CMb/ebxMrpMothKj0=","contractFolio":"mHZ0/JwMcVYfM6LMGdp955/Ag7JzfAKrVXBasvw3CohaM5oYz/eYohV/PQ==","email":""},{"product":"n3nJZo8k0w2YiqanVMXK/US7wd2m6iwgsPXFLo9VJv21","status":"XlY+6tsdlFDIlld8+TGCq+jNjSvwCWjx577KqWA=","psi":"","sku":"","virtualNumber":"","contractFolio":"LrnEfIdtvCa61tUzlmSUv8gq5gzUTbAfObmWrjpG28D6sXhvuZIk1DX9zw==","email":""},{"product":"z1Xaxh3R+6Zh80Cv0H0t9qVya6EDGG8n4/G9Q3KvSPLl","status":"50r1IwZkcksviEgIK5i1vGV3Us9diKKqX6lptk4=","psi":"","sku":"","virtualNumber":"","contractFolio":"7i7YLQD2JNJShdHrjrkmMab2/Ep+xj2fTUoXZOdt1xFZYtzoKk7HQmgvgw==","email":""}]}`

	got, err := Unescape(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != expected {
		t.Errorf("Unescape mismatch:\nGot:      %s\nExpected: %s", got, expected)
	}

	gotString := UnescapeString(input)
	if gotString != expected {
		t.Errorf("UnescapeString mismatch:\nGot:      %s\nExpected: %s", gotString, expected)
	}
}

func TestUnescape_SurrogatePairs(t *testing.T) {
	// \uD83D\uDE00 is 😀
	input := `Hello \uD83D\uDE00 World!`
	expected := `Hello 😀 World!`
	got := UnescapeString(input)
	if got != expected {
		t.Errorf("Surrogate pair failed: got %q, expected %q", got, expected)
	}
}

func TestUnescape_StandardEscapes(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{`Hello\nWorld`, "Hello\nWorld"},
		{`Line1\r\nLine2`, "Line1\r\nLine2"},
		{`Col1\tCol2`, "Col1\tCol2"},
		{`Escaped\/slash`, "Escaped/slash"},
		{`Escaped\"quote\"`, "Escaped\"quote\""},
		{`Escaped\'single\'`, "Escaped'single'"},
		{`Backslash\\here`, "Backslash\\here"},
		{`Hex \x41\x42`, "Hex AB"},
		{`Octal \101\102`, "Octal AB"},
		{`Any \? \. \-`, "Any ? . -"},
		{`Trailing \`, "Trailing \\"},
		{`Plain text without escapes`, "Plain text without escapes"},
	}

	for _, tc := range tests {
		got := UnescapeString(tc.input)
		if got != tc.expected {
			t.Errorf("input %q: got %q, expected %q", tc.input, got, tc.expected)
		}
	}
}
