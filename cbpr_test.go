package Universal

import (
	"os"
	"testing"
)

func TestFastCountLines(t *testing.T) {
	tests := []struct {
		name     string
		content  string
		expected int64
	}{
		{"empty file", "", 0},
		{"single line without newline", "user@yahoo.com:password", 1},
		{"single line with newline", "user@yahoo.com:password\n", 1},
		{"multiple lines without trailing newline", "user1:pass1\nuser2:pass2\nuser3:pass3", 3},
		{"multiple lines with trailing newline", "user1:pass1\nuser2:pass2\nuser3:pass3\n", 3},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tmpFile, err := os.CreateTemp("", "test_count_*.txt")
			if err != nil {
				t.Fatalf("Failed to create temp file: %v", err)
			}
			defer os.Remove(tmpFile.Name())

			if _, err := tmpFile.WriteString(tc.content); err != nil {
				t.Fatalf("Failed to write to temp file: %v", err)
			}
			tmpFile.Close()

			count, err := FastCountLines(tmpFile.Name())
			if err != nil {
				t.Fatalf("FastCountLines error: %v", err)
			}
			if count != tc.expected {
				t.Errorf("FastCountLines() = %d, want %d", count, tc.expected)
			}
		})
	}
}
