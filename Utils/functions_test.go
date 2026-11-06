package Utils

import (
	"strings"
	"testing"
)

func TestReadInt_DirectEnter(t *testing.T) {
	restore := SetStdinReaderForTest(strings.NewReader("\n"))
	defer restore()

	res := ReadInt("Prompt: ", 3)
	if res != 3 {
		t.Fatalf("Expected default value 3, got %d", res)
	}
}

func TestReadInt_WhitespaceEnter(t *testing.T) {
	restore := SetStdinReaderForTest(strings.NewReader("   \r\n"))
	defer restore()

	res := ReadInt("Prompt: ", 10)
	if res != 10 {
		t.Fatalf("Expected default value 10, got %d", res)
	}
}

func TestReadInt_ValidNumber(t *testing.T) {
	restore := SetStdinReaderForTest(strings.NewReader("5\n"))
	defer restore()

	res := ReadInt("Prompt: ", 3)
	if res != 5 {
		t.Fatalf("Expected 5, got %d", res)
	}
}

func TestReadInt_ZeroValid(t *testing.T) {
	restore := SetStdinReaderForTest(strings.NewReader("0\n"))
	defer restore()

	res := ReadInt("Prompt: ", 3)
	if res != 0 {
		t.Fatalf("Expected 0 to be allowed and returned, got %d", res)
	}
}

func TestReadInt_LettersThenValidNumber(t *testing.T) {
	// First input is letters, second is letters with numbers, third is valid number
	input := "letras\nabc123\n7\n"
	restore := SetStdinReaderForTest(strings.NewReader(input))
	defer restore()

	res := ReadInt("Prompt: ", 3)
	if res != 7 {
		t.Fatalf("Expected 7 after invalid attempts, got %d", res)
	}
}

func TestReadInt_NegativeThenValidNumber(t *testing.T) {
	input := "-5\n4\n"
	restore := SetStdinReaderForTest(strings.NewReader(input))
	defer restore()

	res := ReadInt("Prompt: ", 3)
	if res != 4 {
		t.Fatalf("Expected 4 after negative attempt, got %d", res)
	}
}

func TestReadInt_EOFReturnsDefault(t *testing.T) {
	// Immediate EOF (empty reader)
	restore := SetStdinReaderForTest(strings.NewReader(""))
	defer restore()

	res := ReadInt("Prompt: ", 42)
	if res != 42 {
		t.Fatalf("Expected default value 42 on EOF, got %d", res)
	}
}

func TestReadInt_LettersThenEOFReturnsDefault(t *testing.T) {
	// User typed invalid input, then input stream closed (EOF)
	restore := SetStdinReaderForTest(strings.NewReader("bad_input\n"))
	defer restore()

	res := ReadInt("Prompt: ", 15)
	if res != 15 {
		t.Fatalf("Expected default value 15 when EOF hit after invalid input, got %d", res)
	}
}

