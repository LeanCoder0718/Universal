package Utils

import (
	"os"
	"strings"
	"testing"
)

func TestEmailVariantGenerator_Forma1_Gmail(t *testing.T) {
	FormaGmail = "1"
	gen := NewEmailVariantGenerator("example@gmail.com")
	if gen == nil {
		t.Fatal("expected non-nil generator")
	}

	for i := 0; i < 20; i++ {
		email := gen.Next()
		if !strings.HasSuffix(email, "@gmail.com") {
			t.Fatalf("expected @gmail.com suffix, got %s", email)
		}
		// Strip dots and compare base
		parts := strings.Split(email, "@")
		baseWithoutDots := strings.ReplaceAll(parts[0], ".", "")
		if baseWithoutDots != "example" {
			t.Fatalf("expected reconstructed base 'example', got '%s'", baseWithoutDots)
		}
	}
}

func TestEmailVariantGenerator_Forma2_Gmail(t *testing.T) {
	FormaGmail = "2"
	gen := NewEmailVariantGenerator("example@gmail.com")
	if gen == nil {
		t.Fatal("expected non-nil generator")
	}

	for i := 0; i < 20; i++ {
		email := gen.Next()
		if !strings.HasPrefix(email, "example+") || !strings.HasSuffix(email, "@gmail.com") {
			t.Fatalf("expected format example+<digits>@gmail.com, got %s", email)
		}
	}
}

func TestEmailVariantGenerator_Forma3_CustomDomain(t *testing.T) {
	FormaGmail = "3"
	gen := NewEmailVariantGenerator("myemail@domain.com")
	if gen == nil {
		t.Fatal("expected non-nil generator")
	}

	for i := 0; i < 20; i++ {
		email := gen.Next()
		if !strings.Contains(email, "+") || !strings.HasSuffix(email, "@domain.com") {
			t.Fatalf("expected custom inserted '+' format, got %s", email)
		}
	}
}

func TestTomarLineaCorreo_DominiosMios(t *testing.T) {
	dominioGenerado = ""
	NewDominioGen = nil
	NombreArchivoDominios = "test_mydomains.txt"
	defer os.Remove(NombreArchivoDominios)

	DominiosMios = []string{"user1@test.com", "user2@test.com", "user3@test.com"}
	lineasSeleccionadas = make(map[int]bool)

	seen := make(map[string]bool)
	for i := 0; i < 3; i++ {
		email := TomarLineaCorreo()
		if seen[email] {
			t.Fatalf("duplicate email selected: %s", email)
		}
		seen[email] = true
	}

	// Remover uno
	_ = RemoverLineaDeCorreo("user2@test.com")
	if len(DominiosMios) != 2 {
		t.Fatalf("expected 2 remaining domains, got %d", len(DominiosMios))
	}
}
