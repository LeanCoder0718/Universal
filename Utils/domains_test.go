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

func TestEmailVariantGenerator_MissingLocalPart_OnlyDomain(t *testing.T) {
	// Caso 1: Usuario ingresó solo "@gmail.com"
	FormaGmail = "2"
	genAtGmail := NewEmailVariantGenerator("@gmail.com")
	if genAtGmail == nil {
		t.Fatal("expected non-nil generator for @gmail.com")
	}
	email1 := genAtGmail.Next()
	if !strings.HasPrefix(email1, "example") || !strings.Contains(email1, "+") || !strings.HasSuffix(email1, "@gmail.com") {
		t.Fatalf("expected generated format example*+*@gmail.com, got: %s", email1)
	}

	// Caso 2: Usuario ingresó solo el dominio sin arroba "gmail.com"
	genGmail := NewEmailVariantGenerator("gmail.com")
	if genGmail == nil {
		t.Fatal("expected non-nil generator for gmail.com")
	}
	email2 := genGmail.Next()
	if !strings.HasSuffix(email2, "@gmail.com") {
		t.Fatalf("expected @gmail.com suffix, got: %s", email2)
	}

	// Caso 3: Dominio personalizado con arroba "@outlook.com"
	FormaGmail = "3"
	genCustom := NewEmailVariantGenerator("@outlook.com")
	if genCustom == nil {
		t.Fatal("expected non-nil generator for @outlook.com")
	}
	email3 := genCustom.Next()
	if !strings.Contains(email3, "+") || !strings.HasSuffix(email3, "@outlook.com") {
		t.Fatalf("expected custom inserted '+' format, got: %s", email3)
	}

	// Caso 4: Cadena vacía
	genEmpty := NewEmailVariantGenerator("")
	if genEmpty == nil {
		t.Fatal("expected non-nil generator for empty string")
	}
	email4 := genEmpty.Next()
	if !strings.HasSuffix(email4, "@gmail.com") {
		t.Fatalf("expected fallback @gmail.com suffix, got: %s", email4)
	}
}
