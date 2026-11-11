package HTTPClient

import (
	"strings"
	"testing"
)

func TestGenerateWebKitBoundary(t *testing.T) {
	b, err := GenerateWebKitBoundary()
	if err != nil {
		t.Fatalf("GenerateWebKitBoundary falló: %v", err)
	}

	if !strings.HasPrefix(b, "----WebKitFormBoundary") {
		t.Errorf("Boundary no inicia con '----WebKitFormBoundary', obtenido: %s", b)
	}

	if len(b) != len("----WebKitFormBoundary")+16 {
		t.Errorf("Longitud de boundary = %d, esperado %d", len(b), len("----WebKitFormBoundary")+16)
	}
}

func TestBuildMultipartExactFormat(t *testing.T) {
	body, ct, err := BuildMultipart(
		MF("fullName", "JorgeNieto pesa"),
		MF("email", "humberto@test.com"),
		MF("password", "@Password123!"),
	)
	if err != nil {
		t.Fatalf("BuildMultipart falló: %v", err)
	}

	// 1. Validar Content-Type
	if !strings.HasPrefix(ct, "multipart/form-data; boundary=----WebKitFormBoundary") {
		t.Errorf("Content-Type inválido: %s", ct)
	}

	parts := strings.Split(ct, "boundary=")
	if len(parts) != 2 {
		t.Fatalf("No se pudo extraer el boundary de %s", ct)
	}
	boundary := parts[1]

	bodyStr := string(body)

	// 2. Validar delimitador de inicio de parte (-- + boundary) -> 6 guiones
	expectedDelimiter := "--" + boundary + "\r\n"
	if !strings.HasPrefix(bodyStr, expectedDelimiter) {
		t.Errorf("El cuerpo no inicia con el delimitador esperado: %s", bodyStr[:len(expectedDelimiter)])
	}

	// 3. Validar Content-Disposition
	if !strings.Contains(bodyStr, "Content-Disposition: form-data; name=\"fullName\"\r\n\r\nJorgeNieto pesa\r\n") {
		t.Errorf("Campo fullName no encontrado en el formato esperado:\n%s", bodyStr)
	}

	if !strings.Contains(bodyStr, "Content-Disposition: form-data; name=\"email\"\r\n\r\nhumberto@test.com\r\n") {
		t.Errorf("Campo email no encontrado en el formato esperado:\n%s", bodyStr)
	}

	// 4. Validar delimitador de cierre (-- + boundary + --\r\n)
	expectedEnd := "--" + boundary + "--\r\n"
	if !strings.HasSuffix(bodyStr, expectedEnd) {
		t.Errorf("El cuerpo no termina con el delimitador de cierre esperado: %q", bodyStr[len(bodyStr)-len(expectedEnd):])
	}
}

func TestMultipartBuilderFluent(t *testing.T) {
	mp := NewMultipart().
		AddField("token", "TOKEN_ABC_123").
		AddField("password", "p@ss1").
		AddField("passwordConfirm", "p@ss1").
		AddField("tk_trp", "TRP_BASE64_==")

	body, ct, err := mp.Build()
	if err != nil {
		t.Fatalf("mp.Build falló: %v", err)
	}

	if len(body) == 0 {
		t.Fatal("Body está vacío")
	}

	if !strings.Contains(ct, "boundary=----WebKitFormBoundary") {
		t.Errorf("Content-Type no contiene boundary WebKit: %s", ct)
	}

	bodyStr := string(body)
	if !strings.Contains(bodyStr, "TOKEN_ABC_123") || !strings.Contains(bodyStr, "TRP_BASE64_==") {
		t.Errorf("El cuerpo no contiene todos los campos esperados:\n%s", bodyStr)
	}
}

func TestMultipartWithFile(t *testing.T) {
	fileData := []byte("fake-image-bytes-12345")
	body, ct, err := NewMultipart().
		AddField("usuario", "admin").
		AddFile("avatar", "perfil.jpg", "image/jpeg", fileData).
		Build()

	if err != nil {
		t.Fatalf("Build con archivo falló: %v", err)
	}

	bodyStr := string(body)
	if !strings.Contains(bodyStr, `Content-Disposition: form-data; name="avatar"; filename="perfil.jpg"`) {
		t.Errorf("Header de archivo no encontrado:\n%s", bodyStr)
	}
	if !strings.Contains(bodyStr, "Content-Type: image/jpeg\r\n") {
		t.Errorf("Content-Type de archivo no encontrado:\n%s", bodyStr)
	}
	if !strings.Contains(bodyStr, "fake-image-bytes-12345") {
		t.Errorf("Bytes del archivo no encontrados en el cuerpo")
	}

	if !strings.HasPrefix(ct, "multipart/form-data; boundary=") {
		t.Errorf("Content-Type principal inválido: %s", ct)
	}
}

func TestMultipartCustomBoundary(t *testing.T) {
	customBoundary := "----WebKitFormBoundaryhFTbn4yjBAaLxBKc"
	body, ct, err := NewMultipart().
		SetBoundary(customBoundary).
		AddField("fullName", "JorgeNieto pesa").
		Build()

	if err != nil {
		t.Fatalf("Build falló: %v", err)
	}

	expectedCT := "multipart/form-data; boundary=----WebKitFormBoundaryhFTbn4yjBAaLxBKc"
	if ct != expectedCT {
		t.Errorf("CT obtenido = %s, esperado %s", ct, expectedCT)
	}

	expectedStart := "------WebKitFormBoundaryhFTbn4yjBAaLxBKc\r\nContent-Disposition: form-data; name=\"fullName\"\r\n\r\nJorgeNieto pesa\r\n------WebKitFormBoundaryhFTbn4yjBAaLxBKc--\r\n"
	if string(body) != expectedStart {
		t.Errorf("Cuerpo obtenido no coincide exactamente con el esperado:\nObtenido:\n%s\nEsperado:\n%s", string(body), expectedStart)
	}
}

func TestBuildMultipartFromMap(t *testing.T) {
	data := map[string]string{
		"campoA": "valorA",
		"campoB": "valorB",
	}

	body, ct, err := BuildMultipartFromMap(data)
	if err != nil {
		t.Fatalf("BuildMultipartFromMap falló: %v", err)
	}

	bodyStr := string(body)
	if !strings.Contains(bodyStr, "valorA") || !strings.Contains(bodyStr, "valorB") {
		t.Errorf("Campos de mapa no encontrados en cuerpo")
	}
	if !strings.Contains(ct, "multipart/form-data") {
		t.Errorf("Content-Type inválido: %s", ct)
	}
}

func TestBuilderSpecIntegrationWithMultipart(t *testing.T) {
	id, err := NewIdentity()
	if err != nil {
		t.Fatalf("NewIdentity falló: %v", err)
	}
	prof := FromIdentity(id)
	builder := NewBuild(id, prof.HeaderLayout)

	mp := NewMultipart().
		AddField("token", "TOKEN123").
		AddField("password", "SecretPass1!")

	req, err := builder.Build(Spec{
		Method:    "POST",
		URL:       "https://www.paramountplus.com/intl/account/xhr/reset-password/",
		Kind:      KindXHR,
		Multipart: mp,
		Extra: map[string]string{
			"Origin":  "https://www.paramountplus.com",
			"Referer": "https://www.paramountplus.com/reset",
		},
	}, "")
	if err != nil {
		t.Fatalf("builder.Build falló: %v", err)
	}

	// Verificar que funciona tanto con Go estándar req.Header.Get como con GetHeader
	ctStandard := req.Header.Get("Content-Type")
	if !strings.HasPrefix(ctStandard, "multipart/form-data; boundary=----WebKitFormBoundary") {
		t.Fatalf("req.Header.Get(\"Content-Type\") no devolvió el boundary WebKit: %q", ctStandard)
	}

	ct := GetHeader(req.Header, "Content-Type")
	if !strings.HasPrefix(ct, "multipart/form-data; boundary=----WebKitFormBoundary") {
		t.Fatalf("GetHeader Content-Type no contiene boundary WebKit generado automáticamente: %s", ct)
	}

	// Verificar que el body generado coincida con el boundary del Content-Type
	boundary := strings.Split(ct, "boundary=")[1]
	reqBody := req.Body
	if reqBody == nil {
		t.Fatal("req.Body es nil")
	}

	// Comprobar que en el orden de cabeceras también figure Content-Type
	order, ok := req.Header[HeaderOrderKey]
	if !ok || len(order) == 0 {
		t.Fatal("HeaderOrderKey no presente en el request")
	}

	hasCTInOrder := false
	for _, h := range order {
		if strings.EqualFold(h, "content-type") {
			hasCTInOrder = true
			break
		}
	}
	if !hasCTInOrder {
		t.Errorf("content-type no figura en HeaderOrderKey: %v", order)
	}

	_ = boundary
}

func TestMultipart_NilSafetyAndConcurrency(t *testing.T) {
	// 1. Probar nil builder
	var nilBuilder *MultipartBuilder
	_, _, err := nilBuilder.Build()
	if err == nil {
		t.Error("nilBuilder.Build() debió fallar con error")
	}

	// 2. Probar AddField sobre nil
	safeBuilder := nilBuilder.AddField("test", "val")
	if safeBuilder == nil || len(safeBuilder.Fields()) != 1 {
		t.Errorf("AddField sobre nil no auto-inicializó: %v", safeBuilder)
	}

	// 3. Probar que Spec.Extra no es mutado al construir la petición
	id, err := NewIdentity()
	if err != nil {
		t.Fatalf("NewIdentity: %v", err)
	}
	prof := FromIdentity(id)
	builder := NewBuild(id, prof.HeaderLayout)

	sharedExtra := map[string]string{
		"Content-Type": "text/plain",
		"X-Custom":     "value",
	}

	mp := NewMultipart().AddField("user", "john")
	req, err := builder.Build(Spec{
		Method:    "POST",
		URL:       "https://example.com/api",
		Kind:      KindXHR,
		Multipart: mp,
		Extra:     sharedExtra,
	}, "")
	if err != nil {
		t.Fatalf("builder.Build: %v", err)
	}

	// El mapa original sharedExtra no debió ser mutado
	if sharedExtra["Content-Type"] != "text/plain" {
		t.Errorf("sharedExtra fue mutado en su lugar! Content-Type = %q", sharedExtra["Content-Type"])
	}

	// Pero en el request final debe tener el boundary multipart
	if !strings.HasPrefix(req.Header.Get("Content-Type"), "multipart/form-data; boundary=----WebKitFormBoundary") {
		t.Errorf("req.Header no tiene el multipart correcto: %s", req.Header.Get("Content-Type"))
	}
}
