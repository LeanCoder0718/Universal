package HTTPClient

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"sort"
	"strings"
)

const (
	webkitBoundaryPrefix = "----WebKitFormBoundary"
	boundaryCharset      = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
)

// MultipartField representa un campo individual (texto o archivo) en un formulario multipart/form-data.
type MultipartField struct {
	Name        string // Nombre del campo (name="...")
	Value       string // Valor del campo de texto
	Filename    string // Opcional: nombre de archivo (filename="...")
	ContentType string // Opcional: MIME type del archivo (ej. "image/png", "application/pdf")
	Data        []byte // Opcional: contenido binario del archivo
}

// MF es un helper rápido para declarar campos de texto multipart clave-valor.
//
// Ejemplo:
//
//	HTTPClient.MF("email", "usuario@mail.com")
func MF(name, value string) MultipartField {
	return MultipartField{Name: name, Value: value}
}

// MFile es un helper rápido para declarar un archivo dentro del multipart.
//
// Ejemplo:
//
//	HTTPClient.MFile("avatar", "foto.png", "image/png", fileBytes)
func MFile(name, filename, contentType string, data []byte) MultipartField {
	return MultipartField{
		Name:        name,
		Filename:    filename,
		ContentType: contentType,
		Data:        data,
	}
}

// GenerateWebKitBoundary genera un boundary idéntico al de Chrome/WebKit:
// "----WebKitFormBoundary" seguido de 16 caracteres alfanuméricos aleatorios criptoseguros.
//
// Ejemplo en cabecera:
//
//	Content-Type: multipart/form-data; boundary=----WebKitFormBoundaryhFTbn4yjBAaLxBKc
//
// Ejemplo en cuerpo:
//
//	------WebKitFormBoundaryhFTbn4yjBAaLxBKc
func GenerateWebKitBoundary() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("multipart: error leyendo crypto/rand: %w", err)
	}
	var sb strings.Builder
	sb.Grow(len(webkitBoundaryPrefix) + 16)
	sb.WriteString(webkitBoundaryPrefix)
	for i := 0; i < 16; i++ {
		sb.WriteByte(boundaryCharset[int(raw[i])%len(boundaryCharset)])
	}
	return sb.String(), nil
}

// MultipartBuilder construye payloads multipart/form-data emulando al 100%
// el formato, cabeceras y delimitadores de un navegador Chrome/WebKit real.
type MultipartBuilder struct {
	fields   []MultipartField
	boundary string
}

// NewMultipart crea un nuevo constructor de multipart/form-data.
//
// Ejemplo de uso:
//
//	mp := HTTPClient.NewMultipart().
//	    AddField("email", "usuario@mail.com").
//	    AddField("password", "secreto123")
//	body, contentType, err := mp.Build()
func NewMultipart() *MultipartBuilder {
	return &MultipartBuilder{
		fields: make([]MultipartField, 0, 8),
	}
}

// AddField agrega un campo de texto simple al formulario. Retorna el builder para encadenamiento fluido.
//
// Ejemplo:
//
//	mp.AddField("email", "test@mail.com").AddField("tk_trp", token)
func (m *MultipartBuilder) AddField(name, value string) *MultipartBuilder {
	if m == nil {
		m = NewMultipart()
	}
	m.fields = append(m.fields, MultipartField{Name: name, Value: value})
	return m
}

// AddFile agrega un archivo (texto o binario) al formulario multipart.
//
// Ejemplo:
//
//	mp.AddFile("documento", "recibo.pdf", "application/pdf", pdfBytes)
func (m *MultipartBuilder) AddFile(name, filename, contentType string, data []byte) *MultipartBuilder {
	if m == nil {
		m = NewMultipart()
	}
	m.fields = append(m.fields, MultipartField{
		Name:        name,
		Filename:    filename,
		ContentType: contentType,
		Data:        data,
	})
	return m
}

// SetBoundary permite fijar un boundary específico si se requiere reproducir uno existente.
// Si no se especifica, se autogenera uno aleatorio con formato Chrome/WebKit en cada Build().
func (m *MultipartBuilder) SetBoundary(boundary string) *MultipartBuilder {
	m.boundary = boundary
	return m
}

// Fields retorna los campos agregados hasta el momento.
func (m *MultipartBuilder) Fields() []MultipartField {
	return m.fields
}

// estimateSize precalcula el tamaño requerido en bytes para evitar realojos en el buffer.
func (m *MultipartBuilder) estimateSize(boundary string) int {
	if m == nil {
		return 0
	}
	bLen := len(boundary)
	total := 0
	for _, f := range m.fields {
		total += 2 + bLen + 2                 // "--" + boundary + "\r\n"
		total += 38 + len(f.Name)             // Content-Disposition: form-data; name="..."
		if f.Filename != "" {
			total += 13 + len(f.Filename)     // ; filename="..."
		}
		total += 2                            // "\r\n"
		if f.ContentType != "" {
			total += 16 + len(f.ContentType) + 2 // Content-Type: ...\r\n
		} else if f.Filename != "" {
			total += 39                          // Content-Type: application/octet-stream\r\n
		}
		total += 2                            // "\r\n"
		if len(f.Data) > 0 {
			total += len(f.Data) + 2          // data + "\r\n"
		} else {
			total += len(f.Value) + 2         // value + "\r\n"
		}
	}
	total += 2 + bLen + 4                     // "--" + boundary + "--\r\n"
	return total
}

// Build construye el payload en bytes y la cabecera Content-Type exacta.
//
// Retorna:
//   - body: slice de bytes formateado exactamente como Chrome/WebKit.
//   - contentType: "multipart/form-data; boundary=----WebKitFormBoundary..."
//   - err: error en caso de fallo al generar el boundary aleatorio.
func (m *MultipartBuilder) Build() (body []byte, contentType string, err error) {
	if m == nil {
		return nil, "", fmt.Errorf("multipart: MultipartBuilder es nil")
	}
	boundary := m.boundary
	if boundary == "" {
		boundary, err = GenerateWebKitBoundary()
		if err != nil {
			return nil, "", err
		}
	}

	buf := bytes.NewBuffer(make([]byte, 0, m.estimateSize(boundary)))

	for _, f := range m.fields {
		buf.WriteString("--")
		buf.WriteString(boundary)
		buf.WriteString("\r\n")

		if f.Filename != "" {
			fmt.Fprintf(buf, "Content-Disposition: form-data; name=%q; filename=%q\r\n", f.Name, f.Filename)
			if f.ContentType != "" {
				fmt.Fprintf(buf, "Content-Type: %s\r\n", f.ContentType)
			} else {
				buf.WriteString("Content-Type: application/octet-stream\r\n")
			}
		} else {
			fmt.Fprintf(buf, "Content-Disposition: form-data; name=%q\r\n", f.Name)
			if f.ContentType != "" {
				fmt.Fprintf(buf, "Content-Type: %s\r\n", f.ContentType)
			}
		}

		buf.WriteString("\r\n")

		if len(f.Data) > 0 {
			buf.Write(f.Data)
		} else {
			buf.WriteString(f.Value)
		}
		buf.WriteString("\r\n")
	}

	buf.WriteString("--")
	buf.WriteString(boundary)
	buf.WriteString("--\r\n")

	contentType = "multipart/form-data; boundary=" + boundary
	return buf.Bytes(), contentType, nil
}

// BuildMultipart construye un cuerpo multipart/form-data y su cabecera Content-Type
// directamente a partir de campos variádicos declarados con MF() o MFile().
//
// Ejemplo de uso:
//
//	body, contentType, err := HTTPClient.BuildMultipart(
//	    HTTPClient.MF("email", account.Email),
//	    HTTPClient.MF("tk_trp", tk_trp),
//	)
func BuildMultipart(fields ...MultipartField) (body []byte, contentType string, err error) {
	mb := NewMultipart()
	mb.fields = fields
	return mb.Build()
}

// BuildMultipartFromMap construye un cuerpo multipart/form-data a partir de un map[string]string.
// Para garantizar predictibilidad, los campos se ordenan alfabéticamente por nombre de clave.
func BuildMultipartFromMap(m map[string]string) (body []byte, contentType string, err error) {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	mb := NewMultipart()
	for _, k := range keys {
		mb.AddField(k, m[k])
	}
	return mb.Build()
}

