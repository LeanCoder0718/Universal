package GeoRoute

import (
	"testing"
)

func TestULPParser_ParseULPLine(t *testing.T) {
	parser := NewULPParser()

	tests := []struct {
		name        string
		input       string
		wantErr     bool
		wantService string
		wantEmail   string
		wantPass    string
	}{
		{
			name:        "Standard ULP 3 fields",
			input:       "movistarplay.cl:melladomoyar0@gmail.com:rfmm2306",
			wantErr:     false,
			wantService: "movistarplay.cl",
			wantEmail:   "melladomoyar0@gmail.com",
			wantPass:    "rfmm2306",
		},
		{
			name:        "Standard Combo 2 fields",
			input:       "melladomoyar0@gmail.com:rfmm2306",
			wantErr:     false,
			wantService: "",
			wantEmail:   "melladomoyar0@gmail.com",
			wantPass:    "rfmm2306",
		},
		{
			name:        "HTTP URL Service",
			input:       "https://secure.banco.com.ar/login:cliente123:SecretPass!2026",
			wantErr:     false,
			wantService: "https://secure.banco.com.ar/login",
			wantEmail:   "cliente123",
			wantPass:    "SecretPass!2026",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec, err := parser.ParseULPLine(tt.input)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ParseULPLine(%q) error = %v; wantErr %v", tt.input, err, tt.wantErr)
			}
			if !tt.wantErr {
				if rec.Service != tt.wantService {
					t.Errorf("Service = %q; want %q", rec.Service, tt.wantService)
				}
				if rec.Email != tt.wantEmail {
					t.Errorf("Email = %q; want %q", rec.Email, tt.wantEmail)
				}
				if rec.Password != tt.wantPass {
					t.Errorf("Password = %q; want %q", rec.Password, tt.wantPass)
				}
			}
		})
	}
}

func TestCountryDetector_ULPDetection(t *testing.T) {
	detector := NewCountryDetector(1000)

	rec, err := detector.DetectNormalized("movistarplay.cl:melladomoyar0@gmail.com:rfmm2306", FormatULP, PolicyPreferService)
	if err != nil {
		t.Fatalf("DetectNormalized error: %v", err)
	}
	if rec.Country != "CL" {
		t.Errorf("Country = %q; want CL", rec.Country)
	}
	if rec.Source != CountrySourceServiceDomain {
		t.Errorf("Source = %q; want ServiceDomain", rec.Source)
	}
}
