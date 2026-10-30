package GeoRoute

import (
	"testing"
)

func TestUniversalCountryAdaptation(t *testing.T) {
	tests := []struct {
		name           string
		inputRaw       string
		targetCountry  string
		expectedUser   string
		expectedRegion string
	}{
		{
			name:           "711Proxy Standard Format",
			inputRaw:       "global.rotgb.711proxy.com:10000:USER056189-zone-custom-region-US:3cc36d",
			targetCountry:  "ES",
			expectedUser:   "USER056189-zone-custom-region-ES",
			expectedRegion: "us",
		},
		{
			name:           "711Proxy Target Lowercase Input Should Match Original Upper",
			inputRaw:       "global.rotgb.711proxy.com:10000:USER056189-zone-custom-region-US:3cc36d",
			targetCountry:  "ar",
			expectedUser:   "USER056189-zone-custom-region-AR",
			expectedRegion: "us",
		},
		{
			name:           "711Proxy Alternate Zone Host",
			inputRaw:       "global.rotgb.711.proxy.com:10000:USER056189-zone-custom-region-US:123ASD",
			targetCountry:  "PE",
			expectedUser:   "USER056189-zone-custom-region-PE",
			expectedRegion: "us",
		},
		{
			name:           "DataImpulse Format",
			inputRaw:       "gw.dataimpulse.com:823:user123__cr.us:pass123",
			targetCountry:  "es",
			expectedUser:   "user123__cr.es",
			expectedRegion: "us",
		},
		{
			name:           "BrightData Format",
			inputRaw:       "brd.superproxy.io:22225:customer-hl_123-zone-resi-country-us:secret",
			targetCountry:  "CL",
			expectedUser:   "customer-hl_123-zone-resi-country-cl",
			expectedRegion: "us",
		},
		{
			name:           "Oxylabs Format",
			inputRaw:       "pr.oxylabs.io:7777:customer-user1-cc-mx:pass1",
			targetCountry:  "CO",
			expectedUser:   "customer-user1-cc-co",
			expectedRegion: "mx",
		},
		{
			name:           "IPRoyal Format",
			inputRaw:       "geo.iproyal.com:12321:user_country-us:pass2",
			targetCountry:  "AR",
			expectedUser:   "user_country-ar",
			expectedRegion: "us",
		},
		{
			name:           "Hyphen Country Format",
			inputRaw:       "1.2.3.4:8080:user-US:pass",
			targetCountry:  "MX",
			expectedUser:   "user-MX",
			expectedRegion: "us",
		},
		{
			name:           "Underscore Country Format",
			inputRaw:       "1.2.3.4:8080:user_AR:pass",
			targetCountry:  "PE",
			expectedUser:   "user_PE",
			expectedRegion: "ar",
		},
		{
			name:           "Dot Country Format",
			inputRaw:       "1.2.3.4:8080:user.CL:pass",
			targetCountry:  "BR",
			expectedUser:   "user.BR",
			expectedRegion: "cl",
		},
		{
			name:           "Colon Country Tag Format",
			inputRaw:       "http://user%3Acountry%3AES:pass123@1.2.3.4:8080",
			targetCountry:  "US",
			expectedUser:   "user:country:US",
			expectedRegion: "es",
		},
		{
			name:           "Suffix Session Preservation",
			inputRaw:       "1.2.3.4:8080:user-country-us-session-abc:pass",
			targetCountry:  "es",
			expectedUser:   "user-country-es-session-abc",
			expectedRegion: "us",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p, err := ParseProxy(tc.inputRaw)
			if err != nil {
				t.Fatalf("ParseProxy error: %v", err)
			}
			if p.Region != tc.expectedRegion {
				t.Errorf("p.Region = %q, want %q", p.Region, tc.expectedRegion)
			}

			adaptedUser := ApplyCountryToUsername(p.Username, tc.targetCountry)
			if adaptedUser != tc.expectedUser {
				t.Errorf("ApplyCountryToUsername = %q, want %q", adaptedUser, tc.expectedUser)
			}
		})
	}
}

func TestApplyCountryToUsername_RevertToGeneric(t *testing.T) {
	cases := []struct {
		inputUser    string
		countryCode  string
		expectedUser string
	}{
		{"user123__cr.es", "XX", "user123"},
		{"user123__cr.es", "", "user123"},
		{"user123__cr.es", "GENERIC", "user123"},
		{"user123__cr.es", "UNKNOWN", "user123"},
		{"customer-hl_123-zone-resi-country-us", "XX", "customer-hl_123-zone-resi"},
		{"user-country-es", "XX", "user"},
		{"customer-user1-cc-mx", "XX", "customer-user1"},
		{"clean_user", "XX", "clean_user"},
		{"clean_user", "", "clean_user"},
	}

	for _, tc := range cases {
		got := ApplyCountryToUsername(tc.inputUser, tc.countryCode)
		if got != tc.expectedUser {
			t.Errorf("ApplyCountryToUsername(%q, %q) = %q, want %q", tc.inputUser, tc.countryCode, got, tc.expectedUser)
		}
	}
}

// TestActiveApplyGeoFalse_PreservesOriginalProxyTags valida que cuando ActiveApplyGeo es false (Opción 4),
// NUNCA se altere, limpie ni elimine ningún tag existente en la proxy cargada.
func TestActiveApplyGeoFalse_PreservesOriginalProxyTags(t *testing.T) {
	orig := ActiveApplyGeo
	defer func() { ActiveApplyGeo = orig }()
	ActiveApplyGeo = false

	raw := "unlimited.proxiware.com:1337:rzP71UVQeRda-country-mx:PEpvpiSMsj77VsFV"
	p, err := ParseProxy(raw)
	if err != nil {
		t.Fatalf("ParseProxy error: %v", err)
	}

	// 1. ApplyCountryToUsername con "XX" debe mantener el tag original intacto
	got := ApplyCountryToUsername(p.Username, "XX")
	if got != "rzP71UVQeRda-country-mx" {
		t.Errorf("ApplyCountryToUsername want rzP71UVQeRda-country-mx, got %q", got)
	}

	// 2. CleanCountryFromUsername debe mantener el tag original intacto
	gotClean := CleanCountryFromUsername(p.Username)
	if gotClean != "rzP71UVQeRda-country-mx" {
		t.Errorf("CleanCountryFromUsername want rzP71UVQeRda-country-mx, got %q", gotClean)
	}

	// 3. TemplateProvider.Transform debe devolver el proxy String completo intacto
	prov := NewTemplateProvider(DefaultCRProviderConfig())
	transformed, err := prov.Transform(p, "XX")
	if err != nil {
		t.Fatalf("Transform error: %v", err)
	}
	if transformed != raw {
		t.Errorf("Transform want %q, got %q", raw, transformed)
	}
}
