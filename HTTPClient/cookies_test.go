package HTTPClient

import (
	"testing"

	fhttp "github.com/bogdanfinn/fhttp"
)

func TestSplitSetCookieHeader(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{
			name:     "Single cookie without date",
			input:    "foo=bar; Path=/; Secure",
			expected: []string{"foo=bar; Path=/; Secure"},
		},
		{
			name:  "Multiple cookies with comma in Expires",
			input: "MSPOK=; expires=Thu, 30-Oct-1980 16:00:00 GMT; path=/; domain=login.live.com, OParams=11O.DmIchq85L; path=/; domain=login.live.com",
			expected: []string{
				"MSPOK=; expires=Thu, 30-Oct-1980 16:00:00 GMT; path=/; domain=login.live.com",
				"OParams=11O.DmIchq85L; path=/; domain=login.live.com",
			},
		},
		{
			name:  "RFC 850 date in Expires",
			input: "a=1; expires=Monday, 02-Jan-06 15:04:05 GMT, b=2; Max-Age=3600; Path=/",
			expected: []string{
				"a=1; expires=Monday, 02-Jan-06 15:04:05 GMT",
				"b=2; Max-Age=3600; Path=/",
			},
		},
		{
			name:  "Quoted comma inside value",
			input: `c="val,ue"; Path=/, d=4`,
			expected: []string{
				`c="val,ue"; Path=/`,
				"d=4",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := SplitSetCookieHeader(tt.input)
			if len(got) != len(tt.expected) {
				t.Fatalf("expected %d cookies, got %d: %v", len(tt.expected), len(got), got)
			}
			for i := range got {
				if got[i] != tt.expected[i] {
					t.Errorf("cookie[%d] = %q, expected %q", i, got[i], tt.expected[i])
				}
			}
		})
	}
}

func TestParseSetCookies(t *testing.T) {
	header := make(fhttp.Header)
	header["Set-Cookie"] = []string{
		"MSPOK=; expires=Thu, 30-Oct-1980 16:00:00 GMT; path=/; domain=login.live.com, OParams=11O.DmIchq85L; path=/; domain=login.live.com",
		"MSPRequ=lt=123&co=1; path=/; domain=login.live.com",
	}

	cookies := ParseSetCookies(header)
	if len(cookies) != 3 {
		t.Fatalf("expected 3 cookies, got %d", len(cookies))
	}

	cookieMap := make(map[string]*fhttp.Cookie)
	for _, c := range cookies {
		cookieMap[c.Name] = c
	}

	if _, ok := cookieMap["MSPOK"]; !ok {
		t.Errorf("missing MSPOK cookie")
	}
	if oparams, ok := cookieMap["OParams"]; !ok {
		t.Errorf("missing OParams cookie")
	} else {
		if oparams.Value != "11O.DmIchq85L" {
			t.Errorf("OParams value = %q, expected 11O.DmIchq85L", oparams.Value)
		}
		if oparams.Domain != "login.live.com" {
			t.Errorf("OParams domain = %q, expected login.live.com", oparams.Domain)
		}
	}
	if _, ok := cookieMap["MSPRequ"]; !ok {
		t.Errorf("missing MSPRequ cookie")
	}

	// Test Jar storing
	jar := NewJAR(CookieConfig{})
	jar.SetCookies("login.live.com", "/ppsecure/post.srf", cookies)

	// MSPOK has expires in 1980, so it shouldn't be valid, but OParams and MSPRequ must be retrieved
	reqCookies := jar.CookiesFor("login.live.com", "/ppsecure/post.srf", true)
	if len(reqCookies) < 2 {
		t.Fatalf("expected at least 2 cookies in jar, got %d", len(reqCookies))
	}
	hasOParams := false
	for _, c := range reqCookies {
		if c.Name == "OParams" && c.Value == "11O.DmIchq85L" {
			hasOParams = true
		}
	}
	if !hasOParams {
		t.Errorf("OParams cookie not found in jar for login.live.com")
	}
}

func TestJarDeduplicationAndSpecificity(t *testing.T) {
	jar := NewJAR(CookieConfig{})

	// 1. Simular cookie genérica en dominio padre (yahoo.com)
	jar.AddManual("login.yahoo.com", "/", Cookie{
		Name:   "ipqsd",
		Value:  "parent_value",
		Domain: "yahoo.com",
		Path:   "/",
	})

	// 2. Simular cookie específica en subdominio (login.yahoo.com)
	jar.AddManual("login.yahoo.com", "/", Cookie{
		Name:   "ipqsd",
		Value:  "specific_value",
		Domain: "login.yahoo.com",
		Path:   "/",
	})

	// 3. Simular cookie device_id_undefined
	jar.AddManual("login.yahoo.com", "/", Cookie{
		Name:   "device_id_undefined",
		Value:  "device_specific",
		Domain: "login.yahoo.com",
		Path:   "/",
	})
	jar.AddManual("login.yahoo.com", "/", Cookie{
		Name:   "device_id_undefined",
		Value:  "device_parent",
		Domain: "yahoo.com",
		Path:   "/",
	})

	// Verificar deduplicación en petición a login.yahoo.com
	res := jar.CookiesFor("login.yahoo.com", "/account/challenge/password/validate", true)
	if len(res) != 2 {
		t.Fatalf("se esperaban exactamente 2 cookies deduplicadas, se obtuvieron %d", len(res))
	}

	vals := make(map[string]string)
	for _, c := range res {
		if _, exists := vals[c.Name]; exists {
			t.Errorf("cookie DUPLICADA encontrada en CookiesFor: %s", c.Name)
		}
		vals[c.Name] = c.Value
	}

	if vals["ipqsd"] != "specific_value" {
		t.Errorf("se esperaba valor específico 'specific_value' para ipqsd, se obtuvo %q", vals["ipqsd"])
	}
	if vals["device_id_undefined"] != "device_specific" {
		t.Errorf("se esperaba valor específico 'device_specific' para device_id_undefined, se obtuvo %q", vals["device_id_undefined"])
	}

	// 4. Probar purga con Set-Cookie de eliminación (DELETE / Max-Age <= 0)
	deleteHeaders := make(fhttp.Header)
	deleteHeaders["Set-Cookie"] = []string{
		"ipqsd=DELETE; Expires=Wed, 7 Oct 2026 06:38:54 GMT; Max-Age=0; Domain=login.yahoo.com; Path=; Secure; HttpOnly",
		"device_id_undefined=DELETE; Expires=Wed, 7 Oct 2026 06:38:54 GMT; Max-Age=0; Domain=login.yahoo.com; Path=; Secure; HttpOnly",
	}
	deleteCookies := ParseSetCookies(deleteHeaders)
	jar.SetCookies("login.yahoo.com", "/account/challenge/password/validate", deleteCookies)

	// Verificar que tras la eliminación no se devuelven cookies con valor 'DELETE'
	afterDelete := jar.CookiesFor("login.yahoo.com", "/account/challenge/password/validate", true)
	for _, c := range afterDelete {
		if c.Value == "DELETE" {
			t.Errorf("la cookie %s no fue purgada y retornó valor DELETE", c.Name)
		}
		if c.Name == "ipqsd" && c.Value == "specific_value" {
			t.Errorf("la cookie específica ipqsd debería haber sido purgada")
		}
	}
}

