package Profiles

import (
	"github.com/bogdanfinn/tls-client/profiles"
)

// DefaultCatalog construye un Registry pre-poblado con todos los perfiles
// de producción disponibles, organizados por plataforma y tipo de cliente.
//
// Este es el punto de extensión principal: para agregar un nuevo navegador,
// una nueva versión o una nueva plataforma, basta con añadir entradas aquí.
// El resto del sistema (Manager, Strategy, Session) no necesita cambios.
func DefaultCatalog() *Registry {
	r := NewRegistry()

	// ┌─────────────────────────────────────────────────┐
	// │                   WINDOWS                        │
	// └─────────────────────────────────────────────────┘
	registerWindowsChrome(r)
	registerWindowsFirefox(r)
	registerWindowsEdge(r)

	// ┌─────────────────────────────────────────────────┐
	// │                    macOS                         │
	// └─────────────────────────────────────────────────┘
	registerMacOSChrome(r)
	registerMacOSFirefox(r)
	registerMacOSSafari(r)

	// ┌─────────────────────────────────────────────────┐
	// │                    Linux                         │
	// └─────────────────────────────────────────────────┘
	registerLinuxChrome(r)
	registerLinuxFirefox(r)

	// ┌─────────────────────────────────────────────────┐
	// │                     iOS                          │
	// └─────────────────────────────────────────────────┘
	registerIOSSafari(r)

	// ┌─────────────────────────────────────────────────┐
	// │                   Android                        │
	// └─────────────────────────────────────────────────┘
	registerAndroidOkHTTP(r)
	registerAndroidChrome(r)

	r.Freeze()
	return r
}

// ═══════════════════════════════════════════════════════════════════
//  WINDOWS — Chrome
// ═══════════════════════════════════════════════════════════════════

func registerWindowsChrome(r *Registry) {
	winUA := "Windows NT 10.0; Win64; x64"
	chromeCaps := Capabilities{
		SupportsHTTP2:       true,
		SupportsClientHints: true,
		SupportsTLS13:       true,
	}

	chromeSpecs := []struct {
		major     string
		full      string
		profile   profiles.ClientProfile
		notABrand string
	}{
		{"131", "131.0.0.0", profiles.Chrome_131, `"Not(A:Brand"`},
		{"133", "133.0.0.0", profiles.Chrome_133, `"Not/A)Brand"`},
		{"144", "144.0.0.0", profiles.Chrome_144, `"Not(A:Brand"`},
		{"146", "146.0.0.0", profiles.Chrome_146, `"Not(A:Brand"`},
		{"150", "150.0.0.0", profiles.Chrome_150, `"Not(A:Brand"`},
		{"152", "152.0.0.0", profiles.Chrome_152, `"Not(A:Brand"`},
	}

	for _, c := range chromeSpecs {
		_ = r.Register(NewProfileBuilder(PlatformWindows, ClientChrome, c.major).
			UserAgent(chromeUA(winUA, c.full)).
			SecChUA(chromeSecChUA(c.notABrand, c.major)).
			SecChUAMobile("?0").
			SecChUAPlatform(`"Windows"`).
			PlatformName("Windows").
			UAFragment(winUA).
			TLSProfile(c.profile).
			Capabilities(chromeCaps).
			NotABrand(c.notABrand).
			Build())
	}
}

// ═══════════════════════════════════════════════════════════════════
//  WINDOWS — Firefox
// ═══════════════════════════════════════════════════════════════════

func registerWindowsFirefox(r *Registry) {
	winUA := "Windows NT 10.0; Win64; x64"
	ffCaps := Capabilities{
		SupportsHTTP2: true,
		SupportsTLS13: true,
	}

	ffSpecs := []struct {
		major   string
		full    string
		profile profiles.ClientProfile
	}{
		{"132", "132.0", profiles.Firefox_132},
		{"133", "133.0", profiles.Firefox_133},
		{"135", "135.0", profiles.Firefox_135},
		{"147", "147.0", profiles.Firefox_147},
		{"148", "148.0", profiles.Firefox_148},
	}

	for _, f := range ffSpecs {
		_ = r.Register(NewProfileBuilder(PlatformWindows, ClientFirefox, f.major).
			UserAgent(firefoxUA(winUA, f.full)).
			PlatformName("Windows").
			UAFragment(winUA).
			TLSProfile(f.profile).
			Capabilities(ffCaps).
			Build())
	}
}

// ═══════════════════════════════════════════════════════════════════
//  WINDOWS — Edge (Chromium-based, comparte huella TLS con Chrome)
// ═══════════════════════════════════════════════════════════════════

func registerWindowsEdge(r *Registry) {
	winUA := "Windows NT 10.0; Win64; x64"
	edgeCaps := Capabilities{
		SupportsHTTP2:       true,
		SupportsClientHints: true,
		SupportsTLS13:       true,
	}

	edgeSpecs := []struct {
		major      string
		edgeFull   string
		chromeFull string
		profile    profiles.ClientProfile
		notABrand  string
	}{
		{"131", "131.0.2903.86", "131.0.0.0", profiles.Chrome_131, `"Not(A:Brand"`},
		{"133", "133.0.3065.59", "133.0.0.0", profiles.Chrome_133, `"Not/A)Brand"`},
	}

	for _, e := range edgeSpecs {
		ua := "Mozilla/5.0 (" + winUA + ") AppleWebKit/537.36 (KHTML, like Gecko) Chrome/" + e.chromeFull + " Safari/537.36 Edg/" + e.edgeFull
		secUA := e.notABrand + `;v="99", "Microsoft Edge";v="` + e.major + `", "Chromium";v="` + e.major + `"`

		_ = r.Register(NewProfileBuilder(PlatformWindows, ClientEdge, e.major).
			UserAgent(ua).
			SecChUA(secUA).
			SecChUAMobile("?0").
			SecChUAPlatform(`"Windows"`).
			PlatformName("Windows").
			UAFragment(winUA).
			TLSProfile(e.profile).
			Capabilities(edgeCaps).
			NotABrand(e.notABrand).
			Build())
	}
}

// ═══════════════════════════════════════════════════════════════════
//  macOS — Chrome
// ═══════════════════════════════════════════════════════════════════

func registerMacOSChrome(r *Registry) {
	macUA := "Macintosh; Intel Mac OS X 10_15_7"
	chromeCaps := Capabilities{
		SupportsHTTP2:       true,
		SupportsClientHints: true,
		SupportsTLS13:       true,
	}

	macChromeSpecs := []struct {
		major     string
		full      string
		profile   profiles.ClientProfile
		notABrand string
	}{
		{"131", "131.0.0.0", profiles.Chrome_131, `"Not(A:Brand"`},
		{"133", "133.0.0.0", profiles.Chrome_133, `"Not/A)Brand"`},
	}

	for _, c := range macChromeSpecs {
		_ = r.Register(NewProfileBuilder(PlatformMacOS, ClientChrome, c.major).
			UserAgent(chromeUA(macUA, c.full)).
			SecChUA(chromeSecChUA(c.notABrand, c.major)).
			SecChUAMobile("?0").
			SecChUAPlatform(`"macOS"`).
			PlatformName("macOS").
			UAFragment(macUA).
			TLSProfile(c.profile).
			Capabilities(chromeCaps).
			NotABrand(c.notABrand).
			Build())
	}
}

// ═══════════════════════════════════════════════════════════════════
//  macOS — Firefox
// ═══════════════════════════════════════════════════════════════════

func registerMacOSFirefox(r *Registry) {
	macUA := "Macintosh; Intel Mac OS X 10.15"
	ffCaps := Capabilities{
		SupportsHTTP2: true,
		SupportsTLS13: true,
	}

	macFFSpecs := []struct {
		major   string
		full    string
		profile profiles.ClientProfile
	}{
		{"135", "135.0", profiles.Firefox_135},
	}

	for _, f := range macFFSpecs {
		_ = r.Register(NewProfileBuilder(PlatformMacOS, ClientFirefox, f.major).
			UserAgent(firefoxUA(macUA, f.full)).
			PlatformName("macOS").
			UAFragment(macUA).
			TLSProfile(f.profile).
			Capabilities(ffCaps).
			Build())
	}
}

// ═══════════════════════════════════════════════════════════════════
//  macOS — Safari
// ═══════════════════════════════════════════════════════════════════

func registerMacOSSafari(r *Registry) {
	safariCaps := Capabilities{
		SupportsHTTP2: true,
		SupportsTLS13: true,
	}

	safariSpecs := []struct {
		version   string
		safariVer string
		profile   profiles.ClientProfile
	}{
		{"15.6", "605.1.15", profiles.Safari_15_6_1},
		{"16.0", "605.1.15", profiles.Safari_16_0},
	}

	for _, s := range safariSpecs {
		ua := "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/" + s.safariVer + " (KHTML, like Gecko) Version/" + s.version + " Safari/" + s.safariVer

		_ = r.Register(NewProfileBuilder(PlatformMacOS, ClientSafari, s.version).
			UserAgent(ua).
			PlatformName("macOS").
			UAFragment("Macintosh; Intel Mac OS X 10_15_7").
			TLSProfile(s.profile).
			Capabilities(safariCaps).
			Build())
	}
}

// ═══════════════════════════════════════════════════════════════════
//  Linux — Chrome
// ═══════════════════════════════════════════════════════════════════

func registerLinuxChrome(r *Registry) {
	linuxUA := "X11; Linux x86_64"
	chromeCaps := Capabilities{
		SupportsHTTP2:       true,
		SupportsClientHints: true,
		SupportsTLS13:       true,
	}

	linuxChromeSpecs := []struct {
		major     string
		full      string
		profile   profiles.ClientProfile
		notABrand string
	}{
		{"133", "133.0.0.0", profiles.Chrome_133, `"Not/A)Brand"`},
	}

	for _, c := range linuxChromeSpecs {
		_ = r.Register(NewProfileBuilder(PlatformLinux, ClientChrome, c.major).
			UserAgent(chromeUA(linuxUA, c.full)).
			SecChUA(chromeSecChUA(c.notABrand, c.major)).
			SecChUAMobile("?0").
			SecChUAPlatform(`"Linux"`).
			PlatformName("Linux").
			UAFragment(linuxUA).
			TLSProfile(c.profile).
			Capabilities(chromeCaps).
			NotABrand(c.notABrand).
			Build())
	}
}

// ═══════════════════════════════════════════════════════════════════
//  Linux — Firefox
// ═══════════════════════════════════════════════════════════════════

func registerLinuxFirefox(r *Registry) {
	linuxUA := "X11; Linux x86_64"
	ffCaps := Capabilities{
		SupportsHTTP2: true,
		SupportsTLS13: true,
	}

	linuxFFSpecs := []struct {
		major   string
		full    string
		profile profiles.ClientProfile
	}{
		{"135", "135.0", profiles.Firefox_135},
		{"147", "147.0", profiles.Firefox_147},
		{"148", "148.0", profiles.Firefox_148},
	}

	for _, f := range linuxFFSpecs {
		_ = r.Register(NewProfileBuilder(PlatformLinux, ClientFirefox, f.major).
			UserAgent(firefoxUA(linuxUA, f.full)).
			PlatformName("Linux").
			UAFragment(linuxUA).
			TLSProfile(f.profile).
			Capabilities(ffCaps).
			Build())
	}
}

// ═══════════════════════════════════════════════════════════════════
//  iOS — Safari
// ═══════════════════════════════════════════════════════════════════

func registerIOSSafari(r *Registry) {
	iosCaps := Capabilities{
		SupportsHTTP2: true,
		SupportsTLS13: true,
		IsMobile:      true,
	}

	iosSpecs := []struct {
		version   string
		osVersion string
		profile   profiles.ClientProfile
	}{
		{"15.5", "15_5", profiles.Safari_IOS_15_5},
		{"15.6", "15_6_1", profiles.Safari_IOS_15_6},
		{"16.0", "16_0", profiles.Safari_IOS_16_0},
		{"17.0", "17_0", profiles.Safari_IOS_17_0},
		{"18.0", "18_0", profiles.Safari_IOS_18_0},
		{"18.5", "18_5", profiles.Safari_IOS_18_5},
		{"26.0", "26_0", profiles.Safari_IOS_26_0},
	}

	for _, s := range iosSpecs {
		ua := "Mozilla/5.0 (iPhone; CPU iPhone OS " + s.osVersion + " like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/" + s.version + " Mobile/15E148 Safari/604.1"

		_ = r.Register(NewProfileBuilder(PlatformIOS, ClientSafariOS, s.version).
			UserAgent(ua).
			PlatformName("iOS").
			UAFragment("iPhone; CPU iPhone OS " + s.osVersion + " like Mac OS X").
			TLSProfile(s.profile).
			Capabilities(iosCaps).
			Build())
	}
}

// ═══════════════════════════════════════════════════════════════════
//  Android — OkHTTP (Native HTTP Client)
// ═══════════════════════════════════════════════════════════════════

func registerAndroidOkHTTP(r *Registry) {
	okHttpCaps := Capabilities{
		SupportsHTTP2:       true,
		SupportsClientHints: false,
		SupportsTLS13:       true,
		IsMobile:            true,
		IsNativeApp:         true,
	}

	okHttpSpecs := []struct {
		version string
		profile profiles.ClientProfile
	}{
		{"4.13", profiles.Okhttp4Android13},
		{"4.12", profiles.Okhttp4Android12},
		{"4.11", profiles.Okhttp4Android11},
		{"4.9", profiles.Okhttp4Android9},
	}

	for _, o := range okHttpSpecs {
		_ = r.Register(NewProfileBuilder(PlatformAndroid, ClientOkHTTP, o.version).
			UserAgent("okhttp/" + o.version + ".0").
			PlatformName("Android").
			UAFragment("Android").
			TLSProfile(o.profile).
			Capabilities(okHttpCaps).
			Build())
	}
}

// ═══════════════════════════════════════════════════════════════════
//  Android — Chrome Mobile
// ═══════════════════════════════════════════════════════════════════

func registerAndroidChrome(r *Registry) {
	androidChromeCaps := Capabilities{
		SupportsHTTP2:       true,
		SupportsClientHints: true,
		SupportsTLS13:       true,
		IsMobile:            true,
	}

	androidChromeSpecs := []struct {
		major     string
		full      string
		profile   profiles.ClientProfile
		notABrand string
	}{
		{"131", "131.0.6778.135", profiles.Chrome_131, `"Not(A:Brand"`},
		{"133", "133.0.6943.137", profiles.Chrome_133, `"Not/A)Brand"`},
	}

	for _, c := range androidChromeSpecs {
		ua := "Mozilla/5.0 (Linux; Android 14; Pixel 8 Build/AP2A.240805.005) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/" + c.full + " Mobile Safari/537.36"
		secUA := c.notABrand + `;v="99", "Google Chrome";v="` + c.major + `", "Chromium";v="` + c.major + `"`

		_ = r.Register(NewProfileBuilder(PlatformAndroid, ClientChrome, c.major).
			UserAgent(ua).
			SecChUA(secUA).
			SecChUAMobile("?1").
			SecChUAPlatform(`"Android"`).
			PlatformName("Android").
			UAFragment("Linux; Android 14; Pixel 8").
			TLSProfile(c.profile).
			Capabilities(androidChromeCaps).
			NotABrand(c.notABrand).
			Build())
	}
}

// ═══════════════════════════════════════════════════════════════════
//  Helpers para construir User-Agents y sec-ch-ua
// ═══════════════════════════════════════════════════════════════════

func chromeUA(osFrag, chromeVer string) string {
	return "Mozilla/5.0 (" + osFrag + ") AppleWebKit/537.36 (KHTML, like Gecko) Chrome/" + chromeVer + " Safari/537.36"
}

func firefoxUA(osFrag, ffVer string) string {
	return "Mozilla/5.0 (" + osFrag + "; rv:" + ffVer + ") Gecko/20100101 Firefox/" + ffVer
}

func chromeSecChUA(notABrand, major string) string {
	return notABrand + `;v="99", "Google Chrome";v="` + major + `", "Chromium";v="` + major + `"`
}
