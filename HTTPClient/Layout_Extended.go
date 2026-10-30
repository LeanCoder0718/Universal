package HTTPClient

// NativeAppLayoutSet define el orden de cabeceras para aplicaciones nativas
// (OkHTTP en Android, apps iOS). No incluyen sec-fetch-* ni Client Hints
// porque no son navegadores web.
var NativeAppLayoutSet = LayoutSet{
	BrowserName: "native",
	Layouts: map[RequestKind]Layout{
		KindDocument: {
			Kind: KindDocument,
			Slots: withPseudo(
				Slot{Name: "user-agent", Required: true},
				Slot{Name: "accept", Required: true, StaticValue: "*/*"},
				Slot{Name: "accept-encoding", Required: true},
				Slot{Name: "accept-language", Required: true},
				Slot{Name: "content-type", Required: false},
				Slot{Name: "cookie", Required: false},
			),
		},
		KindXHR: {
			Kind: KindXHR,
			Slots: withPseudo(
				Slot{Name: "user-agent", Required: true},
				Slot{Name: "accept", Required: true, StaticValue: "application/json, text/plain, */*"},
				Slot{Name: "accept-encoding", Required: true},
				Slot{Name: "accept-language", Required: true},
				Slot{Name: "content-type", Required: false},
				Slot{Name: "cookie", Required: false},
			),
		},
	},
}

// SafariLayoutSet define el orden de cabeceras para Safari (macOS e iOS).
// Safari no soporta Client Hints (sec-ch-ua-*) pero sí sec-fetch-*.
// El orden de headers es distinto al de Chrome y Firefox.
var SafariLayoutSet = LayoutSet{
	BrowserName: "safari",
	Layouts: map[RequestKind]Layout{
		KindDocument: {
			Kind: KindDocument,
			Slots: withPseudo(
				Slot{Name: "user-agent", Required: true},
				Slot{Name: "accept", Required: true, StaticValue: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"},
				Slot{Name: "accept-language", Required: true},
				Slot{Name: "accept-encoding", Required: true},
				Slot{Name: "sec-fetch-dest", Required: true, StaticValue: "document"},
				Slot{Name: "sec-fetch-mode", Required: true, StaticValue: "navigate"},
				Slot{Name: "sec-fetch-site", Required: true},
				Slot{Name: "cookie", Required: false},
			),
		},
		KindXHR: {
			Kind: KindXHR,
			Slots: withPseudo(
				Slot{Name: "user-agent", Required: true},
				Slot{Name: "accept", Required: true, StaticValue: "*/*"},
				Slot{Name: "accept-language", Required: true},
				Slot{Name: "accept-encoding", Required: true},
				Slot{Name: "content-type", Required: false},
				Slot{Name: "sec-fetch-dest", Required: true, StaticValue: "empty"},
				Slot{Name: "sec-fetch-mode", Required: true, StaticValue: "cors"},
				Slot{Name: "sec-fetch-site", Required: true},
				Slot{Name: "cookie", Required: false},
			),
		},
	},
}

// EdgeLayoutSet: Edge es Chromium-based, usa el mismo layout que Chrome
// pero con sec-ch-ua que dice "Microsoft Edge" en lugar de "Google Chrome".
// Reutilizamos ChromeLayoutSet ya que el orden de headers es idéntico.
var EdgeLayoutSet = ChromeLayoutSet

// BraveLayoutSet: Brave es Chromium-based, mismo layout que Chrome.
var BraveLayoutSet = ChromeLayoutSet

// OperaLayoutSet: Opera es Chromium-based, mismo layout que Chrome.
var OperaLayoutSet = ChromeLayoutSet
