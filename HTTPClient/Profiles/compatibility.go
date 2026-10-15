package Profiles

import "fmt"

// CompatibilityMatrix define de forma explícita y formal la relación de compatibilidad
// entre Plataformas de Sistema Operativo y Familias de Clientes HTTP / Motores TLS.
//
// Esta matriz garantiza que NUNCA se asigne una combinación ficticia o inválida
// (ej. Safari en Linux, OkHTTP en Windows, Firefox con motor Gecko en iOS).
var CompatibilityMatrix = map[PlatformType]map[ClientType]bool{
	PlatformWindows: {
		ClientChrome:  true,
		ClientFirefox: true,
		ClientEdge:    true,
		ClientBrave:   true,
		ClientOpera:   true,
	},
	PlatformMacOS: {
		ClientChrome:  true,
		ClientFirefox: true,
		ClientSafari:  true,
	},
	PlatformLinux: {
		ClientChrome:  true,
		ClientFirefox: true,
	},
	PlatformIOS: {
		ClientSafariOS: true,
		ClientCustom:   true,
	},
	PlatformAndroid: {
		ClientOkHTTP: true,
		ClientChrome: true,
		ClientCustom: true,
	},
}

// IsCompatible consulta la CompatibilityMatrix de forma O(1) e inmutable para
// verificar si una plataforma y tipo de cliente son técnicamente coherentes.
func IsCompatible(platform PlatformType, client ClientType) bool {
	clients, ok := CompatibilityMatrix[platform]
	if !ok {
		return false
	}
	return clients[client]
}

// SupportedClientsForPlatform retorna una lista de todos los ClientTypes válidos
// para una plataforma dada.
func SupportedClientsForPlatform(platform PlatformType) []ClientType {
	clients, ok := CompatibilityMatrix[platform]
	if !ok {
		return nil
	}
	res := make([]ClientType, 0, len(clients))
	for c, supported := range clients {
		if supported {
			res = append(res, c)
		}
	}
	return res
}

// SupportedPlatformsForClient retorna una lista de todas las plataformas válidas
// para un tipo de cliente dado.
func SupportedPlatformsForClient(client ClientType) []PlatformType {
	var res []PlatformType
	for plat, clients := range CompatibilityMatrix {
		if clients[client] {
			res = append(res, plat)
		}
	}
	return res
}

// ValidateCompatibility verifica la validez estricta de una combinación.
// Si no es compatible, retorna un error descriptivo con la causa exacta.
func ValidateCompatibility(platform PlatformType, client ClientType) error {
	if !IsCompatible(platform, client) {
		return fmt.Errorf("compatibility: la combinación de plataforma %s y cliente %s NO ES COMPATIBLE",
			platform.String(), client.String())
	}
	return nil
}
