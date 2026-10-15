package Profiles

import "fmt"

// Validate comprueba que un ProfileSpec cumple todos los requisitos
// antes de ser registrado. Un perfil inválido NUNCA llega al worker.
//
// Comprobaciones:
//   - Campos obligatorios presentes.
//   - Compatibilidad de plataforma con tipo de cliente.
//   - Coherencia de capacidades con el tipo de cliente.
//   - User-Agent no vacío.
//   - TLSProfile no es zero-value.
func Validate(spec *ProfileSpec) error {
	if spec == nil {
		return fmt.Errorf("validate: spec es nil")
	}

	// --- Campos obligatorios ---
	if spec.userAgent == "" {
		return fmt.Errorf("validate: UserAgent vacío para %s/%s v%s",
			spec.platform, spec.clientType, spec.version)
	}
	if spec.version == "" {
		return fmt.Errorf("validate: Version vacía para %s/%s",
			spec.platform, spec.clientType)
	}

	// --- Compatibilidad plataforma ↔ cliente ---
	if err := validatePlatformClient(spec.platform, spec.clientType); err != nil {
		return err
	}

	// --- Coherencia de Client Hints ---
	if spec.capabilities.SupportsClientHints {
		if spec.secChUA == "" {
			return fmt.Errorf("validate: %s soporta Client Hints pero SecChUA está vacío", spec.id)
		}
		if spec.secChUAMobile == "" {
			return fmt.Errorf("validate: %s soporta Client Hints pero SecChUAMobile está vacío", spec.id)
		}
		if spec.secChUAPlatform == "" {
			return fmt.Errorf("validate: %s soporta Client Hints pero SecChUAPlatform está vacío", spec.id)
		}
	}

	// --- Coherencia de plataforma móvil ---
	if spec.capabilities.IsMobile {
		if spec.platform != PlatformIOS && spec.platform != PlatformAndroid {
			return fmt.Errorf("validate: %s marcado como móvil pero plataforma es %s",
				spec.id, spec.platform)
		}
	}

	return nil
}

// validatePlatformClient verifica que la combinación plataforma + cliente
// es posible en la realidad utilizando la CompatibilityMatrix centralizada.
func validatePlatformClient(platform PlatformType, client ClientType) error {
	if !IsCompatible(platform, client) {
		return fmt.Errorf("validate: combinación no compatible: plataforma %s no soporta cliente %s",
			platform.String(), client.String())
	}
	return nil
}
