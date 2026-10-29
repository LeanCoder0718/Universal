package Universal

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"
)

var updateLeanCoderOnce sync.Once

// UpdateLeanCoderProjects comprueba y descarga automáticamente las últimas actualizaciones
// directamente desde GitHub (github.com/LeanCoder0718) antes de pedir los hilos o tareas.
// No depende de rutas locales del PC: todo se descarga y sincroniza directamente desde GitHub.
// Se ejecuta una única vez por sesión y no detiene el programa si no hay conexión.
func UpdateLeanCoderProjects() {
	updateLeanCoderOnce.Do(func() {
		gitPath, err := exec.LookPath("git")
		if err != nil || gitPath == "" {
			return
		}

		var targetDirs []string
		seen := make(map[string]bool)

		addTarget := func(dir string) {
			if dir == "" {
				return
			}
			abs, err := filepath.Abs(dir)
			if err != nil {
				return
			}
			abs = filepath.Clean(abs)
			if seen[abs] {
				return
			}
			seen[abs] = true

			gitTarget := filepath.Join(abs, ".git")
			if _, err := os.Stat(gitTarget); err == nil {
				targetDirs = append(targetDirs, abs)
			}
		}

		// 1. Directorio del módulo Universal (resuelto directamente en runtime)
		if _, currentFile, _, ok := runtime.Caller(0); ok {
			addTarget(filepath.Dir(currentFile))
		}

		// 2. Directorio de trabajo actual del proyecto que ejecuta la aplicación
		if cwd, err := os.Getwd(); err == nil {
			addTarget(cwd)
		}

		// Filtrar únicamente los repositorios cuyo remote apunte directamente a GitHub (LeanCoder)
		type repoTarget struct {
			dir       string
			name      string
			remoteURL string
		}
		var reposToSync []repoTarget

		for _, dir := range targetDirs {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			cmd := exec.CommandContext(ctx, "git", "-C", dir, "remote", "get-url", "origin")
			out, err := cmd.Output()
			cancel()

			if err == nil {
				remoteURL := strings.TrimSpace(string(out))
				remoteLower := strings.ToLower(remoteURL)
				// Validar que el origen sea directamente GitHub y pertenezca a LeanCoder
				if strings.Contains(remoteLower, "github.com") && strings.Contains(remoteLower, "leancoder") {
					name := filepath.Base(dir)
					// Extraer nombre del repo desde la URL de GitHub (ej. Universal.git -> Universal)
					parts := strings.Split(remoteURL, "/")
					if len(parts) > 0 {
						last := strings.TrimSuffix(parts[len(parts)-1], ".git")
						if last != "" {
							name = last
						}
					}
					reposToSync = append(reposToSync, repoTarget{
						dir:       dir,
						name:      name,
						remoteURL: remoteURL,
					})
				}
			}
		}

		if len(reposToSync) == 0 {
			return
		}

		PrintStepDelimiter()
		fmt.Println(Red(" → "), WhiteColor("VERIFICANDO ACTUALIZACIONES"))
		for _, repo := range reposToSync {
			// Descarga directa desde GitHub usando git pull fast-forward sobre el remote oficial
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			cmd := exec.CommandContext(ctx, "git", "-C", repo.dir, "pull", "--ff-only")
			out, err := cmd.CombinedOutput()
			cancel()

			if err == nil {
				outStr := strings.ToLower(string(out))
				if strings.Contains(outStr, "already up to date") || strings.Contains(outStr, "al día") || strings.Contains(outStr, "actualizado") {
					fmt.Printf("   %s %s (GitHub): %s\n", LimeGreen("✔"), repo.name, WhiteColor("Al día"))
				} else {
					fmt.Printf("   %s %s (GitHub): %s\n", LimeGreen("✔"), repo.name, LimeGreen("¡Descargado y actualizado con éxito!"))
				}
			} else {
				fmt.Printf("   %s %s (GitHub): %s\n", OrangeCo("⚠"), repo.name, WhiteColor("Sin conexión a GitHub (continuando...)"))
			}
		}
	})
}
