package Universal

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/debug"
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
			checkGoModuleUpdate()
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

// checkGoModuleUpdate se ejecuta cuando el proyecto se importa como dependencia externa de Go
// (en pkg/mod, sin repositorio .git local). Consulta directamente GitHub via git ls-remote
// y compara el commit remoto con la versión compilada en el binario.
func checkGoModuleUpdate() {
	var currentCommit string

	// 1. Intentar leer desde la metadata del build de Go
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, dep := range info.Deps {
			if strings.Contains(dep.Path, "LeanCoder0718/Universal") {
				parts := strings.Split(dep.Version, "-")
				if len(parts) > 0 {
					currentCommit = parts[len(parts)-1]
				}
				break
			}
		}
	}

	// 2. Fallback: extraer la versión del directorio en pkg/mod (ej: ...!universal@v0.0.0-20261030205929-03007bf345cb)
	if currentCommit == "" {
		if _, currentFile, _, ok := runtime.Caller(0); ok {
			dir := filepath.Dir(currentFile)
			if idx := strings.Index(dir, "@"); idx != -1 {
				verPart := dir[idx+1:]
				parts := strings.Split(verPart, "-")
				if len(parts) > 0 {
					cleanEnd := strings.Split(parts[len(parts)-1], string(filepath.Separator))[0]
					if len(cleanEnd) >= 7 {
						currentCommit = cleanEnd
					}
				}
			}
		}
	}

	PrintStepDelimiter()
	fmt.Println(Red(" → "), WhiteColor("VERIFICANDO ACTUALIZACIONES"))

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, "git", "ls-remote", "https://github.com/LeanCoder0718/Universal.git", "refs/heads/main")
	out, err := cmd.Output()
	if err != nil {
		fmt.Printf("   %s %s (GitHub): %s\n", OrangeCo("⚠"), "Universal", WhiteColor("Sin conexión a GitHub (continuando...)"))
		return
	}

	remoteOutput := strings.TrimSpace(string(out))
	if remoteOutput == "" {
		fmt.Printf("   %s %s (GitHub): %s\n", LimeGreen("✔"), "Universal", WhiteColor("Al día"))
		return
	}

	remoteCommit := strings.Fields(remoteOutput)[0]

	// Si no se pudo determinar el commit actual o si coinciden con el remoto
	if currentCommit != "" && (strings.HasPrefix(remoteCommit, currentCommit) || strings.HasPrefix(currentCommit, remoteCommit)) {
		fmt.Printf("   %s %s (GitHub): %s\n", LimeGreen("✔"), "Universal", WhiteColor("Al día"))
		return
	}

	if currentCommit == "" {
		fmt.Printf("   %s %s (GitHub): %s\n", LimeGreen("✔"), "Universal", WhiteColor("Al día"))
		return
	}

	// Hay una versión nueva en GitHub
	fmt.Printf("   %s %s (GitHub): %s\n", OrangeCo("⚡"), "Universal", LimeGreen("¡Nueva versión disponible en GitHub!"))

	// Si el proyecto que lo ejecuta tiene go.mod, intentar actualizarlo automáticamente
	if cwd, err := os.Getwd(); err == nil {
		goModPath := filepath.Join(cwd, "go.mod")
		if _, err := os.Stat(goModPath); err == nil {
			fmt.Printf("   %s Descargando última versión de Universal en go.mod...\n", Red("→"))
			updCtx, updCancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer updCancel()
			updCmd := exec.CommandContext(updCtx, "go", "get", "-u", "github.com/LeanCoder0718/Universal@main")
			updCmd.Dir = cwd
			if updOut, err := updCmd.CombinedOutput(); err == nil {
				fmt.Printf("   %s %s\n", LimeGreen("✔"), WhiteColor("go.mod actualizado con éxito. Reinicia el programa para aplicar."))
			} else {
				_ = updOut
				fmt.Printf("   %s Para actualizar ejecuta: %s\n", OrangeCo("ℹ"), LimeGreen("go get -u github.com/LeanCoder0718/Universal@main"))
			}
		}
	}
}
