@echo off
title Universal - Git Auto-Sync
color 0B

echo ==============================================================================
echo                      UNIVERSAL - AUTO-SYNC / PUSH
echo ==============================================================================
echo.

cd /d "%~dp0"

echo [*] Verificando estado del repositorio...
git status --short > "%temp%\git_status.tmp"
for %%R in ("%temp%\git_status.tmp") do if %%~zR==0 (
    color 0E
    echo.
    echo [i] No hay cambios pendientes por subir. El repositorio esta al dia.
    del "%temp%\git_status.tmp" >nul 2>&1
    echo.
    pause
    exit /b 0
)
del "%temp%\git_status.tmp" >nul 2>&1

echo.
echo [*] Archivos modificados o nuevos:
git status -s
echo.
echo ------------------------------------------------------------------------------
set /p commit_msg="Ingresa mensaje de commit (o presiona ENTER para auto): "

if not defined commit_msg set "commit_msg=feat(Universal): actualizacion automatica"

echo.
echo [*] Agregando cambios (git add .)...
git add .

echo [*] Creando commit...
git commit -m "%commit_msg%"

echo.
echo [*] Subiendo al servidor GitHub (git push origin main)...
git push origin main

if %ERRORLEVEL% equ 0 (
    color 0A
    echo.
    echo ==============================================================================
    echo  [EXITO] Todos los cambios fueron subidos correctamente al servidor!
    echo ==============================================================================
) else (
    color 0C
    echo.
    echo ==============================================================================
    echo  [ERROR] Ocurrio un error al hacer push. Revisa tu conexion o permisos.
    echo ==============================================================================
)

echo.
pause
