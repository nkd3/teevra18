$hook = @'
@echo off
setlocal enabledelayedexpansion

REM ---- Config ----
set "PROJECT_ROOT=C:\teevra18"
set "PYEXE=%PROJECT_ROOT%\.venv\Scripts\python.exe"
if not exist "%PYEXE%" set "PYEXE=python"

REM ---- Ensure docs folder exists ----
if not exist "%PROJECT_ROOT%\docs_md" mkdir "%PROJECT_ROOT%\docs_md"

echo [pre-commit] Generating docs using %PYEXE% ...
"%PYEXE%" "%PROJECT_ROOT%\scripts\generate_docs.py"
if errorlevel 1 (
  echo [pre-commit] ERROR: generate_docs.py failed. Aborting commit.
  endlocal
  exit /b 1
)

echo [pre-commit] Staging docs_md...
git add "%PROJECT_ROOT%\docs_md"

echo [pre-commit] Done.
endlocal
exit /b 0
'@

Set-Content -Path C:\teevra18\.githooks\pre-commit.cmd -Value $hook -Encoding ASCII
