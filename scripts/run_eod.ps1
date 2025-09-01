# C:\teevra18\scripts\run_eod.ps1
# Robust wrapper: UTF-8 console + full logs + clean exit code handling

$ErrorActionPreference = "Continue"       # Don't fail on native stderr warnings
# Make console & Python I/O UTF-8 so emojis and non-ASCII are safe
[Console]::OutputEncoding = New-Object System.Text.UTF8Encoding $false
[Console]::InputEncoding  = New-Object System.Text.UTF8Encoding $false
$OutputEncoding = [System.Text.UTF8Encoding]::new($false)
$env:PYTHONIOENCODING = "utf-8"
$env:LANG = "C.UTF-8"
$env:LC_ALL = "C.UTF-8"
$env:PYTHONUNBUFFERED = "1"
$env:PYTHONWARNINGS = "ignore"            # optional: quiet FutureWarnings

$root   = "C:\teevra18"
$venvPy = Join-Path $root ".venv\Scripts\python.exe"
$svc    = Join-Path $root "services\kpi\svc_kpi_eod.py"
$logDir = Join-Path $root "logs"
New-Item -ItemType Directory -Force -Path $logDir | Out-Null
$ts     = Get-Date -Format "yyyy-MM-dd_HH-mm-ss"
$log    = Join-Path $logDir "eod_$ts.log"

# Run and tee both stdout+stderr to file
& $venvPy -u $svc --send --archive --tz Asia/Kolkata 2>&1 | Tee-Object -FilePath $log

# Exit code report
$code = $LASTEXITCODE
if ($code -ne 0) {
  Write-Host "❌ EOD FAILED (exit code $code). See log: $log"
  exit $code
} else {
  Write-Host "✅ EOD OK. Log: $log"
}
