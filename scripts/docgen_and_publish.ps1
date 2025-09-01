# C:\teevra18\scripts\docgen_and_publish.ps1
param(
  [string]$ProjectRoot = "C:\teevra18"
)

$ErrorActionPreference = "Stop"

function Load-DotEnv {
  param([string]$Path)
  if (-not (Test-Path $Path)) { return }
  Get-Content $Path | ForEach-Object {
    $line = $_.Trim()
    if (-not $line -or $line.StartsWith("#")) { return }
    if ($line -match "^\s*([^=]+?)\s*=\s*(.*)\s*$") {
      $name  = $matches[1].Trim()
      $value = $matches[2].Trim()
      if ($value.StartsWith('"') -and $value.EndsWith('"')) { $value = $value.Trim('"') }
      if ($value.StartsWith("'") -and $value.EndsWith("'")) { $value = $value.Trim("'") }
      [Environment]::SetEnvironmentVariable($name, $value)
    }
  }
}

# Load environment (.env)
$envPath = Join-Path $ProjectRoot ".env"
Load-DotEnv -Path $envPath

# Activate venv (if present)
$activate = Join-Path $ProjectRoot ".venv\Scripts\Activate.ps1"
if (Test-Path $activate) {
  . $activate
} else {
  Write-Warning "Python venv not found at $activate — continuing with system Python."
}

# 1) Generate docs
$gen = Join-Path $ProjectRoot "scripts\generate_docs.py"
if (-not (Test-Path $gen)) { throw "Missing: $gen" }
Write-Host "Generating docs..." -ForegroundColor Cyan
python $gen

# 2) Publish to GitHub
$pub = Join-Path $ProjectRoot "scripts\publish_github.ps1"
if (Test-Path $pub) {
  Write-Host "Publishing to GitHub..." -ForegroundColor Cyan
  PowerShell -ExecutionPolicy Bypass -File $pub
} else {
  Write-Warning "Skipping GitHub publish — file not found: $pub"
}

# 3) Sync to Notion (only if script + tokens present)
$sync = Join-Path $ProjectRoot "scripts\sync_notion.py"
$hasNotion = (-not [string]::IsNullOrWhiteSpace($env:NOTION_TOKEN)) -and `
             (-not [string]::IsNullOrWhiteSpace($env:NOTION_PARENT_PAGE_ID))

if ((Test-Path $sync) -and $hasNotion) {
  Write-Host "Syncing to Notion..." -ForegroundColor Cyan
  python $sync
}
elseif ((Test-Path $sync) -and (-not $hasNotion)) {
  Write-Host "Notion tokens missing — skipping Notion sync." -ForegroundColor Yellow
}
else {
  Write-Host "No Notion sync script — skipping." -ForegroundColor Yellow
}

Write-Host "All done ✅" -ForegroundColor Green
