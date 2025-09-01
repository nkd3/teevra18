param(
  [string]$ProjectRoot = "C:\Teevra18"
)

$ErrorActionPreference = "Stop"

# Create venv
if (-not (Test-Path "$ProjectRoot\.venv")) {
  py -3.11 -m venv "$ProjectRoot\.venv"
}

# Activate
& "$ProjectRoot\.venv\Scripts\Activate.ps1"

# Upgrade pip
python -m pip install --upgrade pip

# Install deps
pip install pdoc python-dotenv requests pydantic==2.* rich

Write-Host "Bootstrap complete."
