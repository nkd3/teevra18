# C:\teevra18\scripts\run_m11_once.ps1
$ErrorActionPreference = "Stop"

# --- Paths & env ---
$env:DB_PATH = "C:\teevra18\data\teevra18.db"
$LogFile = "C:\teevra18\logs\m11_runner.log"

# --- Activate venv ---
& "C:\teevra18\.venv\Scripts\Activate.ps1"

# --- Ensure logs dir ---
New-Item -ItemType Directory -Path "C:\teevra18\logs" -ErrorAction SilentlyContinue | Out-Null

# --- Header (no parentheses, no here-strings needed) ---
$stamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss 'UTC'"
Add-Content -Path $LogFile -Value "$stamp"
Add-Content -Path $LogFile -Value "---- M11 RUN ----"

# --- Pipeline (each step appends to the same log) ---
python "C:\teevra18\services\m11\build_features_m11.py"  2>&1 | Tee-Object -FilePath $LogFile -Append
python "C:\teevra18\services\m11\train_m11.py"           2>&1 | Tee-Object -FilePath $LogFile -Append
python "C:\teevra18\services\m11\infer_m11.py"           2>&1 | Tee-Object -FilePath $LogFile -Append
python "C:\teevra18\services\m11\gate_alerts_m11.py"     2>&1 | Tee-Object -FilePath $LogFile -Append
