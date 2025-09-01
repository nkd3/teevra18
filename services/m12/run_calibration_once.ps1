$ErrorActionPreference = "Stop"
$env:DB_PATH = "C:\teevra18\data\teevra18.db"
& "C:\teevra18\.venv\Scripts\Activate.ps1"
New-Item -ItemType Directory -Path "C:\teevra18\logs" -ErrorAction SilentlyContinue | Out-Null

# Fit Platt scaling -> writes models\m11\calibration_m11.json and updates model_m11.json
python "C:\teevra18\services\m12\calibrate_fit_platt.py" 2>&1 | Tee-Object -FilePath "C:\teevra18\logs\m11_calibration.log" -Append
