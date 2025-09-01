$ErrorActionPreference = "Stop"
$env:DB_PATH = "C:\teevra18\data\teevra18.db"
& "C:\teevra18\.venv\Scripts\Activate.ps1"
New-Item -ItemType Directory -Path "C:\teevra18\logs" -ErrorAction SilentlyContinue | Out-Null

python "C:\teevra18\services\m11\infer_m11.py" 2>&1   | Tee-Object -FilePath "C:\teevra18\logs\m11_reinfer.log" -Append
python "C:\teevra18\services\m11\gate_alerts_m11.py" 2>&1 | Tee-Object -FilePath "C:\teevra18\logs\m11_reinfer.log" -Append
