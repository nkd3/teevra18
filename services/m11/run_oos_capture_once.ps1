$ErrorActionPreference = "Stop"
$env:DB_PATH = "C:\teevra18\data\teevra18.db"
& "C:\teevra18\.venv\Scripts\Activate.ps1"
python "C:\teevra18\services\m11\oos_capture_from_signals.py" 2>&1 | Tee-Object -FilePath "C:\teevra18\logs\m11_oos_capture.log" -Append
