$ErrorActionPreference = "Stop"
$env:DB_PATH = "C:\teevra18\data\teevra18.db"

# >>> USE YOUR REAL, VERIFIED VALUES <<<
$env:TELEGRAM_BOT_TOKEN = "8215465763:AAEO9WubobM5Eukus1BuSCNTk3ol29IXCXg"
$env:TELEGRAM_CHAT_ID   = "1278521942"

& "C:\teevra18\.venv\Scripts\Activate.ps1"
New-Item -ItemType Directory -Path "C:\teevra18\logs" -ErrorAction SilentlyContinue | Out-Null

python "C:\teevra18\services\m12\notifier_m12.py" 2>&1 | Tee-Object -FilePath "C:\teevra18\logs\m12_notifier.log" -Append
