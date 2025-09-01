# C:\teevra18\scripts\peek_predictions.ps1
$ErrorActionPreference = "Stop"

# Make sure both tools & scripts use the same DB
$env:DB_PATH = "C:\teevra18\data\teevra18.db"

# Activate venv
& C:\teevra18\.venv\Scripts\Activate.ps1

# Peek top predictions by probability (latest first)
python C:\teevra18\tools\query_db.py "SELECT ts_utc, instrument, prob_up, exp_move_abs, created_at FROM predictions_m11 ORDER BY created_at DESC, prob_up DESC LIMIT 50;"
