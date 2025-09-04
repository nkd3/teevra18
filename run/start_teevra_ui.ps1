$ErrorActionPreference = 'Stop'

# Keep output silent and prevent global/user site-packages leakage
$env:PYTHONUTF8       = 1
$env:PYTHONNOUSERSITE = 1

# Prepend venv paths to PATH (no command substitution, just string concat)
$env:PATH = "C:\teevra18\.venv\Scripts;C:\teevra18\.venv;$($env:PATH)"

# Optional: verify we are using the venv binaries
if (-not (Test-Path "C:\teevra18\.venv\Scripts\streamlit.exe")) { throw "Streamlit not found at C:\teevra18\.venv\Scripts\streamlit.exe" }
if (-not (Test-Path "C:\teevra18\app\ui\Home_Landing.py")) { throw "App entry not found at C:\teevra18\app\ui\Home_Landing.py" }

# Launch headless + hidden; avoids browser pop-ups
Start-Process -FilePath "C:\teevra18\.venv\Scripts\streamlit.exe" -ArgumentList @(
  'run', "C:\teevra18\app\ui\Home_Landing.py",
  '--server.headless', 'true',
  '--server.port', '8501',
  '--server.address', '127.0.0.1'
) -WindowStyle Hidden
