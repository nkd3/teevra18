# C:\teevra18\tools\start_autopush.ps1
$ErrorActionPreference = "Stop"

$RepoPath = "C:\teevra18"
$LogDir   = Join-Path $RepoPath "logs"
$LogFile  = Join-Path $LogDir "autopush.log"

if (-not (Test-Path $LogDir)) { New-Item -ItemType Directory -Path $LogDir | Out-Null }

# Stamp a header per session
"[{0}] === Auto-push session starting ===" -f (Get-Date).ToString("yyyy-MM-dd HH:mm:ss") | Out-File -FilePath $LogFile -Append

# Launch the watcher inline so Task Scheduler can keep it running
# (We still get prints into the log file)
& powershell -NoProfile -ExecutionPolicy Bypass `
  -File "C:\teevra18\tools\watch_and_push.ps1" `
  *> $LogFile 2>&1
