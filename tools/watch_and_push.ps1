param(
  [string]$RepoPath = "C:\teevra18",
  [string]$Branch = "main",
  [int]$DebounceSeconds = 10
)

# ------------------ Settings ------------------
$IncludeExt = @(".py",".ps1",".psm1",".md",".json",".yaml",".yml",".toml",".sql",".txt",".csv",".ini")
$ExcludeDirs = @("\.git\", "\.venv\", "\data\", "\logs\", "\dist\", "\build\", "\.idea\", "\.vscode\")

# ------------------ Helpers -------------------
function Write-Log($msg) {
  $ts = Get-Date -Format "HH:mm:ss"
  Write-Host "[$ts] $msg"
}

function In-ExcludedDir($fullPath) {
  $lower = $fullPath.ToLower()
  foreach ($pat in $ExcludeDirs) {
    if ($lower -like "*$pat*") { return $true }
  }
  return $false
}

function Has-IncludedExt($fullPath) {
  $ext = [System.IO.Path]::GetExtension($fullPath).ToLower()
  if ([string]::IsNullOrEmpty($ext)) { return $false }
  return $IncludeExt -contains $ext
}

# ------------------ Pre-flight -----------------
if (-not (Test-Path $RepoPath)) {
  Write-Error "RepoPath not found: $RepoPath"
  exit 1
}
Set-Location $RepoPath

# Basic git sanity
try {
  git rev-parse --is-inside-work-tree *> $null
} catch {
  Write-Error "This is not a git repo: $RepoPath"
  exit 1
}
Write-Log "Auto-push watcher starting for $RepoPath (branch: $Branch, debounce: $DebounceSeconds s)."

# ------------------ Debounce state -------------
$global:Pending = $false
$global:LastEventAt = Get-Date
$Timer = New-Object System.Timers.Timer
$Timer.Interval = 1000
$Timer.AutoReset = $true

$Timer.Add_Elapsed({
  $age = (New-TimeSpan -Start $global:LastEventAt -End (Get-Date)).TotalSeconds
  if ($global:Pending -and $age -ge $DebounceSeconds) {
    $global:Pending = $false
    try {
      # Stage / commit / push if anything changed
      $status = git status --porcelain
      if ($status) {
        # Use a compact message with counts
        $changedCount = ($status -split "`n").Count
        $msg = "[auto] $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') - $changedCount file(s)"
        Write-Log "Changes detected ($changedCount). Committing…"
        git add -A | Out-Null
        git commit -m $msg | Out-Null
        Write-Log "Pushing to origin/$Branch…"
        git push origin $Branch | Out-Null
        Write-Log "Push complete."
      }
    } catch {
      Write-Log "Push error: $($_.Exception.Message)"
    }
  }
})

# ------------------ Watcher --------------------
$fsw = New-Object System.IO.FileSystemWatcher
$fsw.Path = $RepoPath
$fsw.IncludeSubdirectories = $true
$fsw.Filter = "*.*"
$fsw.EnableRaisingEvents = $true

# Event handler (common)
$handler = {
  param($sender, $e)
  $path = $e.FullPath
  if (-not (Test-Path $path)) {
    # Deleted or rapid rename can leave no file; still trigger
    if (-not (In-ExcludedDir $path)) {
      $global:LastEventAt = Get-Date
      $global:Pending = $true
      return
    }
  } else {
    if (In-ExcludedDir $path) { return }
    if (-not (Has-IncludedExt $path)) { return }
    $global:LastEventAt = Get-Date
    $global:Pending = $true
  }
}

# Register events
$createdReg = Register-ObjectEvent -InputObject $fsw -EventName Created -Action $handler
$changedReg = Register-ObjectEvent -InputObject $fsw -EventName Changed -Action $handler
$renamedReg = Register-ObjectEvent -InputObject $fsw -EventName Renamed -Action $handler
$deletedReg = Register-ObjectEvent -InputObject $fsw -EventName Deleted -Action $handler

# Start timer loop
$Timer.Start()
Write-Log "Watching for changes. Press Ctrl+C to stop."

try {
  while ($true) {
    Start-Sleep -Seconds 1
  }
}
finally {
  $Timer.Stop()
  $Timer.Dispose()
  Unregister-Event -SourceIdentifier $createdReg.Name -ErrorAction SilentlyContinue
  Unregister-Event -SourceIdentifier $changedReg.Name -ErrorAction SilentlyContinue
  Unregister-Event -SourceIdentifier $renamedReg.Name -ErrorAction SilentlyContinue
  Unregister-Event -SourceIdentifier $deletedReg.Name -ErrorAction SilentlyContinue
  $fsw.EnableRaisingEvents = $false
  $fsw.Dispose()
  Write-Log "Watcher stopped."
}
