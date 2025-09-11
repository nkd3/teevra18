param(
  [ValidateSet('AUTO','NIGHTLY','INTRADAY')]
  [string]$Mode = 'AUTO',
  [switch]$ForceOneShot
)
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
[Console]::OutputEncoding = [Text.UTF8Encoding]::UTF8

$Root    = 'C:\teevra18'
$CfgPath = Join-Path $Root 'config\orchestrator.config.json'
$VenvPy  = Join-Path $Root '.venv\Scripts\pythonw.exe'
$CorePy  = Join-Path $Root 'services\svc-strategy-core.py'
$PaperPy = Join-Path $Root 'services\svc-paper-trader.py'
$EodPS1  = Join-Path $Root 'scripts\run_eod.ps1'
$LogsDir = Join-Path $Root 'logs'
$StopFlag= Join-Path $Root 'ops\STOP_ORCH.txt'
$LogFile = Join-Path $LogsDir ("orchestrator_{0:yyyyMMdd}.log" -f (Get-Date))

function Write-Log{ param([string]$m) ("{0:yyyy-MM-dd HH:mm:ss} | {1}" -f (Get-Date), $m) | Out-File $LogFile -Append -Encoding UTF8 }
function Test-File{ param([string]$p,[string]$d) if(!(Test-Path -LiteralPath $p)){Write-Log "MISSING: $d at $p"; return $false}; return $true }

# Load config (hard-fail to log if missing)
$cfg = $null
try {
  if (Test-Path -LiteralPath $CfgPath) {
    $cfg = Get-Content $CfgPath -Raw -Encoding UTF8 | ConvertFrom-Json
  } else {
    Write-Log "Config not found at $CfgPath"
  }
} catch { Write-Log "Config parse error: $($_.Exception.Message)" }

function Expand-LogPath { param([string]$pattern)
  $today = (Get-Date -Format 'yyyyMMdd')
  return $pattern -replace '\{YYYYMMDD\}',$today
}

function Invoke-PS1{
  param([string]$ps1,[int]$TimeoutSec=900,[string[]]$psArgs)
  if(!(Test-File $ps1 'Script')){ return $false }
  try{
    $psi=New-Object System.Diagnostics.ProcessStartInfo
    $psi.FileName="$env:SystemRoot\System32\WindowsPowerShell\v1.0\powershell.exe"
    $psi.Arguments="-NoLogo -NoProfile -NonInteractive -WindowStyle Hidden -ExecutionPolicy Bypass -File `"$ps1`" $($psArgs -join ' ')"
    $psi.CreateNoWindow=$true; $psi.UseShellExecute=$false
    $psi.RedirectStandardOutput=$true; $psi.RedirectStandardError=$true
    $p=[System.Diagnostics.Process]::Start($psi)
    if(-not $p.WaitForExit($TimeoutSec*1000)){$p.Kill(); throw "Timeout $TimeoutSec s"}
    $stdout = $p.StandardOutput.ReadToEnd()
    $stderr = $p.StandardError.ReadToEnd()
    if($p.ExitCode -ne 0){
      Write-Log ("PS1 NONZERO {0} | STDERR:{1} | STDOUT:{2}" -f $p.ExitCode, ($stderr.Trim()), ($stdout.Trim()))
      return $false
    }
    Write-Log ("PS1 OK: {0}" -f $ps1); return $true
  }catch{ Write-Log ("PS1 ERROR {0} :: {1}" -f $ps1,$_.Exception.Message); return $false }
}

function Invoke-Py{
  param([string]$py,[int]$TimeoutSec=300,[string[]]$pyArgs)
  if(!(Test-File $VenvPy 'pythonw.exe')){ return $false }
  if(!(Test-File $py 'Python script')){ return $false }
  try{
    Write-Log ("CALL PY: {0} ARGS: {1}" -f $py, ($pyArgs -join ' '))
    $psi=New-Object System.Diagnostics.ProcessStartInfo
    $psi.FileName=$VenvPy
    $psi.Arguments=(@("`"$py`"")+ $pyArgs) -join ' '
    $psi.CreateNoWindow=$true; $psi.UseShellExecute=$false
    $psi.RedirectStandardOutput=$true; $psi.RedirectStandardError=$true
    $p=[System.Diagnostics.Process]::Start($psi)
    if(-not $p.WaitForExit($TimeoutSec*1000)){$p.Kill(); throw "Timeout $TimeoutSec s"}
    $stdout = $p.StandardOutput.ReadToEnd()
    $stderr = $p.StandardError.ReadToEnd()
    if($p.ExitCode -ne 0){
      $sOut = if([string]::IsNullOrWhiteSpace($stdout)){"<empty>"} else {$stdout.Trim()}
      $sErr = if([string]::IsNullOrWhiteSpace($stderr)){"<empty>"} else {$stderr.Trim()}
      Write-Log ("PY NONZERO EXIT {0} | STDERR: {1} | STDOUT: {2}" -f $p.ExitCode,$sErr,$sOut)
      return $false
    }
    Write-Log ("PY OK: {0}" -f $py)
    return $true
  }catch{
    Write-Log ("PY ERROR {0} :: {1}" -f $py,$_.Exception.Message)
    return $false
  }
}

function In-MarketWindow{
  $n=Get-Date; if($n.DayOfWeek -in 'Saturday','Sunday'){ return $false }
  $s=Get-Date -Hour 4 -Minute 45 -Second 0
  $e=Get-Date -Hour 11 -Minute 0 -Second 0
  return ($n -ge $s -and $n -le $e)
}

function Run-CoreOnce {
  if ($null -eq $cfg) { Write-Log "No config; skip Core"; return $false }
  $c = $cfg.core
  $coreArgs = @(
    '--db', "`"$($c.db)`"",
    '--matrix', "`"$($c.matrix)`"",
    '--rr_profile', "`"$($c.rr_profile)`"",
    '--universe', "`"$($c.universe)`"",
    '--log', "`"$(Expand-LogPath $c.log_file)`""
  )
  if ($c.extra_args) { $coreArgs += $c.extra_args }
  return Invoke-Py $CorePy -TimeoutSec 240 -pyArgs $coreArgs
}

function Run-PaperOnce {
  if ($null -eq $cfg) { Write-Log "No config; skip PaperTrader"; return $false }
  $p = $cfg.paper
  $paperArgs = @(
    '--db', "`"$($p.db)`"",
    '--log', "`"$(Expand-LogPath $p.log_file)`""
  )
  if ($p.extra_args) { $paperArgs += $p.extra_args }
  return Invoke-Py $PaperPy -TimeoutSec 240 -pyArgs $paperArgs
}

function Run-Nightly{
  Write-Log "Nightly START"; $ok=$true
  Write-Log "Nightly END ok=$ok"; return $ok
}

function Run-OneTick {
  $okCore = Run-CoreOnce
  if(-not $okCore){ Write-Log "Core failed; retry once"; Start-Sleep 3; $null = Run-CoreOnce }
  $okPaper = Run-PaperOnce
  if(-not $okPaper){ Write-Log "PaperTrader failed; retry once"; Start-Sleep 3; $null = Run-PaperOnce }
}

function Run-IntradayAndEOD{
  Write-Log "Intraday START"
  if ($ForceOneShot) {
    Run-OneTick
  } else {
    while(In-MarketWindow){
      if(Test-Path $StopFlag){ Write-Log "STOP flag detected  exiting intraday"; break }
      Run-OneTick
      $now=Get-Date; $next=$now.AddMinutes(5-($now.Minute%5)).AddSeconds(-$now.Second)
      $sleep=[int]([Math]::Max(5, ($next-$now).TotalSeconds))
      Start-Sleep -Seconds $sleep
    }
  }
  Write-Log "Intraday END"

  if ($cfg -and (Test-Path -LiteralPath $EodPS1)) {
    Write-Log "EOD START"
    $ok=Invoke-PS1 $EodPS1 -TimeoutSec 900
    Write-Log "EOD END ok=$ok"
    return $ok
  } else {
    Write-Log "EOD disabled or missing; skipping"
    return $false
  }
}

try{
  New-Item -ItemType Directory -Force -Path $LogsDir | Out-Null
  Write-Log ("Orchestrator START mode={0}" -f $Mode)
  $result=$true
  switch($Mode){
    'NIGHTLY'  { $result=Run-Nightly }
    'INTRADAY' { $result=Run-IntradayAndEOD }
    default    { $result= if(In-MarketWindow){ Run-IntradayAndEOD } else { Run-Nightly } }
  }
  Write-Log ("Orchestrator END ok={0}" -f $result)
  exit 0
}catch{
  Write-Log ("FATAL: {0}" -f $_.Exception.Message)
  exit 0
}
