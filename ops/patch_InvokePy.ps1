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
