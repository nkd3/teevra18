param(
  [string]$Root = "C:\teevra18"
)

# Use code points instead of literal curly quotes/dashes to avoid parser issues
$repls = @{
  ([char]0x2018) = "'"    # left single quote
  ([char]0x2019) = "'"    # right single quote
  ([char]0x201C) = '"'    # left double quote
  ([char]0x201D) = '"'    # right double quote
  ([char]0x2014) = '-'    # em dash
  ([char]0x2013) = '-'    # en dash
  ([char]0x2026) = '...'  # ellipsis
  ([char]0x00A0) = ' '    # non-breaking space
}

$targets = Get-ChildItem -Path $Root -Recurse -File -Include *.py
foreach ($t in $targets) {
  $txt = Get-Content -Path $t.FullName -Raw -Encoding UTF8
  foreach ($k in $repls.Keys) {
    $txt = $txt -replace [regex]::Escape([string]$k), [string]$repls[$k]
  }
  Set-Content -Path $t.FullName -Value $txt -Encoding UTF8
}

$cnt = ($targets | Measure-Object).Count
Write-Host ("Sanitized {0} files to UTF-8." -f $cnt)
