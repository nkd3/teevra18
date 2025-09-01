param(
  [Parameter(Mandatory=$true)][string]$StageName,
  [Parameter(Mandatory=$true)][string]$Notes
)

$root = "C:\teevra18"
$py = "$root\.venv\Scripts\python.exe"

Write-Host "== Stage Snapshot ==" -ForegroundColor Cyan
& $py "$root\scripts\make_stage_snapshot.py" $StageName $Notes

Write-Host "== Generate Docs ==" -ForegroundColor Cyan
& $py "$root\scripts\generate_docs.py"

Write-Host "== Sync Notion ==" -ForegroundColor Cyan
& $py "$root\scripts\sync_notion.py"

Write-Host "== Git Commit & Push ==" -ForegroundColor Cyan
cd $root
git add .
$commitMsg = "Teevra18 | $StageName complete @ $(Get-Date -Format s) | $Notes"
git commit -m $commitMsg
git push origin main
Write-Host "== DONE ==" -ForegroundColor Green
