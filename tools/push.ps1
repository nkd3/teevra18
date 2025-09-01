param([string]\ = "[sync] update")
git add -A
if ((git status --porcelain)) {
  git commit -m \
  git push origin main
  Write-Host "[OK] Pushed to origin/main"
} else {
  Write-Host "[SKIP] No changes to commit."
}
