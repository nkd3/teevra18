param(
  [string]$ProjectRoot = "C:\teevra18"
)

$ErrorActionPreference = "Stop"

# --- Helper: load .env into process env vars ---
function Load-DotEnv {
  param([string]$Path)
  if (-not (Test-Path $Path)) { return }
  Get-Content $Path | ForEach-Object {
    $line = $_.Trim()
    if (-not $line -or $line.StartsWith("#")) { return }
    if ($line -match '^\s*([^=]+?)\s*=\s*(.*)\s*$') {
      $name  = $matches[1].Trim()
      $value = $matches[2].Trim()
      # Remove surrounding quotes if present
      if ($value.StartsWith('"') -and $value.EndsWith('"')) { $value = $value.Trim('"') }
      if ($value.StartsWith("'") -and $value.EndsWith("'")) { $value = $value.Trim("'") }
      [Environment]::SetEnvironmentVariable($name, $value)
    }
  }
}

# --- Activate venv (optional but handy) ---
$activate = Join-Path $ProjectRoot ".venv\Scripts\Activate.ps1"
if (Test-Path $activate) {
  . $activate
}

# --- Load env ---
$envPath = Join-Path $ProjectRoot ".env"
Load-DotEnv -Path $envPath

$owner  = $env:GITHUB_OWNER
$repo   = $env:GITHUB_REPO
$branch = $env:GITHUB_DEFAULT_BRANCH
$pages  = $env:GITHUB_PAGES

if (-not $owner -or -not $repo) {
  throw "GITHUB_OWNER / GITHUB_REPO not set in $envPath"
}
if (-not $branch) { $branch = "main" }

Set-Location $ProjectRoot

# --- Make sure .github/workflows/docs.yml exists (Pages via Actions) ---
$wfDir = ".github\workflows"
$wfPath = Join-Path $wfDir "docs.yml"
New-Item -ItemType Directory -Force $wfDir | Out-Null
@"
name: docs
on:
  push:
    branches: [$branch]
permissions:
  contents: write
  pages: write
  id-token: write
jobs:
  build:
    runs-on: windows-latest
    steps:
      - uses: actions/checkout@v4
      - name: Setup Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'
      - name: Generate docs
        run: |
          python -m pip install --upgrade pip
          pip install pdoc python-dotenv
          python .\scripts\generate_docs.py
      - name: Upload artifact
        uses: actions/upload-pages-artifact@v3
        with:
          path: docs_md
  deploy:
    needs: build
    environment:
      name: github-pages
    runs-on: ubuntu-latest
    steps:
      - uses: actions/deploy-pages@v4
"@ | Set-Content -Encoding UTF8 $wfPath

# --- Ensure git repo and first commit/push ---
if (-not (Test-Path ".git")) {
  git init -b $branch
  git add .
  git commit -m "feat: initial auto-docs scaffolding"
  # Create remote repo if missing
  $repoExists = $true
  try { gh repo view "$owner/$repo" | Out-Null } catch { $repoExists = $false }
  if (-not $repoExists) {
    gh repo create "$owner/$repo" --private --source "." --remote origin --push
  } else {
    git remote add origin "https://github.com/$owner/$repo.git" 2>$null
    git push -u origin $branch
  }
} else {
  # Always regenerate docs before commit
  if (Test-Path "$ProjectRoot\scripts\generate_docs.py") {
    python "$ProjectRoot\scripts\generate_docs.py"
  }
  git add .
  git commit -m ("chore(docs): auto-update {0}" -f (Get-Date -Format s)) 2>$null
  git push origin $branch
}

# --- Enable GitHub Pages via API (PowerShell-safe) ---
if ($pages -eq "1") {
  # Create/enable Pages with source branch + path "/"
  # Build JSON in PowerShell (no Bash syntax)
  $json = @{ source = @{ branch = $branch; path = "/" } } | ConvertTo-Json -Depth 5
  try {
    $json | gh api "repos/$owner/$repo/pages" `
      --method POST `
      --header "Accept: application/vnd.github+json" `
      --input -
  } catch {
    # If already exists, PATCH settings to ensure correct source
    $json | gh api "repos/$owner/$repo/pages" `
      --method PATCH `
      --header "Accept: application/vnd.github+json" `
      --input -
  }
}

Write-Host "GitHub publish complete. Repo: https://github.com/$owner/$repo"
