# C:\teevra18\scripts\make_stage_snapshot.py
import os, json, subprocess, sys
from pathlib import Path
from datetime import datetime

ROOT = Path(os.getenv("PROJECT_ROOT", r"C:\teevra18"))
DOCS_DIR = ROOT / "docs_md"
OUT = DOCS_DIR / "generated"
OUT.mkdir(parents=True, exist_ok=True)

def run_verify():
    py = ROOT / ".venv" / "Scripts" / "python.exe"
    verify = ROOT / "services" / "verify_all_tables.py"
    if not verify.exists():
        return {}
    res = subprocess.run([str(py if py.exists() else "python"), str(verify)],
                         capture_output=True, text=True)
    try:
        return json.loads(res.stdout.strip())
    except Exception:
        return {"raw_stdout": res.stdout, "raw_stderr": res.stderr}

def main():
    if len(sys.argv) < 3:
        print("Usage: python make_stage_snapshot.py \"<StageName>\" \"<Notes>\"")
        sys.exit(1)

    stage = sys.argv[1]               # e.g., M0 — Core Config & Schema
    notes = sys.argv[2]

    verify = run_verify()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    wal = verify.get("details", {}).get("wal_mode")
    pass_state = verify.get("pass")
    br_state = verify.get("details", {}).get("breaker_state")
    health_rows = verify.get("details", {}).get("health_rows")
    missing_tbl = verify.get("details", {}).get("tables", {}).get("missing", [])
    missing_idx = verify.get("details", {}).get("indexes", {}).get("missing", [])

    md = f"""# {stage} — Snapshot

_Last updated: {now}_

**Notes:** {notes}

## Acceptance Summary
- PASS: `{pass_state}`
- WAL mode: `{wal}`
- Breaker state: `{br_state}`
- Health rows: `{health_rows}`
- Missing tables: `{missing_tbl}`
- Missing indexes: `{missing_idx}`

## Next
- M1 — Ingest (WS ticks to `ticks_raw`, Parquet sharding, ≤3s gap)
"""
    # Write a per-stage file and update README pointer
    safe_stage = stage.replace(" ", "_").replace("—", "-").replace("/", "-")
    snap_file = OUT / f"{safe_stage}_snapshot.md"
    snap_file.write_text(md, encoding="utf-8")
    print(f"[WRITE] {snap_file}")

    # Update docs index (append stage link)
    readme = DOCS_DIR / "README.md"
    if readme.exists():
        existing = readme.read_text(encoding="utf-8")
    else:
        existing = "# Teevra18 — Auto Documentation\n\n"

    link_line = f"- Snapshot: `generated/{snap_file.name}`\n"
    if link_line not in existing:
        existing += "\n" + link_line
        readme.write_text(existing, encoding="utf-8")
        print("[OK] README.md updated with snapshot link.")

if __name__ == "__main__":
    main()
