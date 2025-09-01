# C:\teevra18\scripts\generate_docs.py
import os
import sqlite3
import subprocess
import datetime
from pathlib import Path
import sys

# ---------------- Config (env with safe defaults) -------------------
PROJECT_ROOT = Path(os.getenv("PROJECT_ROOT", r"C:\teevra18"))
DOCS_OUT     = Path(os.getenv("DOCS_OUT",     r"C:\teevra18\docs_md\generated"))
DB_PATH      = Path(os.getenv("DB_PATH",      r"C:\teevra18\data\teevra18.db"))

# Ensure output dirs exist
(PROJECT_ROOT / "docs_md").mkdir(parents=True, exist_ok=True)
DOCS_OUT.mkdir(parents=True, exist_ok=True)

now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def _write(path: Path, text: str):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    print(f"[WRITE] {path}")

# ---------------- 1) Python API docs via pdoc ---------------------------
def run_pdoc():
    out_dir = DOCS_OUT / "api"
    out_dir.mkdir(parents=True, exist_ok=True)
    services_dir = PROJECT_ROOT / "services"

    if not services_dir.exists():
        _write(out_dir / "README.md", "# API\n\nNo services/ directory found.\n")
        return

    # Prefer venv python, fallback to system
    python_exe = PROJECT_ROOT / ".venv" / "Scripts" / "python.exe"
    python_cmd = str(python_exe if python_exe.exists() else "python")

    try:
        print(f"[PDOC] Running pdoc on {services_dir} ...")
        subprocess.run(
            [python_cmd, "-m", "pdoc", "-o", str(out_dir), str(services_dir)],
            check=True
        )
        print(f"[PDOC] Done. Output -> {out_dir}")
    except Exception as e:
        _write(out_dir / "README.md", f"# API\n\nCould not run pdoc.\n\nError: {e}\n")

# ---------------- 2) SQLite schema dump ---------------------------------
def dump_sqlite_schema():
    out_path = DOCS_OUT / "db_schema.md"
    if not DB_PATH.exists():
        _write(out_path,
               f"# Database Schema (auto-generated)\n\n_Last updated: {now}_\n\n"
               f"> ⚠ DB not found at `{DB_PATH}`.\n")
        return

    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
        SELECT name, sql
        FROM sqlite_master
        WHERE type='table' AND name NOT LIKE 'sqlite_%'
        ORDER BY name;
    """)
    rows = cur.fetchall()
    con.close()

    lines = [
        f"# Database Schema (auto-generated)\n",
        f"_Last updated: {now}_\n",
        "## Tables\n"
    ]
    tables = []
    for name, ddl in rows:
        tables.append(name)
        lines.append(f"### {name}\n```sql\n{ddl}\n```\n")

    # Minimal Mermaid (tables only)
    lines.append("## Diagram (Mermaid)\n```mermaid")
    lines.append("flowchart LR")
    for t in tables:
        safe = str(t).replace('"', "'")
        lines.append(f'  {safe}["{safe}"]')
    lines.append("```")
    lines.append("")  # trailing newline

    _write(out_path, "\n".join(lines))

# ---------------- 3) Architecture page ----------------------------------
def write_architecture():
    # Build the markdown without triple-quote pitfalls
    parts = [
        "# Teevra18 — Architecture & Pipeline",
        "",
        f"_Last updated: {now}_",
        "",
        "## System Architecture (Mermaid)",
        "```mermaid",
        "graph TD",
        '  subgraph DhanHQ["DhanHQ APIs"]',
        '    LMF["Live Market Feed (WS)"]',
        '    MQA["Market Quote API"]',
        '    D20["20-Depth (WS)"]',
        '    HIST["Historical API"]',
        '    OCH["Option Chain API"]',
        "  end",
        '  subgraph Local["TEEVRA18 (Windows Laptop)"]',
        '    ING["svc-ingest-dhan"]',
        '    QSN["svc-quote-snap"]',
        '    DEP["svc-depth20"]',
        '    CAN["svc-candles"]',
        '    OCF["svc-option-chain"]',
        '    HIS["svc-historical"]',
        '    RRB["svc-rr-builder"]',
        '    STR["svc-strategy-core"]',
        '    PAP["svc-paper-pm"]',
        '    KPI["svc-kpi-eod"]',
        '    PRD["svc-forecast"]',
        '    UI["ui-control-panel"]',
        '    DB["SQLite DB"]',
        '    PAR["Parquet Files"]',
        "  end",
        '  subgraph Ops["External Ops"]',
        '    TGM["Telegram Bot"]',
        '    NOT["Notion"]',
        '    GIT["GitHub"]',
        "  end",
        "  LMF --> ING --> DB",
        "  D20 --> DEP --> DB",
        "  MQA --> QSN --> DB",
        "  OCH --> OCF --> DB",
        "  HIST --> HIS --> PAR",
        "  DB --> CAN --> DB",
        "  DB --> STR --> DB",
        "  DB --> PAP --> DB",
        "  DB --> KPI --> DB",
        "  DB --> PRD --> DB",
        "  RRB --- STR",
        "  UI --> ING",
        "  UI --> STR",
        "  UI --> PAP",
        "  UI --> KPI",
        "  DB --- UI",
        "  PAR --- UI",
        "  STR --> TGM",
        "  KPI --> TGM",
        "  UI --> NOT",
        "  UI --> GIT",
        "```",
        ""
    ]
    _write(DOCS_OUT / "architecture.md", "\n".join(parts))

# ---------------- 4) Root README ----------------------------------------
def write_root_readme():
    txt = (
        "# Teevra18 — Auto Documentation\n\n"
        f"- Generated: **{now}**\n"
        "- API docs: `docs_md/generated/api/`\n"
        "- DB schema: `docs_md/generated/db_schema.md`\n"
        "- Architecture: `docs_md/generated/architecture.md`\n\n"
        "> This directory is fully generated. Do not manually edit files inside `generated/`.\n"
    )
    _write(PROJECT_ROOT / "docs_md" / "README.md", txt)

# ---------------- Orchestrator ------------------------------------------
def main():
    print("[START] generate_docs.py")
    try:
        run_pdoc()
    except Exception as e:
        print(f"[ERROR] run_pdoc: {e}", file=sys.stderr)
    try:
        dump_sqlite_schema()
    except Exception as e:
        print(f"[ERROR] dump_sqlite_schema: {e}", file=sys.stderr)
    try:
        write_architecture()
    except Exception as e:
        print(f"[ERROR] write_architecture: {e}", file=sys.stderr)
    try:
        write_root_readme()
    except Exception as e:
        print(f"[ERROR] write_root_readme: {e}", file=sys.stderr)

    # Final existence check to fail loudly if nothing got written
    missing = []
    if not (DOCS_OUT / "architecture.md").exists(): missing.append(str(DOCS_OUT / "architecture.md"))
    if not (DOCS_OUT / "db_schema.md").exists():   missing.append(str(DOCS_OUT / "db_schema.md"))
    if not (PROJECT_ROOT / "docs_md" / "README.md").exists(): missing.append(str(PROJECT_ROOT / "docs_md" / "README.md"))
    if missing:
        print("[FAIL] Expected files missing after generation:\n- " + "\n- ".join(missing), file=sys.stderr)
        sys.exit(1)
    print("[OK] Docs generated.")
    print(f"[INFO] Output folder: {DOCS_OUT}")

if __name__ == "__main__":
    main()
