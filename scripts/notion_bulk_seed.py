# C:\teevra18\scripts\notion_bulk_seed.py
import os, io, time, datetime, re, sys
from pathlib import Path
from typing import Tuple
from dotenv import load_dotenv
from notion_client import Client
from notion_client.errors import APIResponseError

# --- Load config ---
ENV_PATH = r"C:\teevra18\.env"
load_dotenv(ENV_PATH, override=True)

NOTION_TOKEN = os.getenv("NOTION_TOKEN")
DATABASE_ID  = os.getenv("NOTION_DB")
ROOT_DIR     = Path(os.getenv("AUTOSYNC_ROOT", r"C:\teevra18"))

if not NOTION_TOKEN or not DATABASE_ID:
    sys.exit("ERROR: NOTION_TOKEN or NOTION_DB missing in C:\\teevra18\\.env")

# --- Notion client ---
notion = Client(auth=NOTION_TOKEN)

# --- Controls ---
MAX_SNIPPET = 1800             # keep under Notion rich text block limits
PAUSE_SEC   = 0.45             # ~2.2 req/sec to avoid rate limiting
DRY_RUN     = False            # set True to preview without writing

# --- Ignore lists ---
IGNORED_DIRS = {".git", ".venv", "__pycache__", ".idea", ".vscode", "dist", "build"}
IGNORED_EXTS = {".png", ".jpg", ".jpeg", ".ico", ".gif", ".zip", ".7z",
                ".parquet", ".db", ".sqlite", ".sqlite3", ".pkl", ".pdf"}
TEXT_HINT_EXTS = {".py", ".txt", ".md", ".json", ".yaml", ".yml", ".js", ".ts", ".css", ".html", ".ini", ".cfg", ".log"}

stage_pattern = re.compile(r"(\\|/)(M[0-1]?[0-9])(\\|/)")  # finds M0..M12 in path

def is_ignored(path: Path) -> bool:
    if any(part in IGNORED_DIRS for part in path.parts):
        return True
    if path.suffix.lower() in IGNORED_EXTS:
        return True
    return False

def looks_like_text(path: Path) -> bool:
    # Treat known texty extensions as text; otherwise try small probe
    if path.suffix.lower() in TEXT_HINT_EXTS:
        return True
    try:
        with open(path, "rb") as f:
            chunk = f.read(512)
        # Heuristic: if it has many NUL bytes, likely binary
        if b"\x00" in chunk:
            return False
        return True
    except Exception:
        return False

def read_text_snippet(file_path: Path) -> str:
    try:
        with io.open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            data = f.read()
        if len(data) > MAX_SNIPPET:
            data = data[:MAX_SNIPPET] + "\n...\n[truncated]"
        return data
    except Exception as e:
        return f"[unable to read file: {e}]"

def infer_stage(relpath: str) -> str:
    m = stage_pattern.search(relpath)
    return m.group(2) if m else ""

def find_page_by_file(relpath: str):
    try:
        resp = notion.databases.query(
            **{
                "database_id": DATABASE_ID,
                "filter": {"property": "File", "rich_text": {"equals": relpath}},
                "page_size": 1,
            }
        )
        res = resp.get("results", [])
        return res[0] if res else None
    except APIResponseError as e:
        print(f"[query] FAIL for {relpath}: {e.status} {e.code} - {e.message}")
        return None

def create_page(relpath: str, content: str, stage: str):
    props = {
        "Name":      {"title":     [{"text": {"content": relpath}}]},
        "File":      {"rich_text": [{"text": {"content": relpath}}]},
        "Content":   {"rich_text": [{"text": {"content": content}}]},
        "Timestamp": {"date":      {"start": datetime.datetime.now().isoformat()}},
    }
    if "Stage" in DB_PROPS:
        props["Stage"] = {"rich_text": [{"text": {"content": stage}}]}
    if not DRY_RUN:
        notion.pages.create(parent={"database_id": DATABASE_ID}, properties=props)

def update_page(page_id: str, content: str, stage: str):
    props = {
        "Content":   {"rich_text": [{"text": {"content": content}}]},
        "Timestamp": {"date":      {"start": datetime.datetime.now().isoformat()}},
    }
    if "Stage" in DB_PROPS:
        props["Stage"] = {"rich_text": [{"text": {"content": stage}}]}
    if not DRY_RUN:
        notion.pages.update(page_id=page_id, properties=props)

def walk_files(root: Path):
    for p in root.rglob("*"):
        if p.is_dir():
            # fast skip of ignored subtree
            if any(part in IGNORED_DIRS for part in p.parts):
                # skip visiting children
                continue
            else:
                continue
        yield p

# --- Retrieve DB properties once (validate schema) ---
try:
    db = notion.databases.retrieve(DATABASE_ID)
    DB_PROPS = db.get("properties", {})
    required = {"Name", "File", "Content", "Timestamp"}
    missing = [k for k in required if k not in DB_PROPS.keys()]
    if missing:
        sys.exit(f"ERROR: Notion DB missing properties: {missing}. Create them exactly with these names.")
    print("[seed] DB properties OK:", list(DB_PROPS.keys()))
except APIResponseError as e:
    sys.exit(f"ERROR: Could not retrieve DB: {e.status} {e.code} - {e.message}")

# --- Main run ---
total_files = 0
processed = 0
created = 0
updated = 0
skipped  = 0
failed   = 0

print(f"[seed] Scanning: {ROOT_DIR}")
for path in walk_files(ROOT_DIR):
    total_files += 1
    if is_ignored(path) or not looks_like_text(path):
        skipped += 1
        continue

    rel = str(path.relative_to(ROOT_DIR))
    stage = infer_stage(rel)
    content = read_text_snippet(path)

    try:
        page = find_page_by_file(rel)
        if page:
            update_page(page["id"], content, stage)
            updated += 1
            action = "UPD"
        else:
            create_page(rel, content, stage)
            created += 1
            action = "NEW"
        processed += 1
        print(f"[{action}] {rel} (stage={stage})")
    except APIResponseError as e:
        failed += 1
        print(f"[FAIL] {rel}: {e.status} {e.code} - {e.message}")
    except Exception as e:
        failed += 1
        print(f"[FAIL] {rel}: {e}")

    # throttle to respect Notion API limits
    time.sleep(PAUSE_SEC)

print("\n[seed] DONE")
print(f"  scanned   : {total_files}")
print(f"  processed : {processed}  (created={created}, updated={updated})")
print(f"  skipped   : {skipped}")
print(f"  failed    : {failed}")
