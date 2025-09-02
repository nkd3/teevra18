# C:\teevra18\scripts\notion_autosync_upsert.py
# VER: upsert-PO-1.2  (properties + safe AUTODOC body; strict .env logging; db-wal ignore)

import os, time, datetime, re, random, threading, queue
from pathlib import Path
from dotenv import load_dotenv
from notion_client import Client
from notion_client.errors import APIResponseError
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, FileModifiedEvent, FileCreatedEvent, FileMovedEvent

# ----- Paths & constants -----
ENV_PATH      = r"C:\teevra18\.env"
ROOT_FALLBACK = r"C:\teevra18"
RUNTIME_DIR   = Path(r"C:\teevra18_runtime")
LOG_FILE      = RUNTIME_DIR / "autosync.log"

SUMMARY_TAG   = "My Laptop <=> Notion DB"

UPDATE_BODY = True                      # body mirroring via a single marker paragraph
BODY_MARKER_PREFIX = "AUTODOC: "        # must match reset tool
MAX_BODY_CHARS = 1950                   # leave headroom for prefix; Notion hard limit is 2000
TRUNC_TAIL = "\n...\n[truncated {n} chars]"

DEBOUNCE_S  = 1.2
PAUSE_SEC   = 0.1

RUNTIME_DIR.mkdir(parents=True, exist_ok=True)

def log(msg: str):
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"[{datetime.datetime.now().isoformat(timespec='seconds')}] {msg}\n")

log("=== autosync start (VER upsert-PO-1.2) ===")

# ----- Robust .env loading with extra diagnostics -----
def load_env_strict():
    loaded = load_dotenv(ENV_PATH, override=True)
    tok = os.getenv("NOTION_TOKEN") or ""
    db  = os.getenv("NOTION_DB") or ""
    # Fallback: very simple manual parse if load_dotenv returned False
    if not loaded or not tok or not db:
        try:
            with open(ENV_PATH, "r", encoding="utf-8") as fh:
                for line in fh:
                    if line.startswith("NOTION_TOKEN=") and not tok:
                        tok = line.strip().split("=", 1)[1]
                    elif line.startswith("NOTION_DB=") and not db:
                        db = line.strip().split("=", 1)[1]
        except Exception as e:
            log(f"[env] FATAL cannot read .env: {e}")
    # Log diagnostics
    tok_prefix = tok[:4] if tok else ""
    tok_len    = len(tok)
    db_len     = len(db)
    log(f"[env] .env loaded={loaded} token_prefix={tok_prefix} token_len={tok_len} db_len={db_len}")
    return tok, db

NOTION_TOKEN, DATABASE_ID = load_env_strict()
ROOT_DIR = Path(os.getenv("AUTOSYNC_ROOT", ROOT_FALLBACK))
if not NOTION_TOKEN or not DATABASE_ID:
    log("FATAL: NOTION_TOKEN/NOTION_DB missing"); raise SystemExit(1)

notion = Client(auth=NOTION_TOKEN)

# ----- Filters -----
IGNORED_DIRS = {
    ".git", ".venv", "__pycache__", ".idea", ".vscode",
    "dist", "build", "logs", RUNTIME_DIR.name,
}
IGNORED_EXTS = {
    ".png", ".jpg", ".jpeg", ".ico", ".gif",
    ".zip", ".7z", ".parquet", ".db", ".sqlite", ".sqlite3", ".pkl", ".pdf",
    ".cmd", ".bak",
}
IGNORED_SUFFIXES = {".db-wal", ".db-shm"}   # sqlite journaling files

TEXT_HINT_EXTS = {
    ".py", ".txt", ".md", ".json", ".yaml", ".yml", ".js", ".ts",
    ".css", ".html", ".ini", ".cfg", ".log",
}

stage_pattern = re.compile(r"(\\|/)(M[0-1]?[0-9])(\\|/)")

def is_ignored(path: Path) -> bool:
    if any(part in IGNORED_DIRS for part in path.parts):
        return True
    # suffix checks first (e.g., file.db-wal)
    for s in IGNORED_SUFFIXES:
        if str(path).endswith(s):
            return True
    if path.suffix.lower() in IGNORED_EXTS:
        return True
    return False

def looks_like_text(path: Path) -> bool:
    if path.suffix.lower() in TEXT_HINT_EXTS: return True
    try:
        with open(path, "rb") as f:
            return b"\x00" not in f.read(512)
    except Exception:
        return False

def infer_stage(relpath: str) -> str:
    m = stage_pattern.search(relpath)
    return m.group(2) if m else ""

def backoff_sleep(attempt): time.sleep(0.4 * (2 ** attempt) + random.uniform(0, 0.2))

def query_latest_by_file(relpath: str):
    return notion.databases.query(
        **{
            "database_id": DATABASE_ID,
            "filter": {"property": "File", "rich_text": {"equals": relpath}},
            "sorts": [{"timestamp": "last_edited_time", "direction": "descending"}],
            "page_size": 1,
        }
    )

# ----- DB properties / index -----
DB_PROPS = notion.databases.retrieve(DATABASE_ID).get("properties", {})
log(f"[startup] DB properties: {list(DB_PROPS.keys())}")

FILE_INDEX: dict[str, str] = {}
_last_seen: dict[str, float] = {}

def ensure_page(rel: str, stage: str) -> str:
    pid = FILE_INDEX.get(rel)
    if pid: return pid
    try:
        res = query_latest_by_file(rel); results = res.get("results", [])
        if results:
            pid = results[0]["id"]; FILE_INDEX[rel] = pid; return pid
    except APIResponseError as e:
        log(f"[query latest] {e}")
    # create new
    attempts = 0
    while True:
        try:
            props = {
                "Name":      {"title":     [{"text": {"content": rel}}]},
                "File":      {"rich_text": [{"text": {"content": rel}}]},
                "Content":   {"rich_text": [{"text": {"content": f'{Path(rel).name} | {SUMMARY_TAG}'}}]},
                "Timestamp": {"date":      {"start": datetime.datetime.now().isoformat()}},
            }
            if "Stage" in DB_PROPS:
                props["Stage"] = {"rich_text": [{"text": {"content": stage}}]}
            pg = notion.pages.create(parent={"database_id": DATABASE_ID}, properties=props)
            pid = pg["id"]; FILE_INDEX[rel] = pid; return pid
        except APIResponseError as e:
            if e.status in (409, 429, 503) and attempts < 5:
                attempts += 1; backoff_sleep(attempts); continue
            raise

def update_props(page_id: str, rel: str, stage: str):
    summary = f"{Path(rel).name} | {SUMMARY_TAG}"
    props = {
        "Content":   {"rich_text": [{"text": {"content": summary}}]},
        "Timestamp": {"date":      {"start": datetime.datetime.now().isoformat()}},
    }
    if "Stage" in DB_PROPS:
        props["Stage"] = {"rich_text": [{"text": {"content": stage}}]}
    attempts = 0
    while True:
        try:
            resp = notion.pages.update(page_id=page_id, archived=False, properties=props)
            log(f"[OK] {rel} last_edited_time={resp.get('last_edited_time')}")
            return
        except APIResponseError as e:
            if e.status in (409, 429, 503) and attempts < 5:
                attempts += 1; backoff_sleep(attempts); continue
            raise

# ----- Safe AUTODOC body upsert (clamped) -----
def _clamp_for_body(text: str) -> str:
    # Make sure BODY_MARKER_PREFIX + content <= 2000
    max_payload = MAX_BODY_CHARS
    if len(text) > max_payload:
        over = len(text) - max_payload
        text = text[:max_payload] + TRUNC_TAIL.format(n=over)
    # If after adding tail we overshoot, hard clamp
    max_payload = MAX_BODY_CHARS
    if len(text) > max_payload:
        text = text[:max_payload]
    return text

def upsert_marker_paragraph(page_id: str, full_text: str):
    """Update or append one paragraph starting with BODY_MARKER_PREFIX.
       No deletes, no multi-block writes to avoid conflicts."""
    snippet = _clamp_for_body(full_text)
    content = BODY_MARKER_PREFIX + snippet
    # Secondary safety: Notion limit ~2000 per rich_text item
    if len(content) > 1990:
        content = content[:1990]

    try:
        kids = notion.blocks.children.list(block_id=page_id, page_size=50).get("results", [])
        for b in kids:
            if b.get("type") != "paragraph": continue
            rts = b["paragraph"].get("rich_text", [])
            plain = "".join(rt.get("plain_text", "") for rt in rts)
            if plain.startswith(BODY_MARKER_PREFIX):
                notion.blocks.update(
                    block_id=b["id"],
                    paragraph={"rich_text": [{"type": "text", "text": {"content": content}}]},
                )
                return
    except APIResponseError as e:
        log(f"[body] list fail: {e}")

    try:
        notion.blocks.children.append(
            block_id=page_id,
            children=[{
                "object": "block",
                "type": "paragraph",
                "paragraph": {"rich_text": [{"type": "text", "text": {"content": content}}]},
            }],
        )
    except APIResponseError as e:
        log(f"[body] append fail: {e}")

# ----- Work queue (serialize writes) -----
work_q: "queue.Queue[str]" = queue.Queue()
in_q: set[str] = set()
lock = threading.Lock()

def enqueue(rel: str):
    with lock:
        if rel not in in_q:
            in_q.add(rel)
            work_q.put(rel)

def worker():
    while True:
        rel = work_q.get()
        try:
            stage = infer_stage(rel)
            pid = ensure_page(rel, stage)
            log(f"[TARGET] {rel} -> {pid}")
            update_props(pid, rel, stage)

            if UPDATE_BODY:
                try:
                    file_path = ROOT_DIR / rel
                    text = ""
                    try:
                        with open(file_path, "r", encoding="utf-8", errors="ignore") as fh:
                            text = fh.read()
                    except Exception as ee:
                        text = f"[unable to read: {ee}]"
                    upsert_marker_paragraph(pid, text)
                except Exception as e:
                    log(f"[body] upsert fail {rel}: {e}")

            log(f"[UPD] {rel}")
            time.sleep(PAUSE_SEC)
        except Exception as e:
            log(f"[worker] FAIL {rel}: {e}")
        finally:
            with lock: in_q.discard(rel)
            work_q.task_done()

threading.Thread(target=worker, daemon=True).start()

# ----- Watcher -----
class Handler(FileSystemEventHandler):
    def _maybe(self, path: Path):
        if path.is_dir(): return
        if is_ignored(path) or not looks_like_text(path): return
        rel = str(path.relative_to(ROOT_DIR))
        now = time.time()
        if now - _last_seen.get(rel, 0) < DEBOUNCE_S: return
        _last_seen[rel] = now
        enqueue(rel)

    def on_modified(self, e: FileModifiedEvent): self._maybe(Path(e.src_path))
    def on_created (self, e: FileCreatedEvent):  self._maybe(Path(e.src_path))
    def on_moved   (self, e: FileMovedEvent):    self._maybe(Path(e.dest_path))

def startup_sweep(minutes=5):
    cutoff = time.time() - (minutes * 60)
    count = 0
    for p in ROOT_DIR.rglob("*"):
        if p.is_dir() or is_ignored(p) or not looks_like_text(p): continue
        try:
            if p.stat().st_mtime >= cutoff:
                enqueue(str(p.relative_to(ROOT_DIR))); count += 1
        except Exception: continue
    log(f"[startup] sweep enqueued: {count}")

if __name__ == "__main__":
    log(f"[startup] watching: {ROOT_DIR}")
    startup_sweep(5)
    obs = Observer(); h = Handler()
    obs.schedule(h, str(ROOT_DIR), recursive=True)
    obs.start()
    try:
        while True: time.sleep(1.0)
    except KeyboardInterrupt:
        obs.stop()
    obs.join()
