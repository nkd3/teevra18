# C:\teevra18\scripts\sync_notion.py
import os, json, time, requests, subprocess
from pathlib import Path
from typing import Optional

NOTION_VERSION = "2022-06-28"

ROOT_ENV = Path(r"C:\teevra18\.env")
if ROOT_ENV.exists():
    # Manual .env loader (no dependency on python-dotenv)
    for line in ROOT_ENV.read_text(encoding="utf-8").splitlines():
        if not line or line.strip().startswith("#") or "=" not in line: continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip())

NOTION_TOKEN = os.getenv("NOTION_TOKEN", "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")
PARENT_ID    = os.getenv("NOTION_PARENT_PAGE_ID", "25ae38d133c78056aae9f128cea53e38")
PROJECT_ROOT = Path(os.getenv("PROJECT_ROOT", r"C:\teevra18"))
DOCS_OUT     = Path(os.getenv("DOCS_OUT", r"C:\teevra18\docs_md\generated"))

HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": NOTION_VERSION,
    "Content-Type": "application/json"
}

def _assert_env():
    missing = []
    if not NOTION_TOKEN: missing.append("NOTION_TOKEN")
    if not PARENT_ID:    missing.append("NOTION_PARENT_PAGE_ID")
    if missing:
        raise SystemExit(f"Missing in .env: {', '.join(missing)}")

def _req(method: str, url: str, **kwargs):
    r = requests.request(method, url, headers=HEADERS, **kwargs)
    if r.status_code >= 300:
        raise RuntimeError(f"{method} {url} -> {r.status_code}: {r.text[:500]}")
    return r.json()

def get_or_create_child_page(title: str) -> str:
    # Search by title, then filter by parent
    search_payload = {"query": title, "page_size": 25}
    res = _req("POST", "https://api.notion.com/v1/search", json=search_payload)
    for obj in res.get("results", []):
        if obj.get("object") == "page":
            props = obj.get("properties", {})
            p = obj.get("parent", {})
            if p.get("type") == "page_id" and p.get("page_id") == PARENT_ID:
                # Title match?
                tprop = props.get("title") or props.get("Name") or {}
                title_text = ""
                if "title" in tprop:
                    title_text = "".join([x["plain_text"] for x in tprop["title"] if x.get("plain_text")])
                if title_text.strip().lower() == title.strip().lower():
                    print(f"[Notion] Found existing page '{title}' -> {obj['id']}")
                    return obj["id"]

    # Create new child page
    payload = {
        "parent": {"type": "page_id", "page_id": PARENT_ID},
        "properties": {
            "title": {"title": [{"type": "text", "text": {"content": title}}]}
        }
    }
    page = _req("POST", "https://api.notion.com/v1/pages", json=payload)
    print(f"[Notion] Created page '{title}' -> {page['id']}")
    return page["id"]

def replace_page_content_with_markdown(page_id: str, md_text: str):
    # 1) List existing children and archive them (clear page)
    #    A Notion page is a block; children are blocks.
    #    Paginate if many children (rare for our pages).
    while True:
        res = _req("GET", f"https://api.notion.com/v1/blocks/{page_id}/children?page_size=100")
        results = res.get("results", [])
        if not results: break
        for block in results:
            bid = block["id"]
            _req("PATCH", f"https://api.notion.com/v1/blocks/{bid}", json={"archived": True})
        if not res.get("has_more"): break
        time.sleep(0.3)

    # 2) Append one big code block with Markdown for fidelity & speed
    new_blocks = [{
        "object": "block",
        "type": "code",
        "code": {
            "language": "markdown",
            "rich_text": [{"type": "text", "text": {"content": md_text[:190000]}}],
            "caption": [{"type": "text", "text": {"content": "Auto-synced from Teevra18"}}]
        }
    }]
    _req("PATCH", f"https://api.notion.com/v1/blocks/{page_id}/children", json={"children": new_blocks})

def read_file(path: Path) -> Optional[str]:
    if path.exists():
        return path.read_text(encoding="utf-8")
    print(f"[WARN] Missing file: {path}")
    return None

def ensure_docs_generated():
    gen = PROJECT_ROOT / "scripts" / "generate_docs.py"
    if gen.exists():
        print("[Docs] Generating local docs...")
        subprocess.run([str(PROJECT_ROOT / ".venv" / "Scripts" / "python.exe"), str(gen)], check=False)
    else:
        print("[Docs] Skipping generate_docs.py (not found)")

def main():
    _assert_env()
    ensure_docs_generated()

    files = [
        ("Architecture & Pipeline", DOCS_OUT / "architecture.md"),
        ("Database Schema",         DOCS_OUT / "db_schema.md"),
        ("Docs Index",              PROJECT_ROOT / "docs_md" / "README.md")
    ]
    api_index = DOCS_OUT / "api" / "index.md"
    if api_index.exists():
        files.append(("API Reference", api_index))

    print(f"[Notion] Parent page: {PARENT_ID}")
    for title, path in files:
        content = read_file(path)
        if not content: 
            print(f"[Skip] {title} (no content)")
            continue
        page_id = get_or_create_child_page(title)
        replace_page_content_with_markdown(page_id, content)
        print(f"[OK] Synced '{title}'.")

if __name__ == "__main__":
    try:
        main()
        print("[DONE] Notion sync completed.")
    except Exception as e:
        print(f"[ERROR] {e}")
        raise
