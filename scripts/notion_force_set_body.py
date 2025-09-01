# C:\teevra18\scripts\notion_force_set_body.py
import os, sys, datetime, io
from pathlib import Path
from dotenv import load_dotenv
from notion_client import Client

load_dotenv(r"C:\teevra18\.env", override=True)
token = os.getenv("NOTION_TOKEN"); dbid = os.getenv("NOTION_DB")
if not token or not dbid:
    raise SystemExit("Missing NOTION_TOKEN/NOTION_DB in C:\\teevra18\\.env")

if len(sys.argv) < 3 or sys.argv[1] not in ("--page",):
    raise SystemExit("Usage: notion_force_set_body.py --page <PAGE_ID> [--file <PATH>] [--text <STRING>]")

page_id = sys.argv[2]
# read content from --file (preferred) or --text (single-line)
text = None
if "--file" in sys.argv:
    p = Path(sys.argv[sys.argv.index("--file")+1])
    text = p.read_text(encoding="utf-8", errors="ignore")
elif "--text" in sys.argv:
    text = sys.argv[sys.argv.index("--text")+1]
else:
    raise SystemExit("Provide --file <PATH> (recommended) or --text <STRING>")

client = Client(auth=token)

# 1) Update DB properties (Content, Timestamp)
props = {
    "Content":   {"rich_text": [{"text": {"content": text}}]},
    "Timestamp": {"date": {"start": datetime.datetime.now().isoformat()}},
}
# If your DB has Status and you want it:
# props["Status"] = {"rich_text": [{"text": {"content": "OK-FORCE"}}]}

client.pages.update(page_id=page_id, properties=props)

# 2) Replace page body (clear children then add one paragraph)
kids = client.blocks.children.list(block_id=page_id, page_size=100).get("results", [])
for b in kids:
    try:
        client.blocks.delete(block_id=b["id"])
    except Exception:
        pass

snippet = text if len(text) <= 1800 else (text[:1800] + "\n...\n[truncated]")
client.blocks.children.append(
    block_id=page_id,
    children=[{
        "object": "block",
        "type": "paragraph",
        "paragraph": {"rich_text": [{"type": "text", "text": {"content": snippet}}]},
    }],
)

print("OK force-updated page:", page_id)
