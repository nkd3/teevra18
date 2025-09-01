import os, sys, datetime
from dotenv import load_dotenv
from notion_client import Client

load_dotenv(r"C:\teevra18\.env", override=True)
token = os.getenv("NOTION_TOKEN"); dbid = os.getenv("NOTION_DB")
if not token or not dbid: raise SystemExit("Missing NOTION_TOKEN/NOTION_DB")
if len(sys.argv) < 3:
    raise SystemExit("Usage: notion_force_update.py <PAGE_ID> <TEXT>")

page_id = sys.argv[1]
text    = sys.argv[2]

client = Client(auth=token)

# 1) Update properties
props = {
    "Content":   {"rich_text": [{"text": {"content": text}}]},
    "Timestamp": {"date": {"start": datetime.datetime.now().isoformat()}},
}
# Optional: also set Status if you have it
# props["Status"] = {"rich_text": [{"text": {"content": "OK-FORCE"}}]}

client.pages.update(page_id=page_id, properties=props)

# 2) Replace page body with that text
# Delete children (best-effort)
kids = client.blocks.children.list(block_id=page_id, page_size=100).get("results", [])
for b in kids:
    try:
        client.blocks.delete(block_id=b["id"])
    except Exception:
        pass

client.blocks.children.append(
    block_id=page_id,
    children=[{
        "object": "block",
        "type": "paragraph",
        "paragraph": {"rich_text": [{"type": "text", "text": {"content": text}}]},
    }],
)

print("OK force-updated page:", page_id)
