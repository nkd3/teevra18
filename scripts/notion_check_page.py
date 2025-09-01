import os, sys, json
from dotenv import load_dotenv
from notion_client import Client

load_dotenv(r"C:\teevra18\.env", override=True)
token = os.getenv("NOTION_TOKEN"); dbid = os.getenv("NOTION_DB")
if not token or not dbid: raise SystemExit("Missing NOTION_TOKEN/NOTION_DB")

if len(sys.argv) < 2: raise SystemExit("Usage: notion_check_page.py <PAGE_ID>")
page_id = sys.argv[1]

client = Client(auth=token)

# Get page meta (properties + last_edited_time)
page = client.pages.retrieve(page_id=page_id)
print("last_edited_time:", page.get("last_edited_time"))
props = page.get("properties", {})

def get_prop_text(p):
    if "title" in p:
        return "".join(t.get("plain_text","") for t in p["title"])
    if "rich_text" in p:
        return "".join(t.get("plain_text","") for t in p["rich_text"])
    return ""

for k, v in props.items():
    print(f"PROP {k}: {get_prop_text(v)!r}")

# Get first body block if any
children = client.blocks.children.list(block_id=page_id, page_size=1).get("results", [])
if children:
    b = children[0]
    t = ""
    if b.get("type") == "paragraph":
        t = "".join(rt["plain_text"] for rt in b["paragraph"].get("rich_text", []))
    print("BODY[0]:", repr(t))
else:
    print("BODY: (no children)")
