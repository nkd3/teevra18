# C:\teevra18\scripts\notion_force_props.py
import os, sys, datetime, uuid
from dotenv import load_dotenv
from notion_client import Client

load_dotenv(r"C:\teevra18\.env", override=True)
token = os.getenv("NOTION_TOKEN"); dbid = os.getenv("NOTION_DB")
if not token or not dbid:
    raise SystemExit("Missing NOTION_TOKEN/NOTION_DB in C:\\teevra18\\.env")

if len(sys.argv) < 3:
    raise SystemExit("Usage: notion_force_props.py <PAGE_ID> <SUMMARY_TEXT>")

page_id = sys.argv[1]
summary = sys.argv[2]

client = Client(auth=token)

# always change timestamp; add tiny suffix so Content definitely differs
stamp = datetime.datetime.now().isoformat()
summary2 = f"{summary} | {uuid.uuid4().hex[:6]}"

props = {
    "Content":   {"rich_text": [{"text": {"content": summary2}}]},
    "Timestamp": {"date": {"start": stamp}},
}

resp = client.pages.update(page_id=page_id, archived=False, properties=props)
print("OK pages.update()")
print("last_edited_time:", resp.get("last_edited_time"))
