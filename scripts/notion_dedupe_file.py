# C:\teevra18\scripts\notion_dedupe_file.py
import os, sys
from dotenv import load_dotenv
from notion_client import Client

load_dotenv(r"C:\teevra18\.env", override=True)
token = os.getenv("NOTION_TOKEN")
dbid  = os.getenv("NOTION_DB")
if not token or not dbid:
    raise SystemExit("Missing NOTION_TOKEN/NOTION_DB in C:\\teevra18\\.env")

if len(sys.argv) < 2:
    raise SystemExit("Usage: notion_dedupe_file.py <RELATIVE_PATH>  (e.g., README.md)")

relpath = sys.argv[1]
client = Client(auth=token)

# newest first by last_edited_time (timestamp sort)
resp = client.databases.query(
    **{
        "database_id": dbid,
        "filter": {"property": "File", "rich_text": {"equals": relpath}},
        "sorts": [{"timestamp": "last_edited_time", "direction": "descending"}],
        "page_size": 100,
    }
)
results = resp.get("results", [])

print(f"Found {len(results)} rows for {relpath}")
for i, page in enumerate(results, 1):
    pid = page["id"]
    let = page.get("last_edited_time")
    if i == 1:
        print(f"KEEP  : {pid}  last_edited_time={let}")
        continue
    print(f"ARCHIVE: {pid}  last_edited_time={let}")
    client.pages.update(page_id=pid, archived=True)

print("Done.")
