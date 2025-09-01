# C:\teevra18\scripts\notion_debug_lookup.py
import os, sys
from dotenv import load_dotenv
from notion_client import Client

load_dotenv(r"C:\teevra18\.env", override=True)
token = os.getenv("NOTION_TOKEN")
dbid  = os.getenv("NOTION_DB")
if not token or not dbid:
    raise SystemExit("Missing NOTION_TOKEN/NOTION_DB in C:\\teevra18\\.env")

relpath = sys.argv[1] if len(sys.argv) > 1 else "README.md"

client = Client(auth=token)

# Use timestamp sort (NOT property sort) for last_edited_time
resp = client.databases.query(
    **{
        "database_id": dbid,
        "filter": {"property": "File", "rich_text": {"equals": relpath}},
        "sorts": [{"timestamp": "last_edited_time", "direction": "descending"}],
        "page_size": 25,
    }
)

results = resp.get("results", [])
print(f"Matches for File == {relpath!r}: {len(results)}")
for i, page in enumerate(results, 1):
    pid = page["id"]
    let = page.get("last_edited_time")
    url = page.get("url")
    # title text
    name = "".join(t["plain_text"] for t in page["properties"]["Name"]["title"])
    print(f"{i}. id={pid}  last_edited_time={let}  Name={name!r}  url={url}")
