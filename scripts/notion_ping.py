# C:\teevra18\scripts\notion_ping.py
import os, json
from dotenv import load_dotenv

ENV_PATH = r"C:\teevra18\.env"
loaded = load_dotenv(ENV_PATH)

# 1) Show what we loaded
token = os.getenv("NOTION_TOKEN", "")
dbid  = os.getenv("NOTION_DB", "")

print("Loaded .env:", ENV_PATH, "loaded:", loaded)
print("NOTION_TOKEN present:", bool(token))
print("NOTION_TOKEN prefix :", token[:4])        # 'ntn_' or 'secr'
print("NOTION_TOKEN length :", len(token))
print("NOTION_DB length    :", len(dbid))
print("-" * 60)

if not token:
    raise SystemExit("ABORT: NOTION_TOKEN is empty. Fix C:\\teevra18\\.env and retry.")

# 2) Raw HTTP check (mirrors your PowerShell test)
import httpx
headers = {
    "Authorization": f"Bearer {token}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
}
r = httpx.get("https://api.notion.com/v1/users/me", headers=headers, timeout=15.0)
print("Raw /users/me status:", r.status_code)
if r.status_code != 200:
    print("Body:", r.text)
    raise SystemExit("Raw HTTP failed. If PowerShell works, compare tokens being used.")
print("Raw /users/me OK ->", r.json().get("name"))
print("-" * 60)

# 3) SDK check
from notion_client import Client
from notion_client.errors import APIResponseError

client = Client(auth=token)

try:
    me = client.users.me()
    print("SDK /users/me OK  ->", me.get("name") or me.get("bot", {}).get("owner", {}))
except APIResponseError as e:
    print("SDK /users/me FAIL:", e.status, e.code, "-", e.message)
    print("Hint: old notion-client or a path/env issue. Re-run pip upgrade and recheck ENV_PATH.")
    raise SystemExit(1)

# 4) Optional: DB access (requires DB shared with integration)
if dbid and len(dbid) == 32:
    try:
        db = client.databases.retrieve(dbid)
        title = "".join(t["plain_text"] for t in db.get("title", []))
        print("DB retrieve OK    ->", title or "(no title)")
        print("DB properties     ->", list(db.get("properties", {}).keys()))
    except APIResponseError as e:
        print("DB retrieve FAIL  :", e.status, e.code, "-", e.message)
        print("Hint: In Notion, open DB → Share → Invite your integration → Can edit.")
else:
    print("Skipping DB check (missing/invalid NOTION_DB).")
