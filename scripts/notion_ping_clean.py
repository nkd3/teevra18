# C:\teevra18\scripts\notion_ping_clean.py
import os, sys, httpx
from dotenv import load_dotenv
from notion_client import Client
from notion_client.errors import APIResponseError

# Load .env and override any stale env vars
ENV_PATH = r"C:\teevra18\.env"
load_dotenv(ENV_PATH, override=True)

token = os.getenv("NOTION_TOKEN", "")
dbid  = os.getenv("NOTION_DB", "")

if not token:
    sys.exit("ABORT: NOTION_TOKEN missing in C:\\teevra18\\.env")

def visualize(s: str):
    head = [f"{ord(c):02X}" for c in s[:3]]
    tail = [f"{ord(c):02X}" for c in s[-3:]]
    return {"repr": repr(s), "len": len(s), "head_hex": head, "tail_hex": tail}

print("Loaded .env from:", ENV_PATH)
print("Token info :", visualize(token))
print("DB ID length:", len(dbid))
print("-" * 60)

# --- Raw HTTP test ---
headers = {
    "Authorization": f"Bearer {token}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
}
r = httpx.get("https://api.notion.com/v1/users/me", headers=headers, timeout=15.0)
print("RAW /users/me status:", r.status_code)
if r.status_code != 200:
    print("RAW body:", r.text)
    sys.exit("FAIL: Raw HTTP /users/me did not return 200")
print("RAW OK ->", r.json().get("name"))
print("-" * 60)

# --- SDK test ---
client = Client(auth=token)
try:
    me = client.users.me()
    print("SDK OK ->", me.get("name") or me.get("bot", {}).get("owner", {}))
except APIResponseError as e:
    print("SDK FAIL:", e.status, e.code, "-", e.message)
    sys.exit(1)

# --- DB test ---
if dbid and len(dbid) == 32:
    try:
        db = client.databases.retrieve(dbid)
        title = "".join(t["plain_text"] for t in db.get("title", []))
        print("DB OK ->", title or "(no title)")
        print("DB properties:", list(db.get("properties", {}).keys()))
    except APIResponseError as e:
        print("DB FAIL:", e.status, e.code, "-", e.message)
        print("Hint: In Notion, open DB → Share → Invite your integration → Can edit")
else:
    print("Skipping DB check (missing/invalid NOTION_DB).")
