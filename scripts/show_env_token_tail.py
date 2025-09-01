# C:\teevra18\scripts\show_env_token_tail.py
import os
from dotenv import load_dotenv

# Force reload .env and override any existing env var
load_dotenv(r"C:\teevra18\.env", override=True)

token = os.getenv("NOTION_TOKEN", "")
dbid  = os.getenv("NOTION_DB", "")

print("Python token tail:", token[-6:], "len:", len(token), "prefix:", token[:4])
print("DB ID length     :", len(dbid))
