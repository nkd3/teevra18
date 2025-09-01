# C:\teevra18\scripts\quick_env_check.py
import os
from dotenv import load_dotenv

ENV_PATH = r"C:\teevra18\.env"
loaded = load_dotenv(ENV_PATH)

token = os.getenv("NOTION_TOKEN", "")
dbid  = os.getenv("NOTION_DB", "")

print("Loaded .env from:", ENV_PATH, "| loaded:", loaded)
print("NOTION_TOKEN present:", bool(token))
print("NOTION_TOKEN prefix :", token[:4])   # 'ntn_' or 'secret'
print("NOTION_TOKEN length :", len(token))
print("NOTION_DB length    :", len(dbid))
print("NOTION_DB looks 32? :", len(dbid) == 32)
