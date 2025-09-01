import os
from pathlib import Path
try:
    from dotenv import load_dotenv
except ImportError:
    print("python-dotenv not installed. Run: pip install python-dotenv"); raise

env_path = Path(r"C:\teevra18\.env")
print("Loading .env from:", env_path, "exists?", env_path.exists())
load_dotenv(env_path, override=True)

print("DHAN_REST_BASE =", os.getenv("DHAN_REST_BASE"))
print("DHAN_CLIENT_ID =", os.getenv("DHAN_CLIENT_ID"))
tok = os.getenv("DHAN_ACCESS_TOKEN")
print("DHAN_ACCESS_TOKEN set?", bool(tok), "length:", (len(tok) if tok else 0))
