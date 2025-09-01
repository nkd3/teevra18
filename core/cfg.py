# C:\teevra18\core\cfg.py
import json
from pathlib import Path

CFG_PATH = r"C:\teevra18\teevra18.config.json"

def load_cfg(path: str = CFG_PATH) -> dict:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Config not found at {path}")
    text = p.read_text(encoding="utf-8-sig").strip()  # tolerate BOM
    if not text:
        raise ValueError(f"Config at {path} is empty")
    try:
        return json.loads(text)
    except Exception as e:
        raise ValueError(f"Invalid JSON in {path}: {e}")

def get_db_path(path: str = CFG_PATH) -> str:
    cfg = load_cfg(path)
    db = cfg.get("db_path")
    if not db:
        raise KeyError("db_path missing in config")
    return db
