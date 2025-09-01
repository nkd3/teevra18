# -*- coding: utf-8 -*-
import os
from pathlib import Path

try:
    from dotenv import dotenv_values
except Exception:
    dotenv_values = None

BASE = Path(r"C:\teevra18")
ROOT_ENV = BASE / ".env"
CONFIG_ENV = BASE / "config" / ".env"

def _load_file(path: Path) -> dict:
    if dotenv_values and path.exists():
        return {k: str(v) for k, v in dotenv_values(path).items() if v is not None}
    data = {}
    if path.exists():
        for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            data[k.strip()] = v.strip()
    return data

def load_environment():
    """Merge ROOT .env + CONFIG .env. CONFIG wins. Export into os.environ."""
    merged = {}
    merged.update(_load_file(ROOT_ENV))   # UI/session
    merged.update(_load_file(CONFIG_ENV)) # Runtime/broker (overrides)

    for k, v in merged.items():
        if v is not None:
            os.environ[k] = str(v)

    # DHAN compatibility mapping
    cid = os.environ.get("DHAN_CLIENT_ID") or os.environ.get("DHAN_API_KEY")
    if cid:
        os.environ["DHAN_CLIENT_ID"] = cid
        os.environ["DHAN_API_KEY"]   = cid
