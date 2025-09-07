# -*- coding: utf-8 -*-
"""
Local-only auth for Teevra18 (M12)
- users.json structure:
  {
    "users": {
      "admin": {"salt":"<hex>", "hash":"<hex>", "active": true, "display":"admin"}
    }
  }
"""

import json, os, secrets, hashlib
from pathlib import Path

DATA_DIR = Path(r"C:\teevra18\data")
USERS_JSON = DATA_DIR / "users.json"

def _load():
    if USERS_JSON.exists():
        try:
            return json.loads(USERS_JSON.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {"users": {}}

def _save(obj):
    USERS_JSON.parent.mkdir(parents=True, exist_ok=True)
    USERS_JSON.write_text(json.dumps(obj, indent=2), encoding="utf-8")

def _hash_pw(pw: str, salt_hex: str) -> str:
    # salted sha256 (sufficient for local dev M12; upgrade to argon2/bcrypt later)
    h = hashlib.sha256(bytes.fromhex(salt_hex) + pw.encode("utf-8")).hexdigest()
    return h

def verify_credentials(username: str, password: str):
    u = (username or "").strip().lower()
    db = _load()["users"]
    rec = db.get(u)
    if not rec or not rec.get("active", True):
        return None
    salt = rec["salt"]; expected = rec["hash"]
    if _hash_pw(password or "", salt) != expected:
        return None
    # return a user dict (LandingPage expects dict-like)
    return {"username": rec.get("display", u), "id": u, "active": True}

def change_password(username: str, current_password: str, new_password: str) -> bool:
    """Return True if changed; False if current_password wrong or user missing."""
    u = (username or "").strip().lower()
    store = _load()
    db = store["users"]
    rec = db.get(u)
    if not rec or not rec.get("active", True):
        return False
    # verify current pw
    if _hash_pw(current_password or "", rec["salt"]) != rec["hash"]:
        return False
    # set new pw
    new_salt = secrets.token_hex(16)
    db[u]["salt"] = new_salt
    db[u]["hash"] = _hash_pw(new_password or "", new_salt)
    _save(store)
    return True

# --- helper for first-time bootstrap (optional) ---
def _bootstrap_default_admin():
    store = _load()
    if "admin" not in store["users"]:
        salt = secrets.token_hex(16)
        store["users"]["admin"] = {
            "salt": salt,
            "hash": _hash_pw("admin", salt),  # default admin/admin
            "active": True,
            "display": "admin",
        }
        _save(store)

if __name__ == "__main__":
    _bootstrap_default_admin()
