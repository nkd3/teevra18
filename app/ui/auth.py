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
import json, secrets, hashlib
from pathlib import Path
from typing import Dict, Any, Iterable, Union

DATA_DIR = Path(r"C:\teevra18\data")
USERS_JSON = DATA_DIR / "users.json"

def _load() -> Dict[str, Any]:
    if USERS_JSON.exists():
        try:
            return json.loads(USERS_JSON.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {"users": {}}

def _save(obj: Dict[str, Any]) -> None:
    USERS_JSON.parent.mkdir(parents=True, exist_ok=True)
    USERS_JSON.write_text(json.dumps(obj, indent=2), encoding="utf-8")

def _hash_pw(pw: str, salt_hex: str) -> str:
    # salted sha256 (sufficient for local dev M12; upgrade to argon2/bcrypt later)
    h = hashlib.sha256(bytes.fromhex(salt_hex) + (pw or "").encode("utf-8")).hexdigest()
    return h

def _resolve_role(user_id: str) -> str:
    # Minimal role resolution without changing UI:
    # treat 'admin' user-id as admin; everyone else is 'user'
    return "admin" if (user_id or "").lower() == "admin" else "user"

def verify_credentials(username: str, password: str):
    u = (username or "").strip().lower()
    store = _load()
    rec = store.get("users", {}).get(u)
    if not rec or not rec.get("active", True):
        return None
    salt = rec["salt"]; expected = rec["hash"]
    if _hash_pw(password or "", salt) != expected:
        return None
    # Return dict expected by LandingPage + pages (adds role to avoid KeyError)
    return {
        "username": rec.get("display", u),
        "id": u,
        "active": True,
        "role": _resolve_role(u),
    }

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

# --- minimal role guard used by Account_Users.py (keeps UI unchanged) ---
def require_role(session_state, required: Union[str, Iterable[str]]) -> bool:
    """
    Usage (as in Account_Users.py):
        if not require_role(st.session_state, "admin"): ...
    Returns True if current session user has the required role.
    """
    user = getattr(session_state, "get", lambda *_: None)("user")
    if not user:
        return False
    have = user.get("role", "user")
    if isinstance(required, str):
        return have == required
    return have in set(required)

if __name__ == "__main__":
    _bootstrap_default_admin()
