# C:\teevra18\core\alerts.py
import json, requests
from pathlib import Path

CFG_PATH = Path(r"C:\teevra18\teevra18.config.json")

def _load_cfg():
    return json.loads(CFG_PATH.read_text(encoding="utf-8"))

def telegram_send(msg: str) -> bool:
    cfg = _load_cfg()
    tcfg = cfg.get("telegram", {})
    if not tcfg.get("enabled"): return False
    token = tcfg.get("bot_token"); chat_id = tcfg.get("chat_id")
    if not token or not chat_id: return False
    try:
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        resp = requests.post(url, data={"chat_id": chat_id, "text": msg})
        return resp.ok
    except Exception:
        return False
