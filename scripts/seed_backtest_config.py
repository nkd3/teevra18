# C:\teevra18\scripts\seed_backtest_config.py
import json
from pathlib import Path
import sys

# Bootstrap sys.path for core/
import sys as _sys
from pathlib import Path as _Path
PROJECT_ROOT = _Path(r"C:\teevra18")
if str(PROJECT_ROOT) not in _sys.path:
    _sys.path.insert(0, str(PROJECT_ROOT))

from core.config_store import ConfigStore

CFG = json.loads(Path(r"C:\teevra18\teevra18.config.json").read_text(encoding="utf-8"))
STORE = ConfigStore(CFG["db_path"])

name = "MyStrategy"
notes = "Seeded via seed_backtest_config.py"

cid = STORE.create_config(name, "Backtest", notes)
STORE.add_params(cid, {
    "ema_fast": 9, "ema_slow": 21, "rsi_len": 14, "rsi_buy": 40, "rsi_sell": 60,
    "entry_delay_secs": 1, "exit_on_reverse": True
})
STORE.set_policies(cid, {
    "capital_mode": "Fixed",
    "fixed_capital": 150000, "risk_per_trade_pct": 1.0,
    "max_trades_per_day": 5, "rr_min": 2.0, "sl_max_per_lot": 1000.0,
    "daily_loss_limit": 0.0, "group_exposure_cap_pct": 100.0,
    "breaker_threshold": 0.0, "trading_windows": "09:20-15:20"
})
STORE.set_liquidity(cid, {
    "min_oi": 0, "min_volume": 0, "max_spread_paisa": 50, "slippage_bps": 5, "fees_per_lot": 30.0
})
tcfg = CFG.get("telegram", {})
STORE.set_notif(cid, {
    "telegram_enabled": tcfg.get("enabled", False),
    "t_bot_token": tcfg.get("bot_token",""),
    "t_chat_id": tcfg.get("chat_id",""),
    "eod_summary": True
})
print(f"Seeded Backtest config with ID {cid}")
