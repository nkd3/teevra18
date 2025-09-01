# C:\teevra18\services\rr_builder\rr_rules_v2.py
# Charges-aware R:R validation for options (NIFTY=75, BANKNIFTY=35)
from __future__ import annotations
import json, sqlite3
from dataclasses import dataclass
from typing import Dict

LOT_SIZE = {"NIFTY": 75, "BANKNIFTY": 35}

@dataclass
class ChargesModel:
    brokerage_per_order: float
    gst_rate: float
    stt_sell_rate: float
    exch_txn_rate: float
    sebi_rate: float
    stamp_buy_rate: float

def _load_rr_profile(conn: sqlite3.Connection, profile_name: str):
    row = conn.execute("""
        SELECT rr_min, sl_cap_per_trade, include_charges, charges_broker, charges_overrides_json
        FROM rr_profiles WHERE profile_name=?
    """, (profile_name,)).fetchone()
    if not row:
        raise RuntimeError(f"RR profile '{profile_name}' not found")
    rr_min, sl_cap_per_trade, include_charges, charges_broker, charges_json = row
    overrides = json.loads(charges_json) if charges_json else {}
    models = {}
    for root, cfg in overrides.items():
        models[root.upper()] = ChargesModel(
            brokerage_per_order=float(cfg.get("brokerage_per_order", 20)),
            gst_rate=float(cfg.get("gst_rate", 0.18)),
            stt_sell_rate=float(cfg.get("stt_sell_rate", 0.001)),
            exch_txn_rate=float(cfg.get("exch_txn_rate", 0.0003503)),
            sebi_rate=float(cfg.get("sebi_rate", 0.000001)),
            stamp_buy_rate=float(cfg.get("stamp_buy_rate", 0.00003)),
        )
    return (
        {"rr_min": float(rr_min),
         "sl_cap_per_trade": float(sl_cap_per_trade),
         "include_charges": bool(include_charges),
         "charges_broker": charges_broker},
        models
    )

def _infer_root_from_symbol(symbol: str) -> str:
    s = symbol.upper()
    if s.startswith("BANKNIFTY") or s.startswith("NIFTYBANK"): 
        return "BANKNIFTY"
    if s.startswith("NIFTY"): 
        return "NIFTY"
    return "NIFTY"

def estimate_roundtrip_charges(entry: float, exit_: float, qty: int, cm: ChargesModel) -> float:
    buy_turnover = entry * qty
    sell_turnover = max(exit_, 0.0) * qty
    brokerage = cm.brokerage_per_order * 2.0
    exch = cm.exch_txn_rate * (buy_turnover + sell_turnover)
    sebi = cm.sebi_rate * (buy_turnover + sell_turnover)
    stt  = cm.stt_sell_rate * sell_turnover
    stamp= cm.stamp_buy_rate * buy_turnover
    gst  = cm.gst_rate * (brokerage + exch + sebi)
    return brokerage + exch + sebi + stt + stamp + gst

def compute_effective_metrics(underlying_root: str, entry_price: float,
                              sl_pts: float, tp_pts: float, lots: int,
                              cm: ChargesModel) -> dict:
    lot = LOT_SIZE[underlying_root]
    qty = lot * max(lots, 1)
    gross_risk = sl_pts * qty
    gross_reward = tp_pts * qty
    stop_exit = max(entry_price - sl_pts, 0.0)   # long options
    tp_exit   = max(entry_price + tp_pts, 0.0)
    charges_at_stop = estimate_roundtrip_charges(entry_price, stop_exit, qty, cm)
    charges_at_tp   = estimate_roundtrip_charges(entry_price, tp_exit, qty, cm)
    eff_risk   = gross_risk + charges_at_stop
    eff_reward = gross_reward - charges_at_tp
    rr_eff = (eff_reward / eff_risk) if eff_risk > 0 else 0.0
    return {
        "qty": qty,
        "gross_risk": gross_risk,
        "gross_reward": gross_reward,
        "charges_at_stop": charges_at_stop,
        "charges_at_tp": charges_at_tp,
        "effective_risk": eff_risk,
        "effective_reward": eff_reward,
        "rr_eff": rr_eff,
        "stop_exit": stop_exit,
        "tp_exit": tp_exit,
    }

def validate_signal_row(conn: sqlite3.Connection, signal: dict,
                        profile_name: str = "BASELINE_V2") -> tuple[bool, str, dict]:
    rr_cfg, models = _load_rr_profile(conn, profile_name)
    option_symbol = signal["option_symbol"]
    entry_price   = float(signal["entry_price"])
    raw_side      = (signal.get("side") or "LONG").upper()

    # Normalize BUY/SELL into LONG/SHORT so inserts work
    if raw_side == "BUY": 
        side = "LONG"
    elif raw_side == "SELL":
        side = "SHORT"
    else:
        side = raw_side

    # For now we only support LONG logic
    if side != "LONG":
        return False, "side_not_supported_v2", {"rr_eff": 0.0, "effective_risk": 9e9}

    sl_points     = float(signal["sl_points"])
    tp_points     = float(signal["tp_points"])
    lots          = int(signal.get("lots", 1))
    underlying    = (signal.get("underlying_root") or _infer_root_from_symbol(option_symbol)).upper()
    cm = models.get(underlying) or next(iter(models.values()))
    m = compute_effective_metrics(underlying, entry_price, sl_points, tp_points, lots, cm)

    if m["effective_risk"] > rr_cfg["sl_cap_per_trade"]:
        return False, f"risk>{rr_cfg['sl_cap_per_trade']:.0f}", m
    if m["rr_eff"] < rr_cfg["rr_min"]:
        return False, f"rr_eff<{rr_cfg['rr_min']:.2f}", m
    return True, "ok", m
