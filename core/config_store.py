# C:\teevra18\core\config_store.py
import sqlite3, json
from pathlib import Path

class ConfigStore:
    def __init__(self, db_path: str):
        self.db = db_path

    def _connect(self):
        return sqlite3.connect(self.db)

    def create_config(self, name: str, stage: str, notes: str = "") -> int:
        con = self._connect(); cur = con.cursor()
        cur.execute("INSERT INTO strategy_configs(name,stage,notes,is_active) VALUES (?,?,?,0)", (name, stage, notes))
        cid = cur.lastrowid
        con.commit(); con.close()
        return cid

    def add_params(self, config_id: int, param_dict: dict):
        con = self._connect(); cur = con.cursor()
        for k,v in param_dict.items():
            cur.execute("INSERT INTO strategy_params(config_id,param_key,param_value) VALUES (?,?,?)", (config_id, k, json.dumps(v)))
        con.commit(); con.close()

    def set_policies(self, config_id: int, policies: dict):
        con = self._connect(); cur = con.cursor()
        cur.execute("DELETE FROM risk_policies WHERE config_id=?", (config_id,))
        cur.execute("""
            INSERT INTO risk_policies(config_id, capital_mode, fixed_capital, risk_per_trade_pct, max_trades_per_day, rr_min, sl_max_per_lot, daily_loss_limit, group_exposure_cap_pct, breaker_threshold, trading_windows)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """, (
            config_id,
            policies.get("capital_mode","Fixed"),
            float(policies.get("fixed_capital",150000)),
            float(policies.get("risk_per_trade_pct",1.0)),
            int(policies.get("max_trades_per_day",5)),
            float(policies.get("rr_min",2.0)),
            float(policies.get("sl_max_per_lot",1000.0)),
            float(policies.get("daily_loss_limit",0.0)),
            float(policies.get("group_exposure_cap_pct",100.0)),
            float(policies.get("breaker_threshold",0.0)),
            policies.get("trading_windows","09:20-15:20"),
        ))
        con.commit(); con.close()

    def set_liquidity(self, config_id: int, filt: dict):
        con = self._connect(); cur = con.cursor()
        cur.execute("DELETE FROM liquidity_filters WHERE config_id=?", (config_id,))
        cur.execute("""
            INSERT INTO liquidity_filters(config_id, min_oi, min_volume, max_spread_paisa, slippage_bps, fees_per_lot)
            VALUES (?,?,?,?,?,?)
        """, (
            config_id,
            int(filt.get("min_oi",0)),
            int(filt.get("min_volume",0)),
            int(filt.get("max_spread_paisa",50)),
            int(filt.get("slippage_bps",5)),
            float(filt.get("fees_per_lot",30.0))
        ))
        con.commit(); con.close()

    def set_notif(self, config_id: int, notif: dict):
        con = self._connect(); cur = con.cursor()
        cur.execute("DELETE FROM notif_settings WHERE config_id=?", (config_id,))
        cur.execute("""
            INSERT INTO notif_settings(config_id, telegram_enabled, t_bot_token, t_chat_id, eod_summary)
            VALUES (?,?,?,?,?)
        """, (
            config_id,
            1 if notif.get("telegram_enabled", True) else 0,
            notif.get("t_bot_token"),
            notif.get("t_chat_id"),
            1 if notif.get("eod_summary", True) else 0
        ))
        con.commit(); con.close()

    def list_configs(self, stage=None):
        con = self._connect(); cur = con.cursor()
        if stage:
            cur.execute("SELECT id,name,stage,version,is_active,notes FROM strategy_configs WHERE stage=? ORDER BY updated_at DESC", (stage,))
        else:
            cur.execute("SELECT id,name,stage,version,is_active,notes FROM strategy_configs ORDER BY updated_at DESC")
        rows = cur.fetchall(); con.close()
        return rows

    def get_config_bundle(self, config_id: int) -> dict:
        con = self._connect(); cur = con.cursor()
        cur.execute("SELECT id,name,stage,version,is_active,notes FROM strategy_configs WHERE id=?", (config_id,))
        cfg = cur.fetchone()
        cur.execute("SELECT param_key,param_value FROM strategy_params WHERE config_id=?", (config_id,))
        params = {k: json.loads(v) for (k,v) in cur.fetchall()}
        cur.execute("SELECT capital_mode,fixed_capital,risk_per_trade_pct,max_trades_per_day,rr_min,sl_max_per_lot,daily_loss_limit,group_exposure_cap_pct,breaker_threshold,trading_windows FROM risk_policies WHERE config_id=?", (config_id,))
        rp = cur.fetchone()
        policies = None
        if rp:
            policies = {
                "capital_mode": rp[0],
                "fixed_capital": rp[1],
                "risk_per_trade_pct": rp[2],
                "max_trades_per_day": rp[3],
                "rr_min": rp[4],
                "sl_max_per_lot": rp[5],
                "daily_loss_limit": rp[6],
                "group_exposure_cap_pct": rp[7],
                "breaker_threshold": rp[8],
                "trading_windows": rp[9],
            }
        cur.execute("SELECT min_oi,min_volume,max_spread_paisa,slippage_bps,fees_per_lot FROM liquidity_filters WHERE config_id=?", (config_id,))
        lf = cur.fetchone()
        liquidity = None
        if lf:
            liquidity = {
                "min_oi": lf[0], "min_volume": lf[1], "max_spread_paisa": lf[2],
                "slippage_bps": lf[3], "fees_per_lot": lf[4]
            }
        cur.execute("SELECT telegram_enabled,t_bot_token,t_chat_id,eod_summary FROM notif_settings WHERE config_id=?", (config_id,))
        ns = cur.fetchone()
        notif = None
        if ns:
            notif = {
                "telegram_enabled": bool(ns[0]), "t_bot_token": ns[1], "t_chat_id": ns[2], "eod_summary": bool(ns[3])
            }
        con.close()
        return {
            "meta": {"id": cfg[0], "name": cfg[1], "stage": cfg[2], "version": cfg[3], "is_active": bool(cfg[4]), "notes": cfg[5]},
            "params": params,
            "policies": policies,
            "liquidity": liquidity,
            "notif": notif
        }

    def snapshot(self, config_id: int, label: str, bundle: dict):
        con = self._connect(); cur = con.cursor()
        cur.execute("INSERT INTO config_versions(config_id, snapshot_json, label) VALUES (?,?,?)", (config_id, json.dumps(bundle), label))
        con.commit(); con.close()

    def import_snapshot(self, stage: str, name: str, snapshot: dict, notes: str = "") -> int:
        new_id = self.create_config(name=name, stage=stage, notes=notes)
        self.add_params(new_id, snapshot.get("params", {}))
        if snapshot.get("policies"): self.set_policies(new_id, snapshot["policies"])
        if snapshot.get("liquidity"): self.set_liquidity(new_id, snapshot["liquidity"])
        if snapshot.get("notif"): self.set_notif(new_id, snapshot["notif"])
        self.snapshot(new_id, f"import:{name}", snapshot)
        return new_id
