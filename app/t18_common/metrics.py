# -*- coding: utf-8 -*-
from typing import List, Dict, Any, Optional
from t18_common.db import get_conn, table_exists, columns

def get_today_pl(con=None) -> float:
    own=False
    if con is None:
        con=get_conn(); own=True
    try:
        if table_exists(con, "pl_ledger"):
            row = con.execute(
                "SELECT COALESCE(SUM(pnl),0.0) AS pnl FROM pl_ledger WHERE date(date_ts)=date('now','localtime')"
            ).fetchone()
            return float(row["pnl"] if row else 0.0)
        if table_exists(con, "positions"):
            row = con.execute(
                "SELECT COALESCE(SUM(mtm),0.0) AS pnl FROM positions WHERE status='OPEN'"
            ).fetchone()
            return float(row["pnl"] if row else 0.0)
        return 0.0
    finally:
        if own: con.close()

def get_open_risk(con=None) -> float:
    own=False
    if con is None:
        con=get_conn(); own=True
    try:
        if table_exists(con, "positions"):
            row = con.execute(
                "SELECT COALESCE(SUM(risk_exposure),0.0) AS rx FROM positions WHERE status='OPEN'"
            ).fetchone()
            return float(row["rx"] if row else 0.0)
        return 0.0
    finally:
        if own: con.close()

def _pick(colnames, candidates):
    s = {c.lower() for c in colnames}
    for c in candidates:
        if c.lower() in s:
            return c
    return None

def get_signal_chips(con=None, limit:int=10) -> List[Dict[str,Any]]:
    """
    Returns a list of recent signal rows (dicts). Each row will have a 'signal'
    key synthesized if the table lacks one (based on a numeric strength/score).
    Compatible with Trader_Dashboard which can handle list or dict summary.
    """
    own=False
    if con is None:
        con=get_conn(); own=True
    try:
        if not table_exists(con, "signals"):
            return []

        collist = columns(con, "signals")
        tcol = _pick(collist, ["ts","time","created_at","timestamp","datetime","date_ts"])
        scol = _pick(collist, ["signal","sig","side","direction","action","label","state"])
        xcol = _pick(collist, ["strength","score","weight","prob","probability","confidence","value"])
        sql = "SELECT * FROM signals"
        if tcol:
            sql += f" ORDER BY {tcol} DESC"
        sql += " LIMIT ?"

        rows = con.execute(sql, (limit,)).fetchall()
        out: List[Dict[str,Any]] = []
        for r in rows:
            d = dict(r)
            # ensure there is a 'signal'
            if scol and scol in d and d[scol] is not None:
                sig = str(d[scol]).upper()
            else:
                # synthesize from numeric strength/score if present
                sig = "AMBER"
                if xcol and xcol in d:
                    try:
                        val = float(d[xcol])
                        if val > 0: sig = "GREEN"
                        elif val < 0: sig = "RED"
                        else: sig = "AMBER"
                    except Exception:
                        sig = "AMBER"
            d["signal"] = sig
            out.append(d)
        return out
    finally:
        if own: con.close()
