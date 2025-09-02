from typing import List, Dict, Any
from t18_common.db import get_conn, table_exists, read_df

def get_today_pl(con=None) -> float:
    own=False
    if con is None:
        con=get_conn(); own=True
    try:
        # Prefer a pl_ledger table if present
        if table_exists(con, "pl_ledger"):
            row = con.execute("SELECT COALESCE(SUM(pnl),0.0) AS pnl FROM pl_ledger WHERE date(date_ts)=date('now','localtime')").fetchone()
            return float(row["pnl"] if row else 0.0)
        # Fallback: positions with mtm
        if table_exists(con, "positions"):
            row = con.execute("SELECT COALESCE(SUM(mtm),0.0) AS pnl FROM positions WHERE status='OPEN'").fetchone()
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
            row = con.execute("SELECT COALESCE(SUM(risk_exposure),0.0) AS rx FROM positions WHERE status='OPEN'").fetchone()
            return float(row["rx"] if row else 0.0)
        return 0.0
    finally:
        if own: con.close()

def get_signal_chips(con=None, limit:int=10) -> List[Dict[str,Any]]:
    own=False
    if con is None:
        con=get_conn(); own=True
    try:
        if table_exists(con, "signals"):
            q = """
            SELECT symbol, signal, strength, ts
            FROM signals
            ORDER BY ts DESC
            LIMIT ?
            """
            cur = con.execute(q, (limit,))
            return [dict(r) for r in cur.fetchall()]
        return []
    finally:
        if own: con.close()
