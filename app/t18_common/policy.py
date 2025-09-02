# -*- coding: utf-8 -*-
from typing import Optional, Dict, Any, Tuple, List
from t18_common.db import get_conn, table_exists, columns

def _order_clause(existing: List[str]) -> str:
    """
    Build a safe ORDER BY using only columns that actually exist.
    Falls back to rowid (always available in SQLite).
    """
    priority = ["updated_at", "created_at", "ts", "id", "rowid"]
    fields = [f for f in priority if (f == "rowid" or f in existing)]
    if not fields:
        return ""
    return " ORDER BY " + ", ".join([(f if f == "rowid" else f) + " DESC" for f in fields])

def get_active_policy_row(con=None) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    """
    Returns (row_dict, policy_dict) or (None, None).
    Works whether risk_policies has an 'active'/'is_active'/'enabled' flag or none.
    Never references non-existent columns in ORDER BY.
    """
    own = False
    if con is None:
        con = get_conn(); own = True
    try:
        if not table_exists(con, "risk_policies"):
            return None, None

        cols = set(columns(con, "risk_policies"))
        flag = next((c for c in ("active", "is_active", "enabled") if c in cols), None)
        order_by = _order_clause(list(cols))

        if flag:
            q = f"SELECT * FROM risk_policies WHERE {flag} IN (1,'1','true','TRUE'){order_by} LIMIT 1"
        else:
            q = f"SELECT * FROM risk_policies{order_by} LIMIT 1"

        row = con.execute(q).fetchone()
        if not row:
            return None, None
        d = dict(row)
        return d, d
    finally:
        if own: con.close()
