# C:\teevra18\app\t18_common\strategies.py
from typing import Optional
from t18_common.db import get_conn, table_exists

def get_strategy_pk(con, lab_id_or_name: Optional[str]) -> Optional[int]:
    """
    Accepts a textual strategy_id (e.g., 'lab_ab12cd34') or a human name.
    Returns the SQLite rowid for strategies_catalog so callers can use an integer PK.
    """
    if lab_id_or_name is None:
        return None

    # If numeric string, trust it
    try:
        return int(lab_id_or_name)
    except Exception:
        pass

    # Otherwise resolve by strategy_id or name
    row = con.execute("""
        SELECT rowid FROM strategies_catalog
        WHERE strategy_id = ? OR name = ?
        ORDER BY rowid DESC
        LIMIT 1
    """, (lab_id_or_name, lab_id_or_name)).fetchone()
    return int(row["rowid"]) if row else None

def get_last_strategy_id(con) -> Optional[str]:
    row = con.execute("SELECT strategy_id FROM strategies_catalog ORDER BY rowid DESC LIMIT 1").fetchone()
    return row["strategy_id"] if row else None
