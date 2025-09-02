from typing import Optional, Dict, Any
from t18_common.db import get_conn, table_exists

def get_active_policy_row(con=None) -> Optional[Dict[str, Any]]:
    own=False
    if con is None:
        con=get_conn(); own=True
    try:
        if table_exists(con, "risk_policies"):
            row = con.execute("""
                SELECT *
                FROM risk_policies
                WHERE active=1
                ORDER BY COALESCE(updated_at, created_at) DESC
                LIMIT 1
            """).fetchone()
            return dict(row) if row else None
        return None
    finally:
        if own: con.close()
