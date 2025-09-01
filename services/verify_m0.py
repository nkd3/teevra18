import sqlite3, json
con = sqlite3.connect(r"C:\teevra18\data\teevra18.db")
cur = con.cursor()

def count_rows(tbl):
    try:
        cur.execute(f"SELECT COUNT(*) FROM {tbl};")
        return cur.fetchone()[0]
    except:
        return -1

checks = {
 "WAL": None,
 "breaker_state": None,
 "health_rows": None,
 "rr_profiles": None,
 "strategies_catalog": None
}

cur.execute("PRAGMA journal_mode;")
checks["WAL"] = cur.fetchone()[0]
cur.execute("SELECT state FROM breaker_state WHERE id=1;")
checks["breaker_state"] = (cur.fetchone() or [None])[0]
checks["health_rows"] = count_rows("health")
checks["rr_profiles"] = count_rows("rr_profiles")
checks["strategies_catalog"] = count_rows("strategies_catalog")

print(json.dumps(checks, indent=2))
con.close()
