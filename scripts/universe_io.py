# universe_io.py
import sqlite3, json, os
from pathlib import Path

try:
    import yaml
except ImportError:
    yaml = None

DB_PATH = Path(r"C:\teevra18\data\teevra18.db")
EXPORT_DIR = Path(r"C:\teevra18\data\exports")
EXPORT_DIR.mkdir(parents=True, exist_ok=True)

TABLES = [
    "universe_underlyings",
    "universe_derivatives",
    "universe_watchlist",
    "policies_group",
    "policies_instrument",
    "strategies_catalog"
]

def export_all(fmt="json"):
    conn = sqlite3.connect(DB_PATH)
    data = {}
    for t in TABLES:
        rows = conn.execute(f"SELECT * FROM {t}").fetchall()
        cols = [c[1] for c in conn.execute(f"PRAGMA table_info({t})")]
        data[t] = [dict(zip(cols, r)) for r in rows]
    conn.close()

    outpath = EXPORT_DIR / f"universe_export.{fmt}"
    if fmt == "json":
        outpath.write_text(json.dumps(data, indent=2))
    elif fmt == "yaml" and yaml:
        outpath.write_text(yaml.dump(data))
    else:
        raise ValueError("Unsupported format or missing PyYAML")
    print(f"[OK] Exported → {outpath}")

def import_all(filepath):
    conn = sqlite3.connect(DB_PATH)
    ext = filepath.suffix.lower()
    if ext == ".json":
        data = json.loads(filepath.read_text())
    elif ext in (".yaml", ".yml") and yaml:
        data = yaml.safe_load(filepath.read_text())
    else:
        raise ValueError("Unsupported file type")

    cur = conn.cursor()
    for t, rows in data.items():
        if not rows: 
            continue
        cols = rows[0].keys()
        cur.execute(f"DELETE FROM {t}")
        for r in rows:
            placeholders = ",".join("?" * len(cols))
            cur.execute(
                f"INSERT INTO {t} ({','.join(cols)}) VALUES ({placeholders})",
                tuple(r[c] for c in cols)
            )
    conn.commit()
    conn.close()
    print(f"[OK] Imported from {filepath}")

if __name__ == "__main__":
    # Example usage
    export_all("json")
    # import_all(EXPORT_DIR / "universe_export.json")
