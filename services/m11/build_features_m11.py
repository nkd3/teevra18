# C:\teevra18\services\m11\build_features_m11.py
import os, sqlite3, pandas as pd, numpy as np, yaml
from datetime import datetime, timezone
from pathlib import Path

# ---- Config paths ----
CFG_PATH = Path(r"C:\teevra18\config\m11.yaml")
OUT_DIR  = Path(r"C:\teevra18\models\m11")
DB_PATH  = Path(os.getenv("DB_PATH", r"C:\teevra18\data\teevra18.db"))

# Prefer 1m, then larger frames
CANDIDATE_TABLES = ["candles_1m", "candles_5m", "candles_15m", "candles_60m"]

# ---------- Small utils ----------
def now_utc_str():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

def feature_hash(df: pd.DataFrame) -> str:
    raw = df.to_json(orient="records", date_format="iso", date_unit="s")
    import hashlib
    return hashlib.md5(raw.encode()).hexdigest()

def coerce_ts_utc(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure a ts_utc column exists; infer from common columns; fallback to 'now'."""
    if "ts_utc" in df.columns:
        return df
    # t_start (epoch seconds)
    if "t_start" in df.columns:
        try:
            ts = pd.to_datetime(df["t_start"], unit="s", utc=True, errors="coerce")
            df = df.copy()
            df["ts_utc"] = ts.dt.strftime("%Y-%m-%d %H:%M:%S")
            return df
        except Exception:
            pass
    # other common names
    for cand in ["timestamp", "ts", "time_utc", "dt", "time"]:
        if cand in df.columns:
            try:
                ts = pd.to_datetime(df[cand], utc=True, errors="coerce")
                df = df.copy()
                df["ts_utc"] = ts.dt.strftime("%Y-%m-%d %H:%M:%S")
                return df
            except Exception:
                continue
    # last resort: set to now (keeps pipeline alive)
    df = df.copy()
    df["ts_utc"] = now_utc_str()
    return df

def coerce_instrument(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure an 'instrument' column exists; map common alternatives."""
    if "instrument" in df.columns:
        return df
    for cand in ["instrument_id", "symbol", "ticker", "underlying"]:
        if cand in df.columns:
            df = df.copy()
            df["instrument"] = df[cand]
            return df
    # last resort: constant
    df = df.copy()
    df["instrument"] = "UNKNOWN"
    return df

def normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    df = coerce_ts_utc(df)
    df = coerce_instrument(df)
    return df

def last_per(df: pd.DataFrame, by="instrument") -> pd.DataFrame:
    df = normalize_df(df)
    if df.empty:
        return df
    # safe sort by ts_utc (string timestamps sort OK in ISO format)
    return df.sort_values("ts_utc").groupby(by, as_index=False).tail(1)

def hybrid_expected_move(atr_val, iv_move):
    if pd.isna(iv_move) and pd.isna(atr_val): return 0.0
    if pd.isna(iv_move): return float(atr_val)
    if pd.isna(atr_val): return float(iv_move)
    return float(0.5*atr_val + 0.5*iv_move)

# ---------- Readers ----------
def try_select_candles(conn: sqlite3.Connection, tbl: str) -> pd.DataFrame:
    # Your confirmed candles schema
    sql = f"""
    SELECT
      datetime(t_start,'unixepoch') AS ts_utc,
      instrument_id                 AS instrument,
      close,
      NULL                          AS ema,
      vwap                          AS vwap,
      NULL                          AS atr
    FROM {tbl}
    """
    return pd.read_sql(sql, conn, parse_dates=["ts_utc"])

def try_read_table(conn: sqlite3.Connection, name: str) -> pd.DataFrame:
    """Read only if it's a TABLE; returns empty DataFrame if absent."""
    try:
        t = pd.read_sql("SELECT type FROM sqlite_master WHERE name=?;", conn, params=(name,))
        if not t.empty and t.iloc[0,0] == "table":
            return pd.read_sql(f"SELECT * FROM {name}", conn)
    except Exception:
        pass
    return pd.DataFrame()

# ---------- Main ----------
def main():
    # Optional config (for universe filter)
    try:
        with open(CFG_PATH, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or {}
    except Exception:
        cfg = {}

    # Connect to unified DB
    conn = sqlite3.connect(DB_PATH)
    print(f"[DEBUG] Using DB at: {DB_PATH}")
    print(pd.read_sql("PRAGMA database_list;", conn))

    # Show what this process sees (sanity)
    inv = pd.read_sql("SELECT type,name FROM sqlite_master ORDER BY type,name;", conn)
    print(inv[inv["name"].str.contains("candles", case=False, na=False)])

    # ---- Candles: pick first working table ----
    candles = None
    errors = []
    for tbl in CANDIDATE_TABLES:
        try:
            df = try_select_candles(conn, tbl)
            if not df.empty:
                print(f"[INFO] Using table: {tbl} (rows={len(df)})")
                candles = df
                break
            else:
                errors.append(f"{tbl}: empty")
        except Exception as e:
            errors.append(f"{tbl}: {e}")

    if candles is None:
        raise SystemExit(f"[FATAL] Could not read candles from any table on {DB_PATH}. "
                         f"Attempts -> " + " | ".join(errors))

    # ---- Optional sources (tables only; normalize to have ts_utc/instrument) ----
    depth = normalize_df(try_read_table(conn, "depth20_snap"))
    chain = normalize_df(try_read_table(conn, "option_chain_snap"))
    if chain.empty:
        chain = normalize_df(try_read_table(conn, "option_chain"))
    keylv = normalize_df(try_read_table(conn, "key_levels"))

    # ---- Build features ----
    c_last = last_per(candles)
    d_last = last_per(depth)
    o_last = last_per(chain)

    base = c_last
    if not d_last.empty:
        base = base.merge(d_last, on="instrument", how="left", suffixes=("_c","_d"))
    if not o_last.empty:
        base = base.merge(o_last, on="instrument", how="left")

    # Minimal derived features
    base["atr"] = base.get("atr", pd.Series(np.nan))
    base["iv_move"] = base.get("iv_move", pd.Series(np.nan))
    base["exp_move_abs"] = base.apply(lambda r: hybrid_expected_move(r.get("atr", np.nan), r.get("iv_move", np.nan)), axis=1)

    base["l20_imbalance"] = base.get("l20_imbalance", pd.Series(0.0)).fillna(0.0)
    base["ema_vs_price"]  = base.get("close", pd.Series(np.nan)) - base.get("ema", pd.Series(np.nan))
    base["vwap_gap"]      = base.get("close", pd.Series(np.nan)) - base.get("vwap", pd.Series(np.nan))
    base["vol_norm"]      = base.get("volume", pd.Series(0.0)).astype(float) if "volume" in base.columns else 0.0

    # Optional universe filter
    try:
        univ = set(cfg.get("prediction", {}).get("instrument_universe", []))
        if univ:
            base = base[base["instrument"].isin(univ)].copy()
    except Exception:
        pass

    feat_cols = ["instrument","exp_move_abs","l20_imbalance","ema_vs_price","vwap_gap","vol_norm"]
    feats = base[feat_cols].copy()
    feats = feats.apply(pd.to_numeric, errors="coerce")
    feats = feats.fillna(0.0)
    feats["ts_utc"] = now_utc_str()
    feats["feat_hash"] = feature_hash(feats.drop(columns=["ts_utc","instrument"]))

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUT_DIR / "latest_features.parquet"
    feats.to_parquet(out_path, index=False)
    print(f"[OK] features rows: {len(feats)} saved to {out_path}")

if __name__ == "__main__":
    main()
