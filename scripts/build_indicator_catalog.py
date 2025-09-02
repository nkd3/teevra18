# -*- coding: utf-8 -*-
"""
Build a comprehensive indicator catalog by scanning pandas_ta as installed locally.

Strategy:
  A) Scan top-level pandas_ta (most indicators are re-exported here in many builds)
  B) Best-effort: walk submodules and add any functions defined in those modules

Output:
  C:\teevra18\data\dhan_indicators_full.json
"""
import json
import inspect
import importlib
import pkgutil
from pathlib import Path

OUT_PATH = Path(r"C:\teevra18\data\dhan_indicators_full.json")

# Params to hide from the UI (supplied by your pipeline or too noisy)
IGNORE_PARAMS = {
    "open", "high", "low", "close", "volume", "ohlc",
    "open_", "high_", "low_", "close_", "vol_",
    "append", "offset", "mamode", "talib"
}

ALIASES = {
    "BOLLINGER BANDS": "BBANDS",
    "BB": "BBANDS",
    "SAR": "PSAR",
    "PARABOLIC SAR": "PSAR",
    "SUPER TREND": "SUPERTREND",
    "SUPER_TREND": "SUPERTREND",
}

def to_jsonable(v):
    try:
        json.dumps(v)
        return v
    except TypeError:
        if isinstance(v, (set, tuple)):
            return list(v)
        return str(v)

def harvest_from_module(mod, indicators: dict):
    added = 0
    # Prefer __all__ if present; else fall back to all functions defined in this module
    names = getattr(mod, "__all__", None)
    if not names:
        members = inspect.getmembers(mod, inspect.isfunction)
        names = [n for n, fn in members if fn.__module__ == mod.__name__ and not n.startswith("_")]

    for fname in names:
        fn = getattr(mod, fname, None)
        if not callable(fn):
            continue
        # ensure function belongs to this module (skip re-exports)
        if getattr(fn, "__module__", "") != mod.__name__:
            continue

        key = fname.upper()
        if key in indicators:
            continue

        # pull defaults from signature (skip runtime series/noisy args)
        params = {}
        try:
            sig = inspect.signature(fn)
            for p_name, p in sig.parameters.items():
                if p_name in IGNORE_PARAMS:
                    continue
                if p.default is inspect._empty:
                    continue
                params[p_name] = to_jsonable(p.default)
        except Exception as e:
            # signature parsing failures are rare; just skip params
            pass

        indicators[key] = {
            "friendly": key.replace("_", " "),
            "module": mod.__name__,
            "params": params
        }
        added += 1
    return added

def build_catalog():
    try:
        import pandas_ta as ta
    except Exception as e:
        raise SystemExit(f"[ERR] pandas_ta not available: {e}")

    indicators = {}
    modules_scanned = []

    # A) Top-level scan (this alone usually yields 100+ on common builds)
    added_top = 0
    for name in dir(ta):
        if name.startswith("_"):
            continue
        obj = getattr(ta, name, None)
        if not inspect.isfunction(obj):
            continue
        if not getattr(obj, "__module__", "").startswith("pandas_ta"):
            continue
        key = name.upper()
        if key in indicators:
            continue

        params = {}
        try:
            sig = inspect.signature(obj)
            for p_name, p in sig.parameters.items():
                if p_name in IGNORE_PARAMS:
                    continue
                if p.default is inspect._empty:
                    continue
                params[p_name] = to_jsonable(p.default)
        except Exception:
            pass

        indicators[key] = {
            "friendly": key.replace("_", " "),
            "module": obj.__module__,
            "params": params
        }
        added_top += 1

    # B) Best-effort submodule walk (adds anything not re-exported at top level)
    try:
        for mi in pkgutil.walk_packages(ta.__path__, ta.__name__ + "."):
            modname = mi.name
            # Skip private & obvious internals
            if modname.startswith("pandas_ta._"):
                continue
            try:
                mod = importlib.import_module(modname)
            except Exception:
                continue
            modules_scanned.append(modname)
            harvest_from_module(mod, indicators)
    except Exception:
        # Some builds may not expose __path__; ignore
        pass

    meta = {
        "source": "pandas_ta",
        "modules_scanned": modules_scanned,
        "aliases": ALIASES,
        "count": len(indicators),
        "top_level_functions": added_top
    }
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(json.dumps({"meta": meta, "indicators": indicators}, indent=2), encoding="utf-8")
    print(f"[OK] Wrote indicator catalog with {len(indicators)} entries to {OUT_PATH} "
          f"(top_level={added_top}, modules={len(modules_scanned)})")

if __name__ == "__main__":
    build_catalog()
