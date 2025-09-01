# -*- coding: utf-8 -*-
import sys
from pathlib import Path

ROOT = Path(r"C:\teevra18")

EXCLUDE_NAMES = {".venv", ".git", "node_modules", "__pycache__", "backups"}
EXCLUDE_PREFIXES = ("backups_stale_",)
EXTS = (".py", ".csv", ".txt", ".md", ".env")

def is_excluded(p: Path) -> bool:
    parts = set(p.parts)
    if parts & EXCLUDE_NAMES:
        return True
    for part in p.parts:
        for pref in EXCLUDE_PREFIXES:
            if part.startswith(pref):
                return True
    return False

bad = []
scanned = 0

for ext in EXTS:
    for p in ROOT.rglob(f"*{ext}"):
        if is_excluded(p):
            continue
        if not p.is_file():
            continue
        scanned += 1
        try:
            _ = p.read_text(encoding="utf-8")
        except UnicodeDecodeError as e:
            bad.append((str(p), f"UnicodeDecodeError: {e}"))
        except (FileNotFoundError, OSError):
            continue

if not bad:
    print(f"OK: All scanned files ({scanned}) are UTF-8.")
else:
    print(f"Found {len(bad)} non-UTF-8 files out of {scanned} scanned:")
    for path, err in bad:
        print(f"- {path} :: {err}")
