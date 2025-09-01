# -*- coding: utf-8 -*-
import sys
from pathlib import Path

EXTS = (".py", ".csv", ".txt", ".md", ".env")
EXCLUDE_NAMES = {".venv", ".git", "node_modules", "__pycache__", "backups"}
EXCLUDE_PREFIXES = ("backups_stale_",)

def is_excluded(p: Path) -> bool:
    parts = set(p.parts)
    if parts & EXCLUDE_NAMES:
        return True
    for part in p.parts:
        for pref in EXCLUDE_PREFIXES:
            if part.startswith(pref):
                return True
    return False

def convert_file(path: Path):
    raw = path.read_bytes()
    try:
        raw.decode("utf-8")
        print(f"SKIP: {path} (already utf-8)")
        return False
    except UnicodeDecodeError:
        pass

    for enc in ("cp1252", "latin-1"):
        try:
            text = raw.decode(enc)
            path.write_text(text, encoding="utf-8", newline="\n")
            print(f"CONVERT: {path} ({enc} -> utf-8)")
            return True
        except UnicodeDecodeError:
            continue

    print(f"WARN: {path} could not be decoded with cp1252/latin-1")
    return False

def process_target(target: Path):
    converted = 0
    scanned = 0
    if target.is_file():
        if not is_excluded(target):
            scanned += 1
            try:
                if convert_file(target):
                    converted += 1
            except (FileNotFoundError, OSError):
                pass
        return scanned, converted

    if target.is_dir():
        for ext in EXTS:
            for f in target.rglob(f"*{ext}"):
                if is_excluded(f):
                    continue
                if not f.is_file():
                    continue
                scanned += 1
                try:
                    if convert_file(f):
                        converted += 1
                except (FileNotFoundError, OSError):
                    continue
        return scanned, converted

    print(f"Not found: {target}")
    return 0, 0

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python fix_encoding_cp1252_to_utf8.py <file-or-folder> [...]")
        sys.exit(1)

    total_scanned = 0
    total_converted = 0
    for arg in sys.argv[1:]:
        p = Path(arg)
        scanned, converted = process_target(p)
        total_scanned += scanned
        total_converted += converted

    print(f"Done. Scanned: {total_scanned}, Converted: {total_converted}")
