import sys, json
from pathlib import Path

if len(sys.argv) != 2:
    print("Usage: python validate_json.py <path-to-json>")
    sys.exit(2)

p = Path(sys.argv[1])
# 'utf-8-sig' auto-strips BOM if present
txt = p.read_text(encoding="utf-8-sig")

try:
    json.loads(txt)
    print(f"OK: {p} is valid JSON.")
except json.JSONDecodeError as e:
    print(f"JSON ERROR in {p}")
    print(f"  Line {e.lineno}, Column {e.colno}: {e.msg}")
    lines = txt.splitlines()
    start = max(0, e.lineno - 3)
    end   = min(len(lines), e.lineno + 2)
    for idx in range(start, end):
        marker = ">>" if (idx + 1) == e.lineno else "  "
        print(f"{marker} {idx+1:4d}: {lines[idx]}")
    sys.exit(1)
