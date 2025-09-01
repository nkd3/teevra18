import sys
from pathlib import Path
PROJECT_ROOT = Path(r"C:\teevra18")
if str(PROJECT_ROOT / "lib") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "lib"))

from t18_db_helpers import t18_fetch_lot_size
