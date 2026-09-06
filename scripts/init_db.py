"""Creates all tables (idempotent — safe to re-run)."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from common.db import init_db  # noqa: E402

if __name__ == "__main__":
    init_db()
    print("tables created (or already existed)")
