"""Standalone migration runner.

Applies any pending migrations and exits.

Usage:
    python scripts/migrate.py
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

# Allow running from the project root without installing the package.
sys.path.insert(0, str(Path(__file__).parent.parent))

from app import config, db


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    try:
        settings = config.load()
    except RuntimeError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    conn = db.connect(settings.database_url)
    with conn:
        db.apply_migrations(conn)

    print("All migrations applied.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
