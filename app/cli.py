"""CLI entry point for the mbox importer.

Usage:
    python -m app.cli --mbox /path/to/file.mbox --label "Inbox 2024"
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from app import config, db
from app.importer import import_mbox


def _configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="mbox-import",
        description="Import a Thunderbird mbox file into PostgreSQL.",
    )
    parser.add_argument(
        "--mbox",
        required=True,
        metavar="PATH",
        help="Path to the .mbox file to import.",
    )
    parser.add_argument(
        "--label",
        required=True,
        metavar="LABEL",
        help="Human-readable label for this import run (e.g. folder name).",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable debug-level logging.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    _configure_logging(args.verbose)
    logger = logging.getLogger(__name__)

    mbox_path = Path(args.mbox).resolve()

    if not mbox_path.exists():
        print(f"error: file not found: {mbox_path}", file=sys.stderr)
        return 1
    if not mbox_path.is_file():
        print(f"error: not a file: {mbox_path}", file=sys.stderr)
        return 1

    try:
        settings = config.load()
    except RuntimeError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    try:
        conn = db.connect(settings.database_url)
    except Exception as exc:
        print(f"error: could not connect to database: {exc}", file=sys.stderr)
        return 1

    # import_mbox manages its own commits/rollbacks; we only own the connection
    # lifecycle here, so use an explicit close rather than `with conn:` to avoid
    # a redundant commit/rollback wrapping an already-committed transaction.
    try:
        db.apply_migrations(conn)

        logger.info("Starting import: %s  →  label=%r", mbox_path, args.label)

        result = import_mbox(
            conn,
            mbox_path=mbox_path,
            label=args.label,
            batch_size=settings.import_batch_size,
        )
    finally:
        conn.close()

    print(
        f"\nImport complete (import_id={result.import_id})\n"
        f"  Total messages seen:    {result.total_seen}\n"
        f"  Inserted:               {result.inserted}\n"
        f"  Skipped — duplicate:    {result.skipped_duplicate}\n"
        f"  Skipped — calendar:     {result.skipped_calendar}\n"
        f"  Skipped — empty:        {result.skipped_empty}\n"
        f"  Skipped — other:        {result.skipped_other}\n"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
