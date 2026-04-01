"""Database connection and schema initialisation."""

from __future__ import annotations

import logging
from pathlib import Path

import psycopg

logger = logging.getLogger(__name__)

MIGRATIONS_DIR = Path(__file__).parent.parent / "migrations"

# Ordered list of migration files. Add new entries here when adding migrations.
MIGRATION_FILES = [
    "001_schema.sql",
    "002_resolve_threads.sql",
]


def connect(database_url: str) -> psycopg.Connection:
    """Return an open psycopg3 connection with autocommit disabled."""
    return psycopg.connect(database_url)


def apply_migrations(conn: psycopg.Connection) -> None:
    """Apply any pending migrations in order.

    Each migration is tracked in public.schema_migrations. Migrations are
    applied inside individual transactions so a failure rolls back cleanly.
    """
    # Ensure the tracking table exists (idempotent DDL).
    with conn.transaction():
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS public.schema_migrations (
                version    TEXT PRIMARY KEY,
                applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """
        )

    applied: set[str] = {
        row[0]
        for row in conn.execute("SELECT version FROM public.schema_migrations").fetchall()
    }

    for filename in MIGRATION_FILES:
        if filename in applied:
            logger.debug("Migration already applied: %s", filename)
            continue

        sql = (MIGRATIONS_DIR / filename).read_text()
        logger.info("Applying migration: %s", filename)

        with conn.transaction():
            conn.execute(sql)
            conn.execute(
                "INSERT INTO public.schema_migrations (version) VALUES (%s)",
                (filename,),
            )

        logger.info("Migration applied: %s", filename)
