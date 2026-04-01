"""Orchestrate one mbox import run.

Responsibilities:
  1. Open the mbox file and iterate raw messages.
  2. Parse and filter each message via app.parser.
  3. Insert into PostgreSQL (imports, messages, message_recipients, attachments).
  4. Call mailapp.resolve_threads() after all messages are stored.

All SQL is written explicitly here; no ORM, no helpers that hide queries.
"""

from __future__ import annotations

import json
import logging
import mailbox
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import psycopg

from app import parser as msg_parser
from app.parser import ParsedMessage, SkipMessage

logger = logging.getLogger(__name__)


@dataclass
class ImportResult:
    import_id: int
    total_seen: int
    inserted: int
    skipped_calendar: int
    skipped_empty: int
    skipped_duplicate: int
    skipped_other: int


def import_mbox(
    conn: psycopg.Connection,
    mbox_path: Path,
    label: str,
    batch_size: int = 200,
) -> ImportResult:
    """Import all messages from *mbox_path* into the database.

    Returns an ImportResult with counts for each outcome.
    Calls mailapp.resolve_threads() at the end.

    The function operates inside a single long transaction so that a failure
    rolls back the partial import cleanly.  The imports row is set to 'failed'
    on any exception before re-raising.
    """
    import_id: Optional[int] = None

    try:
        import_id = _create_import_record(conn, label, str(mbox_path))
        result = _process_messages(conn, mbox_path, import_id, batch_size)
        _finalise_import(conn, import_id, result.inserted)
        _resolve_threads(conn)
        conn.commit()
        return result

    except Exception:
        if import_id is not None:
            try:
                conn.execute(
                    "UPDATE mailapp.imports SET status = 'failed' WHERE id = %s",
                    (import_id,),
                )
                conn.commit()
            except Exception:
                pass  # Best-effort; the original exception is what matters.
        raise


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _create_import_record(conn: psycopg.Connection, label: str, source_path: str) -> int:
    row = conn.execute(
        """
        INSERT INTO mailapp.imports (label, source_path, status)
        VALUES (%s, %s, 'pending')
        RETURNING id
        """,
        (label, source_path),
    ).fetchone()
    assert row is not None
    return row[0]


def _process_messages(
    conn: psycopg.Connection,
    mbox_path: Path,
    import_id: int,
    batch_size: int,
) -> ImportResult:
    total_seen = 0
    inserted = 0
    skipped_calendar = 0
    skipped_empty = 0
    skipped_duplicate = 0
    skipped_other = 0

    pending_recipients: list[tuple] = []
    pending_attachments: list[tuple] = []

    mbox = mailbox.mbox(str(mbox_path), factory=None, create=False)

    try:
        for raw_msg in mbox:
            total_seen += 1

            try:
                parsed = msg_parser.parse(raw_msg)
            except SkipMessage as exc:
                reason = str(exc)
                if "calendar" in reason:
                    skipped_calendar += 1
                elif "content" in reason:
                    skipped_empty += 1
                else:
                    skipped_other += 1
                logger.debug("Skipped message: %s", reason)
                continue

            message_id = _insert_message(conn, import_id, parsed)
            if message_id is None:
                skipped_duplicate += 1
                continue

            inserted += 1

            for r in parsed.recipients:
                pending_recipients.append(
                    (message_id, r.recipient_type, r.address, r.display_name)
                )

            for a in parsed.attachments:
                pending_attachments.append(
                    (
                        message_id,
                        a.content_type,
                        a.filename,
                        a.content_id,
                        a.size_bytes,
                        a.data,
                    )
                )

            if inserted % batch_size == 0:
                _flush_recipients(conn, pending_recipients)
                _flush_attachments(conn, pending_attachments)
                pending_recipients.clear()
                pending_attachments.clear()
                logger.info("Progress: %d messages inserted so far", inserted)

    finally:
        mbox.close()

    # Flush remaining rows.
    _flush_recipients(conn, pending_recipients)
    _flush_attachments(conn, pending_attachments)

    return ImportResult(
        import_id=import_id,
        total_seen=total_seen,
        inserted=inserted,
        skipped_calendar=skipped_calendar,
        skipped_empty=skipped_empty,
        skipped_duplicate=skipped_duplicate,
        skipped_other=skipped_other,
    )


def _insert_message(
    conn: psycopg.Connection,
    import_id: int,
    parsed: ParsedMessage,
) -> Optional[int]:
    """Insert one message row.

    Returns the new row's id, or None if the message_id_header already exists
    (deduplicated via ON CONFLICT DO NOTHING).
    """
    row = conn.execute(
        """
        INSERT INTO mailapp.messages (
            import_id,
            message_id_header,
            in_reply_to,
            references_header,
            subject,
            sent_at,
            from_address,
            from_name,
            body_text,
            body_html,
            raw_headers
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (message_id_header) DO NOTHING
        RETURNING id
        """,
        (
            import_id,
            parsed.message_id_header,
            parsed.in_reply_to,
            parsed.references_header,
            parsed.subject,
            parsed.sent_at,
            parsed.from_address,
            parsed.from_name,
            parsed.body_text,
            parsed.body_html,
            json.dumps(parsed.raw_headers),
        ),
    ).fetchone()

    return row[0] if row else None


def _flush_recipients(conn: psycopg.Connection, rows: list[tuple]) -> None:
    if not rows:
        return
    conn.executemany(
        """
        INSERT INTO mailapp.message_recipients
            (message_id, recipient_type, address, display_name)
        VALUES (%s, %s, %s, %s)
        """,
        rows,
    )


def _flush_attachments(conn: psycopg.Connection, rows: list[tuple]) -> None:
    if not rows:
        return
    conn.executemany(
        """
        INSERT INTO mailapp.attachments
            (message_id, content_type, filename, content_id, size_bytes, data)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        rows,
    )


def _finalise_import(conn: psycopg.Connection, import_id: int, message_count: int) -> None:
    conn.execute(
        """
        UPDATE mailapp.imports
        SET message_count = %s, status = 'complete'
        WHERE id = %s
        """,
        (message_count, import_id),
    )


def _resolve_threads(conn: psycopg.Connection) -> None:
    logger.info("Running mailapp.resolve_threads()…")
    conn.execute("SELECT mailapp.resolve_threads()")
    logger.info("Thread resolution complete.")
