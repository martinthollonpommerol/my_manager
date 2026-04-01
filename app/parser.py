"""Parse a single email.message.Message into a structured dict.

This module is pure Python — no I/O, no database.  All filtering logic
(calendar invites, empty messages) is also applied here so the importer
only receives messages that are ready to insert.
"""

from __future__ import annotations

import email.header
import email.message
import email.utils
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)

# Regex to extract all <message-id> tokens from a header value
_MESSAGE_ID_RE = re.compile(r"<([^>]+)>")


# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------


@dataclass
class Recipient:
    recipient_type: str  # "to" | "cc" | "bcc"
    address: str
    display_name: Optional[str]


@dataclass
class Attachment:
    content_type: str
    filename: Optional[str]
    content_id: Optional[str]
    size_bytes: int
    data: bytes


@dataclass
class ParsedMessage:
    message_id_header: str
    in_reply_to: Optional[str]
    references_header: list[str]
    subject: Optional[str]
    sent_at: Optional[datetime]
    from_address: str
    from_name: Optional[str]
    body_text: Optional[str]
    body_html: Optional[str]
    raw_headers: dict[str, str]
    recipients: list[Recipient] = field(default_factory=list)
    attachments: list[Attachment] = field(default_factory=list)


class SkipMessage(Exception):
    """Raised when a message should not be imported."""


# ---------------------------------------------------------------------------
# Header decoding helpers
# ---------------------------------------------------------------------------


def _decode_header(value: Optional[str]) -> Optional[str]:
    """Decode an RFC 2047 encoded header value to a plain string."""
    if value is None:
        return None
    parts = email.header.decode_header(value)
    decoded_parts: list[str] = []
    for raw, charset in parts:
        if isinstance(raw, bytes):
            try:
                decoded_parts.append(raw.decode(charset or "utf-8", errors="replace"))
            except (LookupError, UnicodeDecodeError):
                decoded_parts.append(raw.decode("latin-1", errors="replace"))
        else:
            decoded_parts.append(raw)
    return "".join(decoded_parts).strip()


def _parse_date(value: Optional[str]) -> Optional[datetime]:
    """Parse an RFC 2822 date string.  Returns None on any failure."""
    if not value:
        return None
    try:
        parsed = email.utils.parsedate_to_datetime(value)
        # Normalise to UTC if the date is offset-aware.
        if parsed.tzinfo is not None:
            return parsed.astimezone(timezone.utc)
        # Treat naive datetimes as UTC.
        return parsed.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _parse_addresses(value: Optional[str]) -> list[tuple[Optional[str], str]]:
    """Return a list of (display_name, address) from a header value."""
    if not value:
        return []
    pairs = email.utils.getaddresses([value])
    result: list[tuple[Optional[str], str]] = []
    for name, addr in pairs:
        addr = addr.strip().lower()
        if not addr:
            continue
        result.append((_decode_header(name) or None, addr))
    return result


def _extract_message_ids(value: Optional[str]) -> list[str]:
    """Extract all <message-id> tokens from a header value."""
    if not value:
        return []
    return _MESSAGE_ID_RE.findall(value)


def _clean_message_id(value: Optional[str]) -> Optional[str]:
    """Normalise a Message-ID header to the bare id without angle brackets."""
    if not value:
        return None
    # Try to extract from angle brackets first.
    found = _MESSAGE_ID_RE.findall(value)
    if found:
        return found[0].strip()
    # Fallback: strip whitespace and angle brackets directly.
    return value.strip().strip("<>").strip() or None


# ---------------------------------------------------------------------------
# MIME part helpers
# ---------------------------------------------------------------------------


def _has_calendar_part(msg: email.message.Message) -> bool:
    """Return True if any MIME part has content-type text/calendar."""
    for part in msg.walk():
        ct = part.get_content_type()
        if ct == "text/calendar":
            return True
    return False


def _decode_payload(part: email.message.Message) -> Optional[str]:
    """Decode a text MIME part payload to a string."""
    charset = part.get_content_charset() or "utf-8"
    try:
        raw = part.get_payload(decode=True)
        if not raw:
            return None
        return raw.decode(charset, errors="replace")
    except Exception:
        return None


def _is_attachment_part(part: email.message.Message) -> bool:
    """Return True if this MIME part is an attachment (not inline text/html)."""
    disposition = part.get_content_disposition()
    if disposition and disposition.lower() == "attachment":
        return True
    ct = part.get_content_maintype()
    # Inline text/plain and text/html are body parts, not attachments.
    return ct not in ("text", "multipart")


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def parse(msg: email.message.Message) -> ParsedMessage:
    """Parse *msg* into a ParsedMessage.

    Raises SkipMessage when the message should not be imported:
      - Any MIME part has content-type text/calendar.
      - Message has no Message-ID header (cannot deduplicate).
      - Message has no usable body (text, html, or attachments).
    """
    # --- Skip: calendar invites ---
    if _has_calendar_part(msg):
        raise SkipMessage("contains text/calendar part")

    # --- Skip: no Message-ID (cannot deduplicate) ---
    raw_mid = msg.get("Message-ID")
    message_id_header = _clean_message_id(raw_mid)
    if not message_id_header:
        raise SkipMessage("missing Message-ID header")

    # --- Headers ---
    in_reply_to_raw = msg.get("In-Reply-To")
    in_reply_to_ids = _extract_message_ids(in_reply_to_raw)
    in_reply_to = in_reply_to_ids[0] if in_reply_to_ids else None

    references_header = _extract_message_ids(msg.get("References", ""))

    subject = _decode_header(msg.get("Subject"))

    sent_at = _parse_date(msg.get("Date"))

    raw_from = msg.get("From", "")
    from_pairs = _parse_addresses(raw_from)
    if from_pairs:
        from_name, from_address = from_pairs[0]
    else:
        from_name = None
        from_address = _decode_header(raw_from) or ""

    # Collect all headers as a plain dict for raw_headers JSONB column.
    # Duplicate header names are joined with a newline.
    raw_headers: dict[str, str] = {}
    for key, val in msg.items():
        decoded_val = _decode_header(val) or ""
        if key in raw_headers:
            raw_headers[key] = raw_headers[key] + "\n" + decoded_val
        else:
            raw_headers[key] = decoded_val

    # --- MIME walk: extract body and attachments ---
    body_text: Optional[str] = None
    body_html: Optional[str] = None
    attachments: list[Attachment] = []
    recipients: list[Recipient] = []

    for part in msg.walk():
        ct = part.get_content_type()

        # Always skip calendar parts (belt-and-suspenders; already checked above).
        if ct == "text/calendar":
            continue

        # Skip multipart containers — their children are visited by walk().
        if part.get_content_maintype() == "multipart":
            continue

        if ct == "text/plain" and not _is_attachment_part(part) and body_text is None:
            body_text = _decode_payload(part)

        elif ct == "text/html" and not _is_attachment_part(part) and body_html is None:
            body_html = _decode_payload(part)

        elif _is_attachment_part(part):
            raw_data = part.get_payload(decode=True)
            if raw_data is None:
                continue
            filename = part.get_filename() or part.get_param("name")
            if filename:
                filename = _decode_header(filename)
            content_id_raw = part.get("Content-ID")
            content_id = _clean_message_id(content_id_raw)
            attachments.append(
                Attachment(
                    content_type=ct,
                    filename=filename,
                    content_id=content_id,
                    size_bytes=len(raw_data),
                    data=raw_data,
                )
            )

    # --- Skip: empty message ---
    has_content = bool(body_text or body_html or attachments)
    if not has_content:
        raise SkipMessage("no usable content (no body text, html, or attachments)")

    # --- Recipients ---
    for header_name, rtype in (("To", "to"), ("Cc", "cc"), ("Bcc", "bcc")):
        for display_name, address in _parse_addresses(msg.get(header_name)):
            recipients.append(
                Recipient(recipient_type=rtype, address=address, display_name=display_name)
            )

    return ParsedMessage(
        message_id_header=message_id_header,
        in_reply_to=in_reply_to,
        references_header=references_header,
        subject=subject,
        sent_at=sent_at,
        from_address=from_address,
        from_name=from_name,
        body_text=body_text,
        body_html=body_html,
        raw_headers=raw_headers,
        recipients=recipients,
        attachments=attachments,
    )
