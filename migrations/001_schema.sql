-- Migration 001: create mailapp schema and all tables

CREATE SCHEMA IF NOT EXISTS mailapp;

-- Track applied migrations
CREATE TABLE IF NOT EXISTS public.schema_migrations (
    version TEXT PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- -----------------------------------------------------------------------
-- imports: one row per mbox import run
-- -----------------------------------------------------------------------
CREATE TABLE mailapp.imports (
    id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    label          TEXT NOT NULL,
    source_path    TEXT NOT NULL,
    imported_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    message_count  INT NOT NULL DEFAULT 0,
    status         TEXT NOT NULL DEFAULT 'pending'
                   CHECK (status IN ('pending', 'complete', 'failed'))
);

-- -----------------------------------------------------------------------
-- threads: one row per reconstructed conversation
-- -----------------------------------------------------------------------
CREATE TABLE mailapp.threads (
    id                      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    root_message_id_header  TEXT NOT NULL UNIQUE,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- -----------------------------------------------------------------------
-- messages: one row per unique email
-- -----------------------------------------------------------------------
CREATE TABLE mailapp.messages (
    id                  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    import_id           BIGINT NOT NULL REFERENCES mailapp.imports(id),
    thread_id           BIGINT REFERENCES mailapp.threads(id),
    message_id_header   TEXT NOT NULL,
    in_reply_to         TEXT,
    references_header   TEXT[] NOT NULL DEFAULT '{}',
    subject             TEXT,
    sent_at             TIMESTAMPTZ,
    from_address        TEXT NOT NULL,
    from_name           TEXT,
    body_text           TEXT,
    body_html           TEXT,
    raw_headers         JSONB NOT NULL DEFAULT '{}',
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT messages_message_id_header_unique UNIQUE (message_id_header)
);

CREATE INDEX messages_thread_id_idx    ON mailapp.messages (thread_id);
CREATE INDEX messages_import_id_idx    ON mailapp.messages (import_id);
CREATE INDEX messages_in_reply_to_idx  ON mailapp.messages (in_reply_to)
    WHERE in_reply_to IS NOT NULL;

-- GIN index for searching the references array
CREATE INDEX messages_references_idx ON mailapp.messages USING gin (references_header);

-- -----------------------------------------------------------------------
-- message_recipients: normalised To / Cc / Bcc
-- -----------------------------------------------------------------------
CREATE TABLE mailapp.message_recipients (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    message_id      BIGINT NOT NULL REFERENCES mailapp.messages(id) ON DELETE CASCADE,
    recipient_type  TEXT NOT NULL CHECK (recipient_type IN ('to', 'cc', 'bcc')),
    address         TEXT NOT NULL,
    display_name    TEXT
);

CREATE INDEX message_recipients_message_id_idx ON mailapp.message_recipients (message_id, recipient_type);

-- -----------------------------------------------------------------------
-- attachments
-- -----------------------------------------------------------------------
CREATE TABLE mailapp.attachments (
    id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    message_id    BIGINT NOT NULL REFERENCES mailapp.messages(id) ON DELETE CASCADE,
    content_type  TEXT NOT NULL,
    filename      TEXT,
    content_id    TEXT,
    size_bytes    INT,
    data          BYTEA,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX attachments_message_id_idx ON mailapp.attachments (message_id);
