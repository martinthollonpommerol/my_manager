-- Migration 002: resolve_threads() stored function
--
-- Assigns every message in mailapp.messages to a thread.
-- Idempotent: resets and recomputes all thread assignments on each call.
--
-- Algorithm:
--   1. Build a parent map: for each message, find the best candidate parent
--      (last element of references_header, or in_reply_to).
--   2. Walk parent chains with a recursive CTE, capping depth at 50 and
--      detecting cycles via a visited-path array.
--   3. The message at the end of each chain (no known parent) is the root.
--   4. Upsert one thread row per distinct root.
--   5. Bulk-update messages.thread_id.
--   6. Any messages unreachable by the CTE (should not happen; safety net)
--      become their own thread roots.

CREATE OR REPLACE FUNCTION mailapp.resolve_threads()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    -- ----------------------------------------------------------------
    -- Step 1: Reset all thread assignments on existing messages.
    -- We recompute from scratch so every call is idempotent and newly
    -- imported messages are threaded together with existing ones.
    -- ----------------------------------------------------------------
    UPDATE mailapp.messages SET thread_id = NULL;

    -- ----------------------------------------------------------------
    -- Step 2: Recursive ancestry walk.
    --
    -- parent_candidate: the best-guess parent message_id_header for
    -- each message, using References last token > in_reply_to.
    --
    -- ancestry: walks the chain upward; stops when the candidate parent
    -- is not found in messages (orphan/true root), when depth hits 50,
    -- or when a cycle is detected (candidate already in visited path).
    --
    -- Result: (message_row_id, root_message_id_header) for every message.
    -- ----------------------------------------------------------------
    WITH RECURSIVE

    parent_candidate AS (
        SELECT
            m.id,
            m.message_id_header,
            -- References last token takes priority (most direct parent
            -- according to the sender's mail client), then In-Reply-To.
            -- Self-references (a message citing its own ID) are treated
            -- as NULL so the message becomes a root instead of looping.
            CASE
                WHEN array_length(m.references_header, 1) > 0
                    AND m.references_header[array_length(m.references_header, 1)]
                        IS DISTINCT FROM m.message_id_header
                    THEN m.references_header[array_length(m.references_header, 1)]
                WHEN m.in_reply_to IS DISTINCT FROM m.message_id_header
                    THEN m.in_reply_to
                ELSE NULL
            END AS candidate_parent
        FROM mailapp.messages m
    ),

    ancestry AS (
        -- Base case: messages that are roots.
        -- A message is a root when it has no candidate parent, or when
        -- its candidate parent does not exist in the messages table.
        SELECT
            pc.id                    AS msg_id,
            pc.message_id_header     AS msg_mid,
            pc.message_id_header     AS root_mid,
            0                        AS depth,
            ARRAY[pc.id]             AS visited,
            false                    AS cycle
        FROM parent_candidate pc
        WHERE
            pc.candidate_parent IS NULL
            OR NOT EXISTS (
                SELECT 1
                FROM mailapp.messages m2
                WHERE m2.message_id_header = pc.candidate_parent
            )

        UNION ALL

        -- Recursive case: message whose parent is already resolved.
        SELECT
            pc.id                    AS msg_id,
            pc.message_id_header     AS msg_mid,
            a.root_mid               AS root_mid,
            a.depth + 1              AS depth,
            a.visited || pc.id       AS visited,
            pc.id = ANY(a.visited)   AS cycle
        FROM parent_candidate pc
        JOIN ancestry a
            ON a.msg_mid = pc.candidate_parent
        WHERE
            NOT a.cycle
            AND a.depth < 50
    ),

    -- Keep only the deepest (most specific) resolution per message.
    -- If a message matches multiple recursive paths (should not happen
    -- in well-formed data), take the one with the greatest depth.
    best_resolution AS (
        SELECT DISTINCT ON (msg_id)
            msg_id,
            root_mid
        FROM ancestry
        ORDER BY msg_id, depth DESC
    ),

    -- ----------------------------------------------------------------
    -- Step 3: Upsert one thread row per distinct root.
    -- ----------------------------------------------------------------
    inserted_threads AS (
        INSERT INTO mailapp.threads (root_message_id_header)
        SELECT DISTINCT root_mid
        FROM best_resolution
        ON CONFLICT (root_message_id_header) DO UPDATE
            SET updated_at = now()
        RETURNING id, root_message_id_header
    )

    -- ----------------------------------------------------------------
    -- Step 4: Assign thread_id to messages.
    -- ----------------------------------------------------------------
    UPDATE mailapp.messages m
    SET thread_id = t.id
    FROM best_resolution br
    JOIN inserted_threads t ON t.root_message_id_header = br.root_mid
    WHERE m.id = br.msg_id;

    -- ----------------------------------------------------------------
    -- Step 5: Safety net — assign any still-unthreaded messages
    -- (can only happen if the CTE missed them due to a data anomaly).
    -- Each becomes its own thread root.
    -- ----------------------------------------------------------------
    WITH orphan_threads AS (
        INSERT INTO mailapp.threads (root_message_id_header)
        SELECT m.message_id_header
        FROM mailapp.messages m
        WHERE m.thread_id IS NULL
        ON CONFLICT (root_message_id_header) DO UPDATE
            SET updated_at = now()
        RETURNING id, root_message_id_header
    )
    UPDATE mailapp.messages m
    SET thread_id = ot.id
    FROM orphan_threads ot
    WHERE m.thread_id IS NULL
      AND m.message_id_header = ot.root_message_id_header;

    -- ----------------------------------------------------------------
    -- Step 6: Delete thread rows that no longer have any messages.
    -- These accumulate across runs when orphan threads are merged into
    -- their parent thread after the true parent arrives in a later import.
    -- ----------------------------------------------------------------
    DELETE FROM mailapp.threads t
    WHERE NOT EXISTS (
        SELECT 1
        FROM mailapp.messages m
        WHERE m.thread_id = t.id
    );

END;
$$;
