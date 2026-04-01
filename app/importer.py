"""Orchestrate one mbox import run.

Responsibilities:
  1. Open the mbox file and iterate raw messages.
  2. Parse and filter each message via app.parser.
  3. Insert into PostgreSQL (imports, messages, message_recipients, attachments).
  4. Call mailapp.resolve_threads() after all messages are stored.

All SQL is written explicitly here; no ORM, no helpers that hide queries.
"""

from __future__ import annotations  # active les annotations de type en mode chaîne (évite les imports circulaires)

import json          # sérialisation du dictionnaire raw_headers vers la colonne JSONB PostgreSQL
import logging       # journalisation de la progression et des erreurs
import mailbox       # lecture du format mbox de la bibliothèque standard Python
from dataclasses import dataclass  # structure de données pour le résumé de l'import
from pathlib import Path           # manipulation des chemins de fichiers
from typing import Any, Optional   # Any : argument de type générique pour psycopg3 ; Optional : valeur potentiellement None

import psycopg  # pilote PostgreSQL psycopg3

from app import parser as msg_parser      # module de parsing, importé sous alias pour éviter le conflit avec le nom "parser"
from app.parser import ParsedMessage, SkipMessage  # types de données et exception de filtrage

logger = logging.getLogger(__name__)  # logger nommé "app.importer"


@dataclass                         # génère __init__ et __repr__ automatiquement
class ImportResult:
    import_id: int                 # identifiant de la ligne créée dans mailapp.imports
    total_seen: int                # nombre total de messages lus dans le fichier mbox
    inserted: int                  # messages effectivement insérés en base
    skipped_calendar: int          # messages ignorés car contenant une partie text/calendar
    skipped_empty: int             # messages ignorés car sans corps ni pièce jointe
    skipped_duplicate: int         # messages ignorés car le message_id_header existe déjà en base
    skipped_other: int             # messages ignorés pour toute autre raison (ex: Message-ID absent)


def import_mbox(
    conn: psycopg.Connection[Any],  # connexion PostgreSQL ouverte, gérée par l'appelant
    mbox_path: Path,                # chemin vers le fichier .mbox à importer
    label: str,                     # libellé lisible pour identifier cet import (ex: "Inbox 2024")
    batch_size: int = 200,          # taille des lots pour les inserts de destinataires et pièces jointes
) -> ImportResult:
    """Import all messages from *mbox_path* into the database.

    Returns an ImportResult with counts for each outcome.
    Calls mailapp.resolve_threads() at the end.

    The function operates inside a single long transaction so that a failure
    rolls back the partial import cleanly.  The imports row is set to 'failed'
    on any exception before re-raising.
    """
    import_id: Optional[int] = None  # identifiant de l'import, utilisé dans le bloc except pour marquer l'échec

    try:
        import_id = _create_import_record(conn, label, str(mbox_path))  # crée la ligne import en base, récupère son id
        result = _process_messages(conn, mbox_path, import_id, batch_size)  # itère sur tous les messages et les insère
        _finalise_import(conn, import_id, result.inserted)  # met à jour le compteur et passe le statut à "complete"
        _resolve_threads(conn)   # appelle la fonction PostgreSQL de reconstruction des fils de discussion
        conn.commit()            # valide toutes les opérations en une seule transaction
        return result            # retourne le résumé à l'appelant (cli.py)

    except Exception:                          # toute exception : on tente de marquer l'import comme échoué
        if import_id is not None:              # l'import a bien été créé avant l'erreur
            try:
                conn.execute(                  # met à jour le statut de l'import en "failed"
                    "UPDATE mailapp.imports SET status = 'failed' WHERE id = %s",
                    (import_id,),              # paramètre positionnel pour éviter l'injection SQL
                )
                conn.commit()                  # commit du statut d'échec pour le rendre visible même si la transaction principale échoue
            except Exception:
                pass  # Best-effort; the original exception is what matters.
        raise  # re-propage l'exception originale vers l'appelant


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _create_import_record(conn: psycopg.Connection[Any], label: str, source_path: str) -> int:
    row = conn.execute(        # INSERT avec RETURNING pour obtenir l'id généré sans second SELECT
        """
        INSERT INTO mailapp.imports (label, source_path, status)
        VALUES (%s, %s, 'pending')
        RETURNING id
        """,
        (label, source_path),  # label et chemin passés en paramètres psycopg3 (protection injection)
    ).fetchone()               # récupère la seule ligne retournée par RETURNING
    assert row is not None     # RETURNING garantit une ligne si INSERT réussit ; l'assert documente cette invariante
    return row[0]              # retourne l'id (première et seule colonne du résultat)


def _process_messages(
    conn: psycopg.Connection[Any],  # connexion à la base
    mbox_path: Path,                # chemin du fichier mbox
    import_id: int,                 # id de l'import courant, associé à chaque message inséré
    batch_size: int,                # nombre de messages après lequel on flush les destinataires et pièces jointes
) -> ImportResult:
    total_seen = 0       # compteur : messages lus dans le mbox (incluant les ignorés)
    inserted = 0         # compteur : messages insérés avec succès
    skipped_calendar = 0 # compteur : messages ignorés (invitation calendrier)
    skipped_empty = 0    # compteur : messages ignorés (aucun contenu)
    skipped_duplicate = 0 # compteur : messages ignorés (déjà en base)
    skipped_other = 0    # compteur : messages ignorés (autre raison)

    pending_recipients: list[tuple] = []    # tampon des destinataires à insérer, vidé par lots
    pending_attachments: list[tuple] = []   # tampon des pièces jointes à insérer, vidé par lots

    mbox = mailbox.mbox(str(mbox_path), factory=None, create=False)  # ouvre le fichier mbox en lecture seule (create=False : erreur si absent)

    try:
        for raw_msg in mbox:   # itère sur chaque message du fichier mbox
            total_seen += 1    # incrémente le compteur de messages vus

            try:
                parsed = msg_parser.parse(raw_msg)   # parse et filtre le message ; lève SkipMessage si à ignorer
            except SkipMessage as exc:               # message à ignorer : on catégorise la raison
                reason = str(exc)                    # message de l'exception indique la cause
                if "calendar" in reason:             # invitation calendrier
                    skipped_calendar += 1
                elif "content" in reason:            # message vide (aucun contenu utilisable)
                    skipped_empty += 1
                else:                                # Message-ID absent ou autre anomalie
                    skipped_other += 1
                logger.debug("Skipped message: %s", reason)  # log DEBUG uniquement (pas visible en INFO)
                continue  # passe au message suivant sans insérer

            message_id = _insert_message(conn, import_id, parsed)  # tente l'insertion ; retourne None si doublon
            if message_id is None:      # ON CONFLICT DO NOTHING : message déjà en base
                skipped_duplicate += 1
                continue  # pas la peine d'insérer les destinataires et pièces jointes d'un doublon

            inserted += 1  # message bien inséré : on incrémente le compteur

            for r in parsed.recipients:               # prépare les lignes destinataires pour l'insert par lot
                pending_recipients.append(
                    (message_id, r.recipient_type, r.address, r.display_name)  # tuple correspondant aux colonnes de message_recipients
                )

            for a in parsed.attachments:              # prépare les lignes pièces jointes pour l'insert par lot
                pending_attachments.append(
                    (
                        message_id,     # clé étrangère vers messages.id
                        a.content_type, # type MIME
                        a.filename,     # nom du fichier (peut être None)
                        a.content_id,   # Content-ID pour inline (peut être None)
                        a.size_bytes,   # taille en octets
                        a.data,         # contenu binaire (BYTEA)
                    )
                )

            if inserted % batch_size == 0:             # seuil de lot atteint : on vide les tampons
                _flush_recipients(conn, pending_recipients)    # insert batch des destinataires
                _flush_attachments(conn, pending_attachments)  # insert batch des pièces jointes
                pending_recipients.clear()             # vide le tampon après insertion
                pending_attachments.clear()            # vide le tampon après insertion
                logger.info("Progress: %d messages inserted so far", inserted)  # log de progression

    finally:
        mbox.close()  # ferme le fichier mbox dans tous les cas (succès ou exception)

    # Flush remaining rows.
    _flush_recipients(conn, pending_recipients)    # insère les destinataires restants (dernier lot incomplet)
    _flush_attachments(conn, pending_attachments)  # insère les pièces jointes restantes

    return ImportResult(             # construit le résumé de l'import avec tous les compteurs
        import_id=import_id,
        total_seen=total_seen,
        inserted=inserted,
        skipped_calendar=skipped_calendar,
        skipped_empty=skipped_empty,
        skipped_duplicate=skipped_duplicate,
        skipped_other=skipped_other,
    )


def _insert_message(
    conn: psycopg.Connection[Any],  # connexion à la base
    import_id: int,                 # id de l'import courant
    parsed: ParsedMessage,          # message analysé par parser.py
) -> Optional[int]:
    """Insert one message row.

    Returns the new row's id, or None if the message_id_header already exists
    (deduplicated via ON CONFLICT DO NOTHING).
    """
    row = conn.execute(      # INSERT avec déduplication et récupération de l'id généré
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
        ON CONFLICT (message_id_header) DO NOTHING   -- si le message existe déjà, on n'insère pas et RETURNING ne retourne rien
        RETURNING id                                 -- retourne l'id uniquement si l'INSERT a eu lieu
        """,
        (
            import_id,                         # lien vers l'import courant
            parsed.message_id_header,          # clé de déduplication unique
            parsed.in_reply_to,                # message-ID parent (peut être None)
            parsed.references_header,          # tableau PostgreSQL text[] des ancêtres
            parsed.subject,                    # objet du message
            parsed.sent_at,                    # date d'envoi en UTC (timestamptz)
            parsed.from_address,               # adresse expéditeur
            parsed.from_name,                  # nom expéditeur (peut être None)
            parsed.body_text,                  # corps texte brut (peut être None)
            parsed.body_html,                  # corps HTML (peut être None)
            json.dumps(parsed.raw_headers),    # sérialise le dict Python en JSON pour la colonne JSONB
        ),
    ).fetchone()             # None si ON CONFLICT a bloqué l'INSERT, sinon (id,)

    return row[0] if row else None  # retourne l'id si inséré, None si doublon


def _flush_recipients(conn: psycopg.Connection[Any], rows: list[tuple]) -> None:
    if not rows:   # tampon vide : aucune requête inutile
        return
    with conn.cursor() as cur:   # ouvre un curseur explicite (Connection n'expose pas executemany directement)
        cur.executemany(         # insert batch de toutes les lignes en une seule opération réseau
            """
            INSERT INTO mailapp.message_recipients
                (message_id, recipient_type, address, display_name)
            VALUES (%s, %s, %s, %s)
            """,
            rows,  # liste de tuples (message_id, type, adresse, nom)
        )


def _flush_attachments(conn: psycopg.Connection[Any], rows: list[tuple]) -> None:
    if not rows:   # tampon vide : aucune requête inutile
        return
    with conn.cursor() as cur:   # ouvre un curseur explicite pour accéder à executemany
        cur.executemany(         # insert batch de toutes les pièces jointes
            """
            INSERT INTO mailapp.attachments
                (message_id, content_type, filename, content_id, size_bytes, data)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            rows,  # liste de tuples (message_id, type, nom, cid, taille, bytes)
        )


def _finalise_import(conn: psycopg.Connection[Any], import_id: int, message_count: int) -> None:
    conn.execute(       # met à jour la ligne import avec le nombre de messages insérés et le statut final
        """
        UPDATE mailapp.imports
        SET message_count = %s, status = 'complete'
        WHERE id = %s
        """,
        (message_count, import_id),  # paramètres positionnels psycopg3
    )


def _resolve_threads(conn: psycopg.Connection[Any]) -> None:
    logger.info("Running mailapp.resolve_threads()…")     # annonce l'appel en cours (peut prendre du temps sur un grand volume)
    conn.execute("SELECT mailapp.resolve_threads()")      # appelle la fonction stockée PostgreSQL qui reconstruit tous les fils
    logger.info("Thread resolution complete.")            # confirmation une fois la fonction retournée
