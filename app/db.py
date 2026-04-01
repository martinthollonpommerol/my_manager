"""Database connection and schema initialisation."""

from __future__ import annotations  # permet les annotations de type forward-reference (ex: psycopg.Connection dans les signatures)

import logging          # journalisation des migrations appliquées ou ignorées
from pathlib import Path # manipulation des chemins de fichiers de manière portable (Linux/Windows)

import psycopg  # pilote PostgreSQL pour Python 3 (psycopg3)

logger = logging.getLogger(__name__)  # logger propre à ce module, nommé "app.db"

MIGRATIONS_DIR = Path(__file__).parent.parent / "migrations"  # chemin absolu vers le dossier migrations/ à la racine du projet

# Ordered list of migration files. Add new entries here when adding migrations.
MIGRATION_FILES = [        # liste ordonnée des fichiers SQL à appliquer ; l'ordre est garanti
    "001_schema.sql",      # crée le schéma mailapp et les 5 tables
    "002_resolve_threads.sql",  # crée la fonction stockée resolve_threads()
]


def connect(database_url: str) -> psycopg.Connection:  # ouvre et retourne une connexion psycopg3
    """Return an open psycopg3 connection with autocommit disabled."""
    return psycopg.connect(database_url)  # autocommit=False par défaut : chaque opération est dans une transaction explicite


def apply_migrations(conn: psycopg.Connection) -> None:
    """Apply any pending migrations in order.

    Each migration is tracked in public.schema_migrations. Migrations are
    applied inside individual transactions so a failure rolls back cleanly.
    """
    # Ensure the tracking table exists (idempotent DDL).
    with conn.transaction():   # ouvre une transaction ; commit automatique à la sortie du bloc
        conn.execute(          # CREATE TABLE IF NOT EXISTS : sans danger si déjà présente
            """
            CREATE TABLE IF NOT EXISTS public.schema_migrations (
                version    TEXT PRIMARY KEY,
                applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """
        )

    applied: set[str] = {                                                         # ensemble des migrations déjà enregistrées en base
        row[0]                                                                    # extrait le nom du fichier (colonne version)
        for row in conn.execute("SELECT version FROM public.schema_migrations").fetchall()  # lit toutes les lignes de la table de suivi
    }

    for filename in MIGRATION_FILES:           # itère sur les fichiers dans l'ordre déclaré
        if filename in applied:                # migration déjà exécutée : on la saute
            logger.debug("Migration already applied: %s", filename)  # log niveau DEBUG, pas visible en mode INFO
            continue                           # passe au fichier suivant

        sql = (MIGRATIONS_DIR / filename).read_text()  # lit le contenu SQL du fichier sur disque
        logger.info("Applying migration: %s", filename)  # annonce l'application en cours

        with conn.transaction():   # chaque migration est atomique : erreur = rollback complet de cette migration uniquement
            conn.execute(sql)      # exécute le SQL de la migration (CREATE TABLE, CREATE FUNCTION, etc.)
            conn.execute(          # enregistre la migration dans la table de suivi pour ne pas la rejouer
                "INSERT INTO public.schema_migrations (version) VALUES (%s)",
                (filename,),       # paramètre positionnel psycopg3, protège contre l'injection SQL
            )

        logger.info("Migration applied: %s", filename)  # confirmation après commit réussi
