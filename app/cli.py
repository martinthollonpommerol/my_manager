"""CLI entry point for the mbox importer.

Usage:
    python -m app.cli --mbox /path/to/file.mbox --label "Inbox 2024"
"""

from __future__ import annotations  # active les annotations de type en mode chaîne

import argparse   # parsing des arguments de la ligne de commande
import logging    # journalisation des étapes de l'import
import sys        # sys.stderr pour les messages d'erreur, sys.exit pour le code de retour
from pathlib import Path  # manipulation des chemins de fichiers

from app import config, db           # chargement de la configuration et gestion de la connexion/migrations
from app.importer import import_mbox # fonction principale d'import


def _configure_logging(verbose: bool) -> None:  # configure le niveau et le format de journalisation
    level = logging.DEBUG if verbose else logging.INFO  # DEBUG si --verbose, sinon INFO
    logging.basicConfig(                        # configure le handler racine (une seule fois au démarrage)
        level=level,                            # niveau minimum des messages affichés
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",  # format : date, niveau, module, message
        datefmt="%Y-%m-%dT%H:%M:%S",           # date au format ISO 8601
    )


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:  # définit et parse les arguments CLI
    parser = argparse.ArgumentParser(           # crée le parseur avec description affichée dans --help
        prog="mbox-import",                     # nom affiché dans l'usage
        description="Import a Thunderbird mbox file into PostgreSQL.",
    )
    parser.add_argument(
        "--mbox",                               # argument obligatoire : chemin du fichier mbox
        required=True,
        metavar="PATH",                         # libellé dans le message d'usage
        help="Path to the .mbox file to import.",
    )
    parser.add_argument(
        "--label",                              # argument obligatoire : nom lisible de l'import
        required=True,
        metavar="LABEL",
        help="Human-readable label for this import run (e.g. folder name).",
    )
    parser.add_argument(
        "--verbose", "-v",                      # flag optionnel, raccourci -v disponible
        action="store_true",                    # présence du flag → True, absence → False
        help="Enable debug-level logging.",
    )
    return parser.parse_args(argv)              # parse argv (None = sys.argv par défaut)


def main(argv: list[str] | None = None) -> int:  # point d'entrée principal, retourne un code de sortie (0=succès, 1=erreur)
    args = _parse_args(argv)                   # récupère les arguments parsés
    _configure_logging(args.verbose)           # configure le logging avant toute autre opération
    logger = logging.getLogger(__name__)       # logger nommé "app.cli"

    mbox_path = Path(args.mbox).resolve()      # résout le chemin en absolu (élimine les ".." et les liens symboliques)

    if not mbox_path.exists():                 # le fichier n'existe pas : erreur immédiate avec message clair
        print(f"error: file not found: {mbox_path}", file=sys.stderr)  # sur stderr pour ne pas polluer stdout
        return 1                               # code de retour 1 : erreur
    if not mbox_path.is_file():               # le chemin existe mais c'est un dossier ou un lien spécial
        print(f"error: not a file: {mbox_path}", file=sys.stderr)
        return 1

    try:
        settings = config.load()              # charge DATABASE_URL et IMPORT_BATCH_SIZE depuis l'environnement
    except RuntimeError as exc:               # DATABASE_URL manquante : erreur de configuration
        print(f"error: {exc}", file=sys.stderr)
        return 1

    try:
        conn = db.connect(settings.database_url)  # ouvre la connexion PostgreSQL
    except Exception as exc:                   # hôte inaccessible, mauvais credentials, etc.
        print(f"error: could not connect to database: {exc}", file=sys.stderr)
        return 1

    # import_mbox manages its own commits/rollbacks; we only own the connection
    # lifecycle here, so use an explicit close rather than `with conn:` to avoid
    # a redundant commit/rollback wrapping an already-committed transaction.
    try:
        db.apply_migrations(conn)              # applique les migrations en attente avant l'import

        logger.info("Starting import: %s  →  label=%r", mbox_path, args.label)  # log de démarrage

        result = import_mbox(                  # lance l'import complet et retourne le résumé
            conn,
            mbox_path=mbox_path,              # chemin résolu du fichier mbox
            label=args.label,                 # libellé de l'import
            batch_size=settings.import_batch_size,  # taille des lots depuis la configuration
        )
    finally:
        conn.close()  # ferme la connexion dans tous les cas, même en cas d'exception

    print(                                    # affiche le résumé de l'import sur stdout
        f"\nImport complete (import_id={result.import_id})\n"
        f"  Total messages seen:    {result.total_seen}\n"
        f"  Inserted:               {result.inserted}\n"
        f"  Skipped — duplicate:    {result.skipped_duplicate}\n"
        f"  Skipped — calendar:     {result.skipped_calendar}\n"
        f"  Skipped — empty:        {result.skipped_empty}\n"
        f"  Skipped — other:        {result.skipped_other}\n"
    )
    return 0  # code de retour 0 : succès


if __name__ == "__main__":           # permet d'exécuter le module directement : python -m app.cli
    sys.exit(main())                 # convertit le code de retour en code de sortie du processus
