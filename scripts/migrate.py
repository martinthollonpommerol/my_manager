"""Standalone migration runner.

Applies any pending migrations and exits.

Usage:
    python scripts/migrate.py
"""

from __future__ import annotations  # active les annotations de type en mode chaîne

import logging   # journalisation des migrations appliquées
import sys       # sys.path pour la résolution des imports, sys.exit pour le code de retour
from pathlib import Path  # manipulation du chemin pour trouver la racine du projet

# Allow running from the project root without installing the package.
sys.path.insert(0, str(Path(__file__).parent.parent))  # ajoute la racine du projet au chemin Python pour que "from app import …" fonctionne sans pip install

from app import config, db  # chargement de la configuration et accès aux fonctions de migration


def main() -> int:  # retourne 0 si succès, 1 si erreur de configuration
    logging.basicConfig(                      # configure le logging pour ce script autonome
        level=logging.INFO,                   # niveau INFO : affiche les migrations appliquées
        format="%(asctime)s  %(levelname)-8s  %(message)s",  # format simplifié (sans nom de module)
        datefmt="%Y-%m-%dT%H:%M:%S",         # date ISO 8601
    )

    try:
        settings = config.load()              # charge DATABASE_URL depuis l'environnement / .env
    except RuntimeError as exc:               # DATABASE_URL absente : impossible de se connecter
        print(f"error: {exc}", file=sys.stderr)  # message d'erreur sur stderr
        return 1                              # code de retour 1 : erreur

    conn = db.connect(settings.database_url)  # ouvre la connexion PostgreSQL
    with conn:                                # context manager psycopg3 : commit si succès, rollback si exception, ferme la connexion
        db.apply_migrations(conn)             # applique toutes les migrations en attente dans l'ordre

    print("All migrations applied.")          # confirmation finale sur stdout
    return 0                                  # code de retour 0 : succès


if __name__ == "__main__":      # permet l'exécution directe : python scripts/migrate.py
    sys.exit(main())            # transmet le code de retour au shell
