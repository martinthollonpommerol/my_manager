"""Runtime configuration loaded from environment / .env file."""

import os                          # accès aux variables d'environnement du système
from dataclasses import dataclass  # décorateur pour créer des classes de données immutables

from dotenv import load_dotenv     # charge un fichier .env dans os.environ

load_dotenv()  # lit le fichier .env à la racine du projet et injecte ses variables dans l'environnement


@dataclass(frozen=True)  # frozen=True rend l'instance immuable après création (pas de modification accidentelle)
class Settings:
    database_url: str       # DSN PostgreSQL, ex: postgresql://user:pass@host:5432/db
    import_batch_size: int  # nombre de lignes envoyées en une seule requête pour les destinataires et pièces jointes


def load() -> Settings:  # construit et retourne la configuration à partir de l'environnement
    database_url = os.environ.get("DATABASE_URL")  # lit DATABASE_URL ; retourne None si absente
    if not database_url:  # chaîne vide ou None : la connexion est impossible sans cette variable
        raise RuntimeError(           # lève une erreur explicite plutôt que de laisser échouer plus loin
            "DATABASE_URL is not set. "
            "Add it to your .env file or export it as an environment variable."
        )
    return Settings(                                                          # instancie le dataclass avec les valeurs lues
        database_url=database_url,                                            # URL de connexion validée
        import_batch_size=int(os.environ.get("IMPORT_BATCH_SIZE", "200")),   # 200 par défaut si la variable est absente
    )
