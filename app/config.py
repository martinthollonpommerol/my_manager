"""Runtime configuration loaded from environment / .env file."""

import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Settings:
    database_url: str
    import_batch_size: int  # rows flushed per DB round-trip for recipients/attachments


def load() -> Settings:
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise RuntimeError(
            "DATABASE_URL is not set. "
            "Add it to your .env file or export it as an environment variable."
        )
    return Settings(
        database_url=database_url,
        import_batch_size=int(os.environ.get("IMPORT_BATCH_SIZE", "200")),
    )
