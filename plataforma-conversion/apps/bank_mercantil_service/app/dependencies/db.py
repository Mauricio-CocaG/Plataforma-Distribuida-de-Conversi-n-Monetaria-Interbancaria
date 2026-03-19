from collections.abc import Generator

from src.infrastructure.persistence.postgresql.connection import (
    PostgresMercantilSessionLocal,
)


def get_db() -> Generator:
    db = PostgresMercantilSessionLocal()
    try:
        yield db
    finally:
        db.close()