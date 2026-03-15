from collections.abc import Generator

from src.infrastructure.persistence.mysql.connection import SessionLocal


def get_db() -> Generator:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()