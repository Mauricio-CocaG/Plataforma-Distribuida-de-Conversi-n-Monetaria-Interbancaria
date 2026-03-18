from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.infrastructure.config.settings import get_settings

settings = get_settings()

engine = create_engine(
    settings.mysql_url,
    pool_pre_ping=True,
    echo=settings.app_debug,
    future=True,
)

SessionLocal = sessionmaker(
    bind=engine,
    autoflush=False,
    autocommit=False,
    future=True,
)