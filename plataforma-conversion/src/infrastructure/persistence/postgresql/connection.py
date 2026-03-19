from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.infrastructure.config.settings import get_settings

settings = get_settings()

postgres_mercantil_engine = create_engine(
    settings.postgres_mercantil_url,
    pool_pre_ping=True,
    echo=settings.app_debug,
    future=True,
)

PostgresMercantilSessionLocal = sessionmaker(
    bind=postgres_mercantil_engine,
    autoflush=False,
    autocommit=False,
    future=True,
)