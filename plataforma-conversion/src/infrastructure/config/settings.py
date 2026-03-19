from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # =========================================================
    # Configuración general
    # =========================================================
    app_name: str = "Bank Service"
    app_env: str = "development"
    app_debug: bool = True
    app_host: str = "0.0.0.0"
    app_port: int = 8081

    request_timeout: int = 30
    log_level: str = "INFO"

    asfi_api_url: str = "http://localhost:8090"
    bcb_api_url: str = "http://localhost:8082"

    # =========================================================
    # MySQL - Banco Unión
    # =========================================================
    mysql_host: str = "localhost"
    mysql_port: int = 3306
    mysql_db: str = "banco_union"
    mysql_user: str = "root"
    mysql_password: str = "root123"

    # =========================================================
    # PostgreSQL - Banco Mercantil
    # =========================================================
    postgres_mercantil_host: str = "localhost"
    postgres_mercantil_port: int = 5432
    postgres_mercantil_db: str = "banco_mercantil"
    postgres_mercantil_user: str = "mercantil_user"
    postgres_mercantil_password: str = "mercantil_pass"

    # =========================================================
    # Servicio Banco Mercantil
    # =========================================================
    bank_mercantil_app_name: str = "Banco Mercantil Service"
    bank_mercantil_app_port: int = 8083

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # =========================================================
    # URLs de conexión
    # =========================================================
    @property
    def mysql_url(self) -> str:
        return (
            f"mysql+pymysql://{self.mysql_user}:{self.mysql_password}"
            f"@{self.mysql_host}:{self.mysql_port}/{self.mysql_db}?charset=utf8mb4"
        )

    @property
    def postgres_mercantil_url(self) -> str:
        return (
            f"postgresql+psycopg2://{self.postgres_mercantil_user}:"
            f"{self.postgres_mercantil_password}@{self.postgres_mercantil_host}:"
            f"{self.postgres_mercantil_port}/{self.postgres_mercantil_db}"
        )


@lru_cache
def get_settings() -> Settings:
    return Settings()