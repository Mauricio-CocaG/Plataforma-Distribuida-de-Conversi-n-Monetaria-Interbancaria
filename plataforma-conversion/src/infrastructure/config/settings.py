from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "Bank Service"
    app_env: str = "development"
    app_debug: bool = True
    app_host: str = "0.0.0.0"
    app_port: int = 8081

    mysql_host: str = "localhost"
    mysql_port: int = 3306
    mysql_db: str = "banco_union"
    mysql_user: str = "root"
    mysql_password: str = "root123"

    asfi_api_url: str = "http://localhost:8090"
    bcb_api_url: str = "http://localhost:8082"
    request_timeout: int = 30
    log_level: str = "INFO"

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    @property
    def mysql_url(self) -> str:
        return (
            f"mysql+pymysql://{self.mysql_user}:{self.mysql_password}"
            f"@{self.mysql_host}:{self.mysql_port}/{self.mysql_db}?charset=utf8mb4"
        )


@lru_cache
def get_settings() -> Settings:
    return Settings()