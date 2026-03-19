from fastapi import FastAPI

from apps.bank_mercantil_service.app.api.router import router as api_router
from src.infrastructure.config.settings import get_settings

settings = get_settings()

app = FastAPI(
    title=settings.bank_mercantil_app_name,
    debug=settings.app_debug,
)

app.include_router(api_router)