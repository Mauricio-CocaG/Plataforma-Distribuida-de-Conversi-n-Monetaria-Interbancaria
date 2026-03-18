from fastapi import APIRouter

from apps.bank_service.app.api.v1.endpoints import cuentas, health

router = APIRouter()
router.include_router(health.router)
router.include_router(cuentas.router)