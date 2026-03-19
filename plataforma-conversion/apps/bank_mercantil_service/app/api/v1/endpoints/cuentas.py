from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from apps.bank_mercantil_service.app.dependencies.db import get_db
from apps.bank_mercantil_service.app.schemas.cuenta import (
    ConversionResultUpdate,
    CuentaCreate,
    CuentaResponse,
)
from src.infrastructure.persistence.postgresql.repositories.banco_mercantil_cuenta_repository import (
    BancoMercantilCuentaRepository,
)
from src.infrastructure.security.atbash import atbash_encrypt, build_sensitive_payload

router = APIRouter(prefix="/cuentas", tags=["Cuentas Mercantil"])


@router.get("", response_model=list[CuentaResponse])
def listar_cuentas(db: Session = Depends(get_db)) -> list[CuentaResponse]:
    repo = BancoMercantilCuentaRepository(db)
    return repo.get_all()


@router.get("/pending", response_model=list[CuentaResponse])
def listar_cuentas_pendientes(db: Session = Depends(get_db)) -> list[CuentaResponse]:
    repo = BancoMercantilCuentaRepository(db)
    return repo.get_pending()


@router.get("/{cuenta_id}", response_model=CuentaResponse)
def obtener_cuenta(cuenta_id: int, db: Session = Depends(get_db)) -> CuentaResponse:
    repo = BancoMercantilCuentaRepository(db)
    cuenta = repo.get_by_id(cuenta_id)

    if not cuenta:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Cuenta no encontrada",
        )

    return cuenta


@router.post("", response_model=CuentaResponse, status_code=status.HTTP_201_CREATED)
def crear_cuenta(payload: CuentaCreate, db: Session = Depends(get_db)) -> CuentaResponse:
    repo = BancoMercantilCuentaRepository(db)

    sensitive_payload = build_sensitive_payload(
        nro_identificacion=payload.nro_identificacion,
        nombres=payload.nombres,
        apellidos=payload.apellidos,
        nro_cuenta=payload.nro_cuenta,
        saldo_usd=str(payload.saldo_usd),
    )

    encrypted_data = atbash_encrypt(sensitive_payload)

    data = payload.model_dump()
    data["datos_cifrados"] = encrypted_data

    return repo.create(data)


@router.patch("/{cuenta_id}/resultado-conversion", response_model=CuentaResponse)
def actualizar_resultado_conversion(
    cuenta_id: int,
    payload: ConversionResultUpdate,
    db: Session = Depends(get_db),
) -> CuentaResponse:
    repo = BancoMercantilCuentaRepository(db)

    cuenta = repo.update_conversion_result(
        cuenta_id=cuenta_id,
        saldo_bs=payload.saldo_bs,
        tipo_cambio_aplicado=payload.tipo_cambio_aplicado,
        codigo_verificacion=payload.codigo_verificacion,
        lote_id=payload.lote_id,
        fecha_conversion=payload.fecha_conversion,
    )

    if not cuenta:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Cuenta no encontrada",
        )

    return cuenta