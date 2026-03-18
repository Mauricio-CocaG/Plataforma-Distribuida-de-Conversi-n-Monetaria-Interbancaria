from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from apps.bank_service.app.dependencies.db import get_db
from apps.bank_service.app.schemas.cuenta import CuentaCreate, CuentaResponse
from src.infrastructure.persistence.mysql.repositories.banco_union_cuenta_repository import (
    BancoUnionCuentaRepository,
)

router = APIRouter(prefix="/cuentas", tags=["Cuentas"])


@router.get("", response_model=list[CuentaResponse])
def listar_cuentas(db: Session = Depends(get_db)) -> list[CuentaResponse]:
    repo = BancoUnionCuentaRepository(db)
    return repo.get_all()


@router.get("/{cuenta_id}", response_model=CuentaResponse)
def obtener_cuenta(cuenta_id: int, db: Session = Depends(get_db)) -> CuentaResponse:
    repo = BancoUnionCuentaRepository(db)
    cuenta = repo.get_by_id(cuenta_id)
    if not cuenta:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Cuenta no encontrada",
        )
    return cuenta


@router.post("", response_model=CuentaResponse, status_code=status.HTTP_201_CREATED)
def crear_cuenta(payload: CuentaCreate, db: Session = Depends(get_db)) -> CuentaResponse:
    repo = BancoUnionCuentaRepository(db)
    return repo.create(payload)