from sqlalchemy import select
from sqlalchemy.orm import Session

from apps.bank_service.app.schemas.cuenta import CuentaCreate
from src.infrastructure.persistence.mysql.models.banco_union_cuenta import BancoUnionCuenta


class BancoUnionCuentaRepository:
    def __init__(self, db: Session) -> None:
        self.db = db

    def get_all(self) -> list[BancoUnionCuenta]:
        stmt = select(BancoUnionCuenta).order_by(BancoUnionCuenta.Id.desc())
        return list(self.db.scalars(stmt).all())

    def get_by_id(self, cuenta_id: int) -> BancoUnionCuenta | None:
        stmt = select(BancoUnionCuenta).where(BancoUnionCuenta.Id == cuenta_id)
        return self.db.scalar(stmt)

    def create(self, payload: CuentaCreate) -> BancoUnionCuenta:
        cuenta = BancoUnionCuenta(**payload.model_dump())
        self.db.add(cuenta)
        self.db.commit()
        self.db.refresh(cuenta)
        return cuenta