from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.infrastructure.persistence.postgresql.models.banco_mercantil_cuenta import (
    BancoMercantilCuenta,
)


class BancoMercantilCuentaRepository:
    def __init__(self, db: Session) -> None:
        self.db = db

    def get_all(self) -> list[BancoMercantilCuenta]:
        stmt = select(BancoMercantilCuenta).order_by(BancoMercantilCuenta.id.desc())
        return list(self.db.scalars(stmt).all())

    def get_by_id(self, cuenta_id: int) -> BancoMercantilCuenta | None:
        stmt = select(BancoMercantilCuenta).where(BancoMercantilCuenta.id == cuenta_id)
        return self.db.scalar(stmt)

    def get_pending(self) -> list[BancoMercantilCuenta]:
        stmt = (
            select(BancoMercantilCuenta)
            .where(BancoMercantilCuenta.saldo_bs.is_(None))
            .order_by(BancoMercantilCuenta.id.asc())
        )
        return list(self.db.scalars(stmt).all())

    def create(self, data: dict) -> BancoMercantilCuenta:
        cuenta = BancoMercantilCuenta(**data)
        self.db.add(cuenta)
        self.db.commit()
        self.db.refresh(cuenta)
        return cuenta

    def update_conversion_result(
        self,
        cuenta_id: int,
        saldo_bs,
        tipo_cambio_aplicado,
        codigo_verificacion,
        lote_id,
        fecha_conversion=None,
    ) -> BancoMercantilCuenta | None:
        cuenta = self.get_by_id(cuenta_id)
        if not cuenta:
            return None

        cuenta.saldo_bs = saldo_bs
        cuenta.tipo_cambio_aplicado = tipo_cambio_aplicado
        cuenta.codigo_verificacion = codigo_verificacion
        cuenta.lote_id = lote_id
        cuenta.fecha_conversion = fecha_conversion or datetime.now(UTC).replace(tzinfo=None)

        self.db.commit()
        self.db.refresh(cuenta)
        return cuenta