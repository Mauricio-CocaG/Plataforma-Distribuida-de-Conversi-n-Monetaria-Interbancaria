from datetime import datetime
from decimal import Decimal
from typing import Optional

from sqlalchemy import CHAR, DateTime, Index, Integer, Numeric, String, Text, text
from sqlalchemy.orm import Mapped, mapped_column

from src.infrastructure.persistence.postgresql.base import PostgresBase


class BancoMercantilCuenta(PostgresBase):
    __tablename__ = "cuentas"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    nro_identificacion: Mapped[str] = mapped_column(String(20), nullable=False)
    nombres: Mapped[str] = mapped_column(String(100), nullable=False)
    apellidos: Mapped[str] = mapped_column(String(100), nullable=False)
    nro_cuenta: Mapped[str] = mapped_column(String(30), nullable=False, unique=True)
    id_banco: Mapped[int] = mapped_column(Integer, nullable=False)

    saldo_usd: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False)

    saldo_bs: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    tipo_cambio_aplicado: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    fecha_conversion: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    codigo_verificacion: Mapped[Optional[str]] = mapped_column(CHAR(8), nullable=True)
    lote_id: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)

    datos_cifrados: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    algoritmo_usado: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        server_default=text("'Atbash'"),
    )

    fecha_registro: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP"),
    )

    fecha_actualizacion: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP"),
    )

    __table_args__ = (
        Index("idx_cuentas_nro_identificacion", "nro_identificacion"),
        Index("idx_cuentas_nombre", "nombres", "apellidos"),
        Index("idx_cuentas_nro_cuenta", "nro_cuenta"),
        Index("idx_cuentas_id_banco", "id_banco"),
        Index("idx_cuentas_lote_id", "lote_id"),
        Index("idx_cuentas_codigo_verificacion", "codigo_verificacion"),
        Index("idx_cuentas_fecha_conversion", "fecha_conversion"),
    )