from datetime import datetime
from decimal import Decimal
from typing import Optional

from sqlalchemy import BIGINT, CHAR, DATETIME, DECIMAL, INT, TEXT, VARCHAR, Index, text
from sqlalchemy.orm import Mapped, mapped_column

from src.infrastructure.persistence.mysql.base import Base


class BancoUnionCuenta(Base):
    __tablename__ = "Cuentas"

    Id: Mapped[int] = mapped_column(BIGINT, primary_key=True, autoincrement=True)

    NroIdentificacion: Mapped[str] = mapped_column(VARCHAR(20), nullable=False)
    Nombres: Mapped[str] = mapped_column(VARCHAR(100), nullable=False)
    Apellidos: Mapped[str] = mapped_column(VARCHAR(100), nullable=False)
    NroCuenta: Mapped[str] = mapped_column(VARCHAR(30), nullable=False, unique=True)
    IdBanco: Mapped[int] = mapped_column(INT, nullable=False)

    SaldoUSD: Mapped[Decimal] = mapped_column(DECIMAL(18, 4), nullable=False)

    SaldoBs: Mapped[Optional[Decimal]] = mapped_column(DECIMAL(18, 4), nullable=True)
    TipoCambioAplicado: Mapped[Optional[Decimal]] = mapped_column(DECIMAL(10, 4), nullable=True)
    FechaConversion: Mapped[Optional[datetime]] = mapped_column(DATETIME, nullable=True)
    CodigoVerificacion: Mapped[Optional[str]] = mapped_column(CHAR(8), nullable=True)
    LoteId: Mapped[Optional[str]] = mapped_column(VARCHAR(50), nullable=True)

    DatosCifrados: Mapped[Optional[str]] = mapped_column(TEXT, nullable=True)

    AlgoritmoUsado: Mapped[str] = mapped_column(
        VARCHAR(50),
        nullable=False,
        server_default=text("'Cifrado César'"),
    )

    FechaRegistro: Mapped[datetime] = mapped_column(
        DATETIME,
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP"),
    )

    FechaActualizacion: Mapped[Optional[datetime]] = mapped_column(
        DATETIME,
        nullable=True,
        server_default=text("CURRENT_TIMESTAMP"),
        server_onupdate=text("CURRENT_TIMESTAMP"),
    )

    __table_args__ = (
        Index("idx_cuenta_nroident", "NroIdentificacion"),
        Index("idx_cuenta_nombre", "Nombres", "Apellidos"),
        Index("idx_cuenta_nrocuenta", "NroCuenta"),
        Index("idx_cuenta_idbanco", "IdBanco"),
        Index("idx_cuenta_lote", "LoteId"),
        Index("idx_cuenta_verificacion", "CodigoVerificacion"),
        Index("idx_cuenta_fechaconversion", "FechaConversion"),
    )