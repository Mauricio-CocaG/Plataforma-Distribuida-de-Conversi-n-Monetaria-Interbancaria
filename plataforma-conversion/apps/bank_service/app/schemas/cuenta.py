from datetime import datetime
from decimal import Decimal
from typing import Optional

from pydantic import BaseModel, ConfigDict


class CuentaBase(BaseModel):
    NroIdentificacion: str
    Nombres: str
    Apellidos: str
    NroCuenta: str
    IdBanco: int
    SaldoUSD: Decimal
    DatosCifrados: Optional[str] = None
    AlgoritmoUsado: str = "Cifrado César"


class CuentaCreate(CuentaBase):
    pass


class CuentaResponse(CuentaBase):
    Id: int
    SaldoBs: Optional[Decimal] = None
    TipoCambioAplicado: Optional[Decimal] = None
    FechaConversion: Optional[datetime] = None
    CodigoVerificacion: Optional[str] = None
    LoteId: Optional[str] = None
    FechaRegistro: Optional[datetime] = None
    FechaActualizacion: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)