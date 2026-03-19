from datetime import datetime
from decimal import Decimal
from typing import Optional

from pydantic import BaseModel, ConfigDict


class CuentaBase(BaseModel):
    nro_identificacion: str
    nombres: str
    apellidos: str
    nro_cuenta: str
    id_banco: int
    saldo_usd: Decimal
    algoritmo_usado: str = "Atbash"


class CuentaCreate(CuentaBase):
    pass


class CuentaResponse(CuentaBase):
    id: int
    saldo_bs: Optional[Decimal] = None
    tipo_cambio_aplicado: Optional[Decimal] = None
    fecha_conversion: Optional[datetime] = None
    codigo_verificacion: Optional[str] = None
    lote_id: Optional[str] = None
    datos_cifrados: Optional[str] = None
    fecha_registro: Optional[datetime] = None
    fecha_actualizacion: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)


class ConversionResultUpdate(BaseModel):
    saldo_bs: Decimal
    tipo_cambio_aplicado: Decimal
    codigo_verificacion: str
    lote_id: str
    fecha_conversion: Optional[datetime] = None