import asyncio
import httpx
import os
import logging
import secrets
import uuid
from fastapi import FastAPI, HTTPException
from datetime import datetime
from typing import List, Optional

# Configuración de Logs
logging.basicConfig(
    filename='asfi_operations.log',
    level=logging.INFO,
    format='%(asctime)s - [ASFI_CENTRAL] - %(message)s'
)

app = FastAPI(title="ASFI - Orquestador Central Corregido")

# URLs y Configuración
BCB_URL = os.getenv("BCB_API_URL", "http://bcb_service:8082")
BANK_URL = os.getenv("BANK_API_URL", "http://bank_service:8081")

# Variable global para rastrear el estado del barrido
estado_control = {
    "status": "IDLE",
    "ultima_ejecucion": None,
    "ultimo_lote_id": None,
    "progreso": {}
}

def descifrar_saldo(saldo_cifrado: float, banco_id: int) -> float:
    """Lógica de descifrado heterogéneo (Simulado)"""
    # Aquí conectarás con src/infrastructure/security/
    return float(saldo_cifrado)

async def procesar_lote_banco(client: httpx.AsyncClient, banco_id: int, tasa: float, lote_id: str):
    """Procesa un banco usando paginación de 1000 en 1000"""
    page = 0
    limit = 1000
    total_banco = 0
    
    try:
        while True:
            # 1. Consumir página de 1000 cuentas (Requerimiento Trello)
            params = {"page": page, "limit": limit}
            res = await client.get(f"{BANK_URL}/api/cuentas/{banco_id}", params=params, timeout=15.0)
            
            if res.status_code != 200:
                break
                
            cuentas = res.json()
            if not cuentas: # Si no hay más datos, termina el bucle
                break

            procesadas_para_banco = []
            for c in cuentas:
                saldo_descifrado = descifrar_saldo(c['SaldoUSD'], banco_id)
                saldo_bs = round(saldo_descifrado * tasa, 4)
                cod_verif = secrets.token_hex(4).upper() # 8 caracteres hex
                
                # Estructura para devolver al banco y para tu DB Central
                datos_conversion = {
                    "CuentaId": c['CuentaId'],
                    "CI": c['Identificacion'],
                    "NoCuenta": c['NroCuenta'],
                    "SaldoUSD_Original": saldo_descifrado,
                    "SaldoBs": saldo_bs,
                    "CodigoVerificacion": cod_verif,
                    "LoteId": lote_id,
                    "FechaConversion": datetime.now().isoformat()
                }
                procesadas_para_banco.append(datos_conversion)
                
                # NOTA: Aquí deberías ejecutar el INSERT a tu DB 'asfi_central' 
                # usando SQLAlchemy para las tablas Cuentas y LogsAuditoria

            # 2. Enviar resultados de vuelta al banco (POST /actualizar-lote)
            await client.post(f"{BANK_URL}/api/actualizar-lote", json=procesadas_para_banco)
            
            total_banco += len(procesadas_para_banco)
            page += 1
            
        estado_control["progreso"][f"banco_{banco_id}"] = f"Completado: {total_banco} registros"
        return {"banco": banco_id, "status": "OK", "total": total_banco}

    except Exception as e:
        logging.error(f"Error en banco {banco_id}: {str(e)}")
        return {"banco": banco_id, "status": "Error", "detalle": str(e)}

@app.get("/api/estado-barrido")
async def get_estado_barrido():
    """Consulta el estado del proceso (Requerimiento Trello)"""
    return estado_control

@app.post("/api/iniciar-barrido")
async def iniciar_barrido():
    """Inicia el procesamiento paralelo (Requerimiento Trello)"""
    if estado_control["status"] == "RUNNING":
        raise HTTPException(status_code=400, detail="Ya hay un barrido en curso")

    start_time = datetime.now()
    lote_id = str(uuid.uuid4())[:8] # Generamos un ID de lote corto
    
    estado_control["status"] = "RUNNING"
    estado_control["ultimo_lote_id"] = lote_id
    estado_control["progreso"] = {}

    async with httpx.AsyncClient() as client:
        # 1. Consultar BCB Service
        try:
            res_bcb = await client.get(f"{BCB_URL}/api/tipo-cambio")
            tasa_actual = res_bcb.json()["data"]["valor_actual"]
        except:
            estado_control["status"] = "ERROR_BCB"
            raise HTTPException(status_code=500, detail="No se pudo obtener el tipo de cambio")

        # 2. BARRIDO PARALELO usando asyncio.gather() para 14 bancos
        tareas = [procesar_lote_banco(client, i, tasa_actual, lote_id) for i in range(1, 15)]
        resultados = await asyncio.gather(*tareas)

    estado_control["status"] = "FINISHED"
    estado_control["ultima_ejecucion"] = datetime.now().isoformat()
    
    duracion = (datetime.now() - start_time).total_seconds()
    
    return {
        "lote_id": lote_id,
        "duracion_seg": duracion,
        "tasa_aplicada": tasa_actual,
        "resumen": resultados
    }