import asyncio
import httpx
import os
import logging
import secrets
from fastapi import FastAPI, HTTPException
from datetime import datetime

# Auditoría de ASFI
logging.basicConfig(
    filename='asfi_operations.log',
    level=logging.INFO,
    format='%(asctime)s - [ASFI_CENTRAL] - %(message)s'
)

app = FastAPI(title="ASFI - Orquestador de Conversión")

BCB_URL = os.getenv("BCB_API_URL", "http://bcb_service:8082")
BANK_URL = os.getenv("BANK_API_URL", "http://bank_service:8081")

# Lógica de Descifrado (Simulando la heterogeneidad criptográfica)
def descifrar_saldo(saldo_cifrado: float, banco_id: int) -> float:
    """
    Aquí aplicarás las funciones de src/infrastructure/security/
    según el ID del banco (César, AES, RSA, etc.)
    """
    # Por ahora, devolvemos el valor para procesar; aquí conectarás tus algoritmos
    return float(saldo_cifrado)

async def procesar_banco_paralelo(client: httpx.AsyncClient, banco_id: int, tasa: float):
    """Tarea individual por banco para el barrido"""
    try:
        # 1. Obtener cuentas del banco (lotes de 1000)
       
        res = await client.get(f"{BANK_URL}/api/cuentas/{banco_id}", timeout=10.0)
        if res.status_code != 200:
            return {"banco": banco_id, "status": "Error", "error": "Banco no responde"}

        cuentas = res.json()
        procesadas = []

        for c in cuentas:
            # 2. Descifrar saldo USD
            saldo_descifrado = descifrar_saldo(c['SaldoUSD'], banco_id)
            
            # 3. Convertir a Bs
            saldo_bs = round(saldo_descifrado * tasa, 4)
            
            # 4. Generar Código de Verificación Hexadecimal (8 caracteres)
            cod_verif = secrets.token_hex(4).upper()
            
            procesadas.append({
                "CuentaId": c['CuentaId'],
                "SaldoBs": saldo_bs,
                "CodigoVerificacion": cod_verif,
                "FechaConversion": datetime.now().isoformat()
            })

        # 5. Devolver al banco para actualización masiva
        await client.post(f"{BANK_URL}/api/actualizar-saldos", json=procesadas)
        
        logging.info(f"Barrido exitoso Banco {banco_id}: {len(procesadas)} cuentas.")
        return {"banco": banco_id, "status": "Completado", "registros": len(procesadas)}

    except Exception as e:
        logging.error(f"Falla en barrido Banco {banco_id}: {str(e)}")
        return {"banco": banco_id, "status": "Error", "error": str(e)}

@app.post("/api/iniciar-proceso-conversion")
async def iniciar_conversion():
    start_time = datetime.now()
    
    async with httpx.AsyncClient() as client:
        # 1. Obtener tasa del BCB
        try:
            res_bcb = await client.get(f"{BCB_URL}/api/tipo-cambio")
            tasa_actual = res_bcb.json()["data"]["valor_actual"]
        except:
            raise HTTPException(status_code=500, detail="BCB fuera de línea")

        # 2. BARRIDO PARALELO (Optimización de tiempo requerida)
        # Se lanzan las peticiones a los 14 bancos simultáneamente
        tareas = [procesar_banco_paralelo(client, i, tasa_actual) for i in range(1, 15)]
        resultados = await asyncio.gather(*tareas)

    end_time = datetime.now()
    duracion = (end_time - start_time).total_seconds()

    return {
        "resumen": "Proceso de conversión masiva finalizado",
        "duracion_segundos": duracion,
        "tasa_aplicada": tasa_actual,
        "detalle_por_entidad": resultados
    }