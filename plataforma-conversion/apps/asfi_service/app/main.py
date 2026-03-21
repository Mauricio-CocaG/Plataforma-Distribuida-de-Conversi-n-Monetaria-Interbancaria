import asyncio
import httpx
import logging
import uuid
import mysql.connector
from fastapi import FastAPI, HTTPException
from datetime import datetime

logging.basicConfig(
    filename='asfi_operations.log',
    level=logging.INFO,
    format='%(asctime)s - [ASFI_CENTRAL] - %(message)s'
)

app = FastAPI(title="ASFI - Orquestador Central PRO")

ASFI_DB_CONFIG = {
    "host": "localhost",
    "port": 3308,
    "user": "root",
    "password": "root123",
    "database": "asfi_central"
}

BCB_URL = "http://localhost:8082"
KEY_URL = "http://localhost:8003"

# 🔥 MAPEO DE BANCOS (puertos)
BANKS = {
    3: "http://localhost:8001",  # BNB
    # luego agregas los otros
}

estado_control = {
    "status": "IDLE",
    "ultima_ejecucion": None,
    "ultimo_lote_id": None,
    "progreso": {}
}

# =========================
# DB HELPER
# =========================
def guardar_lote(datos, tasa):
    try:
        conn = mysql.connector.connect(**ASFI_DB_CONFIG)
        cursor = conn.cursor()

        for d in datos:
            cursor.execute("""
                INSERT INTO Cuentas 
                (NoCuenta, IdBanco, SaldoUSD, SaldoBs, FechaConversion, CodigoVerificacion)
                VALUES (%s,%s,%s,%s,%s,%s)
            """, (
                d["NoCuenta"],
                d["IdBanco"],
                d["SaldoUSD"],
                d["SaldoBs"],
                d["FechaConversion"],
                d["Codigo"]
            ))

        conn.commit()

    except Exception as e:
        logging.error(f"DB ERROR: {e}")

    finally:
        cursor.close()
        conn.close()


# =========================
# PROCESAR BANCO
# =========================
async def procesar_banco(client, banco_id, tasa, lote_id):

    if banco_id not in BANKS:
        return {"banco": banco_id, "status": "NO_IMPLEMENTADO"}

    url = BANKS[banco_id]

    try:
        # 1. obtener cuentas cifradas
        res = await client.get(f"{url}/cuentas")
        cuentas = res.json()

        procesadas = []

        for c in cuentas:

            # 2. descifrar (KEY SERVICE)
            dec = await client.post(f"{KEY_URL}/decrypt", json={
                "BancoId": banco_id,
                "SaldoUSD": c["SaldoUSD"]
            })

            saldo = float(dec.json()["saldo"])

            # 3. convertir
            saldo_bs = round(saldo * tasa, 4)

            codigo = uuid.uuid4().hex[:8].upper()

            now = datetime.now()

            # 4. actualizar banco
            await client.post(f"{url}/actualizar", json={
                "CuentaId": c["CuentaId"],
                "SaldoBs": saldo_bs,
                "FechaConversion": str(now),
                "Codigo": codigo
            })

            procesadas.append({
                "NoCuenta": c["NroCuenta"],
                "IdBanco": banco_id,
                "SaldoUSD": saldo,
                "SaldoBs": saldo_bs,
                "Codigo": codigo,
                "FechaConversion": now
            })

        # 5. guardar en ASFI
        guardar_lote(procesadas, tasa)

        estado_control["progreso"][banco_id] = len(procesadas)

        return {
            "banco": banco_id,
            "procesadas": len(procesadas)
        }

    except Exception as e:
        logging.error(f"ERROR BANCO {banco_id}: {e}")
        return {"banco": banco_id, "error": str(e)}


# =========================
# ENDPOINT PRINCIPAL
# =========================
@app.post("/api/iniciar-barrido")
async def iniciar_barrido():

    if estado_control["status"] == "RUNNING":
        raise HTTPException(400, "Ya en ejecución")

    inicio = datetime.now()
    lote_id = uuid.uuid4().hex[:8]

    estado_control["status"] = "RUNNING"
    estado_control["ultimo_lote_id"] = lote_id
    estado_control["progreso"] = {}

    async with httpx.AsyncClient(timeout=30) as client:

        # 🔥 obtener tipo de cambio
        try:
            res = await client.get(f"{BCB_URL}/api/tipo-cambio")
            tasa = res.json()["data"]["valor_actual"]
        except:
            estado_control["status"] = "ERROR_BCB"
            raise HTTPException(500, "BCB no responde")

        # ⚡ PARALELISMO REAL
        tareas = [
            procesar_banco(client, banco_id, tasa, lote_id)
            for banco_id in range(1, 15)
        ]

        resultados = await asyncio.gather(*tareas)

    estado_control["status"] = "FINISHED"
    estado_control["ultima_ejecucion"] = datetime.now().isoformat()

    duracion = (datetime.now() - inicio).total_seconds()

    logging.info(f"LOTE {lote_id} COMPLETADO en {duracion}s")

    return {
        "lote_id": lote_id,
        "tasa": tasa,
        "duracion": duracion,
        "resultados": resultados
    }


# =========================
# ESTADO
# =========================
@app.get("/api/estado")
def estado():
    return estado_control