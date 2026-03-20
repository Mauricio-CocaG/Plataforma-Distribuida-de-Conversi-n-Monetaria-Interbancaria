import asyncio
import httpx
import os
import logging
import secrets
import uuid
import mysql.connector
from fastapi import FastAPI, HTTPException
from datetime import datetime

# Configuración de Logs
logging.basicConfig(filename='asfi_operations.log', level=logging.INFO, format='%(asctime)s - [ASFI_CENTRAL] - %(message)s')

app = FastAPI(title="ASFI - Orquestador Central Corregido")

ASFI_DB_CONFIG = {
    "host": "localhost",
    "port": 3308,
    "user": "root",
    "password": "root123",
    "database": "asfi_central"
}

BCB_URL = os.getenv("BCB_API_URL", "http://bcb_service:8082")
BANK_URL = os.getenv("BANK_API_URL", "http://bank_service:8081")

estado_control = {"status": "IDLE", "ultima_ejecucion": None, "ultimo_lote_id": None, "progreso": {}}

@app.on_event("startup")
async def seed_bancos():
    """Llenado automático de la tabla Bancos al iniciar para evitar errores de FK"""
    bancos = [
        (1, 'Banco Union', 'Cesar'), (2, 'Banco Mercantil', 'Atbash'), (3, 'Banco BNB', 'Vigenere'),
        (4, 'Banco BCP', 'Playfair'), (5, 'Banco BISA', 'Hill'), (6, 'Banco Ganadero', 'DES'),
        (7, 'Banco Economico', '3DES'), (8, 'Banco Prodem', 'Blowfish'), (9, 'Banco Solidario', 'Twofish'),
        (10, 'Banco Fortaleza', 'AES'), (11, 'Banco FIE', 'RSA'), (12, 'Banco PYME', 'ElGamal'),
        (13, 'Banco BDP', 'ECC'), (14, 'Banco Argentina', 'ChaCha20')
    ]
    try:
        conn = mysql.connector.connect(**ASFI_DB_CONFIG)
        cursor = conn.cursor()
        cursor.executemany("INSERT IGNORE INTO Bancos (Id, Nombre, AlgoritmoEncriptacion) VALUES (%s, %s, %s)", bancos)
        conn.commit()
        cursor.close()
        conn.close()
        logging.info("Tabla Bancos verificada/inicializada.")
    except Exception as e:
        logging.error(f"Error en seed de bancos: {e}")

async def guardar_en_db_asfi(datos_lote, tasa):
    try:
        conn = mysql.connector.connect(**ASFI_DB_CONFIG)
        cursor = conn.cursor()
        for d in datos_lote:
            # 1. Cuentas (Usando INSERT IGNORE para evitar duplicados si se reintenta el lote)
            sql_cuenta = """INSERT INTO Cuentas (CI, Nombres, Apellidos, NoCuenta, IdBanco, SaldoBs, LoteId, CodigoVerificacion, FechaConversion)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON DUPLICATE KEY UPDATE SaldoBs = VALUES(SaldoBs), LoteId = VALUES(LoteId)"""
            cursor.execute(sql_cuenta, (d['CI'], d.get('Nombres', 'N/A'), d.get('Apellidos', 'N/A'), d['NoCuenta'], d['IdBanco'], d['SaldoBs'], d['LoteId'], d['CodigoVerificacion'], d['FechaConversion']))
            
            # 2. LogsAuditoria
            cuenta_id = cursor.lastrowid
            sql_log = """INSERT INTO LogsAuditoria (CuentaId, BancoId, NoCuenta, CI, MontoUSD_Original, MontoBs_Resultante, TipoCambioAplicado, LoteId, CodigoVerificacion, FechaConversion)
                         VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
            cursor.execute(sql_log, (cuenta_id, d['IdBanco'], d['NoCuenta'], d['CI'], d['SaldoUSD_Original'], d['SaldoBs'], tasa, d['LoteId'], d['CodigoVerificacion'], d['FechaConversion']))
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        logging.error(f"Error en persistencia ASFI: {e}")

# ... resto de las funciones de barrido iguales ...


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