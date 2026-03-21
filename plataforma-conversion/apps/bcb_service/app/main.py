import asyncio
import random
import logging
import mysql.connector
from datetime import datetime
from fastapi import FastAPI
import uvicorn

# =========================
# LOGGING (AUDITORÍA REAL)
# =========================
logging.basicConfig(
    filename='bcb_changes.log',
    level=logging.INFO,
    format='%(asctime)s - [BCB_AUDIT] - %(message)s'
)

app = FastAPI(title="Servicio BCB - Tipo de Cambio Dinámico")

# =========================
# CONFIG DB (ASFI CENTRAL)
# =========================
ASFI_DB_CONFIG = {
    "host": "localhost",
    "port": 3308,
    "user": "root",
    "password": "root123",
    "database": "asfi_central"
}

# =========================
# ESTADO EN MEMORIA
# =========================
class ExchangeState:
    def __init__(self):
        self.oficial = 6.96
        self.actual = 6.96
        self.ultima_actualizacion = datetime.now()

state = ExchangeState()

# =========================
# FUNCIÓN SEGURA DB
# =========================
def guardar_tipo_cambio(valor):
    try:
        conn = mysql.connector.connect(**ASFI_DB_CONFIG)
        cursor = conn.cursor()

        # cerrar anterior
        cursor.execute("""
            UPDATE TiposCambio 
            SET Activo = FALSE, FechaFin = %s 
            WHERE Activo = TRUE
        """, (datetime.now(),))

        # insertar nuevo
        cursor.execute("""
            INSERT INTO TiposCambio (Valor, FechaInicio, Activo)
            VALUES (%s, %s, TRUE)
        """, (valor, datetime.now()))

        conn.commit()

    except Exception as e:
        logging.error(f"ERROR DB TipoCambio: {e}")

    finally:
        cursor.close()
        conn.close()


# =========================
# BACKGROUND TASK (cada 3 min)
# =========================
async def background_rate_updater():
    while True:
        await asyncio.sleep(180)

        variacion = random.uniform(-0.9999, 0.9999)
        nuevo_valor = round(state.oficial + variacion, 4)

        # Persistir
        guardar_tipo_cambio(nuevo_valor)

        # actualizar memoria
        state.actual = nuevo_valor
        state.ultima_actualizacion = datetime.now()

        logging.info(f"TASA ACTUALIZADA: {nuevo_valor}")


# =========================
# STARTUP
# =========================
@app.on_event("startup")
async def startup_event():
    asyncio.create_task(background_rate_updater())


# =========================
# ENDPOINT PRINCIPAL
# =========================
@app.get("/api/tipo-cambio")
async def get_tipo_cambio():
    return {
        "status": "success",
        "data": {
            "oficial_base": state.oficial,
            "valor_actual": state.actual,
            "ultima_actualizacion": state.ultima_actualizacion.isoformat()
        }
    }


# =========================
# ENDPOINT HISTÓRICO (PRO)
# =========================
@app.get("/api/tipo-cambio/historico")
def historico():
    try:
        conn = mysql.connector.connect(**ASFI_DB_CONFIG)
        cursor = conn.cursor(dictionary=True)

        cursor.execute("""
            SELECT Valor, FechaInicio, FechaFin
            FROM TiposCambio
            ORDER BY FechaInicio DESC
            LIMIT 10
        """)

        data = cursor.fetchall()

        return {"status": "success", "data": data}

    except Exception as e:
        return {"status": "error", "msg": str(e)}

    finally:
        cursor.close()
        conn.close()


# =========================
# MAIN
# =========================
if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8082, reload=True)