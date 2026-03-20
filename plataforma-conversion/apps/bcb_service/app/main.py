import asyncio
import random
import logging
import mysql.connector
from datetime import datetime
from fastapi import FastAPI
import uvicorn

logging.basicConfig(filename='bcb_changes.log', level=logging.INFO, format='%(asctime)s - [BCB_AUDIT] - %(message)s')

app = FastAPI(title="Servicio BCB - Tipo de Cambio Dinámico")

# Configuración para persistir en la DB de ASFI
ASFI_DB_CONFIG = {
    "host": "localhost", "port": 3308, "user": "root", "password": "root123", "database": "asfi_central"
}

class ExchangeState:
    def __init__(self):
        self.oficial = 6.96
        self.actual = 6.96
        self.ultima_actualizacion = datetime.now()

state = ExchangeState()

async def background_rate_updater():
    while True:
        await asyncio.sleep(180)
        variacion = random.uniform(-0.9999, 0.9999)
        nuevo_valor = round(6.96 + variacion, 4)
        
        # PERSISTENCIA EN DB
        try:
            conn = mysql.connector.connect(**ASFI_DB_CONFIG)
            cursor = conn.cursor()
            # Desactivar registros anteriores e insertar el nuevo
            cursor.execute("UPDATE TiposCambio SET Activo = FALSE, FechaFin = %s WHERE Activo = TRUE", (datetime.now(),))
            cursor.execute("INSERT INTO TiposCambio (Valor, FechaInicio, Activo) VALUES (%s, %s, TRUE)", (nuevo_valor, datetime.now()))
            conn.commit()
            cursor.close()
            conn.close()
        except Exception as e:
            logging.error(f"Error persistiendo Tipo de Cambio: {e}")

        state.actual = nuevo_valor
        state.ultima_actualizacion = datetime.now()
        logging.info(f"TASA ACTUALIZADA: {nuevo_valor}")

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(background_rate_updater())

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

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8082)