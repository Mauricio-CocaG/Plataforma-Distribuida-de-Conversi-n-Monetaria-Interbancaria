import asyncio
import random
import logging
from datetime import datetime
from fastapi import FastAPI
import uvicorn

# 1. Configuración del Log de Auditoría (Requerimiento de la práctica)
logging.basicConfig(
    filename='bcb_changes.log',
    level=logging.INFO,
    format='%(asctime)s - [BCB_AUDIT] - %(message)s'
)

app = FastAPI(title="Servicio BCB - Tipo de Cambio Dinámico")

# Estado persistente en memoria
class ExchangeState:
    def __init__(self):
        self.oficial = 6.96
        self.actual = 6.96
        self.ultima_actualizacion = datetime.now()

state = ExchangeState()

async def background_rate_updater():
    """Actualiza el tipo de cambio cada 180 segundos (3 min)"""
    while True:
        await asyncio.sleep(180)
        # Variación aleatoria entre -0.9999 y 0.9999
        variacion = random.uniform(-0.9999, 0.9999)
        nuevo_valor = round(6.96 + variacion, 4)
        
        old_value = state.actual
        state.actual = nuevo_valor
        state.ultima_actualizacion = datetime.now()
        
        # Guardar en log de auditoría
        logging.info(f"TASA ACTUALIZADA: {old_value} -> {nuevo_valor} (Variación: {variacion:.4f})")
        print(f" LOG: Tipo de cambio actualizado a {nuevo_valor}")

@app.on_event("startup")
async def startup_event():
    # Iniciar la tarea asíncrona al arrancar
    asyncio.create_task(background_rate_updater())

@app.get("/api/tipo-cambio")
async def get_tipo_cambio():
    return {
        "status": "success",
        "data": {
            "oficial_base": state.oficial,
            "valor_actual": state.actual,
            "variacion": round(state.actual - state.oficial, 4),
            "ultima_actualizacion": state.ultima_actualizacion.isoformat()
        }
    }

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8082)