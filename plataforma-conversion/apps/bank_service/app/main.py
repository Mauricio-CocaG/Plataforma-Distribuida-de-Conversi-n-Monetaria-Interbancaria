import os
import logging
import mysql.connector
import psycopg2
import oracledb
from pymongo import MongoClient
import redis
from neo4j import GraphDatabase  # Importación para el Banco 14
from fastapi import FastAPI, HTTPException
from typing import List

# Configuración de Logs
logging.basicConfig(
    filename='bank_service.log', 
    level=logging.INFO, 
    format='%(asctime)s - [BANK_ADAPTER] - %(message)s'
)

app = FastAPI(title="Bank Service - Adaptador Oficial ASFI")

# ============================================================
# CONFIGURACIÓN EXACTA (Sincronizada con Script de Carga)
# ============================================================
DB_CONFIG = {
    1: { 'type': 'mysql', 'host': 'localhost', 'port': 3307, 'database': 'banco_union',
         'user': 'root', 'password': 'root123', 'table': 'Cuentas' },
    2: { 'type': 'postgresql', 'host': 'localhost', 'port': 5432, 'database': 'banco_mercantil',
         'user': 'root', 'password': 'root123', 'table': 'cuentas' },
    3: { 'type': 'mongodb', 'host': 'localhost', 'port': 27017, 'database': 'banco_bnb',
         'user': 'root', 'password': 'root123', 'collection': 'Cuentas' },
    4: { 'type': 'redis', 'host': 'localhost', 'port': 6379, 'password': 'root123' },
    5: { 'type': 'oracle', 'host': 'localhost', 'port': 1521, 'service': 'XEPDB1',
         'user': 'BANCO_BISA', 'password': 'root123', 'table': 'CUENTAS' },
    6: { 'type': 'mongodb', 'host': 'localhost', 'port': 27018, 'database': 'banco_ganadero',
         'user': 'root', 'password': 'root123', 'collection': 'Cuentas' },
    7: { 'type': 'mongodb', 'host': 'localhost', 'port': 27019, 'database': 'banco_economico',
         'user': 'root', 'password': 'root123', 'collection': 'Cuentas' },
    8: { 'type': 'mongodb', 'host': 'localhost', 'port': 27020, 'database': 'banco_prodem',
         'user': 'root', 'password': 'root123', 'collection': 'Cuentas' },
    9: { 'type': 'mongodb', 'host': 'localhost', 'port': 27021, 'database': 'banco_solidario',
         'user': 'root', 'password': 'root123', 'collection': 'Cuentas' },
    10: { 'type': 'mongodb', 'host': 'localhost', 'port': 27022, 'database': 'banco_fortaleza',
          'user': 'root', 'password': 'root123', 'collection': 'Cuentas' },
    11: { 'type': 'mongodb', 'host': 'localhost', 'port': 27023, 'database': 'banco_fie',
          'user': 'root', 'password': 'root123', 'collection': 'Cuentas' },
    12: { 'type': 'mongodb', 'host': 'localhost', 'port': 27024, 'database': 'banco_pyme',
          'user': 'root', 'password': 'root123', 'collection': 'Cuentas' },
    13: { 'type': 'mongodb', 'host': 'localhost', 'port': 27025, 'database': 'banco_bdp',
          'user': 'root', 'password': 'root123', 'collection': 'Cuentas' },
    14: { 'type': 'neo4j', 'host': 'localhost', 'port': 7687,
          'database': 'banco_argentina', 'user': 'neo4j', 'password': 'root1234' },
}

# ============================================================
# LÓGICA DE LECTURA (GET)
# ============================================================
def fetch_from_bank(banco_id: int, page: int, limit: int):
    config = DB_CONFIG.get(banco_id)
    if not config: return None
    offset = page * limit
    data = []

    try:
        # ... [Lógica de MySQL, Postgres, Oracle, MongoDB, Redis se mantiene igual] ...
        if config['type'] == 'mysql':
            conn = mysql.connector.connect(host=config['host'], port=config['port'], user=config['user'], password=config['password'], database=config['database'])
            cursor = conn.cursor(dictionary=True)
            cursor.execute(f"SELECT NroIdentificacion as Identificacion, NroCuenta, SaldoUSD, Nombres, Apellidos FROM {config['table']} LIMIT %s OFFSET %s", (limit, offset))
            data = cursor.fetchall()
            conn.close()

        elif config['type'] == 'postgresql':
            conn = psycopg2.connect(host=config['host'], port=config['port'], user=config['user'], password=config['password'], dbname=config['database'])
            conn.set_client_encoding('LATIN1')
            cursor = conn.cursor()
            cursor.execute(f"SELECT nroidentificacion, nrocuenta, saldousd, nombres, apellidos FROM {config['table']} LIMIT %s OFFSET %s", (limit, offset))
            data = [{"Identificacion": r[0], "NroCuenta": r[1], "SaldoUSD": float(r[2]), "Nombres": r[3], "Apellidos": r[4]} for r in cursor.fetchall()]
            conn.close()

        elif config['type'] == 'oracle':
            dsn = oracledb.makedsn(config['host'], config['port'], service_name=config['service'])
            conn = oracledb.connect(user=config['user'], password=config['password'], dsn=dsn)
            cursor = conn.cursor()
            cursor.execute(f"SELECT NroIdentificacion, NroCuenta, SaldoUSD, Nombres, Apellidos FROM {config['table']} OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY")
            data = [{"Identificacion": r[0], "NroCuenta": r[1], "SaldoUSD": float(r[2]), "Nombres": r[3], "Apellidos": r[4]} for r in cursor.fetchall()]
            conn.close()

        elif config['type'] == 'mongodb':
            client = MongoClient(f"mongodb://{config['user']}:{config['password']}@{config['host']}:{config['port']}/")
            db = client[config['database']]
            cursor = db[config['collection']].find({"SaldoUSD": {"$gt": 0}}).skip(offset).limit(limit)
            data = [{"Identificacion": r["NroIdentificacion"], "NroCuenta": r["NroCuenta"], "SaldoUSD": float(r["SaldoUSD"]), "Nombres": r["Nombres"], "Apellidos": r["Apellidos"]} for r in cursor]
            client.close()

        elif config['type'] == 'redis':
            r_client = redis.Redis(host=config['host'], port=config['port'], password=config['password'], decode_responses=True)
            keys = r_client.keys("cuenta:*")
            for k in keys[offset:offset+limit]:
                val = r_client.hgetall(k)
                data.append({"Identificacion": val['id'], "NroCuenta": k.split(":")[1], "SaldoUSD": float(val['saldo']), "Nombres": val['nombres'], "Apellidos": val['apellidos']})
            r_client.close()

        # --- BANCO 14: NEO4J ---
        elif config['type'] == 'neo4j':
            uri = f"bolt://{config['host']}:{config['port']}"
            driver = GraphDatabase.driver(uri, auth=(config['user'], config['password']))
            with driver.session() as session:
                # Buscamos nodos de tipo Cuenta con saldo > 0
                query = """
                MATCH (c:Cuenta) 
                WHERE c.SaldoUSD > 0 
                RETURN c.NroIdentificacion as id, c.NroCuenta as nro, c.SaldoUSD as saldo, c.Nombres as nom, c.Apellidos as ape 
                SKIP $skip LIMIT $limit
                """
                result = session.run(query, skip=offset, limit=limit)
                data = [{"Identificacion": r["id"], "NroCuenta": r["nro"], "SaldoUSD": float(r["saldo"]), "Nombres": r["nom"], "Apellidos": r["ape"]} for r in result]
            driver.close()

        return data
    except Exception as e:
        logging.error(f"Error READ Banco {banco_id}: {e}")
        return []

# ============================================================
# LÓGICA DE ACTUALIZACIÓN (POST)
# ============================================================
def update_bank_status(banco_id: int, lotes: List[dict]):
    config = DB_CONFIG.get(banco_id)
    if not config: return False

    try:
        # ... [Lógica de MySQL, Postgres, Oracle, MongoDB, Redis se mantiene igual] ...
        if config['type'] == 'mysql':
            conn = mysql.connector.connect(host=config['host'], port=config['port'], user=config['user'], password=config['password'], database=config['database'])
            cursor = conn.cursor()
            for r in lotes:
                cursor.execute(f"UPDATE {config['table']} SET SaldoUSD = 0, AlgoritmoUsado = 'PROCESADO' WHERE NroCuenta = %s", (r['NoCuenta'],))
            conn.commit(); conn.close()

        elif config['type'] == 'postgresql':
            conn = psycopg2.connect(host=config['host'], port=config['port'], user=config['user'], password=config['password'], dbname=config['database'])
            cursor = conn.cursor()
            for r in lotes:
                cursor.execute(f"UPDATE {config['table']} SET saldousd = 0, algoritmousado = 'PROCESADO' WHERE nrocuenta = %s", (r['NoCuenta'],))
            conn.commit(); conn.close()

        elif config['type'] == 'oracle':
            dsn = oracledb.makedsn(config['host'], config['port'], service_name=config['service'])
            conn = oracledb.connect(user=config['user'], password=config['password'], dsn=dsn)
            cursor = conn.cursor()
            for r in lotes:
                cursor.execute(f"UPDATE {config['table']} SET SaldoUSD = 0, AlgoritmoUsado = 'PROCESADO' WHERE NroCuenta = :1", (r['NoCuenta'],))
            conn.commit(); conn.close()

        elif config['type'] == 'mongodb':
            client = MongoClient(f"mongodb://{config['user']}:{config['password']}@{config['host']}:{config['port']}/")
            db = client[config['database']]
            for r in lotes:
                db[config['collection']].update_one({"NroCuenta": r['NoCuenta']}, {"$set": {"SaldoUSD": 0, "AlgoritmoUsado": "PROCESADO"}})
            client.close()

        elif config['type'] == 'redis':
            r_client = redis.Redis(host=config['host'], port=config['port'], password=config['password'], decode_responses=True)
            for r in lotes:
                r_client.hset(f"cuenta:{r['NoCuenta']}", mapping={"saldo": 0, "algoritmo": "PROCESADO"})
            r_client.close()

        # --- BANCO 14: NEO4J ---
        elif config['type'] == 'neo4j':
            uri = f"bolt://{config['host']}:{config['port']}"
            driver = GraphDatabase.driver(uri, auth=(config['user'], config['password']))
            with driver.session() as session:
                query = """
                UNWIND $lotes as r
                MATCH (c:Cuenta {NroCuenta: r.NoCuenta})
                SET c.SaldoUSD = 0, c.AlgoritmoUsado = 'PROCESADO'
                """
                session.run(query, lotes=lotes)
            driver.close()

        return True
    except Exception as e:
        logging.error(f"Error UPDATE Banco {banco_id}: {e}")
        return False

# ============================================================
# ENDPOINTS
# ============================================================
@app.get("/api/cuentas/{banco_id}")
async def get_cuentas(banco_id: int, page: int = 0, limit: int = 1000):
    return fetch_from_bank(banco_id, page, limit)

@app.post("/api/actualizar-lote")
async def post_actualizar(payload: List[dict]):
    if not payload: return {"status": "void"}
    banco_id = payload[0].get('IdBanco')
    if update_bank_status(banco_id, payload):
        return {"status": "success"}
    raise HTTPException(status_code=500)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8081)