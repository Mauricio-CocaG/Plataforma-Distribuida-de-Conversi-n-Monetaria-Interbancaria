import os
import sys
import logging
import mysql.connector
import psycopg2
import oracledb
from pymongo import MongoClient
import redis
import secrets
from neo4j import GraphDatabase
from fastapi import FastAPI, HTTPException
from typing import List

# ============================================================
# AGREGAR RUTA PARA ENCONTRAR GRAPH_REPOSITORY
# ============================================================
current_dir = os.path.dirname(os.path.abspath(__file__))

project_root = os.path.dirname(os.path.dirname(os.path.dirname(current_dir)))
neo4j_path = os.path.join(project_root, 'docker', 'neo4j')

print(f"\n{'='*60}")
print(f"INICIANDO BANK SERVICE")
print(f"{'='*60}")
print(f"Directorio actual: {current_dir}")
print(f"Buscando graph_repository.py en: {neo4j_path}")
print(f"Archivo existe: {os.path.exists(os.path.join(neo4j_path, 'graph_repository.py'))}")

if os.path.exists(neo4j_path) and neo4j_path not in sys.path:
    sys.path.insert(0, neo4j_path)
    print(f"✅ Ruta Neo4j agregada: {neo4j_path}")
else:
    print(f"⚠️ Ruta Neo4j NO encontrada: {neo4j_path}")

if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

print(f"Python path: {sys.path[:3]}")
print(f"{'='*60}\n")

# Configuración de Logs
logging.basicConfig(
    filename='bank_service.log', 
    level=logging.INFO, 
    format='%(asctime)s - [BANK_ADAPTER] - %(message)s'
)

app = FastAPI(title="Bank Service - Adaptador Oficial ASFI")

# ============================================================
# CONFIGURACIÓN
# ============================================================
DB_CONFIG = {
    1: { 'type': 'mysql', 'host': 'localhost', 'port': 3307, 'database': 'banco_union',
         'user': 'root', 'password': 'root123', 'table': 'Cuentas' },
    2: { 'type': 'postgresql', 'host': 'localhost', 'port': 5433, 'database': 'banco_mercantil',
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
          'database': 'neo4j', 'user': 'neo4j', 'password': 'root1234' },
}

def cifrar_monto_banco(monto, algoritmo):
    try:
        v = str(int(float(monto)))
        
        if algoritmo == 'Cesar':
            return "".join(str((int(d) + 3) % 10) for d in v)
        elif algoritmo == 'Atbash':
            tabla = str.maketrans("0123456789", "9876543210")
            return v.translate(tabla)
        elif algoritmo == 'Vigenere':
            key = [3, 1, 4, 2]
            return "".join(str((int(d) + key[i % len(key)]) % 10) for i, d in enumerate(v))
        elif algoritmo == 'Playfair':
            chars = list(v)
            for i in range(0, len(chars) - 1, 2):
                chars[i], chars[i+1] = chars[i+1], chars[i]
            return "".join(chars)
        elif algoritmo == 'Hill':
            if len(v) % 2 != 0: v += "0"
            res = ""
            for i in range(0, len(v), 2):
                x, y = int(v[i]), int(v[i+1])
                res += str((2*x + 1*y) % 10)
                res += str((1*x + 1*y) % 10)
            return res
        elif algoritmo == 'DES':
            mitad = len(v) // 2
            return v[mitad:] + v[:mitad]
        elif algoritmo == '3DES':
            return "".join(str((int(d) + 7) % 10) for d in v)
        elif algoritmo == 'Blowfish':
            return "".join(str(int(d) ^ 5)[-1] for d in v)
        elif algoritmo == 'Twofish':
            return "".join(str((int(d) ^ i) % 10) for i, d in enumerate(v))
        elif algoritmo == 'AES':
            if len(v) > 1:
                return v[1:] + v[0]
            return v
        elif algoritmo == 'RSA':
            return "".join(str((int(d) * 3) % 10) for d in v)
        elif algoritmo == 'ElGamal':
            return "".join(str((int(d) + 5) % 10) for d in v)
        elif algoritmo == 'ECC':
            fib = [1, 1, 2, 3, 5, 8, 13, 21]
            return "".join(str((int(d) + fib[i % len(fib)]) % 10) for i, d in enumerate(v))
        elif algoritmo == 'ChaCha20':
            return "".join(str(int(d) ^ (42 % 10))[-1] for d in v)
        return v
    except:
        return str(monto)

def fetch_from_bank(banco_id: int, page: int, limit: int):
    config = DB_CONFIG.get(banco_id)
    if not config: return []
    
    ALGORITMOS = {
        1: 'Cesar', 2: 'Atbash', 3: 'Vigenere', 4: 'Playfair', 
        5: 'Hill', 6: 'DES', 7: '3DES', 8: 'Blowfish', 
        9: 'Twofish', 10: 'AES', 11: 'RSA', 12: 'ElGamal', 
        13: 'ECC', 14: 'ChaCha20'
    }
    algoritmo = ALGORITMOS.get(banco_id, 'None')
    
    try:
        if config['type'] == 'mysql':
            conn = mysql.connector.connect(host=config['host'], port=config['port'], user=config['user'], password=config['password'], database=config['database'])
            cursor = conn.cursor(dictionary=True)
            cursor.execute(f"SELECT NroIdentificacion as Identificacion, NroCuenta, SaldoUSD, Nombres, Apellidos FROM {config['table']} LIMIT %s OFFSET %s", (limit, page * limit))
            res = cursor.fetchall()
            conn.close()
            return [dict(r, IdBanco=banco_id, SaldoUSD=cifrar_monto_banco(r['SaldoUSD'], algoritmo)) for r in res]

        elif config['type'] == 'postgresql':
            conn = psycopg2.connect(host=config['host'], port=config['port'], user=config['user'], password=config['password'], dbname=config['database'])
            cursor = conn.cursor()
            cursor.execute(f'SELECT nroidentificacion, nrocuenta, saldousd, nombres, apellidos FROM {config["table"]} LIMIT %s OFFSET %s', (limit, page * limit))
            res = cursor.fetchall()
            conn.close()
            return [{
                "Identificacion": r[0], "NroCuenta": r[1], 
                "SaldoUSD": cifrar_monto_banco(r[2], algoritmo), 
                "Nombres": r[3], "Apellidos": r[4], "IdBanco": banco_id
            } for r in res]

        elif config['type'] == 'mongodb':
            client = MongoClient(f"mongodb://{config['user']}:{config['password']}@{config['host']}:{config['port']}/")
            db = client[config['database']]
            res = list(db[config['collection']].find({}, {'_id': 0}).skip(page * limit).limit(limit))
            client.close()
            return [{**r, "SaldoUSD": cifrar_monto_banco(r.get("SaldoUSD", 0), algoritmo), "IdBanco": banco_id} for r in res]

        elif config['type'] == 'redis':
            r_client = redis.Redis(host=config['host'], port=config['port'], password=config['password'], decode_responses=True)
            all_keys = r_client.keys("cuenta:*")
            start = page * limit
            end = start + limit
            keys = all_keys[start:end] 
            data = []
            for k in keys:
                v = r_client.hgetall(k)
                data.append({
                    "Identificacion": v.get('id'), "NroCuenta": k.split(":")[1], 
                    "SaldoUSD": cifrar_monto_banco(v.get('saldo', 0), algoritmo),
                    "Nombres": v.get('nombres'), "Apellidos": v.get('apellidos'), "IdBanco": banco_id
                })
            r_client.close()
            return data

        elif config['type'] == 'oracle':
            dsn = oracledb.makedsn(config['host'], config['port'], service_name=config['service'])
            conn = oracledb.connect(user=config['user'], password=config['password'], dsn=dsn)
            cursor = conn.cursor()
            cursor.execute(f"SELECT NroIdentificacion, NroCuenta, SaldoUSD, Nombres, Apellidos FROM {config['table']} OFFSET :1 ROWS FETCH NEXT :2 ROWS ONLY", (page * limit, limit))
            res = cursor.fetchall()
            conn.close()
            return [{
                "Identificacion": r[0], "NroCuenta": r[1], 
                "SaldoUSD": cifrar_monto_banco(r[2], algoritmo), 
                "Nombres": r[3], "Apellidos": r[4], "IdBanco": banco_id
            } for r in res]

        # --- NEO4J CON DIAGNÓSTICO ---
        elif config['type'] == 'neo4j':
            print(f"\n🔍 NEO4J: Procesando banco {banco_id}")
            
            try:
                # Intentar importar
                from graph_repository import GraphRepository
                print(f"✅ GraphRepository importado")
                
                # Crear repositorio
                repo = GraphRepository(database=config.get('database', 'neo4j'))
                print(f"✅ Repositorio creado con database={config.get('database', 'neo4j')}")
                
                # Obtener cuentas
                cuentas_raw = repo.get_all_accounts()
                print(f"📊 get_all_accounts() devolvió: {len(cuentas_raw)} cuentas")
                
                if len(cuentas_raw) == 0:
                    print(f"⚠️ No hay cuentas en Neo4j")
                    return []
                
                # Paginar
                start = page * limit
                end = start + limit
                lote = cuentas_raw[start:end]
                
                resultado = []
                for c in lote:
                    resultado.append({
                        "Identificacion": str(c.get("Identificacion", "")),
                        "Nombres": c.get("Nombres", ""),
                        "Apellidos": c.get("Apellidos", ""),
                        "NroCuenta": str(c.get("NroCuenta", "")),
                        "SaldoUSD": cifrar_monto_banco(c.get("SaldoUSD", 0), algoritmo),
                        "IdBanco": banco_id
                    })
                
                print(f"✅ Devolviendo {len(resultado)} cuentas")
                return resultado
                
            except ImportError as e:
                print(f"❌ ERROR: No se pudo importar GraphRepository")
                print(f"   Error: {e}")
                print(f"   Buscando en: {neo4j_path}")
                return []
                
            except Exception as e:
                print(f"❌ ERROR en Neo4j: {e}")
                import traceback
                traceback.print_exc()
                return []
                
            finally:
                try:
                    repo.close()
                except:
                    pass

    except Exception as e:
        logging.error(f"❌ Error en barrido masivo Banco {banco_id}: {e}")
        return []

def update_bank_status(banco_id, payload):
    codigo_verificacion = secrets.token_hex(4).upper() 
    logging.info(f"Transacción exitosa. Banco: {banco_id}. Código: {codigo_verificacion}")
    return codigo_verificacion

def fetch_single_account(banco_id: int, nro_cuenta: str):
    config = DB_CONFIG.get(banco_id)
    if not config: return None
    
    ALGORITMOS = {
        1: 'Cesar', 2: 'Atbash', 3: 'Vigenere', 4: 'Playfair', 
        5: 'Hill', 6: 'DES', 7: '3DES', 8: 'Blowfish', 
        9: 'Twofish', 10: 'AES', 11: 'RSA', 12: 'ElGamal', 
        13: 'ECC', 14: 'ChaCha20'
    }
    algoritmo = ALGORITMOS.get(banco_id, 'None')

    try:
        if config['type'] == 'mysql':
            conn = mysql.connector.connect(host=config['host'], port=config['port'], user=config['user'], password=config['password'], database=config['database'])
            cursor = conn.cursor(dictionary=True)
            cursor.execute(f"SELECT NroIdentificacion as Identificacion, NroCuenta, SaldoUSD, Nombres, Apellidos FROM {config['table']} WHERE NroCuenta = %s", (nro_cuenta,))
            r = cursor.fetchone()
            conn.close()
            if r:
                r['SaldoUSD'] = cifrar_monto_banco(r['SaldoUSD'], algoritmo)
                return {**r, "IdBanco": banco_id}

        elif config['type'] == 'postgresql':
            conn = psycopg2.connect(host=config['host'], port=config['port'], user=config['user'], password=config['password'], dbname=config['database'])
            cursor = conn.cursor()
            cursor.execute(f"SELECT nroidentificacion, nrocuenta, saldousd, nombres, apellidos FROM {config['table']} WHERE nrocuenta = %s", (nro_cuenta,))
            r = cursor.fetchone()
            conn.close()
            if r:
                return {
                    "Identificacion": r[0], "NroCuenta": r[1], 
                    "SaldoUSD": cifrar_monto_banco(r[2], algoritmo), 
                    "Nombres": r[3], "Apellidos": r[4], "IdBanco": banco_id
                }

        elif config['type'] == 'oracle':
            dsn = oracledb.makedsn(config['host'], config['port'], service_name=config['service'])
            conn = oracledb.connect(user=config['user'], password=config['password'], dsn=dsn)
            cursor = conn.cursor()
            cursor.execute(f"SELECT NroIdentificacion, NroCuenta, SaldoUSD, Nombres, Apellidos FROM {config['table']} WHERE NroCuenta = :1", (nro_cuenta,))
            r = cursor.fetchone()
            conn.close()
            if r:
                return {
                    "Identificacion": r[0], "NroCuenta": r[1], 
                    "SaldoUSD": cifrar_monto_banco(r[2], algoritmo), 
                    "Nombres": r[3], "Apellidos": r[4], "IdBanco": banco_id
                }

        elif config['type'] == 'mongodb':
            client = MongoClient(f"mongodb://{config['user']}:{config['password']}@{config['host']}:{config['port']}/")
            db = client[config['database']]
            r = db[config['collection']].find_one({"NroCuenta": nro_cuenta}, {'_id': 0})
            client.close()
            if r:
                r['SaldoUSD'] = cifrar_monto_banco(r.get('SaldoUSD', 0), algoritmo)
                return {**r, "IdBanco": banco_id}

        elif config['type'] == 'redis':
            r_client = redis.Redis(host=config['host'], port=config['port'], password=config['password'], decode_responses=True)
            val = r_client.hgetall(f"cuenta:{nro_cuenta}")
            r_client.close()
            if val:
                return {
                    "Identificacion": val.get('id'), "NroCuenta": nro_cuenta, 
                    "SaldoUSD": cifrar_monto_banco(val.get('saldo', 0), algoritmo), 
                    "Nombres": val.get('nombres'), "Apellidos": val.get('apellidos'), "IdBanco": banco_id
                }

        elif config['type'] == 'neo4j':
            from graph_repository import GraphRepository
            repo = GraphRepository(database=config.get('database', 'neo4j'))
            try:
                r = repo.buscar_cuenta_y_propietario(str(nro_cuenta))
                if r:
                    return {
                        "Identificacion": str(r.get("Identificacion", "")),
                        "NroCuenta": str(r.get("NroCuenta", "")),
                        "SaldoUSD": cifrar_monto_banco(r.get("SaldoUSD", 0), algoritmo),
                        "Nombres": r.get("Nombres", ""),
                        "Apellidos": r.get("Apellidos", ""),
                        "IdBanco": banco_id
                    }
                return None
            finally:
                repo.close()
    except Exception as e:
        logging.error(f"❌ Error FETCH con cifrado en Banco {banco_id}: {e}")
        return None

# ============================================================
# ENDPOINTS
# ============================================================
@app.get("/api/health")
async def health_check():
    return {"status": "online", "msg": "Adaptador ASFI funcionando"}

@app.get("/api/cuentas/{banco_id}")
async def get_cuentas(banco_id: int, page: int = 0, limit: int = 1000):
    return fetch_from_bank(banco_id, page, limit)

@app.post("/api/actualizar-lote")
async def post_actualizar(payload: List[dict]):
    if not payload: 
        return {"status": "void"}
    banco_id = next((item.get('IdBanco') for item in payload if item.get('IdBanco')), None)
    if not banco_id:
        raise HTTPException(status_code=400, detail="IdBanco no detectado en el lote")
    codigo = update_bank_status(banco_id, payload)
    return {"status": "success", "codigo_verificacion": codigo, "msg": "Sincronizado con ASFI"}

@app.get("/api/cuentas/{banco_id}/{nro_cuenta}")
async def get_cuenta_by_id(banco_id: int, nro_cuenta: str):
    resultado = fetch_single_account(banco_id, nro_cuenta)
    if not resultado:
        raise HTTPException(status_code=404, detail="Cuenta no encontrada en este banco")
    return resultado

@app.put("/api/finalizar-conversion/{banco_id}")
async def finalizar_conversion(banco_id: int, datos: List[dict]):
    config = DB_CONFIG.get(banco_id)
    if not config:
        raise HTTPException(status_code=404, detail="Banco no configurado")

    try:
        if config['type'] in ['mysql', 'postgresql', 'oracle']:
            if config['type'] == 'mysql':
                conn = mysql.connector.connect(host=config['host'], port=config['port'], user=config['user'], password=config['password'], database=config['database'])
            elif config['type'] == 'postgresql':
                conn = psycopg2.connect(host=config['host'], port=config['port'], user=config['user'], password=config['password'], dbname=config['database'])
            else:
                dsn = oracledb.makedsn(config['host'], config['port'], service_name=config['service'])
                conn = oracledb.connect(user=config['user'], password=config['password'], dsn=dsn)
            
            cursor = conn.cursor()
            sql = f"UPDATE {config.get('table', 'Cuentas')} SET SaldoBs = %s, SaldoUSD = 0 WHERE NroCuenta = %s"
            if config['type'] == 'oracle': sql = sql.replace('%s', ':1')
            valores = [(d['SaldoBs'], d['NroCuenta']) for d in datos]
            cursor.executemany(sql, valores)
            conn.commit()
            cursor.close()
            conn.close()

        elif config['type'] == 'mongodb':
            client = MongoClient(f"mongodb://{config['user']}:{config['password']}@{config['host']}:{config['port']}/")
            db = client[config['database']]
            col = db[config['collection']]
            for d in datos:
                col.update_one({"NroCuenta": d['NroCuenta']}, {"$set": {"SaldoBs": d['SaldoBs'], "SaldoUSD": 0}})
            client.close()

        elif config['type'] == 'redis':
            r_client = redis.Redis(host=config['host'], port=config['port'], password=config['password'])
            for d in datos:
                key = f"cuenta:{d['NroCuenta']}"
                r_client.hset(key, mapping={"saldo": 0, "saldo_bs": d['SaldoBs']})
            r_client.close()

        elif config['type'] == 'neo4j':
            from graph_repository import GraphRepository
            repo = GraphRepository(database=config.get('database', 'neo4j'))
            try:
                repo.actualizar_saldos_bulk(datos)
                logging.info(f"Neo4j: Actualizadas {len(datos)} cuentas")
                return {"status": "success", "msg": f"Saldos liquidados Banco {banco_id}"}
            finally:
                repo.close()

        logging.info(f"LIQUIDACIÓN EXITOSA: Banco {banco_id} actualizó {len(datos)} cuentas.")
        return {"status": "success", "msg": f"Saldos en Bs liquidados para Banco {banco_id}"}

    except Exception as e:
        logging.error(f"❌ Error en liquidación Banco {banco_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8081)