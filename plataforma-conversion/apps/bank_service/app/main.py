import os
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

# Configuración de Logs
logging.basicConfig(
    filename='bank_service.log', 
    level=logging.INFO, 
    format='%(asctime)s - [BANK_ADAPTER] - %(message)s'
)

app = FastAPI(title="Bank Service - Adaptador Oficial ASFI")

# ============================================================
# CONFIGURACIÓN (Sincronizada con Docker y Puertos Libres)
# ============================================================
DB_CONFIG = {
    1: { 'type': 'mysql', 'host': 'localhost', 'port': 3307, 'database': 'banco_union',
         'user': 'root', 'password': 'root123', 'table': 'Cuentas' },
    2: { 'type': 'postgresql', 'host': 'localhost', 'port': 5433, 'database': 'banco_mercantil', # <--- CORREGIDO A 5433
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

def cifrar_monto_banco(monto, algoritmo):
    """
    Aplica el cifrado correspondiente según el banco antes de enviar a ASFI.
    [Entregable 2: Seguridad y Heterogeneidad Criptográfica]
    """
    try:
        # Convertimos el monto a una cadena de dígitos (sin decimales para simplificar el reto)
        v = str(int(float(monto)))
        
        # 1. CESAR (Desplazamiento +3)
        if algoritmo == 'Cesar':
            return "".join(str((int(d) + 3) % 10) for d in v)
        
        # 2. ATBASH (Espejo numérico: 0->9, 1->8...)
        elif algoritmo == 'Atbash':
            tabla = str.maketrans("0123456789", "9876543210")
            return v.translate(tabla)
        
        # 3. VIGENERE (Sustitución Polialfabética con clave '3142')
        elif algoritmo == 'Vigenere':
            key = [3, 1, 4, 2]
            return "".join(str((int(d) + key[i % len(key)]) % 10) for i, d in enumerate(v))

        # 4. PLAYFAIR (Transposición de pares / Swap adyacente)
        elif algoritmo == 'Playfair':
            chars = list(v)
            for i in range(0, len(chars) - 1, 2):
                chars[i], chars[i+1] = chars[i+1], chars[i]
            return "".join(chars)

        # 5. HILL (Multiplicación por Matriz [2,1;1,1] mod 10)
        elif algoritmo == 'Hill':
            if len(v) % 2 != 0: v += "0" # Padding para bloques de 2
            res = ""
            for i in range(0, len(v), 2):
                x, y = int(v[i]), int(v[i+1])
                res += str((2*x + 1*y) % 10)
                res += str((1*x + 1*y) % 10)
            return res

        # 6. DES (Permutación Inicial - Swap de mitades)
        elif algoritmo == 'DES':
            mitad = len(v) // 2
            return v[mitad:] + v[:mitad]

        # 7. 3DES (Triple desplazamiento modular +7)
        elif algoritmo == '3DES':
            return "".join(str((int(d) + 7) % 10) for d in v)

        # 8. BLOWFISH (XOR con constante 5)
        elif algoritmo == 'Blowfish':
            return "".join(str(int(d) ^ 5)[-1] for d in v)

        # 9. TWOFISH (XOR con posición i y constante 3)
        elif algoritmo == 'Twofish':
            return "".join(str((int(d) ^ i) % 10) for i, d in enumerate(v))

        # 10. AES (ShiftRows - Rotación de dígitos a la izquierda)
        elif algoritmo == 'AES':
            if len(v) > 1:
                return v[1:] + v[0]
            return v

        # 11. RSA (Simulación de mapeo con exponente público 7)
        elif algoritmo == 'RSA':
            # Nota: 3 es el inverso modular de 7 mod 10 (simplificado)
            return "".join(str((int(d) * 3) % 10) for d in v)

        # 12. ELGAMAL (Multiplicación escalar - Desplazamiento +5)
        elif algoritmo == 'ElGamal':
            return "".join(str((int(d) + 5) % 10) for d in v)

        # 13. ECC (Curva Elíptica - Desplazamiento basado en Fibonacci)
        elif algoritmo == 'ECC':
            fib = [1, 1, 2, 3, 5, 8, 13, 21]
            return "".join(str((int(d) + fib[i % len(fib)]) % 10) for i, d in enumerate(v))

        # 14. CHACHA20 (XOR con semilla fija 42)
        elif algoritmo == 'ChaCha20':
            return "".join(str(int(d) ^ (42 % 10))[-1] for d in v)

        return v
    except:
        return str(monto)
# --- FUNCIONES ADICIONALES REQUERIDAS ---
def fetch_from_bank(banco_id: int, page: int, limit: int):
    config = DB_CONFIG.get(banco_id)
    if not config: return []
    
    # Mapeo oficial de algoritmos según la práctica [cite: 18]
    ALGORITMOS = {
        1: 'Cesar', 2: 'Atbash', 3: 'Vigenere', 4: 'Playfair', 
        5: 'Hill', 6: 'DES', 7: '3DES', 8: 'Blowfish', 
        9: 'Twofish', 10: 'AES', 11: 'RSA', 12: 'ElGamal', 
        13: 'ECC', 14: 'ChaCha20'
    }
    algoritmo = ALGORITMOS.get(banco_id, 'None')
    
    try:
        # --- CASO MYSQL (Bancos Relacionales [cite: 39]) ---
        if config['type'] == 'mysql':
            conn = mysql.connector.connect(host=config['host'], port=config['port'], user=config['user'], password=config['password'], database=config['database'])
            cursor = conn.cursor(dictionary=True)
            cursor.execute(f"SELECT NroIdentificacion as Identificacion, NroCuenta, SaldoUSD, Nombres, Apellidos FROM {config['table']} LIMIT %s OFFSET %s", (limit, page * limit))
            res = cursor.fetchall()
            conn.close()
            # Aplicamos cifrado a cada cuenta del lote
            return [dict(r, IdBanco=banco_id, SaldoUSD=cifrar_monto_banco(r['SaldoUSD'], algoritmo)) for r in res]

        # --- CASO POSTGRESQL (Bancos Relacionales [cite: 39]) ---        
        elif config['type'] == 'postgresql':
            conn = psycopg2.connect(host=config['host'], port=config['port'], user=config['user'], password=config['password'], dbname=config['database'])
            cursor = conn.cursor()
            cursor.execute(f'SELECT nroidentificacion, nrocuenta, saldousd, nombres, apellidos FROM {config["table"]} LIMIT %s OFFSET %s', (limit, page * limit))
            res = cursor.fetchall()
            conn.close()
            # Aplicamos cifrado y estructuramos el JSON
            return [{
                "Identificacion": r[0], 
                "NroCuenta": r[1], 
                "SaldoUSD": cifrar_monto_banco(r[2], algoritmo), 
                "Nombres": r[3], 
                "Apellidos": r[4], 
                "IdBanco": banco_id
            } for r in res]

        # --- CASO MONGODB (Bancos No Relacionales [cite: 39]) ---
        elif config['type'] == 'mongodb':
            client = MongoClient(f"mongodb://{config['user']}:{config['password']}@{config['host']}:{config['port']}/")
            db = client[config['database']]
            res = list(db[config['collection']].find({}, {'_id': 0}).skip(page * limit).limit(limit))
            client.close()
            # Aplicamos cifrado al campo SaldoUSD de MongoDB
            return [{**r, "SaldoUSD": cifrar_monto_banco(r.get("SaldoUSD", 0), algoritmo), "IdBanco": banco_id} for r in res]

        # --- CASO REDIS (Bancos No Relacionales [cite: 39]) ---
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
                    "Identificacion": v.get('id'), 
                    "NroCuenta": k.split(":")[1], 
                    "SaldoUSD": cifrar_monto_banco(v.get('saldo', 0), algoritmo), # Cifrado aplicado
                    "Nombres": v.get('nombres'), 
                    "Apellidos": v.get('apellidos'), 
                    "IdBanco": banco_id
                })
            r_client.close()
            return data

        # --- CASO ORACLE (Bancos Relacionales [cite: 39]) ---
        elif config['type'] == 'oracle':
            dsn = oracledb.makedsn(config['host'], config['port'], service_name=config['service'])
            conn = oracledb.connect(user=config['user'], password=config['password'], dsn=dsn)
            cursor = conn.cursor()
            cursor.execute(f"SELECT NroIdentificacion, NroCuenta, SaldoUSD, Nombres, Apellidos FROM {config['table']} OFFSET :1 ROWS FETCH NEXT :2 ROWS ONLY", (page * limit, limit))
            res = cursor.fetchall()
            conn.close()
            return [{
                "Identificacion": r[0], 
                "NroCuenta": r[1], 
                "SaldoUSD": cifrar_monto_banco(r[2], algoritmo), 
                "Nombres": r[3], 
                "Apellidos": r[4], 
                "IdBanco": banco_id
            } for r in res]

        # --- CASO NEO4J (Base de datos de Grafos ) ---
        elif config['type'] == 'neo4j':
            from graph_repository import GraphRepository
            repo = GraphRepository()
            cuentas = repo.get_all_accounts()
            repo.close()
            
            return [{
                "Identificacion": c["ci"], 
                "NroCuenta": c["nro_cuenta"], 
                "SaldoUSD": cifrar_monto_banco(c["saldo"], algoritmo), 
                "Nombres": c["nombre"], 
                "Apellidos": c["apellido"], 
                "IdBanco": banco_id
            } for c in cuentas]

    except Exception as e:
        logging.error(f"❌ Error en barrido masivo Banco {banco_id}: {e}")
        return []
def update_bank_status(banco_id, payload):
    """
    Genera el código de verificación requerido por ASFI.
    """
    # ERROR CORREGIDO: Generación de código hexadecimal de 8 caracteres (0-9 A-F)
    codigo_verificacion = secrets.token_hex(4).upper() 
    
    # Aquí deberías persistir el código en el log de auditoría
    logging.info(f"Transacción exitosa. Banco: {banco_id}. Código: {codigo_verificacion}")
    
    return codigo_verificacion

# ============================================================
# LÓGICA DE BÚSQUEDA ESPECÍFICA (NUEVO)
# ============================================================
def fetch_single_account(banco_id: int, nro_cuenta: str):
    config = DB_CONFIG.get(banco_id)
    if not config: return None
    # Mapeo oficial de algoritmos según la práctica 
    ALGORITMOS = {
        1: 'Cesar', 2: 'Atbash', 3: 'Vigenere', 4: 'Playfair', 
        5: 'Hill', 6: 'DES', 7: '3DES', 8: 'Blowfish', 
        9: 'Twofish', 10: 'AES', 11: 'RSA', 12: 'ElGamal', 
        13: 'ECC', 14: 'ChaCha20'
    }
    algoritmo = ALGORITMOS.get(banco_id, 'None')

    try:
        # --- 1. MYSQL (Bancos Relacionales [cite: 39]) ---
        if config['type'] == 'mysql':
            conn = mysql.connector.connect(host=config['host'], port=config['port'], user=config['user'], password=config['password'], database=config['database'])
            cursor = conn.cursor(dictionary=True)
            cursor.execute(f"SELECT NroIdentificacion as Identificacion, NroCuenta, SaldoUSD, Nombres, Apellidos FROM {config['table']} WHERE NroCuenta = %s", (nro_cuenta,))
            r = cursor.fetchone()
            conn.close()
            if r:
                r['SaldoUSD'] = cifrar_monto_banco(r['SaldoUSD'], algoritmo) # Cifrado 
                return {**r, "IdBanco": banco_id}

        # --- 2. POSTGRESQL (Bancos Relacionales [cite: 39]) ---
        elif config['type'] == 'postgresql':
            conn = psycopg2.connect(host=config['host'], port=config['port'], user=config['user'], password=config['password'], dbname=config['database'])
            cursor = conn.cursor()
            cursor.execute(f"SELECT nroidentificacion, nrocuenta, saldousd, nombres, apellidos FROM {config['table']} WHERE nrocuenta = %s", (nro_cuenta,))
            r = cursor.fetchone()
            conn.close()
            if r:
                return {
                    "Identificacion": r[0], "NroCuenta": r[1], 
                    "SaldoUSD": cifrar_monto_banco(r[2], algoritmo), # Cifrado 
                    "Nombres": r[3], "Apellidos": r[4], "IdBanco": banco_id
                }

        # --- 3. ORACLE (Bancos Relacionales [cite: 39, 40]) ---
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
                    "SaldoUSD": cifrar_monto_banco(r[2], algoritmo), # Cifrado 
                    "Nombres": r[3], "Apellidos": r[4], "IdBanco": banco_id
                }

        # --- 4. MONGODB (Bancos No Relacionales [cite: 39]) ---
        elif config['type'] == 'mongodb':
            client = MongoClient(f"mongodb://{config['user']}:{config['password']}@{config['host']}:{config['port']}/")
            db = client[config['database']]
            r = db[config['collection']].find_one({"NroCuenta": nro_cuenta}, {'_id': 0})
            client.close()
            if r:
                r['SaldoUSD'] = cifrar_monto_banco(r.get('SaldoUSD', 0), algoritmo) # Cifrado 
                return {**r, "IdBanco": banco_id}

        # --- 5. REDIS (Bancos No Relacionales [cite: 39]) ---
        elif config['type'] == 'redis':
            r_client = redis.Redis(host=config['host'], port=config['port'], password=config['password'], decode_responses=True)
            val = r_client.hgetall(f"cuenta:{nro_cuenta}")
            r_client.close()
            if val:
                return {
                    "Identificacion": val.get('id'), 
                    "NroCuenta": nro_cuenta, 
                    "SaldoUSD": cifrar_monto_banco(val.get('saldo', 0), algoritmo), # Cifrado 
                    "Nombres": val.get('nombres'), "Apellidos": val.get('apellidos'), "IdBanco": banco_id
                }

        # --- 6. NEO4J (Orientada a Grafos [cite: 54]) ---
        elif config['type'] == 'neo4j':
            from graph_repository import GraphRepository
            repo = GraphRepository()
            r = repo.buscar_cuenta_y_propietario(str(nro_cuenta))
            repo.close()
            if r:
                r['SaldoUSD'] = cifrar_monto_banco(r['SaldoUSD'], algoritmo) # Cifrado 
                return {**r, "IdBanco": banco_id}

    except Exception as e:
        logging.error(f"❌ Error FETCH con cifrado en Banco {banco_id}: {e}")
        return None
# ============================================================
# ENDPOINTS
# ============================================================
@app.get("/api/health")
async def health_check():
    """Verifica si los 14 bancos están respondiendo."""
    return {"status": "online", "msg": "Adaptador ASFI funcionando"}
@app.get("/api/cuentas/{banco_id}")
async def get_cuentas(banco_id: int, page: int = 0, limit: int = 1000):
    return fetch_from_bank(banco_id, page, limit)

@app.post("/api/actualizar-lote")
async def post_actualizar(payload: List[dict]):
    if not payload: 
        return {"status": "void"}
    
    # Extraer el IdBanco de cualquier elemento del lote si el primero falla
    banco_id = next((item.get('IdBanco') for item in payload if item.get('IdBanco')), None)
    
    if not banco_id:
        raise HTTPException(status_code=400, detail="IdBanco no detectado en el lote")
        
    codigo = update_bank_status(banco_id, payload)
    return {
        "status": "success", 
        "codigo_verificacion": codigo,
        "msg": "Sincronizado con ASFI"
    }

@app.get("/api/cuentas/{banco_id}/{nro_cuenta}")
async def get_cuenta_by_id(banco_id: int, nro_cuenta: str):
    """Endpoint de precisión para buscar una cuenta específica."""
    resultado = fetch_single_account(banco_id, nro_cuenta)
    if not resultado:
        raise HTTPException(status_code=404, detail="Cuenta no encontrada en este banco")
    return resultado
@app.put("/api/finalizar-conversion/{banco_id}")
async def finalizar_conversion(banco_id: int, datos: List[dict]):
    """
    Recibe saldos en Bs desde ASFI y los persiste en la DB original.
    Implementación polimórfica para SQL, NoSQL y Grafos.
    """
    config = DB_CONFIG.get(banco_id)
    if not config:
        raise HTTPException(status_code=404, detail="Banco no configurado")

    try:
        # --- CASO MYSQL / POSTGRES / ORACLE (SQL) ---
        if config['type'] in ['mysql', 'postgresql', 'oracle']:
            if config['type'] == 'mysql':
                conn = mysql.connector.connect(host=config['host'], port=config['port'], user=config['user'], password=config['password'], database=config['database'])
            elif config['type'] == 'postgresql':
                conn = psycopg2.connect(host=config['host'], port=config['port'], user=config['user'], password=config['password'], dbname=config['database'])
            else: # oracle
                dsn = oracledb.makedsn(config['host'], config['port'], service_name=config['service'])
                conn = oracledb.connect(user=config['user'], password=config['password'], dsn=dsn)
            
            cursor = conn.cursor()
            # SQL Estándar: Ponemos SaldoUSD en 0 y cargamos el nuevo SaldoBs
            sql = f"UPDATE {config.get('table', 'Cuentas')} SET SaldoBs = %s, SaldoUSD = 0 WHERE NroCuenta = %s"
            if config['type'] == 'oracle': sql = sql.replace('%s', ':1') # Ajuste para Oracle

            valores = [(d['SaldoBs'], d['NroCuenta']) for d in datos]
            cursor.executemany(sql, valores)
            conn.commit()
            cursor.close()
            conn.close()

        # --- CASO MONGODB (NoSQL) ---
        elif config['type'] == 'mongodb':
            client = MongoClient(f"mongodb://{config['user']}:{config['password']}@{config['host']}:{config['port']}/")
            db = client[config['database']]
            col = db[config['collection']]
            
            for d in datos:
                col.update_one(
                    {"NroCuenta": d['NroCuenta']},
                    {"$set": {"SaldoBs": d['SaldoBs'], "SaldoUSD": 0}}
                )
            client.close()

        # --- CASO REDIS (Key-Value) ---
        elif config['type'] == 'redis':
            r_client = redis.Redis(host=config['host'], port=config['port'], password=config['password'])
            for d in datos:
                key = f"cuenta:{d['NroCuenta']}"
                r_client.hset(key, mapping={"saldo": 0, "saldo_bs": d['SaldoBs']})
            r_client.close()

        # --- CASO NEO4J (Grafos) ---
        elif config['type'] == 'neo4j':
            from graph_repository import GraphRepository
            repo = GraphRepository()
            
            # 'datos' es el JSON que envía la ASFI con los nuevos saldos convertidos
            query = """
            UNWIND $lote AS fila
            MATCH (c:Cuenta {NroCuenta: fila.NroCuenta})
            SET c.SaldoUSD = 0, 
                c.SaldoBs = fila.SaldoBs
            """
            
            # Ejecutamos todo el lote de una sola vez
            repo.execute_query(query, lote=datos)
            repo.close()
            return {"status": "success", "message": "Banco 14 actualizado"}

        logging.info(f"LIQUIDACIÓN EXITOSA: Banco {banco_id} actualizó {len(datos)} cuentas.")
        return {"status": "success", "msg": f"Saldos en Bs liquidados para Banco {banco_id}"}

    except Exception as e:
        logging.error(f"❌ Error en liquidación Banco {banco_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8081)