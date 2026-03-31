import asyncio
import httpx
import logging
import secrets
import mysql.connector
from datetime import datetime
from fastapi import FastAPI

# Configuración de Logs de Auditoría
logging.basicConfig(
    filename='asfi_operations.log',
    level=logging.INFO,
    format='%(asctime)s - [ASFI_CENTRAL] - %(message)s'
)

app = FastAPI(title="ASFI Central - Motor de Descifrado y Conversión")

# Configuración de puertos por banco
BANCO_PUERTOS = {
    1: 8081, 2: 8082, 3: 8083, 4: 8084, 5: 8085,
    6: 8086, 7: 8087, 8: 8088, 9: 8089, 10: 8090,
    11: 8091, 12: 8092, 13: 8093, 14: 8094
}

ASFI_DB_CONFIG = {
    'host': 'localhost',
    'port': 3308,
    'user': 'root',
    'password': 'root123',
    'database': 'asfi_central'
}

def get_adaptador_url(banco_id):
    """Retorna la URL correcta para cada banco según su puerto"""
    puerto = BANCO_PUERTOS.get(banco_id, 8081)
    return f"http://localhost:{puerto}"

# ============================================================
# MOTOR DE DESCIFRADO HETEROGÉNEO
# ============================================================
def descifrar_monto(valor_cifrado, banco_id):
    """
    Realiza la operación matemática inversa según el banco de origen.
    """
    try:
        v = str(valor_cifrado).split('.')[0]
        if not v: return 0.0
        
        if banco_id == 1:  # CESAR
            return float("".join(str((int(d) - 3) % 10) for d in v))
        
        elif banco_id == 2:  # ATBASH
            tabla = str.maketrans("9876543210", "0123456789")
            return float(v.translate(tabla))
        
        elif banco_id == 3:  # VIGENERE
            key = [3, 1, 4, 2]
            res = "".join(str((int(d) - key[i % len(key)]) % 10) for i, d in enumerate(v))
            return float(res)

        elif banco_id == 4:  # PLAYFAIR
            chars = list(v)
            for i in range(0, len(chars) - 1, 2):
                chars[i], chars[i+1] = chars[i+1], chars[i]
            return float("".join(chars))

        elif banco_id == 5:  # HILL
            if len(v) % 2 != 0: v += "0"
            res = ""
            for i in range(0, len(v), 2):
                x, y = int(v[i]), int(v[i+1])
                res += str((1*x - 1*y) % 10)
                res += str((-1*x + 2*y) % 10)
            return float(res)

        elif banco_id == 6:  # DES
            mitad = len(v) // 2
            return float(v[mitad:] + v[:mitad])

        elif banco_id == 7:  # 3DES
            return float("".join(str((int(d) - 7) % 10) for d in v))

        elif banco_id == 8:  # BLOWFISH
            return float("".join(str(int(d) ^ 5)[-1] for d in v))

        elif banco_id == 9:  # TWOFISH
            return float("".join(str((int(d) ^ i ^ 3) % 10) for i, d in enumerate(v)))

        elif banco_id == 10:  # AES
            return float(v[-1] + v[:-1]) if len(v) > 1 else float(v)

        elif banco_id == 11:  # RSA
            return float("".join(str((int(d) * 7) % 10) for d in v))

        elif banco_id == 12:  # ELGAMAL
            return float("".join(str((int(d) - 5) % 10) for d in v))

        elif banco_id == 13:  # ECC
            fib = [1, 1, 2, 3, 5, 8, 13, 21]
            return float("".join(str((int(d) - fib[i % len(fib)]) % 10) for i, d in enumerate(v)))

        elif banco_id == 14:  # CHACHA20
            res = "".join(str(int(d) ^ 2)[-1] for d in v if d.isdigit())
            return float(res) if res else 0.0

        return float(valor_cifrado)
    except Exception as e:
        logging.warning(f"Error descifrando dato de Banco {banco_id}: {e}")
        return 0.0

# ============================================================
# LÓGICA DE PROCESAMIENTO PARALELO
# ============================================================
async def procesar_banco(client, banco_id, tasa, lote_id):
    adaptador_url = get_adaptador_url(banco_id)
    url = f"{adaptador_url}/api/cuentas/{banco_id}?limit=50000"
    
    logging.info(f"Banco {banco_id}: Conectando a {url}")
    
    conn = None
    try:
        # 1. OBTENCIÓN DE DATOS
        res = await client.get(url, timeout=30.0)
        
        if res.status_code != 200: 
            logging.error(f"Banco {banco_id} devolvió status {res.status_code} en {url}")
            return 0
        
        cuentas = res.json()
        if not cuentas: 
            logging.info(f"Banco {banco_id} devolvió 0 cuentas (vacío)")
            return 0
        
        logging.info(f"Banco {banco_id}: Recibidas {len(cuentas)} cuentas")

        preparados = []
        confirmaciones = []
        datos_retorno = []
        logs_auditoria = []

        for c in cuentas:
            # DESCIFRADO Y CÁLCULO
            monto_real = descifrar_monto(c['SaldoUSD'], banco_id)
            saldo_bs = round(monto_real * tasa, 4)
            
            # Datos para DB Central ASFI (Tabla Cuentas)
            preparados.append((
                c.get('CI', '0'),
                c.get('Nombres', 'S/N'),
                c.get('Apellidos', 'S/A'),
                c['NroCuenta'],
                banco_id,
                saldo_bs,
                lote_id,
                datetime.now(),
                "PENDIENTE"
            ))
            
            # Datos para LogsAuditoria
            logs_auditoria.append((
                None,  # CuentaId se asignará después
                banco_id,
                c['NroCuenta'],
                c.get('CI', '0'),
                monto_real,  # MontoUSD_Original
                saldo_bs,    # MontoBs_Resultante
                tasa,        # TipoCambioAplicado
                lote_id,
                "PENDIENTE",
                datetime.now(),
                None
            ))
            
            confirmaciones.append({"NroCuenta": c['NroCuenta'], "IdBanco": banco_id})
            datos_retorno.append({"NroCuenta": c['NroCuenta'], "SaldoBs": saldo_bs})

        # 2. PERSISTENCIA EN ASFI
        conn = mysql.connector.connect(**ASFI_DB_CONFIG)
        cursor = conn.cursor()
        
        # Insertar en Cuentas
        sql_insert_cuentas = """
            INSERT INTO Cuentas (CI, Nombres, Apellidos, NoCuenta, IdBanco, SaldoBs, LoteId, FechaConversion, CodigoVerificacion)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        cursor.executemany(sql_insert_cuentas, preparados)
        conn.commit()
        
        insertados = cursor.rowcount
        logging.info(f"Banco {banco_id}: Insertadas {insertados} cuentas")

        # Obtener IDs de las cuentas insertadas para los logs
        cursor.execute("""
            SELECT Id, NoCuenta FROM Cuentas 
            WHERE LoteId = %s AND IdBanco = %s
        """, (lote_id, banco_id))
        
        cuentas_ids = {row[1]: row[0] for row in cursor.fetchall()}
        
        # Preparar logs con los CuentaId
        logs_completos = []
        for log in logs_auditoria:
            cuenta_id = cuentas_ids.get(log[2])  # log[2] es NoCuenta
            if cuenta_id:
                logs_completos.append((
                    cuenta_id, log[1], log[2], log[3], log[4],
                    log[5], log[6], log[7], log[8], log[9], log[10]
                ))
        
        # Insertar en LogsAuditoria
        if logs_completos:
            sql_insert_logs = """
                INSERT INTO LogsAuditoria (CuentaId, BancoId, NoCuenta, CI, MontoUSD_Original, 
                    MontoBs_Resultante, TipoCambioAplicado, LoteId, CodigoVerificacion, FechaConversion, IPOrigen)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.executemany(sql_insert_logs, logs_completos)
            conn.commit()
            logging.info(f"Banco {banco_id}: Insertados {len(logs_completos)} registros en LogsAuditoria")

        # 3. FEEDBACK LOOP Y LIQUIDACIÓN
        try:
            res_feedback = await client.post(
                f"{adaptador_url}/api/actualizar-lote", 
                json=confirmaciones,
                timeout=15.0
            )
            
            if res_feedback.status_code == 200:
                codigo_oficial = res_feedback.json().get("codigo_verificacion")
                if codigo_oficial:
                    cursor.execute("""
                        UPDATE Cuentas SET CodigoVerificacion = %s 
                        WHERE LoteId = %s AND IdBanco = %s
                    """, (codigo_oficial[:8], lote_id, banco_id))
                    
                    cursor.execute("""
                        UPDATE LogsAuditoria SET CodigoVerificacion = %s 
                        WHERE LoteId = %s AND BancoId = %s
                    """, (codigo_oficial[:8], lote_id, banco_id))
                    conn.commit()

                    res_retorno = await client.put(
                        f"{adaptador_url}/api/finalizar-conversion/{banco_id}", 
                        json=datos_retorno,
                        timeout=20.0
                    )
                    
                    if res_retorno.status_code == 200:
                        logging.info(f"BANCO {banco_id}: Liquidación exitosa con {len(datos_retorno)} cuentas.")
                    
        except Exception as ef:
            logging.error(f"Error en fase de cierre con Banco {banco_id}: {ef}")

        return len(preparados)

    except Exception as e:
        logging.error(f"Error crítico procesando Banco {banco_id}: {e}")
        if conn: 
            conn.rollback()
        return 0
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

@app.post("/api/ejecutar-conversion")
async def ejecutar():
    lote_id = secrets.token_hex(4).upper()
    inicio = datetime.now()
    
    async with httpx.AsyncClient(timeout=120.0) as client:
        # OBTENCIÓN DINÁMICA DEL TIPO DE CAMBIO
        try:
            res_bcb = await client.get("http://localhost:8082/api/tipo-cambio", timeout=5.0)
            if res_bcb.status_code == 200:
                tasa = res_bcb.json()["data"]["valor_actual"]
            else:
                logging.warning(f"BCB no disponible, usando 6.96")
                tasa = 6.96
        except Exception as e:
            logging.error(f"Error conectando con BCB: {e}")
            tasa = 6.96

        logging.info(f"AUDITORÍA: Inicio Lote {lote_id}. Tasa: {tasa} Bs/USD")
        
        # Procesar todos los bancos en paralelo
        tareas = [procesar_banco(client, i, tasa, lote_id) for i in range(1, 15)]
        resultados = await asyncio.gather(*tareas)
    
    total = sum(resultados)
    tiempo = (datetime.now() - inicio).total_seconds()
    
    logging.info(f"AUDITORÍA: Fin Lote {lote_id}. Total: {total} registros. Tiempo: {tiempo:.2f}s")
    
    return {
        "status": "success",
        "lote": lote_id,
        "cuentas_procesadas": total,
        "tiempo_total_segundos": tiempo,
        "tasa_aplicada": tasa
    }

@app.get("/api/diagnostico")
async def diagnostico():
    """Diagnostica qué bancos están disponibles"""
    resultados = {}
    
    async with httpx.AsyncClient(timeout=5.0) as client:
        for banco_id in range(1, 15):
            adaptador_url = get_adaptador_url(banco_id)
            url = f"{adaptador_url}/api/cuentas/{banco_id}?limit=1"
            
            try:
                res = await client.get(url)
                if res.status_code == 200:
                    data = res.json()
                    resultados[f"Banco_{banco_id}"] = {
                        "status": "OK",
                        "puerto": BANCO_PUERTOS[banco_id],
                        "cuentas": len(data) if data else 0
                    }
                else:
                    resultados[f"Banco_{banco_id}"] = {
                        "status": "ERROR",
                        "puerto": BANCO_PUERTOS[banco_id],
                        "code": res.status_code
                    }
            except Exception as e:
                resultados[f"Banco_{banco_id}"] = {
                    "status": "FALLIDO",
                    "puerto": BANCO_PUERTOS[banco_id],
                    "error": str(e)
                }
    
    return resultados

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8083)