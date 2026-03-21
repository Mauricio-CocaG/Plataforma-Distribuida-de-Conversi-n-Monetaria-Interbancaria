import asyncio
import httpx
import logging
import secrets
import mysql.connector
from datetime import datetime
from fastapi import FastAPI

# Configuración de Logs de Auditoría (Entregable 4)
logging.basicConfig(
    filename='asfi_operations.log',
    level=logging.INFO,
    format='%(asctime)s - [ASFI_CENTRAL] - %(message)s'
)

app = FastAPI(title="ASFI Central - Motor de Descifrado y Conversión")

ADAPTADOR_URL = "http://localhost:8081"
ASFI_DB_CONFIG = {
    'host': 'localhost',
    'port': 3308,
    'user': 'root',
    'password': 'root123',
    'database': 'asfi_central'
}

# ============================================================
# MOTOR DE DESCIFRADO HETEROGÉNEO (Entregable 3)
# ============================================================
def descifrar_monto(valor_cifrado, banco_id):
    """
    Realiza la operación matemática inversa según el banco de origen.
    Implementación robusta para evitar errores de formato.
    """
    try:
        # Limpieza: Convertimos a string y tomamos solo la parte numérica
        v = str(valor_cifrado).split('.')[0].replace('-', '')
        if not v: return 0.0
        
        # 1. CESAR (Inversa: -3 mod 10)
        if banco_id == 1:
            return float("".join(str((int(d) - 3) % 10) for d in v))
        
        # 2. ATBASH (Inversa: Es su propia inversa)
        elif banco_id == 2:
            tabla = str.maketrans("9876543210", "0123456789")
            return float(v.translate(tabla))
        
        # 3. VIGENERE (Inversa: Restar clave [3,1,4,2])
        elif banco_id == 3:
            key = [3, 1, 4, 2]
            res = "".join(str((int(d) - key[i % len(key)]) % 10) for i, d in enumerate(v))
            return float(res)

        # 4. PLAYFAIR (Inversa: Re-swap de adyacentes)
        elif banco_id == 4:
            chars = list(v)
            for i in range(0, len(chars) - 1, 2):
                chars[i], chars[i+1] = chars[i+1], chars[i]
            return float("".join(chars))

        # 5. HILL (Inversa modular: Matriz inversa [1,-1;-1,2] mod 10)
        elif banco_id == 5:
            if len(v) % 2 != 0: v += "0"
            res = ""
            for i in range(0, len(v), 2):
                x, y = int(v[i]), int(v[i+1])
                res += str((1*x - 1*y) % 10)
                res += str((-1*x + 2*y) % 10)
            return float(res)

        # 6. DES (Inversa: Volver a intercambiar mitades)
        elif banco_id == 6:
            mitad = len(v) // 2
            return float(v[mitad:] + v[:mitad])

        # 7. 3DES (Inversa: -7 mod 10)
        elif banco_id == 7:
            return float("".join(str((int(d) - 7) % 10) for d in v))

        # 8. BLOWFISH (Inversa: XOR con 5)
        elif banco_id == 8:
            return float("".join(str(int(d) ^ 5)[-1] for d in v))

        # 9. TWOFISH (Inversa: XOR con posición i y constante 3)
        elif banco_id == 9:
            return float("".join(str((int(d) ^ i ^ 3) % 10) for i, d in enumerate(v)))

        # 10. AES (Inversa: ShiftRows - Rotación a la derecha)
        elif banco_id == 10:
            return float(v[-1] + v[:-1]) if len(v) > 1 else float(v)

        # 11. RSA (Inversa: Multiplicación por 7 mod 10)
        elif banco_id == 11:
            return float("".join(str((int(d) * 7) % 10) for d in v))

        # 12. ELGAMAL (Inversa: -5 mod 10)
        elif banco_id == 12:
            return float("".join(str((int(d) - 5) % 10) for d in v))

        # 13. ECC (Inversa: Restar secuencia Fibonacci)
        elif banco_id == 13:
            fib = [1, 1, 2, 3, 5, 8, 13, 21]
            return float("".join(str((int(d) - fib[i % len(fib)]) % 10) for i, d in enumerate(v)))

        # 14. CHACHA20 (Inversa: XOR con semilla fija 42 % 10 = 2)
        elif banco_id == 14:
            return float("".join(str(int(d) ^ 2)[-1] for d in v))

        return float(v)
    except Exception as e:
        logging.warning(f"Error descifrando dato de Banco {banco_id}: {e}")
        return 0.0

# ============================================================
# LÓGICA DE PROCESAMIENTO PARALELO
# ============================================================
async def procesar_banco(client, banco_id, tasa, lote_id):
    url = f"{ADAPTADOR_URL}/api/cuentas/{banco_id}?limit=50000"
    
    conn = None
    try:
        # 1. OBTENCIÓN DE DATOS
        res = await client.get(url, timeout=120.0)
        if res.status_code != 200: 
            logging.error(f"Banco {banco_id} devolvió status {res.status_code}")
            return 0
        
        cuentas = res.json()
        if not cuentas: 
            logging.info(f"Banco {banco_id} devolvió 0 cuentas (vacío)")
            return 0

        preparados = []
        confirmaciones = []
        datos_retorno = [] # <--- NUEVO: Para enviar saldos finales al banco

        for c in cuentas:
            # DESCIFRADO Y CÁLCULO
            monto_real = descifrar_monto(c['SaldoUSD'], banco_id)
            saldo_bs = round(monto_real * tasa, 4)
            
            # Datos para DB Central ASFI
            preparados.append((
                c.get('CI', '0'),           # Campo CI
                c.get('Nombres', 'S/N'),    # Campo Nombres
                c.get('Apellidos', 'S/A'),  # Campo Apellidos
                c['NroCuenta'],             # Valor para la columna NoCuenta
                banco_id,                   # Campo IdBanco
                saldo_bs,                   # Campo SaldoBs
                lote_id,                    # Campo LoteId
                datetime.now(),             # Campo FechaConversion
                "PENDIENTE"                 # Campo CodigoVerificacion (Max 8 chars)
            ))
            
            # Datos para el Feedback Loop
            confirmaciones.append({"NroCuenta": c['NroCuenta'], "IdBanco": banco_id})
            
            # Datos para la Liquidación en el Banco (Retorno)
            datos_retorno.append({
                "NroCuenta": c['NroCuenta'], 
                "SaldoBs": saldo_bs
            })

        # 2. PERSISTENCIA EN ASFI (DB CENTRAL)
        conn = mysql.connector.connect(**ASFI_DB_CONFIG)
        cursor = conn.cursor()
        
        sql_insert = """
            INSERT INTO Cuentas (CI, Nombres, Apellidos, NoCuenta, IdBanco, SaldoBs, LoteId, FechaConversion, CodigoVerificacion)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        # Pasamos la tupla 'c' completa (que tiene los 9 elementos)
        cursor.executemany(sql_insert, preparados)
        conn.commit()

        # 3. FEEDBACK LOOP Y LIQUIDACIÓN FINAL
        try:
            # A. Notificar lote y obtener código de verificación
            res_feedback = await client.post(
                f"{ADAPTADOR_URL}/api/actualizar-lote", 
                json=confirmaciones,
                timeout=15.0
            )
            
            if res_feedback.status_code == 200:
                codigo_oficial = res_feedback.json().get("codigo_verificacion")
                if codigo_oficial:
                    # B. Actualizar código en ASFI
                    cursor.execute("""
                        UPDATE Cuentas SET CodigoVerificacion = %s 
                        WHERE LoteId = %s AND IdBanco = %s
                    """, (codigo_oficial[:8], lote_id, banco_id))
                    conn.commit()

                    # C. SERVICIO DE RETORNO: Ordenar al Banco actualizar sus saldos locales
                    # Esto cierra el ciclo: El banco pone USD en 0 y activa el SaldoBs
                    res_retorno = await client.put(
                        f"{ADAPTADOR_URL}/api/finalizar-conversion/{banco_id}", 
                        json=datos_retorno,
                        timeout=20.0
                    )
                    
                    if res_retorno.status_code == 200:
                        logging.info(f"BANCO {banco_id}: Liquidación exitosa y saldos locales actualizados.")
                    
        except Exception as ef:
            logging.error(f"Error en fase de cierre con Banco {banco_id}: {ef}")

        return len(preparados)

    except Exception as e:
        logging.error(f"Error crítico procesando Banco {banco_id}: {e}")
        if conn: conn.rollback()
        return 0
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

@app.post("/api/ejecutar-conversion")
async def ejecutar(): # <--- Eliminamos el parámetro 'tasa' manual
    lote_id = secrets.token_hex(4).upper()
    inicio = datetime.now()
    
    async with httpx.AsyncClient(timeout=120.0) as client:
        # 1. OBTENCIÓN DINÁMICA DEL TIPO DE CAMBIO (BCB)
        try:
            # Asumiendo que tu bcb_service está en el puerto 8082
            res_bcb = await client.get("http://localhost:8082/api/tipo-cambio")
            if res_bcb.status_code == 200:
                tasa = res_bcb.json()["data"]["valor_actual"]
            else:
                logging.warning(f"BCB no disponible (Status {res_bcb.status_code}), usando 6.96 por defecto")
                tasa = 6.96
        except Exception as e:
            logging.error(f"Error conectando con BCB Service: {e}")
            tasa = 6.96 # Fallback de seguridad

        # Registro de Auditoría con la tasa real obtenida
        logging.info(f"AUDITORÍA: Inicio de Lote {lote_id}. Tasa Oficial BCB: {tasa} Bs/USD")
        
        # 2. PROCESAMIENTO PARALELO (Se mantiene tu lógica con la nueva tasa)
        tareas = [procesar_banco(client, i, tasa, lote_id) for i in range(1, 15)]
        resultados = await asyncio.gather(*tareas)
    
    total = sum(resultados)
    tiempo = (datetime.now() - inicio).total_seconds()
    
    logging.info(f"AUDITORÍA: Fin de Lote {lote_id}. Total procesado: {total} registros. Tiempo: {tiempo:.2f}s")
    
    return {
        "status": "success",
        "lote": lote_id,
        "cuentas_procesadas": total,
        "tiempo_total_segundos": tiempo,
        "tasa_aplicada": tasa # Aquí verás el valor que vino del BCB
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8083)