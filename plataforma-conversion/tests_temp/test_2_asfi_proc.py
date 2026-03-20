import pandas as pd
import mysql.connector
import os
import glob
from datetime import datetime

# Configuración
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_CONFIG = {
    'host': 'localhost',
    'port': 3308,
    'user': 'root',
    'password': 'root123',
    'database': 'asfi_central'
}

def obtener_ultimo_csv():
    ruta_busqueda = os.path.join(BASE_DIR, 'data', 'processed', '*.csv')
    archivos = glob.glob(ruta_busqueda)
    return max(archivos, key=os.path.getctime) if archivos else None

def test_procesamiento_asfi():
    print("🧪 TEST 2: Procesando en MySQL ASFI...")
    
    csv_path = obtener_ultimo_csv()
    if not csv_path:
        print("❌ ERROR: No se encontró el CSV.")
        return

    print(f"📂 Procesando: {os.path.basename(csv_path)}")
    
    df = pd.read_csv(csv_path, encoding='latin-1')
    df.columns = df.columns.str.replace('ï»¿', '').str.strip()
    
    # Mismo límite que el Test 1
    df_arg = df[df['IdBanco'] == 14].head(50) 

    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT Valor FROM TiposCambio WHERE Activo = TRUE LIMIT 1")
        result = cursor.fetchone()
        if not result:
            print("❌ ERROR: No hay Tipo de Cambio activo.")
            return
        tc = float(result[0])
        print(f"💰 Tipo de Cambio detectado: {tc}")

        lote_id = f"LOTE-{datetime.now().strftime('%Y%m%d-%H%M')}"
        count = 0

        for _, row in df_arg.iterrows():
            monto_usd = float(row['Saldo'])
            monto_bs = monto_usd * tc
            cod_verif = os.urandom(4).hex().upper()
            cuenta_id = int(row['Nro']) # Usamos la columna Nro como ID primario de cuenta

            # 1. INSERTAR O ACTUALIZAR EN TABLA CUENTAS (Padre)
            query_cuenta = """
                INSERT INTO Cuentas (Id, CI, Nombres, Apellidos, NoCuenta, IdBanco, SaldoBs, LoteId, CodigoVerificacion, FechaConversion)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE SaldoBs = VALUES(SaldoBs), LoteId = VALUES(LoteId)
            """
            cursor.execute(query_cuenta, (
                cuenta_id, str(row['Identificacion']), row['Nombres'], row['Apellidos'],
                str(row['NroCuenta']), 14, monto_bs, lote_id, cod_verif, datetime.now()
            ))

            # 2. INSERTAR EN LOGSAUDITORIA (Hijo)
            query_log = """
                INSERT INTO LogsAuditoria 
                (CuentaId, BancoId, NoCuenta, CI, MontoUSD_Original, MontoBs_Resultante, 
                 TipoCambioAplicado, LoteId, CodigoVerificacion, FechaConversion)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(query_log, (
                cuenta_id, 14, str(row['NroCuenta']), str(row['Identificacion']),
                monto_usd, monto_bs, tc, lote_id, cod_verif, datetime.now()
            ))
            count += 1

        conn.commit()
        print(f"✅ ¡Éxito! {count} registros procesados en Cuentas y LogsAuditoria.")

    except Exception as e:
        conn.rollback()
        print(f"❌ Error en MySQL: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    test_procesamiento_asfi()