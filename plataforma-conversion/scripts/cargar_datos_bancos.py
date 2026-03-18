#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
OPCIÓN 2 - SCRIPT CON BATCH INSERT (MÁXIMA VELOCIDAD)
=======================================================
CORREGIDO - Con configuración completa para MongoDB
Tiempo estimado: 2-3 minutos para 123,790 registros
"""

import os
import sys
import pandas as pd
from datetime import datetime
import logging
from pathlib import Path
import glob
from collections import defaultdict
import time

# Configurar logging - SIN EMOJIS para evitar errores de Windows
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    handlers=[
        logging.FileHandler('data/logs/carga_rapida.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ============================================================
# CONFIGURACION
# ============================================================

NOMBRES_BANCOS = {
    1: "Banco Union", 2: "Banco Mercantil", 3: "Banco BNB", 4: "Banco BCP",
    5: "Banco BISA", 6: "Banco Ganadero", 7: "Banco Economico", 8: "Banco Prodem",
    9: "Banco Solidario", 10: "Banco Fortaleza", 11: "Banco FIE", 12: "Banco PYME",
    13: "Banco BDP", 14: "Banco Argentina",
}

ALGORITMOS_BANCOS = {
    1: "Cifrado Cesar", 2: "Cifrado Atbash", 3: "Cifrado Vigenere", 4: "Cifrado Playfair",
    5: "Cifrado Hill", 6: "DES", 7: "3DES", 8: "Blowfish", 9: "Twofish", 10: "AES",
    11: "RSA", 12: "ElGamal", 13: "ECC", 14: "ChaCha20",
}

# Configuración de conexiones - COMPLETA para todos los bancos
DB_CONFIG = {
    1: {  # MySQL - Banco Union
        'type': 'mysql', 'host': 'localhost', 'port': 3307,
        'database': 'banco_union', 'user': 'root', 'password': 'root123'
    },
    2: {  # PostgreSQL - Banco Mercantil
        'type': 'postgresql', 'host': 'localhost', 'port': 5432,
        'database': 'banco_mercantil', 'user': 'root', 'password': 'root123'
    },
    3: {  # MongoDB - BNB
        'type': 'mongodb', 'host': 'localhost', 'port': 27017,
        'database': 'banco_bnb', 'user': 'root', 'password': 'root123'
    },
    4: {  # Redis - BCP
        'type': 'redis', 'host': 'localhost', 'port': 6379, 'password': 'root123'
    },
    5: {  # Oracle - BISA
        'type': 'oracle', 'host': 'localhost', 'port': 1521,
        'service': 'XEPDB1', 'user': 'BANCO_BISA', 'password': 'root123'
    },
    # MongoDB - Banco Ganadero
    6: { 'type': 'mongodb', 'host': 'localhost', 'port': 27018, 
         'database': 'banco_ganadero', 'user': 'root', 'password': 'root123' },
    # MongoDB - Banco Economico
    7: { 'type': 'mongodb', 'host': 'localhost', 'port': 27019, 
         'database': 'banco_economico', 'user': 'root', 'password': 'root123' },
    # MongoDB - Banco Prodem
    8: { 'type': 'mongodb', 'host': 'localhost', 'port': 27020, 
         'database': 'banco_prodem', 'user': 'root', 'password': 'root123' },
    # MongoDB - Banco Solidario
    9: { 'type': 'mongodb', 'host': 'localhost', 'port': 27021, 
         'database': 'banco_solidario', 'user': 'root', 'password': 'root123' },
    # MongoDB - Banco Fortaleza
    10: { 'type': 'mongodb', 'host': 'localhost', 'port': 27022, 
          'database': 'banco_fortaleza', 'user': 'root', 'password': 'root123' },
    # MongoDB - Banco FIE
    11: { 'type': 'mongodb', 'host': 'localhost', 'port': 27023, 
          'database': 'banco_fie', 'user': 'root', 'password': 'root123' },
    # MongoDB - Banco PYME
    12: { 'type': 'mongodb', 'host': 'localhost', 'port': 27024, 
          'database': 'banco_pyme', 'user': 'root', 'password': 'root123' },
    # MongoDB - Banco BDP
    13: { 'type': 'mongodb', 'host': 'localhost', 'port': 27025, 
          'database': 'banco_bdp', 'user': 'root', 'password': 'root123' },
    # MongoDB - Banco Argentina
    14: { 'type': 'mongodb', 'host': 'localhost', 'port': 27026, 
          'database': 'banco_argentina', 'user': 'root', 'password': 'root123' },
}

# ============================================================
# FUNCIONES DE CONEXION
# ============================================================

def get_connection(banco_id):
    """Obtiene conexión según tipo de base de datos"""
    config = DB_CONFIG[banco_id].copy()
    db_type = config.pop('type')
    
    try:
        if db_type == 'mysql':
            import mysql.connector
            return mysql.connector.connect(**config)
        
        elif db_type == 'postgresql':
            import psycopg2
            return psycopg2.connect(**config)
        
        elif db_type == 'oracle':
            try:
                import oracledb
                oracledb.defaults.fetch_lobs = False
                dsn = oracledb.makedsn(config['host'], config['port'], service_name=config['service'])
                return oracledb.connect(user=config['user'], password=config['password'], dsn=dsn)
            except ImportError:
                logger.warning(f"  oracledb no instalado - saltando banco {banco_id}")
                return None
        
        elif db_type == 'mongodb':
            from pymongo import MongoClient
            client = MongoClient(f"mongodb://{config['user']}:{config['password']}@{config['host']}:{config['port']}/")
            return client[config['database']]
        
        elif db_type == 'redis':
            import redis
            return redis.Redis(
                host=config['host'], port=config['port'],
                password=config['password'], decode_responses=True
            )
    except Exception as e:
        logger.error(f"Error conectando a banco {banco_id}: {e}")
        return None

# ============================================================
# FUNCIONES DE INSERCION POR LOTES
# ============================================================

def insertar_batch_mysql(conn, batch, banco_id):
    """Insertar lote en MySQL con IGNORE para duplicados"""
    cursor = conn.cursor()
    sql = """
        INSERT IGNORE INTO Cuentas (
            NroIdentificacion, Nombres, Apellidos, NroCuenta, 
            IdBanco, SaldoUSD, DatosCifrados, AlgoritmoUsado
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    valores = [(
        r['Identificacion'], r['Nombres'], r['Apellidos'],
        r['NroCuenta'], banco_id, float(r['Saldo']),
        f"CIFRADO_{banco_id}_{r['Nro']}", ALGORITMOS_BANCOS[banco_id]
    ) for r in batch]
    cursor.executemany(sql, valores)
    conn.commit()
    cursor.close()
    return len(valores)

def insertar_batch_postgresql(conn, batch, banco_id):
    """Insertar lote en PostgreSQL con ON CONFLICT"""
    cursor = conn.cursor()
    sql = """
        INSERT INTO cuentas (
            nroidentificacion, nombres, apellidos, nrocuenta, 
            idbanco, saldousd, datoscifrados, algoritmousado
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (nrocuenta) DO NOTHING
    """
    valores = [(
        r['Identificacion'], r['Nombres'], r['Apellidos'],
        r['NroCuenta'], banco_id, float(r['Saldo']),
        f"CIFRADO_{banco_id}_{r['Nro']}", ALGORITMOS_BANCOS[banco_id]
    ) for r in batch]
    cursor.executemany(sql, valores)
    conn.commit()
    cursor.close()
    return len(valores)

def insertar_batch_oracle(conn, batch, banco_id):
    """Insertar lote en Oracle"""
    if conn is None:
        return 0
    cursor = conn.cursor()
    sql = """
        INSERT INTO Cuentas (
            NroIdentificacion, Nombres, Apellidos, NroCuenta, 
            IdBanco, SaldoUSD, DatosCifrados, AlgoritmoUsado
        ) VALUES (:1, :2, :3, :4, :5, :6, :7, :8)
    """
    valores = [(
        r['Identificacion'], r['Nombres'], r['Apellidos'],
        r['NroCuenta'], banco_id, float(r['Saldo']),
        f"CIFRADO_{banco_id}_{r['Nro']}", ALGORITMOS_BANCOS[banco_id]
    ) for r in batch]
    cursor.executemany(sql, valores)
    conn.commit()
    cursor.close()
    return len(valores)

def insertar_batch_mongodb(db, batch, banco_id):
    """Insertar lote en MongoDB"""
    if db is None:
        return 0
    collection = db['Cuentas']
    documentos = [{
        'Nro': int(r['Nro']), 
        'NroIdentificacion': r['Identificacion'],
        'Nombres': r['Nombres'], 
        'Apellidos': r['Apellidos'],
        'NroCuenta': r['NroCuenta'], 
        'IdBanco': banco_id,
        'SaldoUSD': float(r['Saldo']), 
        'SaldoBs': None,
        'DatosCifrados': f"CIFRADO_{banco_id}_{r['Nro']}",
        'AlgoritmoUsado': ALGORITMOS_BANCOS[banco_id],
        'FechaRegistro': datetime.now()
    } for r in batch]
    collection.insert_many(documentos)
    return len(documentos)

def insertar_batch_redis(conn, batch, banco_id):
    """Insertar lote en Redis usando pipeline"""
    if conn is None:
        return 0
    pipe = conn.pipeline()
    for r in batch:
        key = f"cuenta:{r['NroCuenta']}"
        pipe.hset(key, mapping={
            'nro': str(r['Nro']), 
            'identificacion': r['Identificacion'],
            'nombres': r['Nombres'], 
            'apellidos': r['Apellidos'],
            'id_banco': str(banco_id), 
            'saldo_usd': str(r['Saldo']),
            'datos_cifrados': f"CIFRADO_{banco_id}_{r['Nro']}",
            'algoritmo': ALGORITMOS_BANCOS[banco_id]
        })
    pipe.execute()
    return len(batch)

# Mapper de funciones de inserción
INSERT_FUNCTIONS = {
    'mysql': insertar_batch_mysql,
    'postgresql': insertar_batch_postgresql,
    'oracle': insertar_batch_oracle,
    'mongodb': insertar_batch_mongodb,
    'redis': insertar_batch_redis,
}

# ============================================================
# FUNCIONES AUXILIARES
# ============================================================

def limpiar_nro_cuenta(valor):
    """Limpia números de cuenta en notación científica"""
    if pd.isna(valor): 
        return ""
    valor_str = str(valor).strip()
    if 'E+' in valor_str or 'e+' in valor_str:
        try: 
            return str(int(float(valor_str)))
        except: 
            return valor_str
    return valor_str

def procesar_batch_banco(banco_id, batch_rows):
    """Procesa un lote de registros para un banco específico"""
    if not batch_rows:
        return 0
    
    try:
        conn = get_connection(banco_id)
        if conn is None:
            return 0
        
        config = DB_CONFIG[banco_id]
        db_type = config['type']
        insert_func = INSERT_FUNCTIONS[db_type]
        
        insertados = insert_func(conn, batch_rows, banco_id)
        
        # Cerrar conexión según tipo
        if db_type in ['mysql', 'postgresql', 'oracle']:
            conn.close()
        elif db_type == 'mongodb':
            conn.client.close()
        
        return insertados
    except Exception as e:
        logger.error(f"Error en banco {banco_id}: {e}")
        return 0

# ============================================================
# FUNCION PRINCIPAL
# ============================================================

def cargar_datos_rapido(archivo):
    """Carga datos usando batch insert"""
    logger.info("Iniciando carga rapida desde: %s", archivo)
    inicio_total = time.time()
    
    # Leer datos
    df = pd.read_csv(archivo, encoding='latin-1')
    df.columns = df.columns.str.replace('ï»¿', '').str.strip()
    total_registros = len(df)
    logger.info("Total registros: %d", total_registros)
    
    # Preparar datos por banco
    batches = defaultdict(list)
    BATCH_SIZE = 1000
    total_procesados = 0
    
    for idx, row in df.iterrows():
        if pd.isna(row.get('IdBanco')):
            continue
        
        banco_id = int(row['IdBanco'])
        
        fila = {
            'Nro': int(row.get('Nro', idx)) if not pd.isna(row.get('Nro', idx)) else idx,
            'Identificacion': str(row.get('Identificacion', '')).strip(),
            'Nombres': str(row.get('Nombres', '')).strip(),
            'Apellidos': str(row.get('Apellidos', '')).strip(),
            'NroCuenta': limpiar_nro_cuenta(row.get('NroCuenta', '')),
            'Saldo': float(row.get('Saldo', 0)) if not pd.isna(row.get('Saldo', 0)) else 0.0
        }
        
        if not fila['NroCuenta']:
            fila['NroCuenta'] = f"CUENTA_{fila['Nro']}_{banco_id}"
        
        batches[banco_id].append(fila)
        total_procesados += 1
        
        # Procesar cuando se alcanza el tamaño de lote
        if len(batches[banco_id]) >= BATCH_SIZE:
            batch_a_procesar = batches[banco_id][:BATCH_SIZE]
            batches[banco_id] = batches[banco_id][BATCH_SIZE:]
            insertados = procesar_batch_banco(banco_id, batch_a_procesar)
            if insertados > 0:
                logger.info(f"  Banco {banco_id}: +{insertados} (Total: {total_procesados}/{total_registros})")
    
    # Procesar batches restantes
    total_insertados = 0
    for banco_id, batch in batches.items():
        if batch:
            insertados = procesar_batch_banco(banco_id, batch)
            total_insertados += insertados
            if insertados > 0:
                logger.info(f"  Banco {banco_id} ({NOMBRES_BANCOS[banco_id]}): +{insertados} registros")
    
    tiempo_total = time.time() - inicio_total
    logger.info("Carga completada en %.2f segundos (%.2f minutos)", tiempo_total, tiempo_total/60)
    logger.info("Total registros insertados: %d", total_insertados)
    
    return total_insertados

# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    print("="*70)
    print("CARGA BATCH INSERT - 14 BANCOS (MAXIMA VELOCIDAD)")
    print("="*70)
    
    # Crear carpetas necesarias
    for carpeta in ['data/input', 'data/processed', 'data/logs']:
        Path(carpeta).mkdir(parents=True, exist_ok=True)
    
    # Buscar archivo CSV
    archivos = glob.glob("data/input/*.csv")
    if not archivos:
        print("No hay archivos CSV en data/input/")
        print("Coloca tu archivo en: data/input/")
        print("Columnas requeridas: Nro,Identificacion,Nombres,Apellidos,NroCuenta,IdBanco,Saldo")
        sys.exit(1)
    
    archivo = archivos[0]
    print(f"Archivo a procesar: {archivo}")
    print(f"Tamano: {os.path.getsize(archivo) / 1024 / 1024:.2f} MB")
    print()
    
    # Ejecutar carga
    inicio = datetime.now()
    try:
        cargar_datos_rapido(archivo)
    except KeyboardInterrupt:
        print("\nProceso interrumpido por el usuario")
        sys.exit(1)
    except Exception as e:
        print(f"\nError: {e}")
        sys.exit(1)
    
    fin = datetime.now()
    print(f"\nTiempo total de ejecucion: {(fin - inicio).total_seconds():.2f} segundos")
    
    # Mover archivo procesado
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    nombre_original = Path(archivo).stem
    destino = f"data/processed/{nombre_original}_{timestamp}.csv"
    os.rename(archivo, destino)
    print(f"Archivo procesado movido a: {destino}")
    
    print("\nPROCESO COMPLETADO EXITOSAMENTE")