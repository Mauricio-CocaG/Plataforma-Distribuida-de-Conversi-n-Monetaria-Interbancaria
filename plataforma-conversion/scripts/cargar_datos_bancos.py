#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
SCRIPT ULTRA OPTIMIZADO - 14 BANCOS
====================================
Versión ultra rápida que procesa los 14 bancos en paralelo
Tiempo estimado: 45-60 segundos para 123,790 registros
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
import uuid
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

# ============================================================
# CONFIGURACION - TODOS LOS 14 BANCOS
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

# Configuración de conexiones - MÁS RÁPIDA
DB_CONFIG = {
    1: { 'type': 'mysql', 'host': 'localhost', 'port': 3307, 'database': 'banco_union', 
         'user': 'root', 'password': 'root123', 'table': 'Cuentas' },
    2: { 'type': 'postgresql', 'host': 'localhost', 'port': 5432, 'database': 'banco_mercantil', 
         'user': 'root', 'password': 'root123', 'table': 'cuentas' },
    3: { 'type': 'mongodb', 'host': 'localhost', 'port': 27017, 'database': 'banco_bnb', 
         'user': 'root', 'password': 'root123', 'collection': 'Cuentas' },
    4: { 'type': 'redis', 'host': 'localhost', 'port': 6379, 'password': 'root123' },
    5: { 'type': 'oracle', 'host': 'localhost', 'port': 1521, 'service': 'XEPDB1', 
         'user': 'BANCO_BISA', 'password': 'root123', 'table': 'Cuentas' },
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
          'database': 'banco_argentina', 'user': 'neo4j', 'password': 'root123' },
}

# ============================================================
# FUNCIONES DE CONEXION - VERSIÓN LIGHT
# ============================================================

def get_connection(config):
    """Obtiene conexión según tipo - VERSIÓN LIGHT"""
    try:
        if config['type'] == 'mysql':
            import mysql.connector
            return mysql.connector.connect(
                host=config['host'], port=config['port'],
                database=config['database'], user=config['user'],
                password=config['password'], connection_timeout=3,
                autocommit=False, use_pure=True
            )
        
        elif config['type'] == 'postgresql':
            import psycopg2
            return psycopg2.connect(
                host=config['host'], port=config['port'],
                database=config['database'], user=config['user'],
                password=config['password'], connect_timeout=3
            )
        
        elif config['type'] == 'oracle':
            import oracledb
            oracledb.defaults.fetch_lobs = False
            dsn = oracledb.makedsn(config['host'], config['port'], service_name=config['service'])
            return oracledb.connect(
                user=config['user'], password=config['password'],
                dsn=dsn, timeout=3
            )
        
        elif config['type'] == 'mongodb':
            from pymongo import MongoClient
            client = MongoClient(
                f"mongodb://{config['user']}:{config['password']}@{config['host']}:{config['port']}/",
                serverSelectionTimeoutMS=3000, connectTimeoutMS=3000
            )
            return client[config['database']]
        
        elif config['type'] == 'redis':
            import redis
            return redis.Redis(
                host=config['host'], port=config['port'],
                password=config['password'], decode_responses=True,
                socket_connect_timeout=3, socket_timeout=3
            )
        elif db_type == 'neo4j':
            # Importamos el repositorio que tienes en docker/neo4j
            sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'docker', 'neo4j')))
            from graph_repository import GraphRepository
            return GraphRepository()
    except Exception as e:
        logger.warning(f"  Error conectando a banco {config.get('type')}: {e}")
        return None

# ============================================================
# FUNCIONES DE INSERCION - MÁS RÁPIDAS
# ============================================================

def insertar_mysql(conn, batch, banco_id):
    cursor = conn.cursor()
    sql = f"INSERT IGNORE INTO Cuentas (NroIdentificacion, Nombres, Apellidos, NroCuenta, IdBanco, SaldoUSD, DatosCifrados, AlgoritmoUsado) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
    valores = [(r['id'], r['nombres'], r['apellidos'], r['cuenta'], banco_id, r['saldo'], f"CIF_{banco_id}_{r['nro']}", ALGORITMOS_BANCOS[banco_id]) for r in batch]
    cursor.executemany(sql, valores)
    conn.commit()
    cnt = cursor.rowcount
    cursor.close()
    return cnt

def insertar_postgresql(conn, batch, banco_id):
    cursor = conn.cursor()
    sql = f"INSERT INTO cuentas (nroidentificacion, nombres, apellidos, nrocuenta, idbanco, saldousd, datoscifrados, algoritmousado) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (nrocuenta) DO NOTHING"
    valores = [(r['id'], r['nombres'], r['apellidos'], r['cuenta'], banco_id, r['saldo'], f"CIF_{banco_id}_{r['nro']}", ALGORITMOS_BANCOS[banco_id]) for r in batch]
    cursor.executemany(sql, valores)
    conn.commit()
    cnt = cursor.rowcount
    cursor.close()
    return cnt

def insertar_oracle(conn, batch, banco_id):
    cursor = conn.cursor()
    sql = f"INSERT INTO Cuentas (NroIdentificacion, Nombres, Apellidos, NroCuenta, IdBanco, SaldoUSD, DatosCifrados, AlgoritmoUsado) VALUES (:1, :2, :3, :4, :5, :6, :7, :8)"
    cnt = 0
    for r in batch:
        try:
            cursor.execute(sql, (r['id'], r['nombres'], r['apellidos'], r['cuenta'], banco_id, r['saldo'], f"CIF_{banco_id}_{r['nro']}", ALGORITMOS_BANCOS[banco_id]))
            cnt += 1
        except:
            pass
    conn.commit()
    cursor.close()
    return cnt

def insertar_mongodb(db, batch, banco_id):
    collection = db['Cuentas']
    docs = [{'Nro': r['nro'], 'NroIdentificacion': r['id'], 'Nombres': r['nombres'], 'Apellidos': r['apellidos'], 
             'NroCuenta': r['cuenta'], 'IdBanco': banco_id, 'SaldoUSD': r['saldo'], 'SaldoBs': None,
             'DatosCifrados': f"CIF_{banco_id}_{r['nro']}", 'AlgoritmoUsado': ALGORITMOS_BANCOS[banco_id],
             'FechaRegistro': datetime.now()} for r in batch]
    result = collection.insert_many(docs, ordered=False)
    return len(result.inserted_ids)

def insertar_redis(conn, batch, banco_id):
    pipe = conn.pipeline()
    for r in batch:
        pipe.hset(f"cuenta:{r['cuenta']}", mapping={'nro': r['nro'], 'id': r['id'], 'nombres': r['nombres'], 
                   'apellidos': r['apellidos'], 'saldo': r['saldo'], 'algoritmo': ALGORITMOS_BANCOS[banco_id]})
    pipe.execute()
    return len(batch)
def insertar_batch_neo4j(repo, batch, banco_id):
    """Insertar lote en Neo4j usando tu GraphRepository"""
    if repo is None:
        return 0
    
    for r in batch:
        # Usamos tu método existente para registrar el flujo
        repo.registrar_flujo_interbancario(
            banco_origen="ASFI_CARGA_INICIAL", 
            banco_destino=NOMBRES_BANCOS[banco_id], 
            monto=float(r['Saldo'])
        )
    return len(batch)

INSERTORES = {
    'mysql': insertar_mysql,
    'postgresql': insertar_postgresql,
    'oracle': insertar_oracle,
    'mongodb': insertar_mongodb,
    'redis': insertar_redis,
    'neo4j': insertar_batch_neo4j
}

# ============================================================
# FUNCION AUXILIAR
# ============================================================

def limpiar_cuenta(valor):
    if pd.isna(valor): return ""
    v = str(valor).strip()
    if 'E+' in v or 'e+' in v:
        try: return str(int(float(v)))
        except: return v
    return v

# ============================================================
# PROCESADOR POR BANCO (PARA PARALELIZAR)
# ============================================================

def procesar_banco(banco_id, batch):
    """Procesa un banco individualmente - para paralelización"""
    if not batch:
        return 0
    
    config = DB_CONFIG[banco_id].copy()
    db_type = config['type']
    
    try:
        conn = get_connection(config)
        if conn is None:
            return 0
        
        insert_func = INSERTORES[db_type]
        insertados = insert_func(conn, batch, banco_id)
        
        if db_type in ['mysql', 'postgresql', 'oracle']:
            conn.close()
        elif db_type == 'mongodb':
            conn.client.close()
        
        return insertados
    except Exception as e:
        logger.warning(f"  Error en banco {banco_id}: {e}")
        return 0

# ============================================================
# FUNCION PRINCIPAL - CON PROCESAMIENTO EN PARALELO
# ============================================================

def cargar_datos_paralelo(archivo):
    """Carga datos usando procesamiento paralelo por banco"""
    logger.info(f"🚀 Iniciando carga paralela desde: {archivo}")
    inicio_total = time.time()
    
    # Leer CSV
    df = pd.read_csv(archivo, encoding='latin-1')
    df.columns = df.columns.str.replace('ï»¿', '').str.strip()
    total_registros = len(df)
    logger.info(f"📊 Total registros: {total_registros}")
    
    # Preparar batches por banco (lotes más grandes)
    batches = {i: [] for i in range(1, 15)}
    BATCH_SIZE = 5000  # ¡LOTES MÁS GRANDES!
    
    for idx, row in df.iterrows():
        if pd.isna(row.get('IdBanco')):
            continue
        
        banco_id = int(row['IdBanco'])
        
        fila = {
            'nro': int(row.get('Nro', idx)) if not pd.isna(row.get('Nro', idx)) else idx,
            'id': str(row.get('Identificacion', '')).strip(),
            'nombres': str(row.get('Nombres', '')).strip()[:50],
            'apellidos': str(row.get('Apellidos', '')).strip()[:50],
            'cuenta': limpiar_cuenta(row.get('NroCuenta', ''))[:30],
            'saldo': float(row.get('Saldo', 0)) if not pd.isna(row.get('Saldo', 0)) else 0.0
        }
        
        if not fila['cuenta']:
            fila['cuenta'] = f"CTA_{fila['nro']}_{banco_id}"
        
        batches[banco_id].append(fila)
    
    # Procesar en PARALELO usando ThreadPoolExecutor
    total_insertados = 0
    bancos_con_datos = [bid for bid, b in batches.items() if b]
    
    logger.info(f"\n🔄 Procesando {len(bancos_con_datos)} bancos en PARALELO...")
    
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(procesar_banco, bid, batches[bid]): bid for bid in bancos_con_datos}
        
        for future in as_completed(futures):
            bid = futures[future]
            try:
                insertados = future.result(timeout=30)
                total_insertados += insertados
                if insertados > 0:
                    logger.info(f"  ✅ Banco {bid} ({NOMBRES_BANCOS[bid]}): +{insertados}")
            except Exception as e:
                logger.warning(f"  ⚠️ Banco {bid}: Error - {e}")
    
    tiempo_total = time.time() - inicio_total
    logger.info(f"\n✅ CARGA COMPLETADA en {tiempo_total:.2f} segundos ({tiempo_total/60:.2f} minutos)")
    logger.info(f"📊 Total registros insertados: {total_insertados}")
    
    return total_insertados

# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    print("="*70)
    print("🚀 CARGA PARALELA ULTRA RÁPIDA - 14 BANCOS")
    print("="*70)
    
    Path("data/input").mkdir(parents=True, exist_ok=True)
    
    archivos = glob.glob("data/input/*.csv")
    if not archivos:
        print("❌ No hay archivos CSV en data/input/")
        sys.exit(1)
    
    archivo = archivos[0]
    print(f"📄 Archivo: {archivo}")
    print(f"📊 Tamaño: {os.path.getsize(archivo)/1024/1024:.2f} MB")
    print("="*70)
    
    inicio = datetime.now()
    try:
        cargar_datos_paralelo(archivo)
    except KeyboardInterrupt:
        print("\n⚠️ Proceso interrumpido")
        sys.exit(1)
    
    fin = datetime.now()
    print(f"\n⏱️ Tiempo total: {(fin-inicio).total_seconds():.2f} segundos")