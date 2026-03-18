#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
SCRIPT DEFINITIVO - CARGA DATOS A LOS 14 BANCOS
================================================
Versión con Oracle en lugar de SQL Server
Soporta: MySQL, PostgreSQL, Oracle, MongoDB, Redis
"""

import os
import sys
import pandas as pd
from datetime import datetime
import logging
from pathlib import Path
import json
import shutil
import glob
import time

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data/logs/carga_datos.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ============================================================
# CONFIGURACION DE BANCOS
# ============================================================

NOMBRES_BANCOS = {
    1: "Banco Union",
    2: "Banco Mercantil",
    3: "Banco BNB",
    4: "Banco BCP",
    5: "Banco BISA",
    6: "Banco Ganadero",
    7: "Banco Economico",
    8: "Banco Prodem",
    9: "Banco Solidario",
    10: "Banco Fortaleza",
    11: "Banco FIE",
    12: "Banco PYME",
    13: "Banco BDP",
    14: "Banco Argentina",
}

ALGORITMOS_BANCOS = {
    1: "Cifrado Cesar",
    2: "Cifrado Atbash",
    3: "Cifrado Vigenere",
    4: "Cifrado Playfair",
    5: "Cifrado Hill",
    6: "DES",
    7: "3DES",
    8: "Blowfish",
    9: "Twofish",
    10: "AES",
    11: "RSA",
    12: "ElGamal",
    13: "ECC",
    14: "ChaCha20",
}

# ============================================================
# FUNCIONES DE CREACION DE TABLAS
# ============================================================

def crear_tablas_mysql():
    """Crear tabla en MySQL si no existe"""
    try:
        import mysql.connector
        conn = mysql.connector.connect(
            host='localhost',
            port=3307,
            user='root',
            password='root123',
            connection_timeout=10
        )
        cursor = conn.cursor()
        
        cursor.execute("CREATE DATABASE IF NOT EXISTS banco_union")
        cursor.execute("USE banco_union")
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS Cuentas (
                Id BIGINT PRIMARY KEY AUTO_INCREMENT,
                NroIdentificacion VARCHAR(20) NOT NULL,
                Nombres VARCHAR(100) NOT NULL,
                Apellidos VARCHAR(100) NOT NULL,
                NroCuenta VARCHAR(30) NOT NULL UNIQUE,
                IdBanco INT NOT NULL,
                SaldoUSD DECIMAL(18,4) NOT NULL,
                SaldoBs DECIMAL(18,4) NULL,
                TipoCambioAplicado DECIMAL(10,4) NULL,
                FechaConversion DATETIME NULL,
                CodigoVerificacion CHAR(8) NULL,
                LoteId VARCHAR(50) NULL,
                DatosCifrados TEXT,
                AlgoritmoUsado VARCHAR(50) DEFAULT 'Cifrado Cesar',
                FechaRegistro DATETIME DEFAULT CURRENT_TIMESTAMP,
                FechaActualizacion DATETIME ON UPDATE CURRENT_TIMESTAMP,
                INDEX idx_nrocuenta (NroCuenta),
                INDEX idx_idbanco (IdBanco)
            )
        """)
        conn.commit()
        cursor.close()
        conn.close()
        logger.info("✅ Tabla MySQL creada/verificada")
        return True
    except Exception as e:
        logger.error(f"❌ Error creando tabla MySQL: {e}")
        return False

def crear_tablas_postgresql():
    """Crear tabla en PostgreSQL si no existe (con minúsculas)"""
    try:
        import psycopg2
        conn = psycopg2.connect(
            host='localhost',
            port=5432,
            user='root',
            password='root123',
            database='banco_mercantil',
            connect_timeout=10
        )
        cursor = conn.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS cuentas (
                id BIGSERIAL PRIMARY KEY,
                nroidentificacion VARCHAR(20) NOT NULL,
                nombres VARCHAR(100) NOT NULL,
                apellidos VARCHAR(100) NOT NULL,
                nrocuenta VARCHAR(30) NOT NULL UNIQUE,
                idbanco INT NOT NULL,
                saldousd DECIMAL(18,4) NOT NULL,
                saldobs DECIMAL(18,4) NULL,
                tipocambioaplicado DECIMAL(10,4) NULL,
                fechaconversion TIMESTAMP NULL,
                codigoverificacion CHAR(8) NULL,
                loteid VARCHAR(50) NULL,
                datoscifrados TEXT,
                algoritmousado VARCHAR(50) DEFAULT 'Cifrado Atbash',
                fecharegistro TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                fechaactualizacion TIMESTAMP NULL
            )
        """)
        
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_nrocuenta ON cuentas(nrocuenta)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_idbanco ON cuentas(idbanco)")
        
        conn.commit()
        cursor.close()
        conn.close()
        logger.info("✅ Tabla PostgreSQL creada/verificada")
        return True
    except Exception as e:
        logger.error(f"❌ Error creando tabla PostgreSQL: {e}")
        return False

def crear_tablas_oracle():
    """Crear tabla en Oracle si no existe"""
    try:
        import cx_Oracle
        
        # Conectar como sysdba para crear usuario
        dsn = cx_Oracle.makedsn('localhost', 1521, service_name='XEPDB1')
        conn = cx_Oracle.connect(
            user='system',
            password='root123',
            dsn=dsn,
            timeout=10
        )
        cursor = conn.cursor()
        
        # Crear usuario si no existe
        try:
            cursor.execute("""
                BEGIN
                    EXECUTE IMMEDIATE 'CREATE USER BANCO_BISA IDENTIFIED BY root123';
                    EXECUTE IMMEDIATE 'GRANT CONNECT, RESOURCE TO BANCO_BISA';
                    EXECUTE IMMEDIATE 'GRANT UNLIMITED TABLESPACE TO BANCO_BISA';
                EXCEPTION
                    WHEN OTHERS THEN
                        IF SQLCODE != -01920 THEN RAISE; END IF;
                END;
            """)
            conn.commit()
        except:
            pass
        
        cursor.close()
        conn.close()
        
        # Conectar como el usuario para crear tabla
        conn = cx_Oracle.connect(
            user='BANCO_BISA',
            password='root123',
            dsn=dsn,
            timeout=10
        )
        cursor = conn.cursor()
        
        cursor.execute("""
            BEGIN
                EXECUTE IMMEDIATE '
                    CREATE TABLE Cuentas (
                        Id NUMBER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                        NroIdentificacion VARCHAR2(20) NOT NULL,
                        Nombres VARCHAR2(100) NOT NULL,
                        Apellidos VARCHAR2(100) NOT NULL,
                        NroCuenta VARCHAR2(30) NOT NULL UNIQUE,
                        IdBanco NUMBER(2) DEFAULT 5 NOT NULL,
                        SaldoUSD NUMBER(18,4) NOT NULL,
                        SaldoBs NUMBER(18,4) NULL,
                        TipoCambioAplicado NUMBER(10,4) NULL,
                        FechaConversion TIMESTAMP NULL,
                        CodigoVerificacion CHAR(8) NULL,
                        LoteId VARCHAR2(50) NULL,
                        DatosCifrados CLOB,
                        AlgoritmoUsado VARCHAR2(50) DEFAULT ''Cifrado Hill'',
                        FechaRegistro TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        FechaActualizacion TIMESTAMP NULL
                    )
                ';
                EXCEPTION
                    WHEN OTHERS THEN
                        IF SQLCODE != -00955 THEN RAISE; END IF;
            END;
        """)
        
        # Crear índices
        try:
            cursor.execute("CREATE INDEX idx_nrocuenta ON Cuentas(NroCuenta)")
        except: pass
        try:
            cursor.execute("CREATE INDEX idx_idbanco ON Cuentas(IdBanco)")
        except: pass
        
        conn.commit()
        cursor.close()
        conn.close()
        logger.info("✅ Tabla Oracle creada/verificada")
        return True
    except Exception as e:
        logger.error(f"❌ Error creando tabla Oracle: {e}")
        return False

# ============================================================
# CONEXIONES A BASES DE DATOS
# ============================================================

def conectar_mysql(banco_id):
    """Conectar a MySQL (Banco Union)"""
    try:
        import mysql.connector
        if banco_id == 1:
            conn = mysql.connector.connect(
                host='localhost',
                port=3307,
                database='banco_union',
                user='root',
                password='root123',
                connection_timeout=10
            )
            return conn
        return None
    except Exception as e:
        logger.error(f"❌ Error conectando a MySQL: {e}")
        return None

def conectar_postgresql(banco_id):
    """Conectar a PostgreSQL (Banco Mercantil)"""
    try:
        import psycopg2
        if banco_id == 2:
            conn = psycopg2.connect(
                host='localhost',
                port=5432,
                database='banco_mercantil',
                user='root',
                password='root123',
                connect_timeout=10
            )
            return conn
        return None
    except Exception as e:
        logger.error(f"❌ Error conectando a PostgreSQL: {e}")
        return None

def conectar_oracle(banco_id):
    """Conectar a Oracle (Banco BISA)"""
    try:
        import cx_Oracle
        if banco_id == 5:
            dsn = cx_Oracle.makedsn('localhost', 1521, service_name='XEPDB1')
            conn = cx_Oracle.connect(
                user='BANCO_BISA',
                password='root123',
                dsn=dsn,
                timeout=10
            )
            logger.info("✅ Conectado a Oracle Banco BISA")
            return conn
        return None
    except Exception as e:
        logger.error(f"❌ Error conectando a Oracle: {e}")
        return None

def conectar_mongodb(banco_id):
    """Conectar a MongoDB"""
    try:
        from pymongo import MongoClient
        puertos = {
            3: 27017,   # BNB
            6: 27018,   # Ganadero
            7: 27019,   # Economico
            8: 27020,   # Prodem
            9: 27021,   # Solidario
            10: 27022,  # Fortaleza
            11: 27023,  # FIE
            12: 27024,  # PYME
            13: 27025,  # BDP
            14: 27026,  # Argentina
        }
        puerto = puertos.get(banco_id)
        if puerto:
            client = MongoClient(f'mongodb://root:root123@localhost:{puerto}/', serverSelectionTimeoutMS=5000)
            client.admin.command('ping')
            nombre_db = f"banco_{NOMBRES_BANCOS[banco_id].lower().replace(' ', '_')}"
            db = client[nombre_db]
            return db
        return None
    except Exception as e:
        logger.error(f"❌ Error conectando a MongoDB: {e}")
        return None

def conectar_redis(banco_id):
    """Conectar a Redis (Banco BCP)"""
    try:
        import redis
        if banco_id == 4:
            redis_client = redis.Redis(
                host='localhost',
                port=6379,
                password='root123',
                decode_responses=True,
                socket_connect_timeout=5
            )
            redis_client.ping()
            return redis_client
        return None
    except Exception as e:
        logger.error(f"❌ Error conectando a Redis: {e}")
        return None

# ============================================================
# FUNCIONES DE VERIFICACION
# ============================================================

def cuenta_existe_mysql(conn, nro_cuenta):
    """Verificar si una cuenta existe en MySQL"""
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM Cuentas WHERE NroCuenta = %s", (str(nro_cuenta),))
        existe = cursor.fetchone()[0] > 0
        cursor.close()
        return existe
    except Exception as e:
        logger.error(f"Error verificando MySQL: {e}")
        return False

def cuenta_existe_postgresql(conn, nro_cuenta):
    """Verificar si una cuenta existe en PostgreSQL"""
    try:
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM cuentas WHERE nrocuenta = %s', (str(nro_cuenta),))
        existe = cursor.fetchone()[0] > 0
        cursor.close()
        return existe
    except Exception as e:
        logger.error(f"Error verificando PostgreSQL: {e}")
        return False

def cuenta_existe_oracle(conn, nro_cuenta):
    """Verificar si una cuenta existe en Oracle"""
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM Cuentas WHERE NroCuenta = :1", (str(nro_cuenta),))
        existe = cursor.fetchone()[0] > 0
        cursor.close()
        return existe
    except Exception as e:
        logger.error(f"Error verificando Oracle: {e}")
        return False

def cuenta_existe_mongodb(db, nro_cuenta):
    """Verificar si una cuenta existe en MongoDB"""
    try:
        if db is None:
            return False
        collection = db['Cuentas']
        return collection.count_documents({'NroCuenta': str(nro_cuenta)}) > 0
    except Exception as e:
        logger.error(f"Error verificando MongoDB: {e}")
        return False

def cuenta_existe_redis(redis_client, nro_cuenta):
    """Verificar si una cuenta existe en Redis"""
    try:
        if redis_client is None:
            return False
        return redis_client.exists(f"cuenta:{nro_cuenta}") > 0
    except Exception as e:
        logger.error(f"Error verificando Redis: {e}")
        return False

# ============================================================
# FUNCIONES DE INSERCION
# ============================================================

def insertar_en_mysql(conn, row):
    """Insertar registro en MySQL"""
    try:
        cursor = conn.cursor()
        sql = """
            INSERT INTO Cuentas (
                NroIdentificacion, Nombres, Apellidos, NroCuenta, 
                IdBanco, SaldoUSD, DatosCifrados, AlgoritmoUsado
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        valores = (
            str(row['Identificacion']),
            str(row['Nombres']),
            str(row['Apellidos']),
            str(row['NroCuenta']),
            int(row['IdBanco']),
            float(row['Saldo']),
            f"CIFRADO_{row['IdBanco']}_{row['Nro']}",
            ALGORITMOS_BANCOS[row['IdBanco']]
        )
        cursor.execute(sql, valores)
        conn.commit()
        cursor.close()
        return True
    except Exception as e:
        logger.error(f"Error insertando en MySQL: {e}")
        return False

def insertar_en_postgresql(conn, row):
    """Insertar registro en PostgreSQL"""
    try:
        cursor = conn.cursor()
        sql = """
            INSERT INTO cuentas (
                nroidentificacion, nombres, apellidos, nrocuenta, 
                idbanco, saldousd, datoscifrados, algoritmousado
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        valores = (
            str(row['Identificacion']),
            str(row['Nombres']),
            str(row['Apellidos']),
            str(row['NroCuenta']),
            int(row['IdBanco']),
            float(row['Saldo']),
            f"CIFRADO_{row['IdBanco']}_{row['Nro']}",
            ALGORITMOS_BANCOS[row['IdBanco']]
        )
        cursor.execute(sql, valores)
        conn.commit()
        cursor.close()
        return True
    except Exception as e:
        logger.error(f"Error insertando en PostgreSQL: {e}")
        return False

def insertar_en_oracle(conn, row):
    """Insertar registro en Oracle"""
    try:
        cursor = conn.cursor()
        sql = """
            INSERT INTO Cuentas (
                NroIdentificacion, Nombres, Apellidos, NroCuenta, 
                IdBanco, SaldoUSD, DatosCifrados, AlgoritmoUsado
            ) VALUES (
                :1, :2, :3, :4, :5, :6, :7, :8
            )
        """
        cursor.execute(sql, (
            str(row['Identificacion']),
            str(row['Nombres']),
            str(row['Apellidos']),
            str(row['NroCuenta']),
            int(row['IdBanco']),
            float(row['Saldo']),
            f"CIFRADO_{row['IdBanco']}_{row['Nro']}",
            ALGORITMOS_BANCOS[row['IdBanco']]
        ))
        conn.commit()
        cursor.close()
        return True
    except Exception as e:
        logger.error(f"Error insertando en Oracle: {e}")
        return False

def insertar_en_mongodb(db, row):
    """Insertar registro en MongoDB"""
    try:
        if db is None:
            return False
        collection = db['Cuentas']
        documento = {
            'Nro': int(row['Nro']),
            'NroIdentificacion': str(row['Identificacion']),
            'Nombres': str(row['Nombres']),
            'Apellidos': str(row['Apellidos']),
            'NroCuenta': str(row['NroCuenta']),
            'IdBanco': int(row['IdBanco']),
            'SaldoUSD': float(row['Saldo']),
            'SaldoBs': None,
            'DatosCifrados': f"CIFRADO_{row['IdBanco']}_{row['Nro']}",
            'AlgoritmoUsado': ALGORITMOS_BANCOS[row['IdBanco']],
            'FechaRegistro': datetime.now()
        }
        collection.insert_one(documento)
        return True
    except Exception as e:
        logger.error(f"Error insertando en MongoDB: {e}")
        return False

def insertar_en_redis(redis_client, row):
    """Insertar registro en Redis"""
    try:
        if redis_client is None:
            return False
        key = f"cuenta:{row['NroCuenta']}"
        value = {
            'nro': str(row['Nro']),
            'identificacion': str(row['Identificacion']),
            'nombres': str(row['Nombres']),
            'apellidos': str(row['Apellidos']),
            'id_banco': int(row['IdBanco']),
            'saldo_usd': float(row['Saldo']),
            'datos_cifrados': f"CIFRADO_{row['IdBanco']}_{row['Nro']}",
            'algoritmo': ALGORITMOS_BANCOS[row['IdBanco']]
        }
        redis_client.hset(key, mapping=value)
        return True
    except Exception as e:
        logger.error(f"Error insertando en Redis: {e}")
        return False

def limpiar_valor_cuenta(valor):
    """Limpia el valor de cuenta (notacion cientifica)"""
    if pd.isna(valor):
        return ""
    valor_str = str(valor).strip()
    if 'E+' in valor_str or 'e+' in valor_str:
        try:
            return str(int(float(valor_str)))
        except:
            return valor_str
    return valor_str

# ============================================================
# FUNCION PRINCIPAL
# ============================================================

def cargar_datos_desde_archivo(archivo):
    """Lee el archivo y carga los datos"""
    logger.info(f"🚀 Iniciando carga de datos desde: {archivo}")
    logger.info("="*60)
    
    # Crear directorios
    for carpeta in ['data/processed', 'data/errors', 'data/logs']:
        Path(carpeta).mkdir(parents=True, exist_ok=True)
    
    # Leer archivo
    try:
        df = pd.read_csv(archivo, encoding='latin-1')
        df.columns = df.columns.str.replace('ï»¿', '').str.strip()
        
        logger.info(f"✅ Archivo leido: {len(df)} registros")
        logger.info(f"📋 Columnas: {list(df.columns)}")
        
    except Exception as e:
        logger.error(f"❌ Error leyendo archivo: {e}")
        return
    
    # Crear tablas
    logger.info("🔧 Verificando/Creando tablas en bases de datos...")
    crear_tablas_mysql()
    crear_tablas_postgresql()
    crear_tablas_oracle()
    time.sleep(2)
    
    total_registros = len(df)
    exitosos = 0
    duplicados = 0
    errores = []
    registros_por_banco = {i: 0 for i in range(1, 15)}
    
    logger.info("🔄 Procesando registros...")
    
    for idx, row in df.iterrows():
        try:
            if pd.isna(row.get('IdBanco')):
                continue
            
            banco_id = int(row['IdBanco'])
            nro_cuenta = limpiar_valor_cuenta(row.get('NroCuenta', ''))
            
            fila_limpia = {
                'Nro': int(row.get('Nro', idx)) if not pd.isna(row.get('Nro', idx)) else idx,
                'Identificacion': str(row.get('Identificacion', '')).strip(),
                'Nombres': str(row.get('Nombres', '')).strip(),
                'Apellidos': str(row.get('Apellidos', '')).strip(),
                'NroCuenta': nro_cuenta,
                'IdBanco': banco_id,
                'Saldo': float(row.get('Saldo', 0)) if not pd.isna(row.get('Saldo', 0)) else 0.0
            }
            
            if not fila_limpia['NroCuenta']:
                fila_limpia['NroCuenta'] = f"CUENTA_{fila_limpia['Nro']}_{banco_id}"
            
            insertado = False
            
            if banco_id == 1:  # MySQL - Union
                conn = conectar_mysql(banco_id)
                if conn is not None:
                    if not cuenta_existe_mysql(conn, fila_limpia['NroCuenta']):
                        if insertar_en_mysql(conn, fila_limpia):
                            insertado = True
                    else:
                        duplicados += 1
                    conn.close()
                    
            elif banco_id == 2:  # PostgreSQL - Mercantil
                conn = conectar_postgresql(banco_id)
                if conn is not None:
                    if not cuenta_existe_postgresql(conn, fila_limpia['NroCuenta']):
                        if insertar_en_postgresql(conn, fila_limpia):
                            insertado = True
                    else:
                        duplicados += 1
                    conn.close()
                    
            elif banco_id == 5:  # Oracle - BISA
                conn = conectar_oracle(banco_id)
                if conn is not None:
                    if not cuenta_existe_oracle(conn, fila_limpia['NroCuenta']):
                        if insertar_en_oracle(conn, fila_limpia):
                            insertado = True
                    else:
                        duplicados += 1
                    conn.close()
            
            elif banco_id in [3, 6, 7, 8, 9, 10, 11, 12, 13, 14]:  # MongoDB
                db = conectar_mongodb(banco_id)
                if db is not None:
                    if not cuenta_existe_mongodb(db, fila_limpia['NroCuenta']):
                        if insertar_en_mongodb(db, fila_limpia):
                            insertado = True
                    else:
                        duplicados += 1
                    
            elif banco_id == 4:  # Redis - BCP
                redis_client = conectar_redis(banco_id)
                if redis_client is not None:
                    if not cuenta_existe_redis(redis_client, fila_limpia['NroCuenta']):
                        if insertar_en_redis(redis_client, fila_limpia):
                            insertado = True
                    else:
                        duplicados += 1
            
            if insertado:
                exitosos += 1
                registros_por_banco[banco_id] = registros_por_banco.get(banco_id, 0) + 1
            
            if (idx + 1) % 1000 == 0:
                logger.info(f"  Procesados {idx + 1} registros... (Insertados: {exitosos}, Duplicados: {duplicados})")
                
        except Exception as e:
            error_msg = f"Error en registro {idx}: {str(e)}"
            logger.error(error_msg)
            errores.append({
                'fila': idx,
                'registro': row.to_dict() if hasattr(row, 'to_dict') else dict(row),
                'error': str(e)
            })
    
    # Resumen final
    logger.info("="*60)
    logger.info(f"✅ CARGA COMPLETADA")
    logger.info(f"📊 Total registros: {total_registros}")
    logger.info(f"✅ Nuevos insertados: {exitosos}")
    logger.info(f"🔄 Duplicados saltados: {duplicados}")
    logger.info(f"❌ Errores: {len(errores)}")
    logger.info("📊 Registros por banco:")
    
    for banco_id in sorted(registros_por_banco.keys()):
        count = registros_por_banco[banco_id]
        if count > 0:
            logger.info(f"   Banco {banco_id} - {NOMBRES_BANCOS.get(banco_id)}: {count}")
    
    if errores:
        with open('data/errors/registros_error.json', 'w', encoding='utf-8') as f:
            json.dump(errores, f, indent=2, ensure_ascii=False, default=str)
        logger.info(f"❌ Detalles guardados en data/errors/registros_error.json")
    
    # Mover archivo procesado
    try:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        nombre_archivo = Path(archivo).stem
        extension = Path(archivo).suffix
        archivo_procesado = f"data/processed/{nombre_archivo}_{timestamp}{extension}"
        shutil.move(archivo, archivo_procesado)
        logger.info(f"📁 Archivo movido a: {archivo_procesado}")
    except Exception as e:
        logger.error(f"Error moviendo archivo: {e}")

# ============================================================
# PUNTO DE ENTRADA
# ============================================================

if __name__ == "__main__":
    print("="*60)
    print("🚀 CARGADOR DE DATOS PARA 14 BANCOS - ASFI (CON ORACLE)")
    print("="*60)
    
    Path("data/input").mkdir(parents=True, exist_ok=True)
    
    archivos = glob.glob("data/input/*.csv") + glob.glob("data/input/*.xlsx") + glob.glob("data/input/*.xls")
    
    if not archivos:
        logger.error("❌ No se encontraron archivos en data/input/")
        logger.info("📁 Coloca tu archivo CSV/Excel en: data/input/")
        sys.exit(1)
    
    logger.info("📁 Archivos encontrados:")
    for i, archivo in enumerate(archivos):
        logger.info(f"   {i+1}. {archivo}")
    
    archivo_a_procesar = archivos[0]
    logger.info(f"\n📄 Procesando: {archivo_a_procesar}")
    cargar_datos_desde_archivo(archivo_a_procesar)