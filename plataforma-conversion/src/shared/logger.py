#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Módulo de Logs de Auditoría para ASFI
======================================
Registra todas las operaciones de conversión monetaria
en tabla MySQL y archivos rotativos
"""

import logging
import logging.handlers
import mysql.connector
from datetime import datetime
from pathlib import Path
import json
import os

class AuditoriaLogger:
    """
    Clase para manejar logs de auditoría de conversiones
    """
    
    def __init__(self, db_config=None, log_dir="logs/auditoria"):
        """
        Inicializa el logger de auditoría
        
        Args:
            db_config: Diccionario con configuración de MySQL
            log_dir: Directorio para archivos de log
        """
        self.db_config = db_config or {
            'host': 'localhost',
            'port': 3308,
            'database': 'asfi_central',
            'user': 'root',
            'password': 'root123'
        }
        
        # Crear directorio de logs
        Path(log_dir).mkdir(parents=True, exist_ok=True)
        
        # Configurar logger para archivos rotativos
        self.file_logger = logging.getLogger('auditoria_file')
        self.file_logger.setLevel(logging.INFO)
        
        # Handler para archivo rotativo (10 MB por archivo, 5 backups)
        handler = logging.handlers.RotatingFileHandler(
            f"{log_dir}/auditoria.log",
            maxBytes=10*1024*1024,  # 10 MB
            backupCount=5,
            encoding='utf-8'
        )
        
        # Formato estructurado para fácil parsing
        formatter = logging.Formatter(
            '%(asctime)s|%(levelname)s|%(message)s'
        )
        handler.setFormatter(formatter)
        self.file_logger.addHandler(handler)
        
        # Logger para consola (opcional)
        console = logging.StreamHandler()
        console.setLevel(logging.INFO)
        console.setFormatter(formatter)
        self.file_logger.addHandler(console)
    
    def conectar_db(self):
        """Conecta a MySQL para insertar logs"""
        try:
            conn = mysql.connector.connect(**self.db_config)
            return conn
        except Exception as e:
            self.file_logger.error(f"Error conectando a MySQL: {e}")
            return None
    
    def registrar_conversion(self, cuenta_id, banco_id, no_cuenta, ci, 
                            monto_usd, monto_bs, tipo_cambio, codigo_verificacion, 
                            lote_id, ip_origen="127.0.0.1"):
        """
        Registra una operación de conversión
        
        Args:
            cuenta_id: ID de la cuenta (BIGINT)
            banco_id: ID del banco (INT)
            no_cuenta: Número de cuenta (VARCHAR)
            ci: Cédula de identidad (VARCHAR)
            monto_usd: Monto original en USD (DECIMAL)
            monto_bs: Monto convertido en Bs (DECIMAL)
            tipo_cambio: Tipo de cambio aplicado (DECIMAL)
            codigo_verificacion: Código de 8 caracteres
            lote_id: ID del lote de procesamiento
            ip_origen: IP del origen (opcional)
        """
        
        # 1. REGISTRAR EN ARCHIVO DE LOG
        log_entry = {
            'timestamp': datetime.now().isoformat(),
            'cuenta_id': cuenta_id,
            'banco_id': banco_id,
            'no_cuenta': no_cuenta,
            'ci': ci,
            'monto_usd': float(monto_usd),
            'monto_bs': float(monto_bs),
            'tipo_cambio': float(tipo_cambio),
            'codigo_verificacion': codigo_verificacion,
            'lote_id': lote_id,
            'ip_origen': ip_origen
        }
        
        # Formato: TIMESTAMP|CUENTA|BANCO|USD|BS|TC|CODIGO
        msg = (f"{cuenta_id}|{banco_id}|{no_cuenta}|{ci[:5]}...|"
               f"{monto_usd:.2f}|{monto_bs:.2f}|{tipo_cambio:.4f}|"
               f"{codigo_verificacion}|{lote_id}")
        
        self.file_logger.info(msg)
        
        # También guardar JSON para análisis posterior
        self._guardar_json(log_entry)
        
        # 2. REGISTRAR EN BASE DE DATOS
        self._registrar_en_db(log_entry)
    
    def _guardar_json(self, log_entry):
        """Guarda log en formato JSON para análisis"""
        fecha = datetime.now().strftime('%Y%m%d')
        json_file = f"logs/auditoria/auditoria_{fecha}.json"
        
        # Leer logs existentes
        logs = []
        if os.path.exists(json_file):
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    logs = json.load(f)
            except:
                logs = []
        
        # Agregar nuevo log
        logs.append(log_entry)
        
        # Guardar (mantener últimos 1000 por archivo)
        if len(logs) > 1000:
            logs = logs[-1000:]
        
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(logs, f, indent=2, ensure_ascii=False)
    
    def _registrar_en_db(self, log_entry):
        """Inserta log en tabla LogsAuditoria de MySQL"""
        conn = self.conectar_db()
        if not conn:
            return
        
        try:
            cursor = conn.cursor()
            sql = """
                INSERT INTO LogsAuditoria (
                    CuentaId, BancoId, NoCuenta, CI,
                    MontoUSD_Original, MontoBs_Resultante, TipoCambioAplicado,
                    LoteId, CodigoVerificacion, FechaConversion, IPOrigen
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
            """
            valores = (
                log_entry['cuenta_id'],
                log_entry['banco_id'],
                log_entry['no_cuenta'],
                log_entry['ci'],
                log_entry['monto_usd'],
                log_entry['monto_bs'],
                log_entry['tipo_cambio'],
                log_entry['lote_id'],
                log_entry['codigo_verificacion'],
                log_entry['timestamp'],
                log_entry['ip_origen']
            )
            cursor.execute(sql, valores)
            conn.commit()
            cursor.close()
            
        except Exception as e:
            self.file_logger.error(f"Error insertando en DB: {e}")
        finally:
            conn.close()
    
    def obtener_logs_recientes(self, limite=100):
        """Obtiene los últimos logs de la base de datos"""
        conn = self.conectar_db()
        if not conn:
            return []
        
        try:
            cursor = conn.cursor(dictionary=True)
            cursor.execute("""
                SELECT * FROM LogsAuditoria 
                ORDER BY FechaRegistroLog DESC 
                LIMIT %s
            """, (limite,))
            resultados = cursor.fetchall()
            cursor.close()
            return resultados
        except Exception as e:
            self.file_logger.error(f"Error consultando logs: {e}")
            return []
        finally:
            conn.close()
    
    def obtener_logs_por_banco(self, banco_id, fecha_inicio=None, fecha_fin=None):
        """Obtiene logs filtrados por banco y fechas"""
        conn = self.conectar_db()
        if not conn:
            return []
        
        try:
            cursor = conn.cursor(dictionary=True)
            sql = "SELECT * FROM LogsAuditoria WHERE BancoId = %s"
            params = [banco_id]
            
            if fecha_inicio:
                sql += " AND FechaConversion >= %s"
                params.append(fecha_inicio)
            if fecha_fin:
                sql += " AND FechaConversion <= %s"
                params.append(fecha_fin)
            
            sql += " ORDER BY FechaConversion DESC"
            
            cursor.execute(sql, params)
            resultados = cursor.fetchall()
            cursor.close()
            return resultados
        except Exception as e:
            self.file_logger.error(f"Error consultando logs: {e}")
            return []
        finally:
            conn.close()


# ============================================================
# EJEMPLO DE USO
# ============================================================

def ejemplo_uso():
    """Ejemplo de cómo usar el logger de auditoría"""
    
    # Crear instancia del logger
    logger = AuditoriaLogger()
    
    # Registrar una conversión de ejemplo
    logger.registrar_conversion(
        cuenta_id=123456789,
        banco_id=1,
        no_cuenta="401-00000001",
        ci="1234567",
        monto_usd=1500.50,
        monto_bs=10443.48,
        tipo_cambio=6.96,
        codigo_verificacion="A3F5C8D2",
        lote_id="LOTE_20240318_001",
        ip_origen="192.168.1.100"
    )
    
    print("✅ Log registrado correctamente")
    
    # Mostrar últimos logs
    print("\n📊 Últimos 5 logs en DB:")
    logs = logger.obtener_logs_recientes(5)
    for log in logs:
        print(f"  {log['FechaConversion']} | Banco {log['BancoId']} | "
              f"${log['MontoUSD_Original']} USD → Bs{log['MontoBs_Resultante']} | "
              f"Código: {log['CodigoVerificacion']}")


if __name__ == "__main__":
    ejemplo_uso()