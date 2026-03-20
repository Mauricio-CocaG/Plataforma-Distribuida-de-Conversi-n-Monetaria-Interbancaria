#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script para consultar los logs de auditoría
"""

import mysql.connector
from datetime import datetime, timedelta
import sys
import os
from pathlib import Path

# Agregar el path del proyecto para importar módulos
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.shared.logger import AuditoriaLogger

def conectar_asfi():
    """Conecta a la base de datos ASFI Central"""
    return mysql.connector.connect(
        host='localhost',
        port=3308,
        database='asfi_central',
        user='root',
        password='root123'
    )

def ver_ultimos_logs(limite=20):
    """Muestra los últimos logs de auditoría"""
    logger = AuditoriaLogger()
    logs = logger.obtener_logs_recientes(limite)
    
    print("="*100)
    print(f"ÚLTIMOS {len(logs)} LOGS DE AUDITORÍA")
    print("="*100)
    
    for log in logs:
        print(f"\n📝 Registro #{log['Id']}")
        print(f"   Fecha: {log['FechaRegistroLog']}")
        print(f"   Cuenta: {log['NoCuenta']} (Banco {log['BancoId']})")
        print(f"   CI: {log['CI']}")
        print(f"   Monto: USD {log['MontoUSD_Original']:,.2f} → Bs {log['MontoBs_Resultante']:,.2f}")
        print(f"   Tipo Cambio: {log['TipoCambioAplicado']}")
        print(f"   Código Verificación: {log['CodigoVerificacion']}")
        print(f"   Lote: {log['LoteId']}")
        print("-"*50)

def ver_resumen_por_fecha(fecha=None):
    """Muestra resumen de logs por fecha"""
    if not fecha:
        fecha = datetime.now().date()
    
    conn = conectar_asfi()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT 
            DATE(FechaRegistroLog) as Fecha,
            COUNT(*) as TotalConversiones,
            SUM(MontoUSD_Original) as TotalUSD,
            SUM(MontoBs_Resultante) as TotalBs,
            AVG(TipoCambioAplicado) as TCPromedio
        FROM LogsAuditoria
        WHERE DATE(FechaRegistroLog) = %s
        GROUP BY DATE(FechaRegistroLog)
    """, (fecha,))
    
    row = cursor.fetchone()
    
    print("="*60)
    print(f"RESUMEN DE LOGS PARA {fecha}")
    print("="*60)
    if row:
        print(f"Total conversiones: {row[1]}")
        print(f"Total USD convertidos: {row[2]:,.2f}")
        print(f"Total Bs obtenidos: {row[3]:,.2f}")
        print(f"Tipo de cambio promedio: {row[4]:.4f}")
    else:
        print("No hay registros para esta fecha")
    
    cursor.close()
    conn.close()

def buscar_por_codigo(codigo):
    """Busca un log por código de verificación"""
    conn = conectar_asfi()
    cursor = conn.cursor(dictionary=True)
    
    cursor.execute("""
        SELECT * FROM LogsAuditoria 
        WHERE CodigoVerificacion = %s
    """, (codigo,))
    
    log = cursor.fetchone()
    
    if log:
        print("="*60)
        print(f"LOG ENCONTRADO PARA CÓDIGO {codigo}")
        print("="*60)
        for key, value in log.items():
            print(f"{key}: {value}")
    else:
        print(f"No se encontró log con código {codigo}")
    
    cursor.close()
    conn.close()

def ver_archivos_log():
    """Muestra los archivos de log rotativos"""
    log_dir = Path("logs/auditoria")
    if log_dir.exists():
        print("="*60)
        print("ARCHIVOS DE LOG ROTATIVOS")
        print("="*60)
        for log_file in sorted(log_dir.glob("*.log*"), reverse=True):
            size = log_file.stat().st_size / 1024
            print(f"  {log_file.name} - {size:.2f} KB")
        
        # Mostrar últimas líneas del log principal
        main_log = log_dir / "auditoria.log"
        if main_log.exists():
            print("\n" + "="*60)
            print("ÚLTIMAS 10 LÍNEAS DEL LOG PRINCIPAL")
            print("="*60)
            with open(main_log, 'r', encoding='utf-8') as f:
                lines = f.readlines()[-10:]
                for line in lines:
                    print(f"  {line.strip()}")
    else:
        print("No hay archivos de log")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        if sys.argv[1] == "ultimos":
            limite = int(sys.argv[2]) if len(sys.argv) > 2 else 20
            ver_ultimos_logs(limite)
        elif sys.argv[1] == "resumen":
            fecha = sys.argv[2] if len(sys.argv) > 2 else None
            ver_resumen_por_fecha(fecha)
        elif sys.argv[1] == "buscar":
            if len(sys.argv) > 2:
                buscar_por_codigo(sys.argv[2])
            else:
                print("Uso: python consultar_logs.py buscar <codigo>")
        elif sys.argv[1] == "archivos":
            ver_archivos_log()
        else:
            print("Comandos: ultimos [n], resumen [fecha], buscar <codigo>, archivos")
    else:
        ver_ultimos_logs()