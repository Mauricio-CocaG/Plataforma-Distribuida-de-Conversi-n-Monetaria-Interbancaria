import mysql.connector
import sys
import os
from neo4j import GraphDatabase

# Configuración de rutas para importar el repositorio
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.join(BASE_DIR, 'docker', 'neo4j'))
from graph_repository import GraphRepository

# Configuración MySQL
DB_CONFIG = {
    'host': 'localhost',
    'port': 3308,
    'user': 'root',
    'password': 'root123',
    'database': 'asfi_central'
}

def test_devolucion_al_grafo():
    print("🧪 TEST 3: Devolviendo datos de ASFI (MySQL) al Banco (Neo4j)...")
    
    # 1. Conectar a MySQL para obtener las conversiones
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)
        
        # Obtenemos los últimos registros procesados para el Banco 14
        cursor.execute("""
            SELECT NoCuenta, MontoBs_Resultante, CodigoVerificacion, LoteId 
            FROM LogsAuditoria 
            WHERE BancoId = 14
        """)
        conversiones = cursor.fetchall()
        
        if not conversiones:
            print("❌ No se encontraron datos convertidos en MySQL para el Banco 14.")
            return

        print(f"📊 Se encontraron {len(conversiones)} registros para sincronizar.")

        # 2. Conectar a Neo4j para actualizar
        repo = GraphRepository()
        
        with repo.driver.session() as session:
            for reg in conversiones:
                # Actualizamos la relación CUENTA en el grafo
                session.run("""
                    MATCH (c:Cliente)-[r:CUENTA]->(b:Banco {id: 14})
                    WHERE r.nro = $nro
                    SET r.saldo_bs = $bs,
                        r.codigo_verificacion = $cod,
                        r.lote_asfi = $lote,
                        r.estado = 'CONVERTIDO',
                        r.fecha_sincro = datetime()
                """, nro=reg['NoCuenta'], bs=float(reg['MontoBs_Resultante']), 
                   cod=reg['CodigoVerificacion'], lote=reg['LoteId'])
        
        print(f"✅ Sincronización exitosa. {len(conversiones)} nodos actualizados en Neo4j.")
        repo.close()

    except Exception as e:
        print(f"❌ Error durante la sincronización: {e}")
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()

if __name__ == "__main__":
    test_devolucion_al_grafo()