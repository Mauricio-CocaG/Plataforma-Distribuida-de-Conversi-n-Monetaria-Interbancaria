import os
import sys
from concurrent.futures import ThreadPoolExecutor
# Importamos la configuración de tu script original
from cargar_datos_bancos import DB_CONFIG, get_connection, logger

def ejecutar_truncate_rapido():
    print("🚀 Iniciando truncado de emergencia...")
    
    def borrar(bid):
        config = DB_CONFIG[bid]
        try:
            conn = get_connection(config)
            if conn is None: return
            tipo = config['type']
            
            if tipo in ['mysql', 'postgresql', 'oracle']:
                cursor = conn.cursor()
                # Para SQL
                tabla = config['table']
                cmd = f"TRUNCATE TABLE {tabla} RESTART IDENTITY CASCADE" if tipo == 'postgresql' else f"TRUNCATE TABLE {tabla}"
                cursor.execute(cmd)
                conn.commit()
                print(f"✅ {config['type'].upper()} (Banco {bid}): Limpio")
            
            elif tipo == 'mongodb':
                conn['Cuentas'].drop()
                print(f"✅ MONGODB (Banco {bid}): Limpio")
            
            elif tipo == 'redis':
                conn.flushdb()
                print(f"✅ REDIS (Banco {bid}): Limpio")
                
            elif tipo == 'neo4j':
                # Limpia nodos si quedaron colgados
                with conn.driver.session() as s: 
                    s.run("MATCH (n:Cuenta) DETACH DELETE n")
                print(f"✅ NEO4J (Banco {bid}): Limpio")

            if hasattr(conn, 'close'): conn.close()
        except Exception as e:
            print(f"❌ Error en Banco {bid}: {e}")

    # Ejecuta la limpieza en paralelo
    with ThreadPoolExecutor(max_workers=10) as executor:
        list(executor.map(borrar, DB_CONFIG.keys()))

if __name__ == "__main__":
    ejecutar_truncate_rapido()
    print("✨ Todas las bases de datos han sido vaciadas.")