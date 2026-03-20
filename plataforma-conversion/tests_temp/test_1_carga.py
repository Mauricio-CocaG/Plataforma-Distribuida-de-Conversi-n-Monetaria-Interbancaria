import pandas as pd
import sys
import os
import glob

# Configuración de rutas
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.join(BASE_DIR, 'docker', 'neo4j'))

try:
    from graph_repository import GraphRepository
except ImportError:
    print("❌ Error: No se encontró graph_repository.py. Revisa la carpeta docker/neo4j")
    sys.exit(1)

def obtener_ruta_csv():
    # Buscamos cualquier CSV en la carpeta processed
    patron = os.path.join(BASE_DIR, 'data', 'processed', '*.csv')
    archivos = glob.glob(patron)
    return archivos[0] if archivos else None

def test_carga_inicial():
    print("🧪 TEST 1: Iniciando carga al Grafo...")
    
    csv_path = obtener_ruta_csv()
    if not csv_path:
        print("❌ Error: No hay archivos CSV en data/processed/")
        return

    print(f"📂 Cargando datos desde: {os.path.basename(csv_path)}")
    
    # Leer CSV
    df = pd.read_csv(csv_path, encoding='latin-1')
    df.columns = df.columns.str.replace('ï»¿', '').str.strip()
    
    # Filtrar Banco Argentina (ID 14) - Tomamos 50 para la prueba
    df_arg = df[df['IdBanco'] == 14].head(50)

    repo = GraphRepository()
    try:
        with repo.driver.session() as session:
            print("🧹 Limpiando nodos previos...")
            session.run("MATCH (n) DETACH DELETE n")
            
            print("🏗️ Creando nodos de prueba...")
            # Crear el nodo del Banco
            session.run("MERGE (b:Banco {id: 14, nombre: 'Banco Argentina'})")
            
            # Crear Clientes y sus relaciones
            for _, row in df_arg.iterrows():
                session.run("""
                    MATCH (b:Banco {id: 14})
                    MERGE (c:Cliente {ci: $ci})
                    SET c.nombres = $nom, c.apellidos = $ape
                    CREATE (c)-[:CUENTA {
                        nro: $nro, 
                        saldo_usd: $usd, 
                        estado: 'PENDIENTE',
                        fecha_carga: datetime()
                    }]->(b)
                """, ci=str(row['Identificacion']), nom=row['Nombres'], 
                   ape=row['Apellidos'], nro=str(row['NroCuenta']), usd=float(row['Saldo']))
        
        print(f"✅ ¡Éxito! Se cargaron {len(df_arg)} clientes al grafo.")
        print("👉 Revisa ahora: http://localhost:7474")
        
    except Exception as e:
        print(f"❌ Error en la carga: {e}")
    finally:
        repo.close()

if __name__ == "__main__":
    test_carga_inicial()