import sys
import os

# 1. Calculamos la ruta a la carpeta 'docker/neo4j' desde la carpeta 'scripts'
# Subimos un nivel (..) y entramos a docker/neo4j
ruta_docker = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'docker', 'neo4j'))

# 2. Le decimos a Python que esa carpeta ahora es parte de su "biblioteca"
sys.path.append(ruta_docker)

# 3. Ahora importamos el archivo que creaste con tu equipo
try:
    from graph_repository import GraphRepository
    print("✅ Módulo cargado con éxito desde docker/neo4j/")
except ImportError as e:
    print(f"❌ Error: No se encontró graph_repository.py en {ruta_docker}")
    sys.exit(1)

def test():
    repo = GraphRepository()
    try:
        print("Conectando a Neo4j...")
        # Usamos nombres de bancos reales de tu docker-compose
        repo.registrar_flujo_interbancario("Banco Mercantil", "Banco Union", 1500.50)
        print("🚀 ¡Éxito! Nodo y relación creados en el grafo.")
    except Exception as e:
        print(f"❌ Error al insertar datos: {e}")
    finally:
        repo.close()

if __name__ == "__main__":
    test()