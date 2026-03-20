from neo4j import GraphDatabase
import os

class GraphRepository:
    def __init__(self):
        # Al estar en Docker, usamos el nombre del servicio definido en docker-compose
        # o localhost si pruebas desde fuera.
        self.uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
        self.user = os.getenv("NEO4J_USER", "neo4j")
        self.password = os.getenv("NEO4J_PASSWORD", "root1234")
        self.driver = GraphDatabase.driver(self.uri, auth=(self.user, self.password))

    def close(self):
        self.driver.close()

    def registrar_flujo_interbancario(self, banco_origen, banco_destino, monto):
        with self.driver.session() as session:
            query = """
            MERGE (b1:Banco {nombre: $b1})
            MERGE (b2:Banco {nombre: $b2})
            CREATE (b1)-[:TRANSFERENCIA {monto: $monto, fecha: datetime()}]->(b2)
            """
            session.run(query, b1=banco_origen, b2=banco_destino, monto=monto)