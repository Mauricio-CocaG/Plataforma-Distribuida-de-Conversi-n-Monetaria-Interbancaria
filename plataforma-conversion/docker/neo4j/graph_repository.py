from neo4j import GraphDatabase
import os
import logging

class GraphRepository:
    def __init__(self):
        self.uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
        self.user = os.getenv("NEO4J_USER", "neo4j")
        self.password = os.getenv("NEO4J_PASSWORD", "root1234")
        self.driver = GraphDatabase.driver(self.uri, auth=(self.user, self.password))

    def close(self):
        self.driver.close()

    # --- MÉTODO PARA LIQUIDACIÓN FINAL (Requerido por el Adaptador) ---
    def execute_query(self, query, **kwargs):
        """Para la liquidación final (SET SaldoUSD = 0, SaldoBs = X)"""
        with self.driver.session() as session:
            session.run(query, **kwargs)

    # --- MÉTODO PARA BARRIDO MASIVO (Requerido por fetch_from_bank) ---
    def get_all_accounts(self):
        """Mapeo exacto basado en las Property Keys de tu Neo4j Browser"""
        with self.driver.session() as session:
            # Usamos los nombres exactos: NroIdentificacion, Nombres, Apellidos, etc.
            query = """
            MATCH (p:Cliente)-[:TIENE_CUENTA]->(c:Cuenta)
            RETURN p.NroIdentificacion AS ci, 
                   p.Nombres AS nombre, 
                   p.Apellidos AS apellido, 
                   c.NroCuenta AS nro_cuenta, 
                   c.SaldoUSD AS saldo
            """
            result = session.run(query)
            cuentas = []
            for record in result:
                cuentas.append({
                    "ci": record["ci"],
                    "nombre": record["nombre"],
                    "apellido": record["apellido"],
                    "nro_cuenta": record["nro_cuenta"],
                    "saldo": record["saldo"]
                })
            return cuentas

    # --- BÚSQUEDA ESPECÍFICA ---
    def buscar_cuenta_y_propietario(self, nro_cuenta):
        with self.driver.session() as session:
            # Usamos los nombres de propiedades exactos de tu carga
            query = """
            MATCH (p:Cliente)-[:TIENE_CUENTA]->(c:Cuenta {NroCuenta: $nro})
            RETURN p.NroIdentificacion AS Identificacion, 
                   p.Nombres AS Nombres, 
                   p.Apellidos AS Apellidos, 
                   c.NroCuenta AS NroCuenta, 
                   c.SaldoUSD AS SaldoUSD
            """
            result = session.run(query, nro=str(nro_cuenta))
            record = result.single()
            
            if record:
                return {
                    "Identificacion": record["Identificacion"],
                    "NroCuenta": record["NroCuenta"],
                    "SaldoUSD": float(record["SaldoUSD"]),
                    "Nombres": record["Nombres"],
                    "Apellidos": record["Apellidos"]
                }
            return None

    # --- CARGA INICIAL ---
    def cargar_cliente_cuenta(self, datos):
        """Crea la estructura de nodos exigida."""
        with self.driver.session() as session:
            query = """
            MERGE (cl:Cliente {NroIdentificacion: $id})
            SET cl.Nombres = $nom, cl.Apellidos = $ape
            MERGE (c:Cuenta {NroCuenta: $nro})
            SET c.SaldoUSD = $saldo
            MERGE (cl)-[:TIENE_CUENTA]->(c)
            """
            session.run(query, 
                id=str(datos['id']), nom=datos['nombres'], ape=datos['apellidos'],
                nro=str(datos['cuenta']), saldo=float(datos['saldo'])
            )