from neo4j import GraphDatabase
import os
import logging

class GraphRepository:
    def __init__(self, database="neo4j"):  # ← USAR neo4j
        self.uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
        self.user = os.getenv("NEO4J_USER", "neo4j")
        self.password = os.getenv("NEO4J_PASSWORD", "root1234")
        self.database = database
        self.driver = GraphDatabase.driver(self.uri, auth=(self.user, self.password))

    def close(self):
        self.driver.close()

    def get_all_accounts(self):
        with self.driver.session(database=self.database) as session:
            query = """
            MATCH (cl:Cliente)-[:TIENE_CUENTA]->(c:Cuenta)
            RETURN 
                cl.NroIdentificacion AS Identificacion, 
                cl.Nombres AS Nombres, 
                cl.Apellidos AS Apellidos, 
                c.NroCuenta AS NroCuenta, 
                c.SaldoUSD AS SaldoUSD
            """
            result = session.run(query)
            return [dict(record) for record in result]

    def buscar_cuenta_y_propietario(self, nro_cuenta):
        with self.driver.session(database=self.database) as session:
            query = """
            MATCH (cl:Cliente)-[:TIENE_CUENTA]->(c:Cuenta {NroCuenta: $nro})
            RETURN 
                cl.NroIdentificacion AS Identificacion,
                cl.Nombres AS Nombres,
                cl.Apellidos AS Apellidos,
                c.NroCuenta AS NroCuenta,
                c.SaldoUSD AS SaldoUSD
            """
            result = session.run(query, nro=nro_cuenta)
            record = result.single()
            return dict(record) if record else None

    def actualizar_saldos_bulk(self, datos_retorno):
        query = """
        UNWIND $data AS item
        MATCH (c:Cuenta {NroCuenta: item.NroCuenta})
        SET c.SaldoUSD = 0, 
            c.SaldoBs = item.SaldoBs, 
            c.Estado = 'LIQUIDADO'
        """
        with self.driver.session(database=self.database) as session:
            session.run(query, data=datos_retorno)

    def cargar_cliente_cuenta(self, datos):
        with self.driver.session(database=self.database) as session:
            query = """
            MERGE (cl:Cliente {NroIdentificacion: $id})
            SET cl.Nombres = $nom, cl.Apellidos = $ape
            MERGE (c:Cuenta {NroCuenta: $nro})
            SET c.SaldoUSD = $saldo,
                c.SaldoBs = null,
                c.Estado = 'ACTIVA'
            MERGE (cl)-[:TIENE_CUENTA]->(c)
            """
            session.run(query, 
                id=str(datos['id']), 
                nom=datos['nombres'], 
                ape=datos['apellidos'],
                nro=str(datos['cuenta']), 
                saldo=float(datos['saldo'])
            )