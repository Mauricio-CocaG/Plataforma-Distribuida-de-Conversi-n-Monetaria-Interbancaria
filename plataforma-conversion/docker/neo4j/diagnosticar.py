# diagnosticar.py
from graph_repository import GraphRepository
from neo4j import GraphDatabase

print("=== DIAGNÓSTICO COMPLETO NEO4J ===\n")

# 1. Conectar directamente a Neo4j para ver si hay datos
uri = "bolt://localhost:7687"
user = "neo4j"
password = "root1234"

driver = GraphDatabase.driver(uri, auth=(user, password))

with driver.session(database="neo4j") as session:
    # Verificar si hay Clientes
    result = session.run("MATCH (cl:Cliente) RETURN count(cl) as total")
    clientes = result.single()["total"]
    print(f"Clientes en Neo4j: {clientes}")
    
    # Verificar si hay Cuentas
    result = session.run("MATCH (c:Cuenta) RETURN count(c) as total")
    cuentas = result.single()["total"]
    print(f"Cuentas en Neo4j: {cuentas}")
    
    # Verificar la relación
    result = session.run("MATCH (cl:Cliente)-[:TIENE_CUENTA]->(c:Cuenta) RETURN count(*) as total")
    relaciones = result.single()["total"]
    print(f"Relaciones: {relaciones}")
    
    # Ver una muestra
    if cuentas > 0:
        result = session.run("""
            MATCH (cl:Cliente)-[:TIENE_CUENTA]->(c:Cuenta) 
            RETURN cl.NroIdentificacion, c.NroCuenta, c.SaldoUSD 
            LIMIT 3
        """)
        print("\nMuestra de datos:")
        for record in result:
            print(f"  Cliente: {record['cl.NroIdentificacion']}, Cuenta: {record['c.NroCuenta']}, Saldo: {record['c.SaldoUSD']}")

driver.close()

# 2. Probar el repositorio
print("\n=== PROBANDO REPOSITORIO ===")
repo = GraphRepository(database="neo4j")
cuentas_repo = repo.get_all_accounts()
print(f"get_all_accounts() devolvió: {len(cuentas_repo)} cuentas")

if len(cuentas_repo) == 0 and cuentas > 0:
    print("\n❌ ERROR: Hay datos en Neo4j pero get_all_accounts() no los encuentra")
    print("El problema está en la consulta de get_all_accounts()")
    
    # Ver la consulta exacta
    with driver.session(database="neo4j") as session:
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
        manual = [dict(record) for record in result]
        print(f"\nConsulta manual devolvió: {len(manual)} cuentas")
        if len(manual) > 0:
            print(f"Primera cuenta manual: {manual[0]}")

repo.close()