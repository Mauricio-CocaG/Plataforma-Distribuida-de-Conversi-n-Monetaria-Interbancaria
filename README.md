🗄️ Scripts de Base de Datos (BDD)
El sistema opera sobre un ecosistema heterogéneo coordinado por un nodo central (ASFI). A continuación, se detallan los esquemas reales utilizados:

1. Nodo Central (MySQL - asfi_central)
Es el cerebro del sistema. Almacena el catálogo de bancos, los tipos de cambio oficiales y el Log de Auditoría que vincula el saldo original en USD con el resultante en Bs.

SQL

    CREATE TABLE Bancos (
        Id INT PRIMARY KEY AUTO_INCREMENT,
        Nombre VARCHAR(100) NOT NULL,
        AlgoritmoEncriptacion VARCHAR(50) NOT NULL,
        PuertoAPI INT DEFAULT 8080
    );

    CREATE TABLE LogsAuditoria (
        Id BIGINT PRIMARY KEY AUTO_INCREMENT,
        MontoUSD_Original DECIMAL(18,4) NOT NULL,
        MontoBs_Resultante DECIMAL(18,4) NOT NULL,
        TipoCambioAplicado DECIMAL(10,4) NOT NULL,
        LoteId VARCHAR(50) NOT NULL,
        CodigoVerificacion CHAR(8) NOT NULL,
        FechaConversion DATETIME NOT NULL
    );
    
2. Bancos Relacionales (SQL)
Cada banco implementa variaciones según su motor:

Banco Unión (MySQL): Usa CamelCase y almacena DatosCifrados en un campo TEXT.

Banco BISA (Oracle): Implementa bloques PL/SQL para asegurar la creación de usuarios y tablas con NUMBER e IDENTITY.

Banco Mercantil (PostgreSQL): Utiliza snake_case (nro_cuenta, saldo_usd) y disparadores (TRIGGER) para auditoría de fechas.

SQL
-- Ejemplo PostgreSQL (Banco 2)

    CREATE TABLE cuentas (
        id BIGSERIAL PRIMARY KEY,
        nro_cuenta VARCHAR(30) NOT NULL UNIQUE,
        saldo_usd NUMERIC(18,4) NOT NULL,
        algoritmo_usado VARCHAR(50) DEFAULT 'Atbash'
);

3. Banco 14 - Grafos (Neo4j)
Utiliza un modelo de relaciones para vincular clientes con cuentas, optimizando las búsquedas de propiedad.

Cypher
// Creación de estructura Clientes-Cuentas

    MERGE (cl:Cliente {NroIdentificacion: $id})
    SET cl.Nombres = $nom, cl.Apellidos = $ape
    MERGE (c:Cuenta {NroCuenta: $nro})
    SET c.SaldoUSD = $saldo, c.Estado = 'ACTIVA'
    MERGE (cl)-[:TIENE_CUENTA]->(c);

4. Bancos NoSQL (MongoDB & Redis)
Banco BNB (MongoDB): Estructura basada en documentos con índices únicos en NroCuenta para alta velocidad de respuesta.

Banco BCP (Redis): Almacenamiento en memoria mediante Hashes para acceso instantáneo a saldos temporales.

JavaScript
// MongoDB Setup (Banco 3)

    db.Cuentas.createIndex({ "NroCuenta": 1 }, { unique: true });
    db.createUser({ user: "app_user", pwd: "app123", roles: ["readWrite"] });

🔗 Documentación
El informe de esta práctica se encuentra en el siguiente enlace:
https://docs.google.com/document/d/1g7zEAZdUip3dg28fJg-9pEw9fNw0LAKXYibCsfTHTMI/edit?usp=sharing