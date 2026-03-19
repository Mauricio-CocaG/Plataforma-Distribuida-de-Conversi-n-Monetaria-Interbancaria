// Conectar a la base de datos
db = db.getSiblingDB('banco_bnb');

// Crear colección Cuentas
db.createCollection('Cuentas');

// Crear índices
db.Cuentas.createIndex({ "NroIdentificacion": 1 });
db.Cuentas.createIndex({ "Nombres": 1, "Apellidos": 1 });
db.Cuentas.createIndex({ "NroCuenta": 1 }, { unique: true });
db.Cuentas.createIndex({ "LoteId": 1 });
db.Cuentas.createIndex({ "CodigoVerificacion": 1 });

// Crear usuario
db.createUser({
    user: "app_user",
    pwd: "app123",
    roles: [{ role: "readWrite", db: "banco_bnb" }]
});

print("Base de datos banco_bnb creada correctamente");