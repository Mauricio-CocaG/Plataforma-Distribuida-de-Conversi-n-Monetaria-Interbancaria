// Este script se usa para todos los bancos MongoDB repetidos
// Los parámetros se pasan por variable de entorno

var nombreDB = db.getName();
print("Creando colecciones para: " + nombreDB);

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
    roles: [{ role: "readWrite", db: nombreDB }]
});

print("Base de datos " + nombreDB + " creada correctamente");