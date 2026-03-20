-- ============================================================
-- BASE DE DATOS CENTRAL ASFI
-- ============================================================
CREATE DATABASE IF NOT EXISTS asfi_central 
CHARACTER SET utf8mb4 
COLLATE utf8mb4_unicode_ci;

USE asfi_central;

-- ============================================================
-- TABLA 1: Bancos (Información de las 14 entidades)
-- ============================================================
CREATE TABLE Bancos (
    Id INT PRIMARY KEY AUTO_INCREMENT,
    Nombre VARCHAR(100) NOT NULL,
    AlgoritmoEncriptacion VARCHAR(50) NOT NULL,
    DigitoVerificacion VARCHAR(10),
    UrlAPI VARCHAR(255),
    PuertoAPI INT DEFAULT 8080,
    LlavePublica TEXT,
    LlaveSimetrica TEXT,
    Activo BOOLEAN DEFAULT TRUE,
    
    -- Timestamps
    FechaRegistro DATETIME DEFAULT CURRENT_TIMESTAMP,
    FechaActualizacion DATETIME ON UPDATE CURRENT_TIMESTAMP,
    
    INDEX idx_banco_algoritmo (AlgoritmoEncriptacion),
    INDEX idx_banco_activo (Activo)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ============================================================
-- TABLA 2: Cuentas (TABLA PRINCIPAL - SOLO BOLIVIANOS)
-- ============================================================
CREATE TABLE Cuentas (
    Id BIGINT PRIMARY KEY AUTO_INCREMENT,
    -- Datos del cliente
    CI VARCHAR(20) NOT NULL,
    Nombres VARCHAR(100) NOT NULL,
    Apellidos VARCHAR(100) NOT NULL,
    NoCuenta VARCHAR(30) NOT NULL UNIQUE,
    IdBanco INT NOT NULL,
    
    -- SOLO SALDO EN BOLIVIANOS
    SaldoBs DECIMAL(18,4) NOT NULL,                 -- Saldo actual en bolivianos
    
    -- Control de conversión
    LoteId VARCHAR(50),                              -- Identificador del lote de procesamiento
    FechaConversion DATETIME,                         -- Cuándo se convirtió
    CodigoVerificacion CHAR(8),                       -- 8 caracteres hexadecimales
    
    -- Timestamps de auditoría
    FechaCreacion DATETIME DEFAULT CURRENT_TIMESTAMP,
    FechaActualizacion DATETIME ON UPDATE CURRENT_TIMESTAMP,
    
    -- Llave foránea
    FOREIGN KEY (IdBanco) REFERENCES Bancos(Id),
    
    -- Índices
    INDEX idx_cuenta_ci (CI),
    INDEX idx_cuenta_nombre (Nombres, Apellidos),
    INDEX idx_cuenta_nocuenta (NoCuenta),
    INDEX idx_cuenta_banco (IdBanco),
    INDEX idx_cuenta_lote (LoteId),
    INDEX idx_cuenta_verificacion (CodigoVerificacion),
    INDEX idx_cuenta_fechas (FechaCreacion, FechaActualizacion)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ============================================================
-- TABLA 3: TiposCambio (Registro del tipo de cambio oficial)
-- ============================================================
CREATE TABLE TiposCambio (
    Id INT PRIMARY KEY AUTO_INCREMENT,
    Valor DECIMAL(10,4) NOT NULL,                    -- 6.9600 (oficial)
    FechaInicio DATETIME NOT NULL,
    FechaFin DATETIME,
    Activo BOOLEAN DEFAULT TRUE,
    
    FechaCreacion DATETIME DEFAULT CURRENT_TIMESTAMP,
    
    INDEX idx_tipo_activo (Activo, FechaInicio)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ============================================================
-- TABLA 4: LogsAuditoria (REGISTRO DE CONVERSIONES - AQUÍ VA EL DÓLAR)
-- ============================================================
CREATE TABLE LogsAuditoria (
    Id BIGINT PRIMARY KEY AUTO_INCREMENT,
    
    -- Referencias a la cuenta
    CuentaId BIGINT NOT NULL,
    BancoId INT NOT NULL,
    NoCuenta VARCHAR(30) NOT NULL,
    CI VARCHAR(20) NOT NULL,
    
    -- INFORMACIÓN CRÍTICA DE LA CONVERSIÓN
    MontoUSD_Original DECIMAL(18,4) NOT NULL,        -- Monto en dólares que se convirtió
    MontoBs_Resultante DECIMAL(18,4) NOT NULL,        -- Resultado en bolivianos
    TipoCambioAplicado DECIMAL(10,4) NOT NULL,        -- Tipo de cambio usado (6.96)
    
    -- Datos de la operación
    LoteId VARCHAR(50) NOT NULL,                      -- Mismo LoteId de Cuentas
    CodigoVerificacion CHAR(8) NOT NULL,              -- Código generado
    
    -- Timestamp exacto de la conversión
    FechaConversion DATETIME NOT NULL,                 -- Cuándo se realizó la conversión
    
    -- Metadatos
    IPOrigen VARCHAR(45),
    
    -- Timestamp del log
    FechaRegistroLog DATETIME DEFAULT CURRENT_TIMESTAMP,
    
    -- Llaves foráneas
    FOREIGN KEY (CuentaId) REFERENCES Cuentas(Id),
    FOREIGN KEY (BancoId) REFERENCES Bancos(Id),
    
    -- Índices para búsquedas rápidas
    INDEX idx_log_fecha (FechaRegistroLog),
    INDEX idx_log_conversion (FechaConversion),
    INDEX idx_log_cuenta (CuentaId),
    INDEX idx_log_banco (BancoId),
    INDEX idx_log_lote (LoteId),
    INDEX idx_log_verificacion (CodigoVerificacion),
    INDEX idx_log_usd (MontoUSD_Original)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ============================================================
-- INSERTAR LOS 14 BANCOS EN LA TABLA Bancos
-- ============================================================
INSERT INTO Bancos (Id, Nombre, AlgoritmoEncriptacion, PuertoAPI) VALUES 
(1, 'Banco Union', 'Cifrado Cesar', 8081),
(2, 'Banco Mercantil', 'Cifrado Atbash', 8082),
(3, 'Banco BNB', 'Cifrado Vigenere', 8083),
(4, 'Banco BCP', 'Cifrado Playfair', 8084),
(5, 'Banco BISA', 'Cifrado Hill', 8085),
(6, 'Banco Ganadero', 'DES', 8086),
(7, 'Banco Economico', '3DES', 8087),
(8, 'Banco Prodem', 'Blowfish', 8088),
(9, 'Banco Solidario', 'Twofish', 8089),
(10, 'Banco Fortaleza', 'AES', 8090),
(11, 'Banco FIE', 'RSA', 8091),
(12, 'Banco PYME', 'ElGamal', 8092),
(13, 'Banco BDP', 'ECC', 8093),
(14, 'Banco Argentina', 'ChaCha20', 8094);