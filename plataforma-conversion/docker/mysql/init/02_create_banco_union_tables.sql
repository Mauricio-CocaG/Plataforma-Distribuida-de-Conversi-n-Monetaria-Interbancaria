USE banco_union;

CREATE TABLE IF NOT EXISTS Cuentas (
    Id BIGINT PRIMARY KEY AUTO_INCREMENT,

    NroIdentificacion VARCHAR(20) NOT NULL,
    Nombres VARCHAR(100) NOT NULL,
    Apellidos VARCHAR(100) NOT NULL,
    NroCuenta VARCHAR(30) NOT NULL UNIQUE,
    IdBanco INT NOT NULL,

    SaldoUSD DECIMAL(18,4) NOT NULL,

    SaldoBs DECIMAL(18,4) NULL,
    TipoCambioAplicado DECIMAL(10,4) NULL,
    FechaConversion DATETIME NULL,
    CodigoVerificacion CHAR(8) NULL,
    LoteId VARCHAR(50) NULL,

    DatosCifrados TEXT,

    AlgoritmoUsado VARCHAR(50) DEFAULT 'Cifrado César',
    FechaRegistro DATETIME DEFAULT CURRENT_TIMESTAMP,
    FechaActualizacion DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    INDEX idx_cuenta_nroident (NroIdentificacion),
    INDEX idx_cuenta_nombre (Nombres, Apellidos),
    INDEX idx_cuenta_nrocuenta (NroCuenta),
    INDEX idx_cuenta_idbanco (IdBanco),
    INDEX idx_cuenta_lote (LoteId),
    INDEX idx_cuenta_verificacion (CodigoVerificacion),
    INDEX idx_cuenta_fechaconversion (FechaConversion)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;