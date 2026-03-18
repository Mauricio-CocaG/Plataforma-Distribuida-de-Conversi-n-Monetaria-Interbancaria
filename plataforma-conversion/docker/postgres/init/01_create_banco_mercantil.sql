-- 01_create_banco_mercantil.sql
-- PostgreSQL usa minúsculas para nombres de columnas por convención

CREATE TABLE IF NOT EXISTS cuentas (
    id BIGSERIAL PRIMARY KEY,
    nroidentificacion VARCHAR(20) NOT NULL,
    nombres VARCHAR(100) NOT NULL,
    apellidos VARCHAR(100) NOT NULL,
    nrocuenta VARCHAR(30) NOT NULL UNIQUE,
    idbanco INT NOT NULL,
    saldousd DECIMAL(18,4) NOT NULL,
    saldobs DECIMAL(18,4) NULL,
    tipocambioaplicado DECIMAL(10,4) NULL,
    fechaconversion TIMESTAMP NULL,
    codigoverificacion CHAR(8) NULL,
    loteid VARCHAR(50) NULL,
    datoscifrados TEXT,
    algoritmousado VARCHAR(50) DEFAULT 'Cifrado Atbash',
    fecharegistro TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    fechaactualizacion TIMESTAMP NULL
);

-- Índices
CREATE INDEX IF NOT EXISTS idx_nrocuenta ON cuentas(nrocuenta);
CREATE INDEX IF NOT EXISTS idx_idbanco ON cuentas(idbanco);