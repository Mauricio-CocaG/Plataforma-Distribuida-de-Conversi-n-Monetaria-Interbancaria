CREATE TABLE IF NOT EXISTS bancos (
    id SERIAL PRIMARY KEY,
    nombre VARCHAR(100) NOT NULL,
    algoritmo_encriptacion VARCHAR(50) NOT NULL,
    digito_verificacion VARCHAR(10),
    url_api VARCHAR(255),
    puerto_api INT DEFAULT 8080,
    llave_publica TEXT,
    llave_simetrica TEXT,
    activo BOOLEAN DEFAULT TRUE,

    fecha_registro TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    fecha_actualizacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_banco_algoritmo ON bancos(algoritmo_encriptacion);
CREATE INDEX IF NOT EXISTS idx_banco_activo ON bancos(activo);

CREATE TABLE IF NOT EXISTS cuentas (
    id BIGSERIAL PRIMARY KEY,

    ci VARCHAR(20) NOT NULL,
    nombres VARCHAR(100) NOT NULL,
    apellidos VARCHAR(100) NOT NULL,
    no_cuenta VARCHAR(30) NOT NULL UNIQUE,
    id_banco INT NOT NULL,

    saldo_bs NUMERIC(18,4) NOT NULL,

    lote_id VARCHAR(50),
    fecha_conversion TIMESTAMP,
    codigo_verificacion CHAR(8),

    fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    fecha_actualizacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_cuentas_banco
        FOREIGN KEY (id_banco) REFERENCES bancos(id)
);

CREATE INDEX IF NOT EXISTS idx_cuenta_ci ON cuentas(ci);
CREATE INDEX IF NOT EXISTS idx_cuenta_nombre ON cuentas(nombres, apellidos);
CREATE INDEX IF NOT EXISTS idx_cuenta_nocuenta ON cuentas(no_cuenta);
CREATE INDEX IF NOT EXISTS idx_cuenta_banco ON cuentas(id_banco);
CREATE INDEX IF NOT EXISTS idx_cuenta_lote ON cuentas(lote_id);
CREATE INDEX IF NOT EXISTS idx_cuenta_verificacion ON cuentas(codigo_verificacion);
CREATE INDEX IF NOT EXISTS idx_cuenta_fechas ON cuentas(fecha_creacion, fecha_actualizacion);

CREATE TABLE IF NOT EXISTS tipos_cambio (
    id SERIAL PRIMARY KEY,
    valor NUMERIC(10,4) NOT NULL,
    fecha_inicio TIMESTAMP NOT NULL,
    fecha_fin TIMESTAMP,
    activo BOOLEAN DEFAULT TRUE,

    fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_tipo_activo ON tipos_cambio(activo, fecha_inicio);

CREATE TABLE IF NOT EXISTS logs_auditoria (
    id BIGSERIAL PRIMARY KEY,

    cuenta_id BIGINT NOT NULL,
    banco_id INT NOT NULL,
    no_cuenta VARCHAR(30) NOT NULL,
    ci VARCHAR(20) NOT NULL,

    monto_usd_original NUMERIC(18,4) NOT NULL,
    monto_bs_resultante NUMERIC(18,4) NOT NULL,
    tipo_cambio_aplicado NUMERIC(10,4) NOT NULL,

    lote_id VARCHAR(50) NOT NULL,
    codigo_verificacion CHAR(8) NOT NULL,

    fecha_conversion TIMESTAMP NOT NULL,

    ip_origen VARCHAR(45),

    fecha_registro_log TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_logs_cuenta
        FOREIGN KEY (cuenta_id) REFERENCES cuentas(id),

    CONSTRAINT fk_logs_banco
        FOREIGN KEY (banco_id) REFERENCES bancos(id)
);

CREATE INDEX IF NOT EXISTS idx_log_fecha ON logs_auditoria(fecha_registro_log);
CREATE INDEX IF NOT EXISTS idx_log_conversion ON logs_auditoria(fecha_conversion);
CREATE INDEX IF NOT EXISTS idx_log_cuenta ON logs_auditoria(cuenta_id);
CREATE INDEX IF NOT EXISTS idx_log_banco ON logs_auditoria(banco_id);
CREATE INDEX IF NOT EXISTS idx_log_lote ON logs_auditoria(lote_id);
CREATE INDEX IF NOT EXISTS idx_log_verificacion ON logs_auditoria(codigo_verificacion);
CREATE INDEX IF NOT EXISTS idx_log_usd ON logs_auditoria(monto_usd_original);

-- ============================================================
-- Trigger para fecha_actualizacion
-- ============================================================

CREATE OR REPLACE FUNCTION update_fecha_actualizacion()
RETURNS TRIGGER AS $$
BEGIN
    NEW.fecha_actualizacion = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_update_bancos_fecha_actualizacion ON bancos;
CREATE TRIGGER trg_update_bancos_fecha_actualizacion
BEFORE UPDATE ON bancos
FOR EACH ROW
EXECUTE FUNCTION update_fecha_actualizacion();

DROP TRIGGER IF EXISTS trg_update_cuentas_fecha_actualizacion ON cuentas;
CREATE TRIGGER trg_update_cuentas_fecha_actualizacion
BEFORE UPDATE ON cuentas
FOR EACH ROW
EXECUTE FUNCTION update_fecha_actualizacion();

-- ============================================================
-- Catálogo inicial de 14 bancos
-- ============================================================

INSERT INTO bancos (nombre, algoritmo_encriptacion, activo)
VALUES
('Banco Union S.A.', 'Cifrado Cesar', TRUE),
('Banco Mercantil Santa Cruz S.A.', 'Cifrado Atbash', TRUE),
('Banco BISA S.A.', 'Cifrado Vigenere', TRUE),
('Banco Nacional de Bolivia S.A.', 'Cifrado Playfair', TRUE),
('Banco Ganadero S.A.', 'Cifrado Hill', TRUE),
('Banco de Credito de Bolivia S.A.', 'DES', TRUE),
('Banco Económico S.A.', '3DES', TRUE),
('Banco Sol S.A.', 'Blowfish', TRUE),
('Banco FIE S.A.', 'Twofish', TRUE),
('Banco Fortaleza S.A.', 'AES', TRUE),
('Banco Prodem S.A.', 'RSA', TRUE),
('Banco Pyme de la Comunidad S.A.', 'ElGamal', TRUE),
('Banco Pyme Ecofuturo S.A.', 'ECC', TRUE),
('Banco Fassil S.A.', 'ChaCha20', TRUE)
ON CONFLICT DO NOTHING;

-- ============================================================
-- Tipo de cambio inicial
-- ============================================================

INSERT INTO tipos_cambio (valor, fecha_inicio, activo)
VALUES (6.9600, CURRENT_TIMESTAMP, TRUE)
ON CONFLICT DO NOTHING;