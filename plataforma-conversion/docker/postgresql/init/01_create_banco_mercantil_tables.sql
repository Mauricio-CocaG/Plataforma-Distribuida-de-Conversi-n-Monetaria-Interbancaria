CREATE TABLE IF NOT EXISTS cuentas (
    id BIGSERIAL PRIMARY KEY,
    nro_identificacion VARCHAR(20) NOT NULL,
    nombres VARCHAR(100) NOT NULL,
    apellidos VARCHAR(100) NOT NULL,
    nro_cuenta VARCHAR(30) NOT NULL UNIQUE,
    id_banco INT NOT NULL,

    saldo_usd NUMERIC(18,4) NOT NULL,

    saldo_bs NUMERIC(18,4) NULL,
    tipo_cambio_aplicado NUMERIC(10,4) NULL,
    fecha_conversion TIMESTAMP NULL,
    codigo_verificacion CHAR(8) NULL,
    lote_id VARCHAR(50) NULL,

    datos_cifrados TEXT NULL,

    algoritmo_usado VARCHAR(50) NOT NULL DEFAULT 'Atbash',
    fecha_registro TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    fecha_actualizacion TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_cuentas_nro_identificacion ON cuentas(nro_identificacion);
CREATE INDEX IF NOT EXISTS idx_cuentas_nombre ON cuentas(nombres, apellidos);
CREATE INDEX IF NOT EXISTS idx_cuentas_nro_cuenta ON cuentas(nro_cuenta);
CREATE INDEX IF NOT EXISTS idx_cuentas_id_banco ON cuentas(id_banco);
CREATE INDEX IF NOT EXISTS idx_cuentas_lote_id ON cuentas(lote_id);
CREATE INDEX IF NOT EXISTS idx_cuentas_codigo_verificacion ON cuentas(codigo_verificacion);
CREATE INDEX IF NOT EXISTS idx_cuentas_fecha_conversion ON cuentas(fecha_conversion);

CREATE OR REPLACE FUNCTION update_fecha_actualizacion()
RETURNS TRIGGER AS $$
BEGIN
    NEW.fecha_actualizacion = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_update_fecha_actualizacion ON cuentas;

CREATE TRIGGER trg_update_fecha_actualizacion
BEFORE UPDATE ON cuentas
FOR EACH ROW
EXECUTE FUNCTION update_fecha_actualizacion();