#!/bin/bash
echo "Configurando Redis para Banco BCP..."
redis-cli -a root123 <<EOF
FLUSHALL

# Estructura: hash:cuenta:{NroCuenta}
# Ejemplo de datos iniciales (se poblarán después)
EOF
echo "Redis configurado correctamente"