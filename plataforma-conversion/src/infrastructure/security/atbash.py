import string


LOWER = string.ascii_lowercase
UPPER = string.ascii_uppercase
LOWER_REVERSED = LOWER[::-1]
UPPER_REVERSED = UPPER[::-1]


def atbash_encrypt(text: str) -> str:
    result = []

    for char in text:
        if char in LOWER:
            result.append(LOWER_REVERSED[LOWER.index(char)])
        elif char in UPPER:
            result.append(UPPER_REVERSED[UPPER.index(char)])
        else:
            result.append(char)

    return "".join(result)


def atbash_decrypt(text: str) -> str:
    return atbash_encrypt(text)


def build_sensitive_payload(
    nro_identificacion: str,
    nombres: str,
    apellidos: str,
    nro_cuenta: str,
    saldo_usd: str,
) -> str:
    return f"{nro_identificacion}|{nombres}|{apellidos}|{nro_cuenta}|{saldo_usd}"