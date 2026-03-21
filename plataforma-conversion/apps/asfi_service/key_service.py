from fastapi import FastAPI

app = FastAPI()

KEYS = {
    3: {"alg": "vigenere", "key": "BNBKEY"}
}

def vigenere_decrypt(text, key):
    result = ""
    for i, c in enumerate(text):
        shift = ord(key[i % len(key)]) % 10
        result += str((int(c) - shift) % 10)
    return result

@app.post("/decrypt")
def decrypt(data: dict):
    conf = KEYS[data["BancoId"]]

    return {
        "saldo": float(vigenere_decrypt(data["SaldoUSD"], conf["key"]))
    }