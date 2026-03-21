from fastapi import FastAPI
from pymongo import MongoClient

app = FastAPI(title="BNB Service")

client = MongoClient("mongodb://localhost:27017")
db = client["bnb"]
col = db["cuentas"]

def vigenere_encrypt(text, key):
    result = ""
    for i, c in enumerate(text):
        shift = ord(key[i % len(key)]) % 10
        result += str((int(c) + shift) % 10)
    return result

@app.get("/cuentas")
def cuentas():
    data = list(col.find({}, {"_id": 0}))
    for c in data:
        c["SaldoUSD"] = vigenere_encrypt(str(c["SaldoUSD"]), "BNBKEY")
    return data

@app.post("/actualizar")
def actualizar(data: dict):
    col.update_one(
        {"CuentaId": data["CuentaId"]},
        {"$set": {
            "SaldoBs": data["SaldoBs"],
            "FechaConversion": data["FechaConversion"],
            "CodigoVerificacion": data["Codigo"]
        }}
    )
    return {"msg": "Actualizado"}