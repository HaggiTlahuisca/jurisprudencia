import os
import time
import threading
from datetime import datetime

import requests
from pymongo.mongo_client import MongoClient
from pymongo import UpdateOne, ReturnDocument
from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse, JSONResponse
from openai import OpenAI
from dotenv import load_dotenv

# ============================
# 1. CONFIGURACIÓN
# ============================

load_dotenv()
client_ai = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

def conectar_mongo():
    while True:
        try:
            client = MongoClient(os.getenv("MONGO_URI"), serverSelectionTimeoutMS=5000)
            client.server_info()
            print("🔗 Conectado a MongoDB")
            return client
        except Exception as e:
            print(f"⚠️ Error conectando a MongoDB, reintentando: {e}")
            time.sleep(5)

client_mongo = conectar_mongo()
db = client_mongo["tepantlatia_db"]
coleccion = db["acervo_historico"]
cola = db["cola_tesis"]
meta = db["meta"]

URL_BASE = "https://bicentenario.scjn.gob.mx/repositorio-scjn/api/v1/tesis/"

app = FastAPI(title="Acervo Worker Dashboard")


# ============================
# 2. EMBEDDINGS
# ============================

def obtener_vector(texto: str):
    for _ in range(3):
        try:
            response = client_ai.embeddings.create(
                input=texto,
                model="text-embedding-3-small",
            )
            return response.data[0].embedding
        except Exception as e:
            print(f"❌ Error al vectorizar, reintentando: {e}")
            time.sleep(2)
    return None


# ============================
# 3. LEYES FUNDAMENTALES
# ============================

def cargar_leyes_fundamentales():
    print("⚖️ Cargando leyes fundamentales...")

    leyes = [
        {
            "registro": "L-CFF-38",
            "rubro": "CFF ARTÍCULO 38 - REQUISITOS DE LOS ACTOS ADMINISTRATIVOS",
            "texto": "Los actos administrativos que se deban notificar deberán contener...",
            "epoca": "LEY VIGENTE",
            "materia": "FISCAL",
        },
        {
            "registro": "L-CFF-42",
            "rubro": "CFF ARTÍCULO 42 - FACULTADES DE COMPROBACIÓN",
            "texto": "Las autoridades fiscales a fin de comprobar...",
            "epoca": "LEY VIGENTE",
            "materia": "FISCAL",
        },
    ]

    for ley in leyes:
        if coleccion.find_one({"registro": ley["registro"], "procesado": True}):
            print(f"⏭️ Ley ya procesada: {ley['registro']}")
            continue

        vector = obtener_vector(f"{ley['rubro']} {ley['texto']}")
        if vector:
            ley["vector_busqueda"] = vector
            ley["procesado"] = True
            coleccion.update_one(
                {"registro": ley["registro"]}, {"$set": ley}, upsert=True
            )
            print(f"✅ Ley cargada: {ley['rubro']}")


# ============================
# 4. SISTEMA DE COLAS
# ============================

BLOQUES = [
    (206000, 207000),
    (160000, 161000),
    (2023000, 2028000),
]

def inicializar_cola():
    """Llena la cola con registros pendientes si no existen."""
    existente = meta.find_one({"tipo": "cola_inicializada"})
    if existente:
        print("📦 Cola ya inicializada, no se vuelve a poblar.")
        return

    print("📦 Inicializando cola de tesis...")
    bulk = []

    for inicio, fin in BLOQUES:
        for registro_id in range(inicio, fin):
