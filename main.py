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
