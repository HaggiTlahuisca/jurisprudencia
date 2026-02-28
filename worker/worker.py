
import os
import time
from datetime import datetime
from collections import deque

import requests
from pymongo.mongo_client import MongoClient
from pymongo import UpdateOne, ReturnDocument
from openai import OpenAI
from dotenv import load_dotenv

# ============================
# CONFIGURACIÓN
# ============================

load_dotenv()
client_ai = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

client_mongo = None
db = None
coleccion = None
cola = None
meta = None

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

URL_BASE = "https://bicentenario.scjn.gob.mx/repositorio-scjn/api/v1/tesis/"

# ============================
# EMBEDDINGS
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
# NORMALIZACIÓN DE MATERIA
# ============================

def extraer_materia(data):
    materia = data.get("materias") or data.get("materia")

    if not materia:
        return "N/A"

    if isinstance(materia, str):
        return materia

    if isinstance(materia, list) and all(isinstance(x, str) for x in materia):
        return ", ".join(materia)

    if isinstance(materia, dict):
        return materia.get("descripcion") or materia.get("clave") or "N/A"

    if isinstance(materia, list) and all(isinstance(x, dict) for x in materia):
        return ", ".join(
            x.get("descripcion") or x.get("clave") or "N/A"
            for x in materia
        )

    return "N/A"

# ============================
# SISTEMA DE COLAS
# ============================

BLOQUES = [
    (292564, 350000),
    (350000, 400000),
    (400000, 450000),
    (450000, 500000),
    (500000, 550000),
    (550000, 600000),
    (600000, 650000),
    (650000, 700000),
    (700000, 750000),
    (750000, 800000),
    (800000, 850000),
    (850000, 900000),
    (900000, 950000),
    (950000, 1000000),
    (1000000, 1050000),
    (1050000, 1100000),
    (1100000, 1150000),
    (1150000, 1200000),
    (1200000, 1250000),
    (1250000, 1300000),
    (1300000, 1350000),
    (1350000, 1400000),
    (1400000, 1450000),
    (1450000, 1500000),
    (1500000, 1550000),
    (1550000, 1600000),
    (161000, 206000),
    (207000, 2023000),
    (2028000, 2031780),
]

def inicializar_cola():
    existente = meta.find_one({"tipo": "cola_inicializada"})
    if existente:
        print("📦 Cola ya inicializada.")
        return

    print("📦 Inicializando cola...")
    bulk = []

    for inicio, fin in BLOQUES:
        for registro_id in range(inicio, fin):

            bulk.append(
                UpdateOne(
                    {"registro": str(registro_id)},
                    {
                        "$setOnInsert": {
                            "registro": str(registro_id),
                            "estado": "pendiente",
                            "intentos": 0,
                            "creado_en": datetime.utcnow(),
                        }
                    },
                    upsert=True
                )
            )

            if len(bulk) >= 1000:
                cola.bulk_write(bulk, ordered=False)
                bulk = []

    if bulk:
        cola.bulk_write(bulk, ordered=False)

    meta.update_one(
        {"tipo": "cola_inicializada"},
        {"$set": {"fecha": datetime.utcnow()}},
        upsert=True,
    )

    print("✅ Cola inicializada.")

def tomar_siguiente_de_cola():
    return cola.find_one_and_update(
        {"estado": "pendiente"},
        {
            "$set": {"estado": "procesando", "tomado_en": datetime.utcnow()},
            "$inc": {"intentos": 1},
        },
        return_document=ReturnDocument.AFTER,
    )

def marcar_completado(registro: str):
    cola.update_one(
        {"registro": registro},
        {"$set": {"estado": "completado", "completado_en": datetime.utcnow()}},
    )

def marcar_error(registro: str, mensaje: str):
    cola.update_one(
        {"registro": registro},
        {
            "$set": {
                "estado": "error",
                "error_en": datetime.utcnow(),
                "mensaje_error": mensaje,
            }
        },
    )

# ============================
# WORKER PRINCIPAL (CON ESPERA INTELIGENTE)
# ============================

MAX_ERRORES = 5
ESPERA_BASE = 3
errores_consecutivos = 0

def procesar_registro(doc_cola):
    registro_id = doc_cola["registro"]

    if coleccion.find_one({"registro": registro_id, "procesado": True}):
        marcar_completado(registro_id)
        return

    try:
        url = f"{URL_BASE}{registro_id}"
        resp = requests.get(url, timeout=10)

        if resp.status_code != 200:
            marcar_error(registro_id, f"HTTP {resp.status_code}")
            print(f"⚠️ {registro_id}: HTTP {resp.status_code}")
            return

        data = resp.json()
        rubro = data.get("rubro", "")
        texto = data.get("texto", "")

        if not rubro or not texto:
            marcar_error(registro_id, "Sin rubro o texto")
            print(f"⚠️ {registro_id}: sin rubro o texto")
            return

        print(f"🔄 Procesando {registro_id}...")
        vector = obtener_vector(f"{rubro} {texto}")
        if not vector:
            marcar_error(registro_id, "Error al vectorizar")
            return

        documento = {
            "registro": registro_id,
            "rubro": rubro,
            "texto": texto,
            "epoca": data.get("epoca", "N/A"),
            "materia": extraer_materia(data),
            "vector_busqueda": vector,
            "fuente": "Repositorio Bicentenario",
            "procesado": True,
            "actualizado_en": datetime.utcnow(),
        }

        coleccion.update_one(
            {"registro": registro_id}, {"$set": documento}, upsert=True
        )
        marcar_completado(registro_id)

    except Exception as e:
        print(f"⚠️ Error en {registro_id}: {e}")
        marcar_error(registro_id, str(e))

def worker_loop():
    global client_mongo, db, coleccion, cola, meta, errores_consecutivos

    client_mongo = conectar_mongo()
    db = client_mongo["tepantlatia_db"]
    coleccion = db["acervo_historico"]
    cola = db["cola_tesis"]
    meta = db["meta"]

    inicializar_cola()

    tiempos = deque(maxlen=20)

    while True:
        doc = tomar_siguiente_de_cola()
        if not doc:
            print("⏸️ No hay más pendientes. Esperando...")
            time.sleep(10)
            continue

        try:
            procesar_registro(doc)
            errores_consecutivos = 0
        except:
            errores_consecutivos += 1

        tiempos.append(time.time())

        if len(tiempos) >= 5:
            tps = len(tiempos) / (tiempos[-1] - tiempos[0])
            print(f"⚡ Velocidad: {tps:.2f} tesis/segundo")

        if errores_consecutivos >= MAX_ERRORES:
            espera = ESPERA_BASE * errores_consecutivos
            print(f"⏳ SCJN inestable, esperando {espera} segundos...")
            time.sleep(espera)
            errores_consecutivos = 0

        time.sleep(0.4)

if __name__ == "__main__":
    worker_loop()
