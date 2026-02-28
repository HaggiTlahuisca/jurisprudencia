import os
import time
import random
from datetime import datetime, timedelta
from collections import deque

import requests
from dotenv import load_dotenv
from pymongo.mongo_client import MongoClient
from pymongo import UpdateOne, ReturnDocument
from openai import OpenAI

load_dotenv()

# =========================
# Config
# =========================
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("MONGODB_DB", "tepantlatia_db")

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
EMBED_MODEL = os.getenv("EMBED_MODEL", "text-embedding-3-small")

URL_BASE_TESIS = os.getenv(
    "URL_BASE_TESIS",
    "https://bicentenario.scjn.gob.mx/repositorio-scjn/api/v1/tesis/",
)

if not MONGO_URI:
    raise RuntimeError("Falta MONGO_URI.")
if not OPENAI_API_KEY:
    raise RuntimeError("Falta OPENAI_API_KEY.")

client_ai = OpenAI(api_key=OPENAI_API_KEY)
http = requests.Session()

# SCJN retries/backoff
RETRY_ATTEMPTS = int(os.getenv("RETRY_ATTEMPTS", "3"))
RETRY_BACKOFF_BASE = float(os.getenv("RETRY_BACKOFF_BASE", "1.0"))
RETRY_JITTER_MAX = float(os.getenv("RETRY_JITTER_MAX", "0.6"))
RETRY_STATUS_CODES = {429, 500, 502, 503, 504}

MAX_ERRORES_SCJN = int(os.getenv("MAX_ERRORES_SCJN", "5"))
ESPERA_PAUSA_SCJN = int(os.getenv("ESPERA_PAUSA_SCJN", str(20 * 60)))  # 20 min

# Loop
ESPERA_NORMAL = float(os.getenv("ESPERA_NORMAL", "0.35"))
LOCK_STALE_MIN = int(os.getenv("LOCK_STALE_MIN", "30"))

# Round-robin: 6 tesis por 1 TFJA (ajústalo si quieres)
W_TESIS = int(os.getenv("W_TESIS", "6"))
W_TFJA = int(os.getenv("W_TFJA", "1"))
SCHEDULE = (["tesis"] * W_TESIS) + (["tfja"] * W_TFJA)

# =========================
# Mongo globals
# =========================
client_mongo = None
db = None

acervo_historico = None
cola_tesis = None
meta = None

sources_tfja = None
cola_tfja = None

# =========================
# Helpers
# =========================
def conectar_mongo():
    while True:
        try:
            client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
            client.server_info()
            print("Conectado a MongoDB")
            return client
        except Exception as e:
            print(f"Error conectando a MongoDB, reintentando: {e}")
            time.sleep(5)

def obtener_vector(texto: str):
    texto = (texto or "").strip()
    if not texto:
        return None

    for _ in range(3):
        try:
            resp = client_ai.embeddings.create(
                input=texto[:8000],
                model=EMBED_MODEL,
            )
            return resp.data[0].embedding
        except Exception as e:
            print(f"Error al vectorizar, reintentando: {e}")
            time.sleep(2)

    return None

def tomar_siguiente(cola):
    return cola.find_one_and_update(
        {"estado": "pendiente"},
        {"$set": {"estado": "procesando", "tomado_en": datetime.utcnow()}, "$inc": {"intentos": 1}},
        return_document=ReturnDocument.AFTER,
    )

def marcar_completado(cola, filtro: dict):
    cola.update_one(filtro, {"$set": {"estado": "completado", "completado_en": datetime.utcnow()}})

def marcar_error(cola, filtro: dict, mensaje: str):
    cola.update_one(
        filtro,
        {"$set": {"estado": "error", "error_en": datetime.utcnow(), "mensaje_error": str(mensaje)[:800]}},
    )

def liberar_locks_stale(cola):
    limite = datetime.utcnow() - timedelta(minutes=LOCK_STALE_MIN)
    res = cola.update_many(
        {"estado": "procesando", "tomado_en": {"$lt": limite}},
        {"$set": {"estado": "pendiente", "liberado_en": datetime.utcnow()}},
    )
    if res.modified_count:
        print(f"Liberados {res.modified_count} locks stale en {cola.name}")

# =========================
# TESIS (SCJN)
# =========================
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

def inicializar_cola_tesis():
    existente = meta.find_one({"tipo": "cola_inicializada"})
    if existente:
        print("Cola de tesis ya inicializada.")
        return

    print("Inicializando cola de tesis...")
    bulk = []
    for inicio, fin in BLOQUES:
        for registro_id in range(inicio, fin):
            bulk.append(
                UpdateOne(
                    {"registro": str(registro_id)},
                    {"$setOnInsert": {"registro": str(registro_id), "estado": "pendiente", "intentos": 0, "creado_en": datetime.utcnow()}},
                    upsert=True,
                )
            )
            if len(bulk) >= 1000:
                cola_tesis.bulk_write(bulk, ordered=False)
                bulk = []
    if bulk:
        cola_tesis.bulk_write(bulk, ordered=False)

    meta.update_one({"tipo": "cola_inicializada"}, {"$set": {"fecha": datetime.utcnow()}}, upsert=True)
    print("Cola de tesis inicializada.")

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
        return ", ".join((x.get("descripcion") or x.get("clave") or "N/A") for x in materia)
    return "N/A"

def _sleep_backoff(attempt_index: int):
    base = RETRY_BACKOFF_BASE * (2 ** attempt_index)
    jitter = random.uniform(0, RETRY_JITTER_MAX)
    time.sleep(base + jitter)

def pedir_tesis_con_reintentos(registro_id: str):
    url = f"{URL_BASE_TESIS}{registro_id}"
    last_resp = None
    last_err = None
    agotado = False

    for i in range(RETRY_ATTEMPTS):
        try:
            resp = http.get(url, timeout=10)
            last_resp = resp

            if resp.status_code == 200:
                return resp, None, False

            if resp.status_code in RETRY_STATUS_CODES:
                last_err = f"HTTP {resp.status_code}"
                if i < RETRY_ATTEMPTS - 1:
                    _sleep_backoff(i)
                    continue
                agotado = True
                return resp, f"HTTP {resp.status_code} (agotó reintentos)", True

            return resp, f"HTTP {resp.status_code} (no-retry)", False

        except requests.RequestException as e:
            last_err = f"RequestException: {e}"
            if i < RETRY_ATTEMPTS - 1:
                _sleep_backoff(i)
                continue
            agotado = True
            return last_resp, f"{last_err} (agotó reintentos)", True

    agotado = True
    return last_resp, (last_err or "Fallo desconocido (agotó reintentos)"), True

def procesar_tesis(doc_cola):
    """
    Regresa: (ok: bool, scjn_error_real: bool)
    """
    registro_id = str(doc_cola.get("registro", "")).strip()
    if not registro_id:
        marcar_error(cola_tesis, {"_id": doc_cola["_id"]}, "Falta registro")
        return True, False

    if acervo_historico.find_one({"registro": registro_id, "procesado": True}):
        marcar_completado(cola_tesis, {"registro": registro_id})
        return True, False

    resp, err, agotado = pedir_tesis_con_reintentos(registro_id)

    if resp is None:
        marcar_error(cola_tesis, {"registro": registro_id}, err or "Sin respuesta")
        return False, True

    if resp.status_code != 200:
        if resp.status_code in (404, 410):
            marcar_error(cola_tesis, {"registro": registro_id}, err or f"HTTP {resp.status_code}")
            marcar_completado(cola_tesis, {"registro": registro_id})
            return True, False

        marcar_error(cola_tesis, {"registro": registro_id}, err or f"HTTP {resp.status_code}")
        if agotado and resp.status_code in RETRY_STATUS_CODES:
            return False, True

        marcar_completado(cola_tesis, {"registro": registro_id})
        return True, False

    try:
        data = resp.json()
    except Exception as e:
        marcar_error(cola_tesis, {"registro": registro_id}, f"JSON inválido: {e}")
        return False, False

    rubro = (data.get("rubro") or "").strip()
    texto = (data.get("texto") or "").strip()
    if not rubro or not texto:
        marcar_error(cola_tesis, {"registro": registro_id}, "Sin rubro o texto")
        marcar_completado(cola_tesis, {"registro": registro_id})
        return True, False

    vector = obtener_vector(f"{rubro} {texto}")
    if not vector:
        marcar_error(cola_tesis, {"registro": registro_id}, "Error al vectorizar")
        return False, False

    out = {
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

    acervo_historico.update_one({"registro": registro_id}, {"$set": out}, upsert=True)
    marcar_completado(cola_tesis, {"registro": registro_id})
    return True, False

# =========================
# TFJA
# =========================
def procesar_tfja(doc_cola):
    """
    Espera documentos en cola_tfja con:
    - doc_id (string)
    - texto (string)
    - rubro (opcional)
    - epoca/anio/mes (opcionales)
    """
    doc_id = doc_cola.get("doc_id")
    if not doc_id:
        marcar_error(cola_tfja, {"_id": doc_cola["_id"]}, "Falta doc_id")
        return True

    if sources_tfja.find_one({"doc_id": doc_id, "procesado": True}):
        marcar_completado(cola_tfja, {"doc_id": doc_id})
        return True

    rubro = (doc_cola.get("rubro") or "").strip()
    texto = (doc_cola.get("texto") or "").strip()

    if not texto:
        marcar_error(cola_tfja, {"doc_id": doc_id}, "Sin texto")
        marcar_completado(cola_tfja, {"doc_id": doc_id})
        return True

    epoca = doc_cola.get("epoca", "N/A")
    anio = doc_cola.get("anio", "N/A")
    mes = doc_cola.get("mes", "N/A")

    prompt = (
        "TFJA\n"
        f"Época: {epoca}\n"
        f"Año: {anio}\n"
        f"Mes: {mes}\n"
        f"Rubro: {rubro}\n\n"
        f"{texto}"
    )
    vector = obtener_vector(prompt)
    if not vector:
        marcar_error(cola_tfja, {"doc_id": doc_id}, "Error al vectorizar")
        return False

    out = {
        "doc_id": doc_id,
        "tipo": doc_cola.get("tipo", "TFJA"),
        "epoca": epoca,
        "anio": anio,
        "mes": mes,
        "rubro": rubro,
        "texto": texto,
        "source_file": doc_cola.get("source_file"),
        "source_path": doc_cola.get("source_path"),
        "vector_busqueda": vector,
        "procesado": True,
        "actualizado_en": datetime.utcnow(),
    }

    sources_tfja.update_one({"doc_id": doc_id}, {"$set": out}, upsert=True)
    marcar_completado(cola_tfja, {"doc_id": doc_id})
    return True

# =========================
# Main loop
# =========================
def worker_loop():
    global client_mongo, db
    global acervo_historico, cola_tesis, meta
    global sources_tfja, cola_tfja

    client_mongo = conectar_mongo()
    db = client_mongo[DB_NAME]

    acervo_historico = db["acervo_historico"]
    cola_tesis = db["cola_tesis"]
    meta = db["meta"]

    sources_tfja = db["sources_tfja"]
    cola_tfja = db["cola_tfja"]

    acervo_historico.create_index("registro", unique=True)
    sources_tfja.create_index("doc_id", unique=True)
    cola_tfja.create_index("doc_id", unique=True, sparse=True)

    inicializar_cola_tesis()

    tiempos = deque(maxlen=20)
    errores_scjn_consecutivos = 0
    i = 0

    while True:
        if i % 200 == 0:
            liberar_locks_stale(cola_tesis)
            liberar_locks_stale(cola_tfja)

        tipo = SCHEDULE[i % len(SCHEDULE)]
        i += 1

        procesado_algo = False

        if tipo == "tesis":
            doc = tomar_siguiente(cola_tesis)
            if doc:
                procesado_algo = True
                ok, scjn_error_real = procesar_tesis(doc)
                if scjn_error_real and not ok:
                    errores_scjn_consecutivos += 1
                elif ok:
                    errores_scjn_consecutivos = 0

                if errores_scjn_consecutivos >= MAX_ERRORES_SCJN:
                    print(
                        f"SCJN inestable ({errores_scjn_consecutivos} errores seguidos). "
                        f"Pausando {ESPERA_PAUSA_SCJN // 60} minutos..."
                    )
                    time.sleep(ESPERA_PAUSA_SCJN)
                    print("Retomando procesamiento después de la pausa.")
                    errores_scjn_consecutivos = 0

        else:
            doc = tomar_siguiente(cola_tfja)
            if doc:
                procesado_algo = True
                _ = procesar_tfja(doc)

        if procesado_algo:
            tiempos.append(time.time())
            if len(tiempos) >= 10 and (tiempos[-1] - tiempos[0]) > 0:
                tps = len(tiempos) / (tiempos[-1] - tiempos[0])
                print(f"Velocidad (ventana): {tps:.2f} items/seg")
            time.sleep(ESPERA_NORMAL)
        else:
            time.sleep(1)

if __name__ == "__main__":
    worker_loop()
