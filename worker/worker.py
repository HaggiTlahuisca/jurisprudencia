import os
import time
import random
from datetime import datetime, timedelta
from collections import deque

import requests
from dotenv import load_dotenv
from pymongo import MongoClient 
from pymongo import UpdateOne, ReturnDocument
from openai import OpenAI

load_dotenv()

# =========================
# Config base
# =========================
MONGOURI = os.getenv("MONGOURI")
DBNAME = os.getenv("MONGODBDB", "tepantlatia_db")

OPENAIAPIKEY = os.getenv("OPENAIAPIKEY")
EMBEDMODEL = os.getenv("EMBEDMODEL", "text-embedding-3-small")

URLBASETESIS = os.getenv(
    "URLBASETESIS",
    "https://bicentenario.scjn.gob.mx/repositorio-scjn/api/v1/tesis/",
)

if not MONGOURI:
    raise RuntimeError("Falta MONGOURI.")
if not OPENAIAPIKEY:
    raise RuntimeError("Falta OPENAIAPIKEY.")

clientai = OpenAI(api_key=OPENAIAPIKEY)
http = requests.Session()

# =========================
# Retries/backoff SCJN
# =========================
RETRYATTEMPTS = int(os.getenv("RETRYATTEMPTS", "3"))
RETRYBACKOFFBASE = float(os.getenv("RETRYBACKOFFBASE", "1.0"))
RETRYJITTERMAX = float(os.getenv("RETRYJITTERMAX", "0.6"))
RETRYSTATUSCODES = {429, 500, 502, 503, 504}

MAXERRORESSCJN = int(os.getenv("MAXERRORESSCJN", "5"))
ESPERAPAUSASCJN = int(os.getenv("ESPERAPAUSASCJN", str(20 * 60)))  # 20 min

# Loop
ESPERANORMAL = float(os.getenv("ESPERANORMAL", "0.35"))
LOCKSTALEMIN = int(os.getenv("LOCKSTALEMIN", "30"))

# Round-robin: 6 tesis por 1 TFJA
WTESIS = int(os.getenv("WTESIS", "6"))
WTFJA = int(os.getenv("WTFJA", "1"))
SCHEDULE = (["tesis"] * WTESIS) + (["tfja"] * WTFJA)

# =========================
# Diferido / No disponible
# =========================
DIFERIDO_MINUTOS = int(os.getenv("DIFERIDO_MINUTOS", "60"))
NO_DISPONIBLE_DIAS = int(os.getenv("NO_DISPONIBLE_DIAS", "3"))

# Siembra masiva (opcional)
SEEDCOLATESIS = os.getenv("SEEDCOLATESIS", "0").strip()  # "1" para sembrar, "0" para NO

# Vectorización por rango de años (opcional)
VECTORRANGO = os.getenv("VECTORRANGO", "0").strip()  # "1" = solo si anio en rango; "0" = vectoriza todo
VECTORANIO_MIN = int(os.getenv("VECTORANIO_MIN", "1980"))
VECTORANIO_MAX = int(os.getenv("VECTORANIO_MAX", "2026"))
VECTOR_ANIO_DESCONOCIDO = os.getenv("VECTOR_ANIO_DESCONOCIDO", "0").strip()

# Timeout de request SCJN (sube esto si ves muchos Read timeout)
SCJN_TIMEOUT = int(os.getenv("SCJN_TIMEOUT", "20"))

# =========================
# Mongo globals
# =========================
clientmongo = None
db = None
acervohistorico = None
colatesis = None
meta = None
sourcestfja = None
colatfja = None


def conectarmongo():
    while True:
        try:
            client = MongoClient(MONGOURI, serverSelectionTimeoutMS=5000)
            client.server_info()
            print("Conectado a MongoDB")
            return client
        except Exception as e:
            print(f"Error conectando a MongoDB, reintentando: {e}")
            time.sleep(5)


def obtenervector(texto: str):
    texto = (texto or "").strip()
    if not texto:
        return None
    for _ in range(3):
        try:
            resp = clientai.embeddings.create(input=texto[:8000], model=EMBEDMODEL)
            return resp.data[0].embedding
        except Exception as e:
            print(f"Error al vectorizar, reintentando: {e}")
            time.sleep(2)
    return None


def _leer_creado_en(doc: dict) -> datetime:
    if not doc:
        return datetime.utcnow()
    ce = doc.get("creado_en") or doc.get("creadoen")
    if isinstance(ce, datetime):
        return ce
    return datetime.utcnow()


def tomarsiguientecola(cola):
    ahora = datetime.utcnow()
    filtro = {
        "$or": [
            {"estado": "pendiente"},
            {"estado": "diferido", "next_run_at": {"$lte": ahora}},
        ]
    }
    update = {
        "$set": {"estado": "procesando", "tomadoen": ahora},
        "$inc": {"intentos": 1},
    }
    return cola.find_one_and_update(
        filtro,
        update,
        sort=[("next_run_at", 1), ("creadoen", 1)],
        return_document=ReturnDocument.AFTER,
    )


def marcarcompletado(cola, filtro: dict):
    cola.update_one(filtro, {"$set": {"estado": "completado", "completadoen": datetime.utcnow()}})


def marcarerror(cola, filtro: dict, mensaje: str):
    ahora = datetime.utcnow()
    msg = str(mensaje)[:800]
    cola.update_one(
        filtro,
        {"$set": {"estado": "error", "erroren": ahora, "mensajeerror": msg, "error_en": ahora, "mensaje_error": msg}},
    )


def marcar_diferido_o_no_disponible(cola, filtro: dict, mensaje: str):
    """
    Para errores temporales de la SCJN (500/502/503/504 y timeouts):
    - Lee creado_en / creadoen para compatibilidad
    - < NO_DISPONIBLE_DIAS -> estado "diferido", next_run_at = ahora + DIFERIDO_MINUTOS
    - >= NO_DISPONIBLE_DIAS -> estado "no_disponible"
    Guarda error en mensajeerror y también en mensaje_error (compat).
    """
    ahora = datetime.utcnow()
    msg = str(mensaje)[:800]

    doc = cola.find_one(filtro, {"creadoen": 1, "creado_en": 1})
    creado_en = _leer_creado_en(doc) if doc else ahora
    dias_transcurridos = (ahora - creado_en).total_seconds() / 86400.0

    if dias_transcurridos >= NO_DISPONIBLE_DIAS:
        cola.update_one(
            filtro,
            {
                "$set": {
                    "estado": "no_disponible",
                    "no_disponible_en": ahora,
                    "erroren": ahora,
                    "mensajeerror": msg,
                    "error_en": ahora,
                    "mensaje_error": msg,
                }
            },
        )
        print(f"  -> no_disponible tras {dias_transcurridos:.1f} dias: {filtro}")
    else:
        next_run = ahora + timedelta(minutes=DIFERIDO_MINUTOS)
        cola.update_one(
            filtro,
            {
                "$set": {
                    "estado": "diferido",
                    "next_run_at": next_run,
                    "diferido_en": ahora,
                    "erroren": ahora,
                    "mensajeerror": msg,
                    "error_en": ahora,
                    "mensaje_error": msg,
                }
            },
        )


def liberarlocksstale(cola):
    limite = datetime.utcnow() - timedelta(minutes=LOCKSTALEMIN)
    res = cola.update_many(
        {"estado": "procesando", "tomadoen": {"$lt": limite}},
        {"$set": {"estado": "pendiente", "liberadoen": datetime.utcnow()}},
    )
    if res.modified_count:
        print(f"Liberados {res.modified_count} locks stale en {cola.name}")


def sleepbackoff(attemptindex: int):
    base = RETRYBACKOFFBASE * (2 ** attemptindex)
    jitter = random.uniform(0, RETRYJITTERMAX)
    time.sleep(base + jitter)


def extraermateriadata(data: dict):
    materia = data.get("materias") or data.get("materia")
    if not materia:
        return "NA"
    if isinstance(materia, str):
        return materia
    if isinstance(materia, list) and all(isinstance(x, str) for x in materia):
        return ", ".join(materia)
    if isinstance(materia, dict):
        return materia.get("descripcion") or materia.get("clave") or "NA"
    if isinstance(materia, list) and all(isinstance(x, dict) for x in materia):
        return ", ".join((x.get("descripcion") or x.get("clave") or "NA") for x in materia)
    return "NA"


def _to_int_or_none(x):
    try:
        if x is None:
            return None
        return int(x)
    except Exception:
        return None


def _decidir_vectorizar(anio):
    if VECTORRANGO != "1":
        return True
    if anio is None:
        return VECTOR_ANIO_DESCONOCIDO == "1"
    return VECTORANIO_MIN <= anio <= VECTORANIO_MAX


def pedirtesisconreintentos(registroid: str):
    url = f"{URLBASETESIS}{registroid}"
    lastresp = None
    lasterr = None

    for i in range(RETRYATTEMPTS):
        try:
            resp = http.get(url, timeout=SCJN_TIMEOUT)
            lastresp = resp

            if resp.status_code == 200:
                return resp, None, False

            if resp.status_code in RETRYSTATUSCODES:
                lasterr = f"HTTP {resp.status_code}"
                if i < RETRYATTEMPTS - 1:
                    sleepbackoff(i)
                    continue
                return resp, f"HTTP {resp.status_code} agotó reintentos", True

            return resp, f"HTTP {resp.status_code} no-retry", False

        except requests.RequestException as e:
            lasterr = f"RequestException: {e}"
            if i < RETRYATTEMPTS - 1:
                sleepbackoff(i)
                continue
            return lastresp, f"{lasterr} agotó reintentos", True

    return lastresp, (lasterr or "Fallo desconocido agotó reintentos"), True


# =========================
# Cola tesis (siembra original)
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


def inicializarcolatesis():
    if SEEDCOLATESIS != "1":
        print("Siembra de cola_tesis desactivada (SEEDCOLATESIS=0).")
        return

    existente = meta.find_one({"tipo": "colainicializada"})
    if existente:
        print("Cola de tesis ya inicializada.")
        return

    print("Inicializando cola de tesis...")
    bulk = []
    for inicio, fin in BLOQUES:
        for registroid in range(inicio, fin):
            bulk.append(
                UpdateOne(
                    {"registro": str(registroid)},
                    {
                        "$setOnInsert": {
                            "registro": str(registroid),
                            "estado": "pendiente",
                            "intentos": 0,
                            "creadoen": datetime.utcnow(),
                        }
                    },
                    upsert=True,
                )
            )
            if len(bulk) >= 1000:
                colatesis.bulk_write(bulk, ordered=False)
                bulk = []

    if bulk:
        colatesis.bulk_write(bulk, ordered=False)

    meta.update_one({"tipo": "colainicializada"}, {"$set": {"fecha": datetime.utcnow()}}, upsert=True)
    print("Cola de tesis inicializada.")


def procesartesisdoc(doccola):
    registroid = str(doccola.get("registro", "")).strip()
    if not registroid:
        marcarerror(colatesis, {"_id": doccola["_id"]}, "Falta registro")
        return True, False

    if acervohistorico.find_one({"registro": registroid, "procesado": True}):
        marcarcompletado(colatesis, {"registro": registroid})
        return True, False

    resp, err, agotado = pedirtesisconreintentos(registroid)

    if resp is None:
        # Timeout / conexión: diferir
        marcar_diferido_o_no_disponible(colatesis, {"registro": registroid}, err or "Sin respuesta")
        return False, True

    if resp.status_code != 200:
        if resp.status_code in (404, 410):
            marcarerror(colatesis, {"registro": registroid}, err or f"HTTP {resp.status_code}")
            marcarcompletado(colatesis, {"registro": registroid})
            return True, False

        # Para 500/502/503/504/429: si agotó reintentos, difiere; si no, ya viene manejado por pedirtesisconreintentos
        if agotado and resp.status_code in RETRYSTATUSCODES:
            marcar_diferido_o_no_disponible(colatesis, {"registro": registroid}, err or f"HTTP {resp.status_code}")
            return False, True

        marcarerror(colatesis, {"registro": registroid}, err or f"HTTP {resp.status_code}")
        return True, False

    try:
        data = resp.json()
    except Exception as e:
        marcarerror(colatesis, {"registro": registroid}, f"JSON inválido: {e}")
        return False, False

    rubro = (data.get("rubro") or "").strip()
    texto = (data.get("texto") or "").strip()
    if not rubro or not texto:
        marcarerror(colatesis, {"registro": registroid}, "Sin rubro o texto")
        marcarcompletado(colatesis, {"registro": registroid})
        return True, False

    anio = _to_int_or_none(data.get("anio"))
    mes = (data.get("mes") or "").strip() or None
    tipotesis = (data.get("tipoTesis") or "").strip() or None
    notapublica = (data.get("notaPublica") or "").strip() or None
    localizacion = (data.get("localizacion") or "").strip() or None

    vector = None
    if _decidir_vectorizar(anio):
        prompt = "\n".join(
            [
                "SCJN/SJF",
                f"Registro: {registroid}",
                f"Año: {anio}",
                f"Mes: {mes}",
                f"TipoTesis: {tipotesis}",
                f"Época: {data.get('epoca', 'N/A')}",
                f"Instancia: {data.get('instancia', 'N/A')}",
                f"Materias: {extraermateriadata(data)}",
                f"Rubro: {rubro}",
                "",
                texto,
            ]
        )
        vector = obtenervector(prompt)
        if not vector:
            marcarerror(colatesis, {"registro": registroid}, "Error al vectorizar")
            return False, False

    out = {
        "registro": registroid,
        "idTesis": data.get("idTesis"),
        "rubro": rubro,
        "texto": texto,
        "epoca": data.get("epoca", "N/A"),
        "instancia": data.get("instancia", "N/A"),
        "organoJuris": data.get("organoJuris"),
        "fuente": data.get("fuente", "Repositorio Bicentenario"),
        "tesis": data.get("tesis"),
        "tipoTesis": tipotesis,
        "anio": anio,
        "mes": mes,
        "notaPublica": notapublica,
        "localizacion": localizacion,
        "precedentes": data.get("precedentes"),
        "huellaDigital": data.get("huellaDigital"),
        "materia": extraermateriadata(data),
        "vectorbusqueda": vector,
        "vectorbusqueda_ok": bool(vector),
        "procesado": True,
        "actualizadoen": datetime.utcnow(),
    }

    acervohistorico.update_one({"registro": registroid}, {"$set": out}, upsert=True)
    marcarcompletado(colatesis, {"registro": registroid})
    return True, False


# =========================
# TFJA (sin cambios mayores)
# =========================
def procesartfjadoc(doccola):
    docid = doccola.get("docid")
    if not docid:
        marcarerror(colatfja, {"_id": doccola["_id"]}, "Falta docid")
        return True

    if sourcestfja.find_one({"docid": docid, "procesado": True}):
        marcarcompletado(colatfja, {"docid": docid})
        return True

    rubro = (doccola.get("rubro") or "").strip()
    texto = (doccola.get("texto") or "").strip()
    if not texto:
        marcarerror(colatfja, {"docid": docid}, "Sin texto")
        marcarcompletado(colatfja, {"docid": docid})
        return True

    epoca = doccola.get("epoca", "N/A")
    anio = doccola.get("anio", "N/A")
    mes = doccola.get("mes", "N/A")

    prompt = "\n".join(
        [
            "TFJA",
            f"Época: {epoca}",
            f"Año: {anio}",
            f"Mes: {mes}",
            f"Rubro: {rubro}",
            "",
            texto,
        ]
    )
    vector = obtenervector(prompt)
    if not vector:
        marcarerror(colatfja, {"docid": docid}, "Error al vectorizar")
        return False

    out = {
        "docid": docid,
        "tipo": doccola.get("tipo", "TFJA"),
        "epoca": epoca,
        "anio": anio,
        "mes": mes,
        "rubro": rubro,
        "texto": texto,
        "sourcefile": doccola.get("sourcefile"),
        "sourcepath": doccola.get("sourcepath"),
        "vectorbusqueda": vector,
        "procesado": True,
        "actualizadoen": datetime.utcnow(),
    }

    sourcestfja.update_one({"docid": docid}, {"$set": out}, upsert=True)
    marcarcompletado(colatfja, {"docid": docid})
    return True


def workerloop():
    global clientmongo, db
    global acervohistorico, colatesis, meta
    global sourcestfja, colatfja

    clientmongo = conectarmongo()
    db = clientmongo[DBNAME]

    acervohistorico = db["acervo_historico"]
    colatesis = db["cola_tesis"]
    meta = db["meta"]

    sourcestfja = db["sources_tfja"]
    colatfja = db["cola_tfja"]

    # Índices
    try:
        acervohistorico.create_index("registro", unique=True)
    except Exception as e:
        print(f"Índice ya existe o se omite: {e}")

    try:
        sourcestfja.create_index("docid", unique=True)
    except Exception as e:
        print(f"Índice ya existe o se omite: {e}")

    try:
        colatfja.create_index("docid", unique=True)
    except Exception as e:
        print(f"Índice ya existe o se omite: {e}")

    # Para el diferido
    try:
        colatesis.create_index([("estado", 1), ("next_run_at", 1)])
    except Exception as e:
        print(f"Índice ya existe o se omite: {e}")

    inicializarcolatesis()

    tiempos = deque(maxlen=20)
    erroresscjnconsecutivos = 0
    i = 0

    while True:
        if i % 200 == 0:
            liberarlocksstale(colatesis)
            liberarlocksstale(colatfja)

        tipo = SCHEDULE[i % len(SCHEDULE)]
        i += 1
        procesadoalgo = False

        if tipo == "tesis":
            doc = tomarsiguientecola(colatesis)
            if doc:
                procesadoalgo = True
                ok, scjnerrorreal = procesartesisdoc(doc)

                if scjnerrorreal and not ok:
                    erroresscjnconsecutivos += 1
                elif ok:
                    erroresscjnconsecutivos = 0

                if erroresscjnconsecutivos >= MAXERRORESSCJN:
                    print(
                        f"SCJN inestable ({erroresscjnconsecutivos} errores seguidos). "
                        f"Pausando {ESPERAPAUSASCJN // 60} minutos..."
                    )
                    time.sleep(ESPERAPAUSASCJN)
                    print("Retomando procesamiento después de la pausa.")
                    erroresscjnconsecutivos = 0
        else:
            doc = tomarsiguientecola(colatfja)
            if doc:
                procesadoalgo = True
                procesartfjadoc(doc)

        if procesadoalgo:
            tiempos.append(time.time())
            if len(tiempos) >= 10 and (tiempos[-1] - tiempos[0]) > 0:
                tps = len(tiempos) / (tiempos[-1] - tiempos[0])
                print(f"Velocidad (ventana): {tps:.2f} items/seg")
            time.sleep(ESPERANORMAL)
        else:
            time.sleep(1)


if __name__ == "__main__":
    workerloop()
