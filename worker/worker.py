import os
import time
import random
import json
import threading
from datetime import datetime, timedelta
from collections import deque
from http.server import HTTPServer, BaseHTTPRequestHandler

import requests
from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo import UpdateOne, ReturnDocument
from openai import OpenAI

load_dotenv()

# =========================
# Config base
# =========================
MONGOURI = os.getenv("MONGO_URI")
DBNAME = os.getenv("MONGODB_DB", "tepantlatia_db")
OPENAIAPIKEY = os.getenv("OPENAI_API_KEY")
EMBEDMODEL = os.getenv("EMBED_MODEL", "text-embedding-3-small")
URLBASETESIS = os.getenv("URL_BASE_TESIS", "https://bicentenario.scjn.gob.mx/repositorio-scjn/api/v1/tesis/")

if not MONGOURI:
    raise RuntimeError("Falta MONGO_URI.")
if not OPENAIAPIKEY:
    raise RuntimeError("Falta OPENAI_API_KEY.")

clientai = OpenAI(api_key=OPENAIAPIKEY)
http = requests.Session()

# =========================
# Retries/backoff SCJN
# =========================
RETRYATTEMPTS = int(os.getenv("RETRY_ATTEMPTS", "3"))
RETRYBACKOFFBASE = float(os.getenv("RETRY_BACKOFF_BASE", "1.0"))
RETRYJITTERMAX = float(os.getenv("RETRY_JITTER_MAX", "0.6"))
RETRYSTATUSCODES = {429, 500, 502, 503, 504}
MAXERRORESSCJN = int(os.getenv("MAX_ERRORES_SCJN", "40"))
ESPERAPAUSASCJN = int(os.getenv("ESPERA_PAUSA_SCJN", str(5 * 60)))

# Loop (Le damos un respiro un poco mayor por defecto para salvar el CPU de Mongo)
ESPERANORMAL = float(os.getenv("ESPERA_NORMAL", "0.60"))
LOCKSTALEMIN = int(os.getenv("LOCK_STALE_MIN", "30"))

# Round-robin: 6 tesis por 1 TFJA
WTESIS = int(os.getenv("W_TESIS", "6"))
WTFJA = int(os.getenv("W_TFJA", "1"))
SCHEDULE = (["tesis"] * WTESIS) + (["tfja"] * WTFJA)

# =========================
# Diferido / No disponible
# =========================
DIFERIDO_MINUTOS = int(os.getenv("DIFERIDO_MINUTOS", "60"))
NO_DISPONIBLE_DIAS = int(os.getenv("NO_DISPONIBLE_DIAS", "3"))
MAX_INTENTOS_NO_DISPONIBLE = int(os.getenv("MAX_INTENTOS_NO_DISPONIBLE", "200"))
MIN_INTENTOS_PARA_NO_DISPONIBLE = int(os.getenv("MIN_INTENTOS_PARA_NO_DISPONIBLE", "5"))
INDEXAR_SIN_VECTOR = os.getenv("INDEXAR_SIN_VECTOR", "0").strip()
VECTORRANGO = os.getenv("VECTOR_SOLO_RANGO", "0").strip()
VECTORANIO_MIN = int(os.getenv("VECTOR_ANIO_MIN", "1980"))
VECTORANIO_MAX = int(os.getenv("VECTOR_ANIO_MAX", "2026"))
VECTOR_ANIO_DESCONOCIDO = os.getenv("VECTOR_SI_ANIO_DESCONOCIDO", "0").strip()
SCJN_TIMEOUT = int(os.getenv("SCJN_TIMEOUT", "20"))
ORDEN_REGISTRO = os.getenv("ORDEN_REGISTRO", "asc").strip().lower()
_ORDEN = 1 if ORDEN_REGISTRO == "asc" else -1
EMBED_RETRY_ATTEMPTS = int(os.getenv("EMBED_RETRY_ATTEMPTS", "3"))
EMBED_RETRY_BACKOFF_BASE = float(os.getenv("EMBED_RETRY_BACKOFF_BASE", "1.0"))
EMBED_RETRY_JITTER_MAX = float(os.getenv("EMBED_RETRY_JITTER_MAX", "0.4"))

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

# =========================
# Mini servidor HTTP
# =========================
class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"ok")
    def log_message(self, format, *args):
        pass

def iniciar_servidor_health():
    puerto = int(os.getenv("PORT", "8080"))
    servidor = HTTPServer(("0.0.0.0", puerto), HealthHandler)
    hilo = threading.Thread(target=servidor.serve_forever, daemon=True)
    hilo.start()
    print(f"Health server corriendo en puerto {puerto}")

# =========================
# Helpers generales
# =========================
def log_event(event: str, **fields):
    payload = {"event": event, "ts": datetime.utcnow().isoformat() + "Z"}
    payload.update(fields)
    try:
        print(json.dumps(payload, ensure_ascii=False))
    except Exception:
        print(f"{event} | {fields}")

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
    last_err = None
    for attempt in range(EMBED_RETRY_ATTEMPTS):
        try:
            resp = clientai.embeddings.create(input=texto[:8000], model=EMBEDMODEL)
            return resp.data[0].embedding
        except Exception as e:
            last_err = e
            if attempt < EMBED_RETRY_ATTEMPTS - 1:
                base = EMBED_RETRY_BACKOFF_BASE * (2 ** attempt)
                jitter = random.uniform(0, EMBED_RETRY_JITTER_MAX)
                time.sleep(base + jitter)
    print(f"Error al vectorizar (agoto reintentos): {last_err}")
    return None

def _leer_creado_en(doc: dict) -> datetime:
    if not doc:
        return datetime.utcnow()
    ce = doc.get("creadoen") or doc.get("creado_en")
    if isinstance(ce, datetime):
        return ce
    return datetime.utcnow()

def tomarsiguientecola(cola):
    ahora = datetime.utcnow()
    update = {
        "$set": {"estado": "procesando", "tomadoen": ahora},
        "$inc": {"intentos": 1},
    }
    for filtro, sort in [
        ({"estado": "pendiente"}, [("creadoen", _ORDEN)]),
        ({"estado": "diferido", "next_run_at": {"$lte": ahora}}, [("next_run_at", 1)]),
    ]:
        doc = cola.find_one_and_update(filtro, update, sort=sort, return_document=ReturnDocument.AFTER)
        if doc:
            return doc
    return None

def backfill_cola_campos(cola):
    ahora = datetime.utcnow()
    res1 = cola.update_many(
        {"estado": {"$in": ["pendiente", "diferido"]}, "next_run_at": {"$exists": False}},
        {"$set": {"next_run_at": ahora}},
    )
    res2 = cola.update_many(
        {"estado": {"$in": ["pendiente", "diferido"]}, "next_run_at": None},
        {"$set": {"next_run_at": ahora}},
    )
    res3 = cola.update_many({"creadoen": {"$exists": False}}, {"$set": {"creadoen": ahora}})
    if any([res1.modified_count, res2.modified_count, res3.modified_count]):
        print(f"Backfill {cola.name}: next_run_at(sin campo)={res1.modified_count}, "
              f"next_run_at(null)={res2.modified_count}, creadoen(sin campo)={res3.modified_count}")

def marcarcompletado(cola, filtro: dict):
    cola.update_one(filtro, {"$set": {"estado": "completado", "completadoen": datetime.utcnow()}})

def marcarerror(cola, filtro: dict, mensaje: str):
    ahora = datetime.utcnow()
    msg = str(mensaje)[:800]
    cola.update_one(filtro, {"$set": {"estado": "error", "erroren": ahora, "mensajeerror": msg,
                                       "error_en": ahora, "mensaje_error": msg}})

def marcar_no_encontrado(cola, filtro: dict, mensaje: str, http_status=None):
    ahora = datetime.utcnow()
    msg = str(mensaje)[:800]
    cola.update_one(filtro, {"$set": {"estado": "no_encontrado", "no_encontradoen": ahora,
                                       "mensajeerror": msg, "erroren": ahora, "http_status": http_status}})

def marcar_diferido(cola, filtro: dict, mensaje: str, minutos=None):
    ahora = datetime.utcnow()
    msg = str(mensaje)[:800]
    delay = DIFERIDO_MINUTOS if minutos is None else int(minutos)
    next_run = ahora + timedelta(minutes=delay)
    cola.update_one(filtro, {"$set": {"estado": "diferido", "next_run_at": next_run, "diferido_en": ahora,
                                       "erroren": ahora, "mensajeerror": msg, "error_en": ahora, "mensaje_error": msg}})

def marcar_diferido_o_no_disponible(cola, filtro: dict, mensaje: str):
    ahora = datetime.utcnow()
    msg = str(mensaje)[:800]
    doc = cola.find_one(filtro, {"creadoen": 1, "creado_en": 1, "intentos": 1})
    creado_en = _leer_creado_en(doc) if doc else ahora
    dias_transcurridos = (ahora - creado_en).total_seconds() / 86400.0
    intentos = int((doc or {}).get("intentos", 0) or 0)
    supera_por_intentos = (MAX_INTENTOS_NO_DISPONIBLE > 0) and (intentos >= MAX_INTENTOS_NO_DISPONIBLE)
    supera_por_dias_y_intentos = (dias_transcurridos >= NO_DISPONIBLE_DIAS) and (intentos >= MIN_INTENTOS_PARA_NO_DISPONIBLE)
    if supera_por_intentos or supera_por_dias_y_intentos:
        cola.update_one(filtro, {"$set": {"estado": "no_disponible", "no_disponible_en": ahora,
                                           "erroren": ahora, "mensajeerror": msg, "error_en": ahora, "mensaje_error": msg}})
        print(f" -> no_disponible tras {dias_transcurridos:.1f} dias e intentos={intentos}: {filtro}")
        log_event("cola_no_disponible", cola=cola.name, filtro=str(filtro),
                  dias=round(dias_transcurridos, 1), intentos=intentos)
    else:
        next_run = ahora + timedelta(minutes=DIFERIDO_MINUTOS)
        cola.update_one(filtro, {"$set": {"estado": "diferido", "next_run_at": next_run, "diferido_en": ahora,
                                           "erroren": ahora, "mensajeerror": msg, "error_en": ahora, "mensaje_error": msg}})
        log_event("cola_diferido", cola=cola.name, filtro=str(filtro),
                  intentos=intentos, next_run_at=next_run.isoformat() + "Z")

def liberarlocksstale(cola):
    limite = datetime.utcnow() - timedelta(minutes=LOCKSTALEMIN)
    res = cola.update_many(
        {"estado": "procesando", "tomadoen": {"$lt": limite}},
        {"$set": {"estado": "pendiente", "next_run_at": datetime.utcnow(), "liberadoen": datetime.utcnow()}},
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

# =========================
# SCJN
# =========================
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
            return resp, f"HTTP {resp.status_code} no-retry", False
        except requests.RequestException as e:
            lasterr = f"RequestException: {e}"
            if i < RETRYATTEMPTS - 1:
                sleepbackoff(i)
            continue
    return lastresp, (lasterr or "Fallo desconocido agoto reintentos"), True

# =========================
# Procesamiento Tesis
# =========================
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
        marcar_diferido_o_no_disponible(colatesis, {"registro": registroid}, err or "Sin respuesta")
        return False, True
    if resp.status_code != 200:
        if resp.status_code in (404, 410):
            marcar_no_encontrado(colatesis, {"registro": registroid},
                                  err or f"HTTP {resp.status_code}", http_status=resp.status_code)
            log_event("tesis_no_encontrado", registro=registroid, http_status=resp.status_code)
            return True, False
        if resp.status_code == 500:
            try:
                body = resp.json()
                es_jhipster = (body.get("message") == "error.http.500"
                               or "problem-with-message" in str(body.get("type", "")))
            except Exception:
                es_jhipster = False
            if es_jhipster:
                marcar_no_encontrado(colatesis, {"registro": registroid},
                                      "JHipster 500 permanente", http_status=500)
                log_event("tesis_no_encontrado_jhipster", registro=registroid)
                return True, False
        if agotado and resp.status_code in RETRYSTATUSCODES:
            marcar_diferido_o_no_disponible(colatesis, {"registro": registroid},
                                             err or f"HTTP {resp.status_code}")
            return False, True
        marcarerror(colatesis, {"registro": registroid}, err or f"HTTP {resp.status_code}")
        return True, False
    try:
        data = resp.json()
    except Exception as e:
        marcarerror(colatesis, {"registro": registroid}, f"JSON invalido: {e}")
        return False, False
    rubro = (data.get("rubro") or "").strip()
    texto = (data.get("texto") or "").strip()
    if not rubro or not texto:
        marcarcompletado(colatesis, {"registro": registroid})
        return True, False
    anio = _to_int_or_none(data.get("anio"))
    mes = (data.get("mes") or "").strip() or None
    tipotesis = (data.get("tipoTesis") or "").strip() or None
    notapublica = (data.get("notaPublica") or "").strip() or None
    localizacion = (data.get("localizacion") or "").strip() or None
    vector = None
    if _decidir_vectorizar(anio):
        prompt = "\n".join([
            "SCJN/SJF", f"Registro: {registroid}", f"Anio: {anio}", f"Mes: {mes}",
            f"TipoTesis: {tipotesis}", f"Epoca: {data.get('epoca', 'N/A')}",
            f"Instancia: {data.get('instancia', 'N/A')}", f"Materias: {extraermateriadata(data)}",
            f"Rubro: {rubro}", "", texto,
        ])
        vector = obtenervector(prompt)
        if not vector:
            if INDEXAR_SIN_VECTOR == "1":
                log_event("tesis_vector_fallo_indexa_sin_vector", registro=registroid)
                vector = None
            else:
                marcar_diferido(colatesis, {"registro": registroid}, "Embeddings fallo (temporal)")
                log_event("tesis_vector_fallo_diferido", registro=registroid)
                return False, False
    out = {
        "registro": registroid, "idTesis": data.get("idTesis"), "rubro": rubro, "texto": texto,
        "epoca": data.get("epoca", "N/A"), "instancia": data.get("instancia", "N/A"),
        "organoJuris": data.get("organoJuris"), "fuente": data.get("fuente", "Repositorio Bicentenario"),
        "tesis": data.get("tesis"), "tipoTesis": tipotesis, "anio": anio, "mes": mes,
        "notaPublica": notapublica, "localizacion": localizacion, "precedentes": data.get("precedentes"),
        "huellaDigital": data.get("huellaDigital"), "materia": extraermateriadata(data),
        "vectorbusqueda": vector, "vectorbusqueda_ok": bool(vector),
        "procesado": True, "actualizadoen": datetime.utcnow(),
    }
    acervohistorico.update_one({"registro": registroid}, {"$set": out}, upsert=True)
    marcarcompletado(colatesis, {"registro": registroid})
    return True, False

# =========================
# TFJA
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
    prompt = "\n".join(["TFJA", f"Epoca: {epoca}", f"Anio: {anio}", f"Mes: {mes}",
                         f"Rubro: {rubro}", "", texto])
    vector = obtenervector(prompt)
    if not vector:
        if INDEXAR_SIN_VECTOR == "1":
            log_event("tfja_vector_fallo_indexa_sin_vector", docid=str(docid))
            vector = None
        else:
            marcar_diferido(colatfja, {"docid": docid}, "Embeddings fallo (temporal)")
            log_event("tfja_vector_fallo_diferido", docid=str(docid))
            return False
    out = {
        "docid": docid, "tipo": doccola.get("tipo", "TFJA"), "epoca": epoca, "anio": anio,
        "mes": mes, "rubro": rubro, "texto": texto, "sourcefile": doccola.get("sourcefile"),
        "sourcepath": doccola.get("sourcepath"), "vectorbusqueda": vector,
        "procesado": True, "actualizadoen": datetime.utcnow(),
    }
    sourcestfja.update_one({"docid": docid}, {"$set": out}, upsert=True)
    marcarcompletado(colatfja, {"docid": docid})
    return True

# =========================
# Main loop
# =========================
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

    # CORRECCIÓN DE ÍNDICES: 
    # Añadimos los índices exactos para que Mongo no tenga que escanear toda la colección
    # al buscar el estado "pendiente" ordenado por "creadoen". Esto bajará el CPU a la normalidad.
    for nombre, fn in [
        ("registro único en cola_tesis", lambda: colatesis.create_index("registro", unique=True)),
        ("docid en sources_tfja", lambda: sourcestfja.create_index("docid", unique=True)),
        ("registro en acervo_historico", lambda: acervohistorico.create_index("registro", unique=True)),
        ("docid en sources_tfja", lambda: sourcestfja.create_index("docid", unique=True)),
        ("docid en cola_tfja", lambda: colatfja.create_index("docid")),
        ("estado+creadoen en cola_tesis", lambda: colatesis.create_index([("estado", 1), ("creadoen", 1)])),
        ("estado+next_run_at en cola_tesis", lambda: colatesis.create_index([("estado", 1), ("next_run_at", 1)])),
        ("estado+tomadoen en cola_tesis", lambda: colatesis.create_index([("estado", 1), ("tomadoen", 1)])),
        ("estado+creadoen en cola_tfja", lambda: colatfja.create_index([("estado", 1), ("creadoen", 1)])),
        ("estado+next_run_at en cola_tfja", lambda: colatfja.create_index([("estado", 1), ("next_run_at", 1)])),
        ("estado+tomadoen en cola_tfja", lambda: colatfja.create_index([("estado", 1), ("tomadoen", 1)])),
    ]:
        try:
            fn()
        except Exception as e:
            print(f"Indice '{nombre}' ya existe o se omite: {e}")

    backfill_cola_campos(colatesis)
    backfill_cola_campos(colatfja)
    
    tiempos = deque(maxlen=20)
    erroresscjnconsecutivos = 0
    scjn_pause_until = 0.0
    i = 0

    while True:
        if i % 200 == 0:
            liberarlocksstale(colatesis)
            liberarlocksstale(colatfja)

        tipo = SCHEDULE[i % len(SCHEDULE)]
        i += 1
        procesadoalgo = False

        if time.time() < scjn_pause_until and tipo == "tesis" and WTFJA > 0:
            tipo = "tfja"

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
                    scjn_pause_until = time.time() + ESPERAPAUSASCJN
                    print(f"SCJN inestable ({erroresscjnconsecutivos} errores seguidos). "
                          f"Pausando tesis {ESPERAPAUSASCJN // 60} min (TFJA sigue).")
                    log_event("scjn_pause", errores=erroresscjnconsecutivos, pausa_seg=ESPERAPAUSASCJN)
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
    iniciar_servidor_health()
    workerloop()
