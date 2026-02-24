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
# 3. NORMALIZACIÓN DE MATERIA
# ============================

def extraer_materia(data):
    # Soportar tanto "materias" (lista) como "materia" (string/objeto)
    materia = data.get("materias") or data.get("materia")

    if not materia:
        return "N/A"

    # Caso 1: string directo
    if isinstance(materia, str):
        return materia

    # Caso 2: lista de strings
    if isinstance(materia, list) and all(isinstance(x, str) for x in materia):
        return ", ".join(materia)

    # Caso 3: objeto con clave/descripcion
    if isinstance(materia, dict):
        return materia.get("descripcion") or materia.get("clave") or "N/A"

    # Caso 4: lista de objetos
    if isinstance(materia, list) and all(isinstance(x, dict) for x in materia):
        return ", ".join(
            x.get("descripcion") or x.get("clave") or "N/A"
            for x in materia
        )

    return "N/A"


# ============================
# 4. CORRECCIÓN MASIVA DE TESIS EXISTENTES
# ============================

def corregir_materias_existentes():
    print("🔍 Buscando tesis con materia 'N/A' para corregir...")

    filtro = {"materia": "N/A", "procesado": True}
    total = coleccion.count_documents(filtro)

    if total == 0:
        print("✅ No hay tesis que corregir.")
        return

    print(f"🔧 Se encontraron {total} tesis sin materia. Corrigiendo...")

    cursor = coleccion.find(filtro)

    for doc in cursor:
        registro = doc["registro"]
        try:
            url = f"{URL_BASE}{registro}"
            resp = requests.get(url, timeout=10)

            if resp.status_code != 200:
                print(f"⚠️ No se pudo corregir {registro}: HTTP {resp.status_code}")
                continue

            data = resp.json()
            nueva_materia = extraer_materia(data)

            coleccion.update_one(
                {"registro": registro},
                {"$set": {"materia": nueva_materia, "actualizado_en": datetime.utcnow()}}
            )

            print(f"🔧 Materia corregida para {registro}: {nueva_materia}")

        except Exception as e:
            print(f"⚠️ Error corrigiendo {registro}: {e}")

    print("🎉 Corrección de materias completada.")


# ============================
# 5. LEYES FUNDAMENTALES
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
# 6. SISTEMA DE COLAS
# ============================

BLOQUES = [
    (206000, 207000),
    (160000, 161000),
    (2023000, 2028000),
]

def inicializar_cola():
    existente = meta.find_one({"tipo": "cola_inicializada"})
    if existente:
        print("📦 Cola ya inicializada, no se vuelve a poblar.")
        return

    print("📦 Inicializando cola de tesis...")
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


def reintentar_errores(limit: int | None = None):
    cursor = cola.find({"estado": "error"}).limit(limit or 0)
    count = 0
    for doc in cursor:
        cola.update_one(
            {"_id": doc["_id"]},
            {"$set": {"estado": "pendiente", "reintentado_en": datetime.utcnow()}},
        )
        count += 1
    return count


# ============================
# 7. WORKER PRINCIPAL
# ============================

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

        print(f"🧠 Procesando {registro_id}...")
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
    cargar_leyes_fundamentales()
    corregir_materias_existentes()   # 🔥 CORRECCIÓN MASIVA ANTES DE TODO
    inicializar_cola()

    while True:
        doc = tomar_siguiente_de_cola()
        if not doc:
            print("⏸️ No hay más pendientes en la cola. Esperando...")
            time.sleep(10)
            continue

        procesar_registro(doc)
        time.sleep(0.4)


# ============================
# 8. DASHBOARD
# ============================

@app.get("/", response_class=HTMLResponse)
def dashboard(
    epoca: str | None = Query(default=None),
    materia: str | None = Query(default=None),
):
    total = cola.count_documents({})
    pendientes = cola.count_documents({"estado": "pendiente"})
    procesando = cola.count_documents({"estado": "procesando"})
    completados = cola.count_documents({"estado": "completado"})
    errores = cola.count_documents({"estado": "error"})

    filtro = {"procesado": True}
    if epoca:
        filtro["epoca"] = epoca
    if materia:
        filtro["materia"] = materia

    ultimos = list(
        coleccion.find(filtro)
        .sort("actualizado_en", -1)
        .limit(10)
    )

    filas = ""
    for d in ultimos:
        filas += (
            f"<tr>"
            f"<td>{d.get('registro')}</td>"
            f"<td>{d.get('rubro')[:80]}...</td>"
            f"<td>{d.get('epoca')}</td>"
            f"<td>{d.get('materia')}</td>"
            f"</tr>"
        )

    html = f"""
    <html>
    <head>
        <title>Acervo Worker Dashboard</title>
        <style>
            body {{ font-family: system-ui, sans-serif; margin: 2rem; }}
            .cards {{ display: flex; gap: 1rem; margin-bottom: 2rem; flex-wrap: wrap; }}
            .card {{ padding: 1rem 1.5rem; border-radius: 8px; background: #f5f5f5; }}
            table {{ border-collapse: collapse; width: 100%; }}
            th, td {{ border: 1px solid #ddd; padding: 8px; font-size: 0.9rem; }}
            th {{ background: #eee; }}
            form {{ margin-bottom: 1.5rem; }}
            label {{ margin-right: 0.5rem; }}
            input {{ margin-right: 1rem; }}
        </style>
    </head>
    <body>
        <h1>Acervo Worker Dashboard</h1>
        <div class="cards">
            <div class="card"><strong>Total en cola:</strong> {total}</div>
            <div class="card"><strong>Pendientes:</strong> {pendientes}</div>
            <div class="card"><strong>Procesando:</strong> {procesando}</div>
            <div class="card"><strong>Completados:</strong> {completados}</div>
            <div class="card"><strong>Errores:</strong> {errores}</div>
        </div>

        <h2>Filtros</h2>
        <form method="get" action="/">
            <label>Época:</label>
            <input type="text" name="epoca" value="{epoca or ''}" />
            <label>Materia:</label>
            <input type="text" name="materia" value="{materia or ''}" />
            <button type="submit">Filtrar</button>
        </form>

        <h2>Últimos 10 registros procesados</h2>
        <table>
            <tr>
                <th>Registro</th>
                <th>Rubro</th>
                <th>Época</th>
                <th>Materia</th>
            </tr>
            {filas}
        </table>
    </body>
    </html>
    """
    return html


@app.post("/reintentar-errores")
def endpoint_reintentar_errores(limit: int | None = Query(default=None, ge=1)):
    count = reintentar_errores(limit=limit)
    return JSONResponse(
        {"mensaje": "Reintentos programados", "reintentos": count, "limit": limit}
    )


# ============================
# 9. ARRANQUE DEL WORKER
# ============================

def iniciar_worker_en_background():
    hilo = threading.Thread(target=worker_loop, daemon=True)
    hilo.start()
    print("🚀 Worker iniciado en background.")


iniciar_worker_en_background()
