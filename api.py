
import os
import time
from datetime import datetime

from pymongo.mongo_client import MongoClient
from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse, JSONResponse
from dotenv import load_dotenv

# ============================
# CONFIGURACIÓN
# ============================

load_dotenv()

client_mongo = None
db = None
coleccion = None
cola = None

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

app = FastAPI(title="Acervo Worker Dashboard")

# ============================
# DASHBOARD
# ============================

@app.on_event("startup")
def startup_event():
    global client_mongo, db, coleccion, cola
    client_mongo = conectar_mongo()
    db = client_mongo["tepantlatia_db"]
    coleccion = db["acervo_historico"]
    cola = db["cola_tesis"]
    print("🚀 API conectada a MongoDB.")

@app.get("/health")
def health_check():
    return JSONResponse({"status": "ok"})

@app.get("/", response_class=HTMLResponse)
def dashboard(
    epoca: str | None = Query(default=None),
    materia: str | None = Query(default=None),
):
    if cola is None:
        return HTMLResponse(
            "<h1>⏳ Conectando a la base de datos...</h1>"
            "<p>La API está iniciando. Recarga en unos segundos.</p>"
            '<meta http-equiv="refresh" content="5">',
            status_code=503,
        )

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
