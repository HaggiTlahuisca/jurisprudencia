import os
import time
import threading
from datetime import datetime

import httpx
from pymongo.mongo_client import MongoClient
from fastapi import FastAPI, Query, HTTPException, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

# ============================
# CONFIGURACIÓN
# ============================
load_dotenv()

CLERK_SECRET_KEY    = os.getenv("CLERK_SECRET_KEY")
CLERK_JWKS_URL      = "https://api.clerk.com/v1/jwks"

client_mongo = None
db           = None
coleccion    = None
cola         = None

security = HTTPBearer()

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

app = FastAPI(title="TepantlatAI API")

# CORS — permite que el frontend (web) se comunique con la API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # luego restringimos al dominio real
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================
# STARTUP
# ============================
def conectar_en_background():
    global client_mongo, db, coleccion, cola
    client_mongo = conectar_mongo()
    db           = client_mongo["tepantlatia_db"]
    coleccion    = db["acervo_historico"]
    cola         = db["cola_tesis"]
    print("🚀 API conectada a MongoDB.")

@app.on_event("startup")
def startup_event():
    hilo = threading.Thread(target=conectar_en_background, daemon=True)
    hilo.start()

# ============================
# VERIFICACIÓN DE SESIÓN CLERK
# ============================
def verificar_sesion(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """
    Verifica el token JWT que envía Clerk desde el frontend.
    Si no es válido, rechaza la petición con 401.
    """
    token = credentials.credentials
    try:
        headers = {"Authorization": f"Bearer {CLERK_SECRET_KEY}"}
        resp = httpx.get(
            f"https://api.clerk.com/v1/tokens/{token}/verify",
            headers=headers,
            timeout=5,
        )
        if resp.status_code != 200:
            raise HTTPException(status_code=401, detail="Sesión inválida o expirada.")
        return resp.json()   # contiene user_id y datos del usuario
    except httpx.RequestError:
        raise HTTPException(status_code=503, detail="No se pudo verificar la sesión.")

# ============================
# ENDPOINTS PÚBLICOS
# ============================
@app.get("/health")
def health_check():
    """Siempre devuelve 200. Fly.io usa este endpoint para el health check."""
    return JSONResponse({"status": "ok"})

@app.get("/", response_class=HTMLResponse)
def dashboard(
    epoca:   str | None = Query(default=None),
    materia: str | None = Query(default=None),
):
    if cola is None:
        return HTMLResponse(
            "<html><body>La API está iniciando. Recarga en unos segundos.</body></html>",
            status_code=503,
        )
    total       = cola.count_documents({})
    pendientes  = cola.count_documents({"estado": "pendiente"})
    procesando  = cola.count_documents({"estado": "procesando"})
    completados = cola.count_documents({"estado": "completado"})
    errores     = cola.count_documents({"estado": "error"})

    filtro = {"procesado": True}
    if epoca:
        filtro["epoca"] = epoca
    if materia:
        filtro["materia"] = materia

    ultimos = list(
        coleccion.find(filtro).sort("actualizado_en", -1).limit(10)
    )
    filas = ""
    for d in ultimos:
        filas += f"<tr><td>{d.get('no_registro','')}</td><td>{d.get('rubro','')[:80]}</td><td>{d.get('epoca','')}</td><td>{d.get('materia','')}</td></tr>"

    html = f"""
    <html><head><title>TepantlatAI Dashboard</title></head>
    <body>
    <h2>TepantlatAI — Estado del sistema</h2>
    <p>Total: {total} | Pendientes: {pendientes} | Procesando: {procesando} | Completados: {completados} | Errores: {errores}</p>
    <table border='1'><tr><th>Registro</th><th>Rubro</th><th>Época</th><th>Materia</th></tr>
    {filas}
    </table>
    </body></html>
    """
    return HTMLResponse(html)

# ============================
# ENDPOINTS PRIVADOS (requieren sesión)
# ============================
@app.get("/yo")
def mi_perfil(sesion: dict = Depends(verificar_sesion)):
    """Devuelve los datos del usuario autenticado."""
    return {
        "user_id": sesion.get("sub"),
        "email":   sesion.get("email"),
        "mensaje": "Sesión válida ✅"
    }

@app.get("/buscar")
def buscar(
    q:      str            = Query(..., description="Pregunta o búsqueda"),
    sesion: dict           = Depends(verificar_sesion),
):
    """
    Endpoint de búsqueda semántica (se completará en la siguiente fase).
    Requiere sesión activa de Clerk.
    """
    return {
        "query":   q,
        "user_id": sesion.get("sub"),
        "mensaje": "Búsqueda recibida. Lógica semántica en construcción."
    }
