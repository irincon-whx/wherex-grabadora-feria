"""Wherex · Grabadora de feria.

App web para grabar audio por empresa, transcribirlo en el servidor (faster-whisper)
y entregarlo al artifact de Claude mediante un conector MCP (POST /mcp/<MCP_TOKEN>).
"""
import base64
import hashlib
import hmac
import json
import os
import re
import secrets
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from queue import Queue

from fastapi import FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse, JSONResponse, Response

APP_PASSWORD = os.environ.get("APP_PASSWORD", "")
MCP_TOKEN = os.environ.get("MCP_TOKEN", "")
DATA_DIR = Path(os.environ.get("DATA_DIR") or ("/data" if Path("/data").exists() else "./storage"))
WHISPER_MODEL = os.environ.get("WHISPER_MODEL", "small")
WHISPER_THREADS = int(os.environ.get("WHISPER_THREADS", "2"))
SECRET = (os.environ.get("SESSION_SECRET") or hashlib.sha256(("wx:" + APP_PASSWORD + MCP_TOKEN).encode()).hexdigest()).encode()
CHUNK = 600 * 1024  # bytes de audio por parte entregada al artifact
BASE = Path(__file__).parent
AGENDA = {"evento": "Feria minera · 21 y 22 de octubre de 2026", "empresas": [{"id": "r01", "nombre": "Salar Blanco", "hora": "16:01", "dia": "2026-10-21"}, {"id": "r02", "nombre": "ENAMI", "hora": "16:11", "dia": "2026-10-21"}, {"id": "r03", "nombre": "Pucobre", "hora": "16:21", "dia": "2026-10-21"}, {"id": "r04", "nombre": "Kinross Chile", "hora": "16:31", "dia": "2026-10-21"}, {"id": "r05", "nombre": "Gold Fields — Salares Norte", "hora": "16:41", "dia": "2026-10-21"}, {"id": "r06", "nombre": "CMP", "hora": "16:51", "dia": "2026-10-21"}, {"id": "r07", "nombre": "Lundin Mining — Caserones", "hora": "17:01", "dia": "2026-10-21"}, {"id": "r08", "nombre": "SCM Carola", "hora": "17:11", "dia": "2026-10-21"}, {"id": "r09", "nombre": "Capstone Copper — Santo Domingo", "hora": "17:21", "dia": "2026-10-21"}, {"id": "r10", "nombre": "Capstone Copper — Mantoverde", "hora": "17:31", "dia": "2026-10-21"}, {"id": "r11", "nombre": "Minera Altair", "hora": "17:41", "dia": "2026-10-21"}, {"id": "r12", "nombre": "Lundin Mining — Candelaria", "hora": "17:51", "dia": "2026-10-21"}]}

if not APP_PASSWORD or len(MCP_TOKEN) < 16:
    raise SystemExit("Define APP_PASSWORD y MCP_TOKEN (16+ caracteres) en las variables de Railway.")

AUDIO_DIR = DATA_DIR / "audio"
AUDIO_DIR.mkdir(parents=True, exist_ok=True)
DB_FILE = DATA_DIR / "db.json"
LOCK = threading.RLock()


def now_iso():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def load_db():
    try:
        d = json.loads(DB_FILE.read_text(encoding="utf-8"))
    except Exception:
        d = {}
    d.setdefault("empresas", {})
    d.setdefault("grabaciones", {})
    for e in AGENDA["empresas"]:
        d["empresas"].setdefault(e["id"], dict(e, origen="agenda"))
    return d


DB = load_db()


def save_db():
    with LOCK:
        tmp = DB_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps(DB, ensure_ascii=False), encoding="utf-8")
        tmp.replace(DB_FILE)


save_db()
OK_ID = re.compile(r"^[A-Za-z0-9_-]{1,64}$")


def ext_for(mime: str) -> str:
    return "m4a" if re.search(r"mp4|m4a|aac", mime or "") else "webm"


def audio_path(g) -> Path:
    return AUDIO_DIR / f"{g['id']}.{ext_for(g.get('mime', ''))}"


# ---------------- Transcripción en segundo plano ----------------
JOBS: "Queue[str]" = Queue()
MODEL = {"m": None, "estado": "sin cargar", "error": ""}


def get_model():
    if MODEL["m"] is None:
        from faster_whisper import WhisperModel
        MODEL["estado"] = "cargando"
        MODEL["m"] = WhisperModel(WHISPER_MODEL, device="cpu", compute_type="int8",
                                  download_root=os.environ.get("WHISPER_DIR", "/srv/models"), cpu_threads=WHISPER_THREADS)
        MODEL["estado"] = "listo"
    return MODEL["m"]


def worker():
    try:
        get_model()
    except Exception as ex:  # el modelo se reintenta con el primer trabajo
        MODEL["estado"], MODEL["error"] = "error", str(ex)[:300]
    while True:
        gid = JOBS.get()
        with LOCK:
            g = DB["grabaciones"].get(gid)
        if not g or not audio_path(g).exists():
            continue
        try:
            with LOCK:
                g["estado_transcripcion"] = "procesando"
                save_db()
            t0 = time.time()
            segments, info = get_model().transcribe(str(audio_path(g)), language="es", vad_filter=True, beam_size=5)
            texto = " ".join(s.text.strip() for s in segments).strip()
            with LOCK:
                g["transcripcion"] = texto
                g["estado_transcripcion"] = "listo"
                g["transcrito_en"] = now_iso()
                g["segundos_proceso"] = round(time.time() - t0, 1)
                g["transcripcion_entregada"] = False
                save_db()
        except Exception as ex:
            with LOCK:
                g["estado_transcripcion"] = "error"
                g["error"] = str(ex)[:300]
                save_db()


threading.Thread(target=worker, daemon=True).start()
with LOCK:
    for g in DB["grabaciones"].values():
        if g.get("estado_transcripcion") in ("pendiente", "procesando"):
            JOBS.put(g["id"])


def guardar_audio(gid, empresa_id, empresa, mime, data: bytes, origen, creado_en=None, duracion_s=0, nota=""):
    with LOCK:
        if empresa_id not in DB["empresas"]:
            DB["empresas"][empresa_id] = {"id": empresa_id, "nombre": empresa or empresa_id, "hora": "", "dia": "", "origen": origen}
        g = DB["grabaciones"].get(gid)
        nueva = g is None
        if nueva:
            g = {"id": gid, "empresa_id": empresa_id, "empresa": empresa or DB["empresas"][empresa_id]["nombre"],
                 "creado_en": creado_en or now_iso(), "subido_en": now_iso(), "duracion_s": float(duracion_s or 0),
                 "mime": mime or "audio/mp4", "nota": nota or "", "transcripcion": "", "estado_transcripcion": "pendiente",
                 "origen": origen, "importada": False, "transcripcion_entregada": False}
            DB["grabaciones"][gid] = g
        audio_path(g).write_bytes(data)
        g["bytes"] = len(data)
        if nota and not g.get("nota"):
            g["nota"] = nota
        save_db()
    if nueva or g.get("estado_transcripcion") == "error":
        with LOCK:
            g["estado_transcripcion"] = "pendiente"
        JOBS.put(gid)
    return g


def resumen(g, con_texto=True):
    partes = max(1, -(-int(g.get("bytes", 0)) // CHUNK))
    r = {k: g.get(k) for k in ("id", "empresa_id", "empresa", "creado_en", "duracion_s", "mime", "bytes", "nota",
                               "estado_transcripcion", "origen", "importada", "error")}
    r["partes"] = partes
    if con_texto:
        r["transcripcion"] = g.get("transcripcion", "")
    return r


# ---------------- Sesión web ----------------
def sign(v: str) -> str:
    return v + "." + hmac.new(SECRET, v.encode(), hashlib.sha256).hexdigest()


def verify(tok) -> bool:
    if not tok or "." not in tok:
        return False
    v = tok.rsplit(".", 1)[0]
    if not hmac.compare_digest(sign(v), tok):
        return False
    try:
        return int(v.split(":")[1]) > time.time()
    except Exception:
        return False


app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)
ATTEMPTS = {}


def require_auth(req: Request):
    if not verify(req.cookies.get("wx_auth")):
        raise HTTPException(401, "Sesión vencida. Ingresa la clave.")


@app.get("/healthz")
def healthz():
    return {"ok": True, "modelo": MODEL["estado"], "cola": JOBS.qsize()}


@app.post("/api/login")
async def login(req: Request):
    ip = req.client.host if req.client else "x"
    a = ATTEMPTS.get(ip, [0, time.time()])
    if time.time() - a[1] > 900:
        a = [0, time.time()]
    if a[0] >= 10:
        raise HTTPException(429, "Demasiados intentos. Espera 15 minutos.")
    body = await req.json()
    if not hmac.compare_digest(str(body.get("password", "")), APP_PASSWORD):
        a[0] += 1
        ATTEMPTS[ip] = a
        raise HTTPException(401, "Clave incorrecta.")
    ATTEMPTS.pop(ip, None)
    exp = int(time.time()) + 60 * 86400
    resp = JSONResponse({"ok": True})
    resp.set_cookie("wx_auth", sign(f"s:{exp}"), max_age=60 * 86400, httponly=True, secure=True, samesite="lax")
    return resp


@app.get("/api/estado")
def estado(req: Request):
    require_auth(req)
    with LOCK:
        emps = sorted(DB["empresas"].values(), key=lambda e: (e.get("dia") or "9", e.get("hora") or "99", e.get("nombre")))
        grs = sorted(DB["grabaciones"].values(), key=lambda g: g["creado_en"], reverse=True)
        return {"evento": AGENDA["evento"], "modelo": MODEL["estado"], "cola": JOBS.qsize(),
                "empresas": emps, "grabaciones": [resumen(g) for g in grs]}


@app.post("/api/empresas")
async def nueva_empresa(req: Request):
    require_auth(req)
    body = await req.json()
    nombre = str(body.get("nombre", "")).strip()[:120]
    if not nombre:
        raise HTTPException(400, "Falta el nombre.")
    eid = "x" + uuid.uuid4().hex[:10]
    with LOCK:
        DB["empresas"][eid] = {"id": eid, "nombre": nombre, "hora": datetime.now().strftime("%H:%M"), "dia": "", "origen": "app"}
        save_db()
    return DB["empresas"][eid]


@app.post("/api/grabaciones")
async def subir(req: Request, id: str = Form(...), empresa_id: str = Form(...), creado_en: str = Form(""),
                duracion_s: float = Form(0), nota: str = Form(""), audio: UploadFile = File(...)):
    require_auth(req)
    if not OK_ID.match(id) or not OK_ID.match(empresa_id):
        raise HTTPException(400, "Identificador inválido.")
    with LOCK:
        if empresa_id not in DB["empresas"]:
            raise HTTPException(404, "Empresa no encontrada.")
        empresa = DB["empresas"][empresa_id]["nombre"]
    data = await audio.read()
    if not data:
        raise HTTPException(400, "Audio vacío.")
    if len(data) > 150 * 1024 * 1024:
        raise HTTPException(413, "Audio demasiado grande.")
    g = guardar_audio(id, empresa_id, empresa, audio.content_type or "audio/mp4", data, "app", creado_en or None, duracion_s, nota[:4000])
    return resumen(g)


@app.patch("/api/grabaciones/{gid}")
async def editar(gid: str, req: Request):
    require_auth(req)
    body = await req.json()
    with LOCK:
        g = DB["grabaciones"].get(gid)
        if not g:
            raise HTTPException(404, "No existe.")
        if isinstance(body.get("nota"), str):
            g["nota"] = body["nota"][:4000]
            g["importada"] = False
        save_db()
        return resumen(g)


@app.delete("/api/grabaciones/{gid}")
def borrar(gid: str, req: Request):
    require_auth(req)
    with LOCK:
        g = DB["grabaciones"].pop(gid, None)
        save_db()
    if g:
        try:
            audio_path(g).unlink()
        except FileNotFoundError:
            pass
    return {"ok": True}


@app.get("/api/audio/{gid}")
def bajar_audio(gid: str, req: Request):
    require_auth(req)
    g = DB["grabaciones"].get(gid)
    if not g or not audio_path(g).exists():
        raise HTTPException(404)
    nombre = re.sub(r"[^A-Za-z0-9]+", "_", g["empresa"]) + "_" + g["creado_en"][:16].replace(":", "")
    return FileResponse(audio_path(g), media_type="audio/mp4" if ext_for(g["mime"]) == "m4a" else "audio/webm",
                        filename=f"Feria_{nombre}.{ext_for(g['mime'])}")


@app.get("/api/transcripcion/{gid}")
def bajar_transcripcion(gid: str, req: Request):
    require_auth(req)
    g = DB["grabaciones"].get(gid)
    if not g:
        raise HTTPException(404)
    nombre = re.sub(r"[^A-Za-z0-9]+", "_", g["empresa"]) + "_" + g["creado_en"][:16].replace(":", "")
    txt = f"Empresa: {g['empresa']}\nFecha: {g['creado_en']}\n\n{g.get('transcripcion') or '(transcripción pendiente)'}\n"
    if g.get("nota"):
        txt += f"\nNota: {g['nota']}\n"
    return Response(("﻿" + txt).encode("utf-8"), media_type="text/plain; charset=utf-8",
                    headers={"Content-Disposition": f'attachment; filename="Feria_{nombre}_transcripcion.txt"'})


# ---------------- Conector MCP para el artifact ----------------
TOOLS = [
    {"name": "listar_grabaciones",
     "description": "Lista las grabaciones de la feria (por empresa) con su transcripción. Por defecto solo las que el artifact aún no importa.",
     "inputSchema": {"type": "object", "properties": {
         "pendientes": {"type": "boolean", "description": "true: solo no importadas (por defecto)."},
         "empresa_id": {"type": "string"}}},
     "annotations": {"readOnlyHint": True}},
    {"name": "obtener_audio",
     "description": "Entrega una parte del audio de una grabación en base64 (partes de 600 KB).",
     "inputSchema": {"type": "object", "required": ["id", "parte"], "properties": {
         "id": {"type": "string"}, "parte": {"type": "integer", "minimum": 0}}},
     "annotations": {"readOnlyHint": True}},
    {"name": "marcar_importada",
     "description": "Marca una grabación como ya importada al artifact.",
     "inputSchema": {"type": "object", "required": ["id"], "properties": {"id": {"type": "string"}}},
     "annotations": {"readOnlyHint": False, "destructiveHint": False}},
    {"name": "transcribir_audio",
     "description": "Recibe un audio subido en el artifact para transcribirlo; la transcripción se entrega luego con listar_grabaciones.",
     "inputSchema": {"type": "object", "required": ["id", "empresa_id", "mime", "base64Content"], "properties": {
         "id": {"type": "string"}, "empresa_id": {"type": "string"}, "empresa": {"type": "string"},
         "mime": {"type": "string"}, "creado_en": {"type": "string"}, "base64Content": {"type": "string"}}},
     "annotations": {"readOnlyHint": False, "destructiveHint": False}},
]


def tool_result(obj, is_error=False):
    return {"content": [{"type": "text", "text": json.dumps(obj, ensure_ascii=False)}], "structuredContent": obj, "isError": is_error}


def call_tool(name, args):
    args = args or {}
    if name == "listar_grabaciones":
        pend = args.get("pendientes", True)
        eid = args.get("empresa_id")
        with LOCK:
            out = []
            for g in sorted(DB["grabaciones"].values(), key=lambda x: x["creado_en"]):
                if eid and g["empresa_id"] != eid:
                    continue
                if pend and g.get("importada") and (g.get("transcripcion_entregada") or g.get("estado_transcripcion") not in ("listo", "error")):
                    continue
                out.append(resumen(g))
            return tool_result({"grabaciones": out, "modelo": MODEL["estado"]})
    if name == "obtener_audio":
        g = DB["grabaciones"].get(str(args.get("id")))
        if not g or not audio_path(g).exists():
            return tool_result({"error": "grabación no encontrada"}, True)
        parte = int(args.get("parte", 0))
        with audio_path(g).open("rb") as f:
            f.seek(parte * CHUNK)
            data = f.read(CHUNK)
        partes = max(1, -(-int(g.get("bytes", 0)) // CHUNK))
        return tool_result({"id": g["id"], "parte": parte, "partes": partes, "mime": g["mime"], "base64": base64.b64encode(data).decode()})
    if name == "marcar_importada":
        with LOCK:
            g = DB["grabaciones"].get(str(args.get("id")))
            if not g:
                return tool_result({"error": "grabación no encontrada"}, True)
            g["importada"] = True
            if g.get("estado_transcripcion") in ("listo", "error"):
                g["transcripcion_entregada"] = True
            g["importada_en"] = now_iso()
            save_db()
        return tool_result({"ok": True, "id": g["id"]})
    if name == "transcribir_audio":
        gid, eid = str(args.get("id", "")), str(args.get("empresa_id", ""))
        if not OK_ID.match(gid) or not OK_ID.match(eid):
            return tool_result({"error": "identificador inválido"}, True)
        try:
            data = base64.b64decode(args.get("base64Content", ""), validate=False)
        except Exception:
            return tool_result({"error": "audio inválido"}, True)
        if not data:
            return tool_result({"error": "audio vacío"}, True)
        g = guardar_audio(gid, eid, args.get("empresa", ""), args.get("mime", "audio/mp4"), data, "artifact", args.get("creado_en"))
        with LOCK:
            g["importada"] = True  # el artifact ya tiene el audio; solo falta entregar el texto
            g["transcripcion_entregada"] = False
            save_db()
        return tool_result({"ok": True, "id": gid, "estado_transcripcion": g["estado_transcripcion"]})
    return tool_result({"error": f"herramienta desconocida: {name}"}, True)


@app.post("/mcp/{token}")
async def mcp(token: str, req: Request):
    if not hmac.compare_digest(token, MCP_TOKEN):
        raise HTTPException(404)
    msg = await req.json()
    batch = msg if isinstance(msg, list) else [msg]
    replies = []
    for m in batch:
        mid, method, params = m.get("id"), m.get("method"), m.get("params") or {}
        if mid is None:  # notificación
            continue
        try:
            if method == "initialize":
                res = {"protocolVersion": params.get("protocolVersion", "2025-06-18"),
                       "capabilities": {"tools": {"listChanged": False}},
                       "serverInfo": {"name": "Wherex Grabadora", "version": "1.0.0"},
                       "instructions": "Grabaciones de la feria minera de Wherex por empresa, con transcripción."}
            elif method == "ping":
                res = {}
            elif method == "tools/list":
                res = {"tools": TOOLS}
            elif method == "tools/call":
                res = call_tool(params.get("name"), params.get("arguments"))
            else:
                replies.append({"jsonrpc": "2.0", "id": mid, "error": {"code": -32601, "message": "Método no soportado"}})
                continue
            replies.append({"jsonrpc": "2.0", "id": mid, "result": res})
        except Exception as ex:
            replies.append({"jsonrpc": "2.0", "id": mid, "error": {"code": -32603, "message": str(ex)[:200]}})
    if not replies:
        return Response(status_code=202)
    return JSONResponse(replies if isinstance(msg, list) else replies[0])


@app.get("/mcp/{token}")
def mcp_get(token: str):
    return Response(status_code=405)


@app.delete("/mcp/{token}")
def mcp_delete(token: str):
    return Response(status_code=200)


ICON_SVG = '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 64 64"><rect width="64" height="64" rx="14" fill="#191D38"/><circle cx="32" cy="32" r="16" fill="#E0515A"/></svg>'
MANIFEST = '{"name":"Grabadora de feria · Wherex","short_name":"Grabadora","start_url":"/","display":"standalone","background_color":"#191D38","theme_color":"#191D38","icons":[{"src":"/icon.svg","sizes":"any","type":"image/svg+xml"}]}'
SW_JS = "// Guarda la página para que abra sin señal; los datos van por la cola local de la app.\nconst C = 'wx-grab-v1';\nself.addEventListener('install', e => { e.waitUntil(caches.open(C).then(c => c.addAll(['/', '/icon.svg', '/manifest.webmanifest']))); self.skipWaiting(); });\nself.addEventListener('activate', e => { e.waitUntil(self.clients.claim()); });\nself.addEventListener('fetch', e => {\n  const u = new URL(e.request.url);\n  if (e.request.method !== 'GET' || u.pathname.startsWith('/api/') || u.pathname.startsWith('/mcp/')) return;\n  e.respondWith(fetch(e.request).then(r => { const cp = r.clone(); caches.open(C).then(c => c.put(e.request, cp)); return r; }).catch(() => caches.match(e.request).then(r => r || caches.match('/'))));\n});\n"


@app.get("/icon.svg")
def icon():
    return Response(ICON_SVG, media_type="image/svg+xml")


@app.get("/manifest.webmanifest")
def manifest():
    return Response(MANIFEST, media_type="application/manifest+json")


@app.get("/sw.js")
def sw():
    return Response(SW_JS, media_type="text/javascript")


@app.get("/")
def index():
    return FileResponse(BASE / "index.html", media_type="text/html", headers={"Cache-Control": "no-cache"})

