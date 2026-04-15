# backend/app/main.py
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware
from fastapi.staticfiles import StaticFiles
from pathlib import Path
from backend.app.core.templating import get_templates
templates = get_templates()

from xrpl.clients import JsonRpcClient
from xrpl.models.requests import ServerInfo

from backend.app.core.settings import (
    SESSION_SECRET,
    SESSION_COOKIE,
    XRPL_RPC_URL,
)

# ✅ define these BEFORE routes use them
BASE_DIR = Path(__file__).resolve().parent  # -> backend/app
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

app = FastAPI(title="PROJXHUB")

def get_nickname(_addr: str):
    return None

templates.env.globals["get_nickname"] = get_nickname


# Signed-cookie sessions
app.add_middleware(
    SessionMiddleware,
    secret_key=SESSION_SECRET,
    session_cookie=SESSION_COOKIE,
)

# Init DB (users / nicknames)



# XRPL client (testnet by default unless overridden in .env)
xrpl_client = JsonRpcClient(XRPL_RPC_URL)


@app.get("/healthz")
def healthz():
    return {"status": "ok"}


@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    user = request.session.get("user")
    return templates.TemplateResponse(
        "index.html",
        {"request": request, "title": "PROJXHUB", "user": user},
    )


@app.get("/xrpl/ping")
def xrpl_ping():
    info = xrpl_client.request(ServerInfo())
    ledger_index = info.result["info"]["validated_ledger"]["seq"]
    return JSONResponse(
        {
            "network": "XRPL Testnet",
            "validated_ledger_index": ledger_index,
        }
    )


# Routers
from backend.app.api.pages import router as pages_router
app.include_router(pages_router)

from backend.app.api.auth import router as auth_router
app.include_router(auth_router, prefix="/auth")

from backend.app.api.profile import router as profile_router
app.include_router(profile_router, prefix="/profile")






# --- DEBUG: list all registered routes to verify what's live ---
@app.get("/_debug/routes")
def _debug_routes():
    out = []
    for r in app.routes:
        try:
            out.append({"path": r.path, "name": getattr(r, "name", None)})
        except Exception:
            pass
    return out


# -------- Static dirs (packs + metadata JSON + uploads) --------

# Starter packs
PACKS_DIR = BASE_DIR / "packs"                    # -> backend/app/packs
PACKS_DIR.mkdir(parents=True, exist_ok=True)

app.mount(
    "/studio/packs",
    StaticFiles(directory=str(PACKS_DIR)),
    name="starter_packs",
)

# Data dir (meta + uploads)
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

META_DIR = DATA_DIR / "meta"                      # -> backend/app/data/meta
META_DIR.mkdir(parents=True, exist_ok=True)

UPLOADS_DIR = DATA_DIR / "uploads"                # -> backend/app/data/uploads
UPLOADS_DIR.mkdir(parents=True, exist_ok=True)

# Hosted NFT metadata JSON
# Hosted NFT metadata JSON (DYNAMIC MINT METADATA ONLY)
# IMPORTANT: mount ONLY /meta/dyn so /meta/state/* routes still work
app.mount(
    "/meta/dyn",
    StaticFiles(directory=str(META_DIR)),
    name="nft_metadata_dyn",
)


# Hosted uploaded images (logos, backgrounds, etc.)
app.mount(
    "/uploads",
    StaticFiles(directory=str(UPLOADS_DIR)),
    name="uploads",
)


from backend.app.services.dnft_store import init_db
init_db()


from backend.app.routes.spec_overview import router as spec_overview_router
app.include_router(spec_overview_router)


from backend.app.routes.spec import router as spec_router
app.include_router(spec_router)




from backend.app.core.settings import SESSION_COOKIE

@app.get("/_debug/session")
def _debug_session(request: Request):
    return {
        "expected_cookie_name": SESSION_COOKIE,
        "cookie_header": request.headers.get("cookie"),
        "cookies_seen": dict(request.cookies),
        "session": dict(request.session),
        "user": request.session.get("user"),
    }
