from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from xrpl.clients import JsonRpcClient
from xrpl.models.requests import ServerInfo

app = FastAPI(title="PROJXHUB")

# Jinja2 templates directory
templates = Jinja2Templates(directory="backend/app/templates")

# XRPL testnet client
XRPL_RPC_URL = "https://s.altnet.rippletest.net:51234"
xrpl_client = JsonRpcClient(XRPL_RPC_URL)

@app.get("/healthz")
def healthz():
    return {"status": "ok"}

@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    return templates.TemplateResponse(
        "index.html",
        {"request": request, "title": "PROJXHUB"}
    )

@app.get("/xrpl/ping")
def xrpl_ping():
    """Simple ledger ping to prove connectivity to XRPL testnet."""
    info = xrpl_client.request(ServerInfo())
    ledger_index = info.result["info"]["validated_ledger"]["seq"]
    return JSONResponse({"network": "XRPL Testnet", "validated_ledger_index": ledger_index})


from backend.app.api.pages import router as pages_router
app.include_router(pages_router)
