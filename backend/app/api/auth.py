import secrets
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse

from backend.app.core.templating import get_templates
from backend.app.services.xumm import (
    create_signin_payload,
    get_payload_status,
    XummConfigError,
    XummAuthError,
    get_sdk,
)

router = APIRouter()
templates = get_templates()


@router.get("/connect-modal", response_class=HTMLResponse)
def connect_modal(request: Request):
    return templates.TemplateResponse("connect_modal.html", {"request": request})

# HTMX HTML (kept for compatibility)
@router.get("/xrpl/request", response_class=HTMLResponse)
def auth_request_html(request: Request):
    try:
        nonce = secrets.token_hex(16)
        created = create_signin_payload(nonce)
        auth_map = request.session.get("auth", {})
        auth_map[created["uuid"]] = {"nonce": nonce}
        request.session["auth"] = auth_map
        return templates.TemplateResponse(
            "_xumm_prompt.html",
            {"request": request, "uuid": created["uuid"], "deeplink": created["deeplink"], "qr_uri": created["qr_png"]},
            status_code=200,
        )
    except (XummConfigError, XummAuthError) as e:
        return templates.TemplateResponse(
            "_xumm_error.html",
            {"request": request, "title": "XUMM authentication failed" if isinstance(e, XummAuthError) else "XUMM configuration problem",
             "message": str(e),
             "hint": "Rotate secret & check .env" if isinstance(e, XummAuthError) else "Set env vars (no quotes) and restart"},
            status_code=200,
        )
    except Exception as e:
        return templates.TemplateResponse(
            "_xumm_error.html",
            {"request": request, "title": "Unexpected error", "message": "Could not start wallet sign-in.", "hint": str(e)},
            status_code=200,
        )

# JSON variant for WebSocket-driven modal
@router.get("/xrpl/request.json", response_class=JSONResponse)
def auth_request_json(request: Request):
    try:
        nonce = secrets.token_hex(16)
        created = create_signin_payload(nonce)
        auth_map = request.session.get("auth", {})
        auth_map[created["uuid"]] = {"nonce": nonce}
        request.session["auth"] = auth_map
        return {"ok": True, **created}
    except (XummConfigError, XummAuthError) as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=200)

@router.get("/xrpl/status", response_class=HTMLResponse)
def auth_status(request: Request, uuid: str):
    try:
        status = get_payload_status(uuid)
    except (XummConfigError, XummAuthError) as e:
        return HTMLResponse(f"<p class='text-red-400'>Error polling XUMM: {e}</p>", status_code=200)

    if status.get("success"):
        addr = status.get("account")
        if addr:
            request.session["user"] = {"address": addr}
        auth_map = request.session.get("auth", {})
        auth_map.pop(uuid, None)
        request.session["auth"] = auth_map
        return HTMLResponse(
            f"""
            <script>
              document.getElementById('wallet-modal')?.remove();
              window.location.reload();
            </script>
            <p class='text-green-400'>Signed as {addr}</p>
            """,
            status_code=200,
        )

    if status.get("expired") or status.get("cancelled"):
        return HTMLResponse("<p class='text-red-400'>Cancelled or expired. Try again.</p>", status_code=200)

    return HTMLResponse("<p>Waiting for signature…</p>", status_code=200)






@router.get("/xrpl/status.json", response_class=JSONResponse)
def auth_status_json(request: Request, uuid: str):
    """
    JSON status endpoint for Studio Canvas pollStatus().
    Path: GET /auth/xrpl/status.json?uuid=...
    """
    try:
        status = get_payload_status(uuid)
    except (XummConfigError, XummAuthError) as e:
        return JSONResponse(
            {"ok": False, "error": str(e)},
            status_code=200,
        )
    except Exception as e:
        return JSONResponse(
            {"ok": False, "error": f"Unexpected XUMM error: {e}"},
            status_code=200,
        )

    # status is what get_payload_status() already returns in your HTML route
    success = bool(status.get("success"))
    expired = bool(status.get("expired"))
    cancelled = bool(status.get("cancelled"))
    account = status.get("account")

    if success and account:
        # mirror your HTML route: set session + clean auth map
        request.session["user"] = {"address": account}
        auth_map = request.session.get("auth", {})
        auth_map.pop(uuid, None)
        request.session["auth"] = auth_map

        return JSONResponse(
            {
                "ok": True,
                "success": True,
                "account": account,
            },
            status_code=200,
        )

    if expired or cancelled:
        return JSONResponse(
            {
                "ok": True,
                "success": False,
                "expired": expired,
                "cancelled": cancelled,
            },
            status_code=200,
        )

    # still waiting
    return JSONResponse(
        {
            "ok": True,
            "success": False,
            "pending": True,
        },
        status_code=200,
    )








# Called by client when WS announces 'resolved'/'signed'
@router.post("/xrpl/complete", response_class=JSONResponse)
async def xrpl_complete(request: Request):
    from backend.app.services.store import get_nickname  # local import to avoid cycles
    data = await request.json()
    uuid = data.get("uuid")
    if not uuid:
        return JSONResponse({"ok": False, "error": "missing uuid"}, status_code=400)

    status = get_payload_status(uuid)
    if (status.get("success") or (status.get("signed") and status.get("account"))) and status.get("account"):
        addr = status["account"]
        request.session["user"] = {"address": addr}

        # If user has no nickname yet, ensure we prompt this session
        nick = get_nickname(addr)
        if not nick:
            request.session["nick_prompted"] = False

        # clean up
        auth_map = request.session.get("auth", {})
        auth_map.pop(uuid, None)
        request.session["auth"] = auth_map

        return {"ok": True, "address": addr}

    return {"ok": False, "pending": True}

# Debug helpers
@router.get("/xrpl/status-debug", response_class=JSONResponse)
def auth_status_debug(uuid: str):
    try:
        sdk = get_sdk()
        p = sdk.payload.get(uuid)
        meta = getattr(p, "meta", None)
        resp = getattr(p, "response", None)
        out = {
            "meta": {
                "opened": getattr(meta, "opened", None),
                "resolved": getattr(meta, "resolved", None),
                "signed": getattr(meta, "signed", None),
                "cancelled": getattr(meta, "cancelled", None),
                "expired": getattr(meta, "expired", None),
            } if meta else None,
            "response": {
                "account": getattr(resp, "account", None),
                "txid": getattr(resp, "txid", None),
                "signed_blob_present": bool(getattr(resp, "txblob", None)),
            } if resp else None,
        }
        return out
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)



# ========= Canvas-friendly XUMM connect (used by Studio Canvas) =========

@router.post("/xrpl/connect", response_class=JSONResponse)
def xrpl_connect_canvas(request: Request):
    """
    Endpoint used by Studio Canvas `startWalletConnect()`.

    Full path (with main include prefix) is expected to be:
      POST /auth/xrpl/connect
    """
    try:
        nonce = secrets.token_hex(16)
        created = create_signin_payload(nonce)
        # Keep the same session storage pattern as the other routes
        auth_map = request.session.get("auth", {})
        auth_map[created["uuid"]] = {"nonce": nonce}
        request.session["auth"] = auth_map

        # created typically has: uuid, qr_png, deeplink, ws
        return {
            "ok": True,
            "uuid": created["uuid"],
            "qr_png": created.get("qr_png"),
            "deeplink": created.get("deeplink"),
        }
    except (XummConfigError, XummAuthError) as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=200)
    except Exception as e:
        return JSONResponse(
            {"ok": False, "error": f"Unexpected XUMM error: {e}"},
            status_code=200,
        )



@router.get("/xrpl/connect/status", response_class=JSONResponse)
def xrpl_connect_status_canvas(request: Request, uuid: str):
    """
    Poll status for Studio Canvas.

    Path: GET /auth/xrpl/connect/status?uuid=...
    """
    try:
        # Use the raw SDK just like auth_status_debug
        sdk = get_sdk()
        p = sdk.payload.get(uuid)

        meta = getattr(p, "meta", None)
        resp = getattr(p, "response", None)

        signed = bool(getattr(meta, "signed", None)) if meta else False
        cancelled = bool(getattr(meta, "cancelled", None)) if meta else False
        expired = bool(getattr(meta, "expired", None)) if meta else False
        account = getattr(resp, "account", None) if resp else None

    except (XummConfigError, XummAuthError) as e:
        return JSONResponse(
            {"connected": False, "error": str(e)},
            status_code=200,
        )
    except Exception as e:
        return JSONResponse(
            {"connected": False, "error": f"Unexpected XUMM error: {e}"},
            status_code=200,
        )

    # ✅ Signed & has account → mark as connected
    if signed and account:
        request.session["user"] = {"address": account}

        # clean up session auth map like your other routes
        auth_map = request.session.get("auth", {})
        auth_map.pop(uuid, None)
        request.session["auth"] = auth_map

        # You can customize this if you later support testnet
        network = "xrpl-mainnet"

        return {
            "connected": True,
            "address": account,
            "network": network,
        }

    # ❌ Explicitly cancelled / expired
    if expired or cancelled:
        return {
            "connected": False,
            "expired": expired,
            "cancelled": cancelled,
        }

    # ⏳ Still pending
    return {
        "connected": False,
    }








@router.get("/xrpl/tx/status", response_class=JSONResponse)
def xrpl_tx_status_canvas(uuid: str):
    """
    JSON status for XUMM transaction payloads used by Studio Canvas
    (payments, offers, burns, etc.).

    Path: GET /auth/xrpl/tx/status?uuid=...
    """
    from backend.app.services.xumm import get_payload_status

    try:
      status = get_payload_status(uuid)
    except (XummConfigError, XummAuthError) as e:
      return JSONResponse(
          {"success": False, "error": str(e)},
          status_code=200,
      )
    except Exception as e:
      return JSONResponse(
          {"success": False, "error": f"Unexpected XUMM error: {e}"},
          status_code=200,
      )

    # get_payload_status is expected to return a dict like:
    # { success: bool, expired: bool, cancelled: bool, txid: str, account: str, ... }
    return status




from fastapi import Request

@router.post("/xrpl/xumm/webhook")
async def auth_xumm_webhook(request: Request):
    payload = await request.json()

    pr = payload.get("payloadResponse", {}) or {}
    signed = pr.get("signed")
    uuid = pr.get("payload_uuidv4") or payload.get("payload_uuidv4")

    print("🔐 AUTH XUMM WEBHOOK HIT:", uuid, "signed:", signed)

    # Always acknowledge so Xumm never retries
    return {"ok": True}




@router.post("/logout")
def logout(request: Request):
    request.session.pop("user", None)
    # Tell HTMX to redirect to home so header updates immediately
    headers = {"HX-Redirect": "/"}
    return JSONResponse({"ok": True}, headers=headers)