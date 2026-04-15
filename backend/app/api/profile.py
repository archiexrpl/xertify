from fastapi import APIRouter, Request, Form
from fastapi.responses import JSONResponse, HTMLResponse

from backend.app.core.templating import get_templates

router = APIRouter()
templates = get_templates()

@router.post("/nickname", response_class=HTMLResponse)
async def set_user_nickname(request: Request, nickname: str = Form(...)):
    user = request.session.get("user")
    if not user or not user.get("address"):
        return HTMLResponse("<p class='text-red-400'>You must be logged in.</p>", status_code=401)
    set_nickname(user["address"], nickname)
    request.session["nick_prompted"] = True
    return HTMLResponse(f"<p class='text-green-400'>Saved! Your nickname is now <b>{nickname}</b>.</p>")

@router.get("/me", response_class=JSONResponse)
def profile_me(request: Request):
    user = request.session.get("user")
    if not user:
        return JSONResponse({"logged_in": False})
    address = user.get("address")
    nickname = get_nickname(address) if address else None
    prompted = bool(request.session.get("nick_prompted", False))
    return JSONResponse({
        "logged_in": True,
        "address": address,
        "nickname": nickname,
        "prompted": prompted
    })

@router.get("/nickname-modal", response_class=HTMLResponse)
def nickname_modal(request: Request):
    return templates.TemplateResponse("nickname_modal.html", {"request": request})

@router.post("/nickname-later", response_class=JSONResponse)
def nickname_later(request: Request):
    request.session["nick_prompted"] = True
    return JSONResponse({"ok": True})

@router.post("/update", response_class=HTMLResponse)
async def update_profile(request: Request,
                         bio: str = Form(""),
                         website: str = Form(""),
                         twitter: str = Form(""),
                         discord: str = Form("")):
    user = request.session.get("user")
    if not user or not user.get("address"):
        return HTMLResponse("<p class='text-red-400'>You must be logged in.</p>", status_code=401)
    upsert_profile(user["address"], bio, website, twitter, discord)
    return HTMLResponse("<p class='text-green-400'>Profile updated.</p>")

@router.post("/achievements/add", response_class=HTMLResponse)
async def achievements_add(request: Request,
                           title: str = Form(...),
                           issuer: str = Form(""),
                           issued_at: str = Form(""),
                           description: str = Form("")):
    user = request.session.get("user")
    if not user or not user.get("address"):
        return HTMLResponse("<p class='text-red-400'>You must be logged in.</p>", status_code=401)
    add_achievement(user["address"], title, issuer, issued_at, description)
    # re-render the list portion only
    ach = list_achievements(user["address"])
    return templates.TemplateResponse("_achievements_list.html", {"request": request, "achievements": ach})

@router.post("/achievements/delete", response_class=HTMLResponse)
async def achievements_delete(request: Request, id: int = Form(...)):
    user = request.session.get("user")
    if not user or not user.get("address"):
        return HTMLResponse("<p class='text-red-400'>You must be logged in.</p>", status_code=401)
    delete_achievement(user["address"], int(id))
    ach = list_achievements(user["address"])
    return templates.TemplateResponse("_achievements_list.html", {"request": request, "achievements": ach})


@router.get("/wallet-overview", response_class=HTMLResponse)
def wallet_overview(request: Request):
    user = request.session.get("user")
    if not user or not user.get("address"):
        return HTMLResponse("<p class='text-slate-400 text-sm'>Not logged in.</p>", status_code=200)

    address = user["address"]

    # 1) XRP balance (XRPL)
    try:
        from backend.app.services.xrpl_account import account_overview
        data = account_overview(address)
    except Exception:
        # Never break the dashboard if XRPL RPC is slow/down
        data = {
            "account": address,
            "funded": False,
            "xrp": {"total": 0.0, "available": 0.0, "reserve": 0.0},
        }

    # 2) DNFT minted counts (your DB/store)
    try:
        from backend.app.services.dnft_store import list_passes_created_by
        items = list_passes_created_by(address, limit=500) or []
        created_count = len(items)
        minted_count = sum(1 for p in items if (p.get("nft_id") or "").strip())
    except Exception:
        created_count = 0
        minted_count = 0

    return templates.TemplateResponse(
        "_wallet_overview.html",
        {
            "request": request,
            "data": data,
            "created_count": created_count,
            "minted_count": minted_count,
        },
    )



from fastapi.responses import HTMLResponse
from backend.app.services.dnft_store import (
    list_passes_created_by,
    list_passes_related_to_address,
    list_recent_events_for_actor,
)

@router.get("/passes/created", response_class=HTMLResponse)
def profile_created_passes(request: Request):
    user = request.session.get("user")
    if not user:
        return "<div class='text-slate-400 text-sm'>Connect wallet to view passes.</div>"

    addr = user.get("address")
    items = list_passes_created_by(addr, limit=200)
    return templates.TemplateResponse(
        "_profile_passes_created.html",
        {"request": request, "items": items, "user": user},
    )


@router.get("/passes/related", response_class=HTMLResponse)
def profile_related_passes(request: Request):
    user = request.session.get("user")
    if not user:
        return "<div class='text-slate-400 text-sm'>Connect wallet to view passes.</div>"

    addr = user.get("address")
    items = list_passes_related_to_address(addr, limit=200)
    return templates.TemplateResponse(
        "_profile_passes_related.html",
        {"request": request, "items": items, "user": user},
    )


@router.get("/activity", response_class=HTMLResponse)
def profile_activity(request: Request):
    user = request.session.get("user")
    if not user:
        return "<div class='text-slate-400 text-sm'>Connect wallet to view activity.</div>"

    addr = user.get("address")
    events = list_recent_events_for_actor(addr, limit=200)
    return templates.TemplateResponse(
        "_profile_activity.html",
        {"request": request, "events": events, "user": user},
    )


from fastapi.responses import JSONResponse
from backend.app.services import xumm
from backend.app.services.xrpl_tx import create_anchor_proof_payload
from backend.app.services.dnft_store import get_pass, get_latest_state_proof, set_proof_anchor, set_proof_anchor_result


@router.post("/passes/{pass_id}/anchor", response_class=JSONResponse)
def anchor_latest_proof(request: Request, pass_id: str):
    user = request.session.get("user")
    if not user or not user.get("address"):
        return JSONResponse({"ok": False, "error": "Not logged in"}, status_code=401)

    addr = user["address"]
    rec = get_pass(pass_id)
    if not rec or rec.get("creator_address") != addr:
        return JSONResponse({"ok": False, "error": "Not allowed"}, status_code=403)

    proof = get_latest_state_proof(pass_id)
    if not proof:
        return JSONResponse({"ok": False, "error": "No proof exists"}, status_code=400)

    # Compact memo: v1|pass|state|proofhash|idhash
    memo = f"v1|{pass_id}|{proof.get('state')}|{proof.get('proof_hash')}|{proof.get('identity_hash')}"

    created = create_anchor_proof_payload(addr, memo)
    xumm_uuid = created["uuid"]

    set_proof_anchor(pass_id, proof["proof_hash"], xumm_uuid)

    # Return the XUMM payload (front-end can show QR)
    return JSONResponse({"ok": True, "uuid": xumm_uuid, "payload": created["payload"]})


@router.get("/passes/{pass_id}/anchor-status/{xumm_uuid}", response_class=JSONResponse)
def anchor_status(request: Request, pass_id: str, xumm_uuid: str):
    st = xumm.get_payload_status(xumm_uuid)

    # When signed, capture the tx hash
    if st.get("resolved") and st.get("signed") and st.get("txid"):
        set_proof_anchor_result(pass_id, xumm_uuid, st["txid"], None)

    return JSONResponse({"ok": True, "status": st})
