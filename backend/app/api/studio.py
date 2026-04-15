# backend/app/api/studio.py



# backend/app/api/studio.py
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from backend.app.services.dnft_store import (
    grant_role, revoke_role, list_roles_for_actor,
)


import hashlib
import io
import json
import os
import zipfile
from fastapi import HTTPException
from backend.app.services.domains.registry import get_registry
from backend.app.services.dnft_store import enforce_authority


from backend.app.services.spec import load_authority_model
from fastapi import APIRouter, Body, Depends, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, StreamingResponse
from pydantic import BaseModel

from backend.app.core.settings import APP_BASE_URL
from backend.app.core.templating import get_templates
from backend.app.services.conditions import CANONICAL_VERDICT_STATUSES, normalize_verdict
from backend.app.services.dnft_store import (
    attach_nft_id,
    
    compute_verdict,
    get_event,
    get_external_facts,
    get_latest_event_by_type,
    get_pass,
    increment_usage,
    list_events,
    log_event,
    set_state,
    store_mint_tx_hash,
)
from backend.app.services.dnft_store import create_pass
from backend.app.services.store import insert_project, list_projects_for_address
from backend.app.services.xrpl_tx import (
    XRPLActionError,
    create_accountset_flag_payload,
    create_escrow_create_payload,
    create_iou_payment_payload,
    create_nft_accept_offer_payload,
    create_nft_burn_payload,
    create_nft_cancel_offer_payload,
    create_nft_mint_payload,
    create_nft_modify_payload,
    create_nft_offer_payload,
    create_nft_send_payload,
    create_simple_xrp_payment_payload,
    create_trustline_payload,
)





router = APIRouter()
templates = get_templates()

# ------------------------------
# Auth helpers (local, session-based)
# ------------------------------
def get_current_user(request: Request) -> dict:
    """
    FastAPI dependency: returns the current session user dict.
    Raises 401 if missing.
    """
    user = request.session.get("user") if hasattr(request, "session") else None
    if not user:
        raise HTTPException(status_code=401, detail="Not logged in")
    if not isinstance(user, dict):
        raise HTTPException(status_code=401, detail="Invalid session user")
    return user


def require_creator_id(user: dict) -> None:
    """
    Minimal gate used by authority endpoints.
    Adjust this later if you want stricter 'creator/admin' rules.
    """
    if not user or not isinstance(user, dict):
        raise HTTPException(status_code=401, detail="Not logged in")
    if not user.get("address"):
        raise HTTPException(status_code=401, detail="Missing creator address")
    







# ------------------------------
# Authority (single source of truth)
# ------------------------------
from backend.app.services.dnft_store import enforce_authority

def _resolve_vertical_id_from_pass(rec: dict) -> str:
    """
    Single canonical way to resolve vertical_id from a pass record.
    Matches your mint stamping (meta.properties.vertical).
    """
    meta = rec.get("metadata_json") or {}
    props = meta.get("properties") if isinstance(meta, dict) else {}

    vertical_id = (
        (rec.get("vertical_id") or "").strip().lower()
        or (meta.get("vertical_id") or "").strip().lower()
        or (
            ((props.get("vertical") or props.get("vertical_id") or "").strip().lower())
            if isinstance(props, dict)
            else ""
        )
    )
    return vertical_id or ""

def require_vertical(rec: dict, expected_vertical_id: str) -> str:
    """
    Enforce expected vertical, return resolved vertical_id (for convenience).
    """
    v = _resolve_vertical_id_from_pass(rec)
    if not v:
        raise HTTPException(status_code=400, detail="Pass missing vertical_id")
    if v != (expected_vertical_id or "").strip().lower():
        raise HTTPException(status_code=400, detail=f"Pass is not {expected_vertical_id}")
    return v

from fastapi import HTTPException
from backend.app.services.dnft_store import enforce_authority

def require_authority(rec: dict, actor_address: str | None, action: str) -> None:
    """
    Enforce authority for an action using dnft_store.enforce_authority ONLY.
    Single gate: it either allows (returns) or raises HTTPException.
    """
    # dnft_store.enforce_authority already raises 401 if viewer_address missing,
    # but keeping this here gives a cleaner message in studio routes.
    if not actor_address:
        raise HTTPException(status_code=401, detail="Not logged in")

    # IMPORTANT: enforce_authority expects (rec, viewer_address, action)
    enforce_authority(rec, actor_address, action)



# Where we store metadata JSON files (must match main.py's META_DIR)
BASE_DIR = Path(__file__).resolve().parent.parent   # backend/app/api -> backend/app
META_DIR = BASE_DIR / "static" / "meta" / "dyn"
META_DIR.mkdir(parents=True, exist_ok=True)

# NEW: where we store uploaded images for passes / logos
UPLOADS_DIR = BASE_DIR / "data" / "uploads"
UPLOADS_DIR.mkdir(parents=True, exist_ok=True)



# ---------- SIMPLE BLUEPRINT (kept for generator route) ----------
def synthesize_blueprint(
    vision: str,
    category: str,
    monetization: List[str],
    features: List[str],
    explain_mode: bool,
    mode: str = "site",
) -> Dict[str, Any]:
    components: List[Dict[str, Any]] = []

    if category == "ecommerce":
        components += [
            {"type": "Wallet.Connect", "props": {"label": "Connect Wallet"}},
            {"type": "Wallet.Pay", "props": {"label": "Pay 1 XRP", "amount_xrp": 1}},
            {"type": "UI.Text", "props": {"text": "Product grid goes here"}},
        ]
    elif category == "nft_market":
        components += [
            {"type": "Wallet.Connect", "props": {"label": "Connect Wallet"}},
            {
                "type": "NFT.Mint",
                "props": {"name": "My NFT", "uri": "https://example.com/meta.json"},
            },
            {"type": "NFT.Gallery", "props": {"filter": "owner"}},
        ]
    elif category == "loyalty":
        components += [
            {"type": "Wallet.Connect", "props": {"label": "Connect Wallet"}},
            {
                "type": "Token.Trustline",
                "props": {"currency": "POINTS", "issuer": "", "limit": "1000000"},
            },
            {"type": "UI.Text", "props": {"text": "Rewards dashboard"}},
        ]
    else:
        components += [
            {"type": "Wallet.Connect", "props": {"label": "Connect Wallet"}},
            {"type": "UI.Text", "props": {"text": "Describe your app"}},
        ]

    blueprint = {
        "theme": {"preset": "indigo", "radius": 16, "container": 1000},
        "pages": [
            {
                "id": "home",
                "title": "Home",
                "slug": "home",
                "sections": [
                    {
                        "id": "sec_hero",
                        "type": "Hero",
                        "title": "Hero",
                        "components": [
                            {
                                "id": "cta",
                                "type": "UI.CTA",
                                "props": {"label": "Get Started", "href": "#"},
                            }
                        ],
                        "data": {
                            "eyebrow": "Welcome",
                            "headline": "Build on XRPL",
                            "sub": "No-code studio to launch products.",
                            "cta": "Get Started",
                            "image": "https://placehold.co/960x420",
                        },
                    },
                    {
                        "id": "sec_body",
                        "type": "TwoColumn",
                        "title": "Body",
                        "components": components,
                        "data": {
                            "headline": "Your app, fast",
                            "left": "Explain your idea",
                            "right": "Drop XRPL components",
                        },
                    },
                ],
            }
        ],
    }

    if explain_mode:
        blueprint["explain"] = {
            "note": "Explain mode enabled. Fields in Inspector show how each part works."
        }
    return blueprint


# ------------------------------
# Request models (Authority)
# ------------------------------
class RoleGrantRequest(BaseModel):
    vertical_id: str = "aviation"
    actor_address: str
    role: str
    scope: dict | None = None

class RoleRevokeRequest(BaseModel):
    vertical_id: str = "aviation"
    actor_address: str
    role: str



# ---------- HUB / NAV ----------
@router.get("/studio", response_class=HTMLResponse)
def studio_home(request: Request):
    user = request.session.get("user")
    return templates.TemplateResponse(
        "studio/studio.html",
        {"request": request, "title": "Studio", "user": user},
    )


@router.get("/studio/presets", response_class=HTMLResponse)
def studio_presets(request: Request):
    user = request.session.get("user")
    return templates.TemplateResponse(
        "studio/presets.html",
        {"request": request, "title": "Studio • Presets", "user": user},
    )


@router.get("/studio/generator", response_class=HTMLResponse)
def studio_generator(request: Request):
    user = request.session.get("user")
    return templates.TemplateResponse(
        "studio/generator.html",
        {"request": request, "title": "Studio • Site Generator", "user": user},
    )


@router.post("/studio/generator", response_class=HTMLResponse)
def studio_generator_post(
    request: Request,
    vision: str = Form(""),
    category: str = Form("custom"),
    explain_mode: str = Form("on"),
    monetization: List[str] = Form(default=[]),
    features: List[str] = Form(default=[]),
):
    user = request.session.get("user")
    if not user or not user.get("address"):
        return RedirectResponse(url="/auth/connect-modal", status_code=302)

    bp = synthesize_blueprint(
        vision=vision.strip(),
        category=category,
        monetization=monetization,
        features=features,
        explain_mode=(explain_mode == "on"),
        mode="site",
    )

    pid = insert_project(
        address=user["address"],
        ptype="studio_blueprint",
        name=(vision.strip() or f"{category.title()} App"),
        params=bp,
    )
    return RedirectResponse(url=f"/studio/canvas/{pid}", status_code=302)


@router.get("/studio/canvas/{project_id}", response_class=HTMLResponse)
def studio_canvas(request: Request, project_id: int):
    user = request.session.get("user")
    addr = user["address"] if user and user.get("address") else None
    projects = list_projects_for_address(addr) if addr else []
    proj = next((p for p in projects if p["id"] == project_id), None)

    if not proj:
        return RedirectResponse(url="/studio/generator", status_code=302)

    import json

    try:
        params = json.loads(proj["params_json"])
    except Exception:
        params = {}

    return templates.TemplateResponse(
        "studio/canvas.html",
        {
            "request": request,
            "title": "Studio • Canvas",
            "user": user,
            "project": proj,
            "params": params,
        },
    )


@router.get("/studio/canvas", response_class=HTMLResponse)
def studio_canvas_blank(request: Request):
    """
    Open the Canvas directly without forcing the Site Generator.
    Users can play immediately; saving will create a project as usual.
    """
    user = request.session.get("user")

    # Optional: seed a starter blueprint if you prefer, otherwise pass {} to use the default in canvas.html
    params: Dict[str, Any] = {}  # or synthesize_blueprint(...)

    return templates.TemplateResponse(
        "studio/canvas.html",
        {
            "request": request,
            "title": "Studio • Canvas",
            "user": user,
            "project": None,  # No existing project yet
            "params": params,  # Canvas template already handles the empty case
        },
    )


# Save endpoint used by canvas
@router.post("/build/studio/save")
def studio_save(request: Request, name: str = Form(...), blueprint: str = Form(...)):
    user = request.session.get("user")
    if not user or not user.get("address"):
        return JSONResponse({"ok": False, "error": "Not logged in"}, status_code=401)
    import json

    try:
        params = json.loads(blueprint)
    except Exception as e:
        return JSONResponse(
            {"ok": False, "error": f"Invalid blueprint JSON: {e}"},
            status_code=400,
        )

    pid = insert_project(user["address"], "studio_blueprint", name, params)
    return JSONResponse({"ok": True, "id": pid})


# ---------- XUMM (simple XRP pay) ----------
@router.post("/studio/actions/xumm/pay")
def action_xumm_pay(
    request: Request,
    amount_drops: str = Form(...),
    destination: str = Form(""),
    memo: str = Form("PROJXHUB Pay"),
):
    user = request.session.get("user")
    if not user or not user.get("address"):
        return JSONResponse({"ok": False, "error": "Login required"}, status_code=401)
    try:
        payload = create_simple_xrp_payment_payload(
            user["address"], destination, int(amount_drops), memo
        )
        return JSONResponse({"ok": True, **payload})
    except XRPLActionError as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=400)
    except Exception as e:
        return JSONResponse(
            {"ok": False, "error": f"Unexpected: {e}"},
            status_code=500,
        )


# ---------- XRPL ACTIONS (Inspector → XUMM QR) ----------
@router.post("/studio/actions/xrpl/trustline")
def action_trustline(
    request: Request,
    currency: str = Form(...),
    issuer: str = Form(...),
    limit: str = Form("100000000000"),
):
    user = request.session.get("user")
    if not user or not user.get("address"):
        return JSONResponse({"ok": False, "error": "Login required"}, status_code=401)
    try:
        payload = create_trustline_payload(
            user["address"], currency, issuer, limit
        )
        return JSONResponse({"ok": True, **payload})
    except XRPLActionError as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=400)
    except Exception as e:
        return JSONResponse(
            {"ok": False, "error": f"Unexpected: {e}"},
            status_code=500,
        )


@router.post("/studio/actions/xrpl/nft_mint")
def action_nft_mint(
    request: Request,
    uri: str = Form(...),
    flags: str = Form(""),
    transfer_fee: str = Form(""),
):
    user = request.session.get("user")
    if not user or not user.get("address"):
        return JSONResponse({"ok": False, "error": "Login required"}, status_code=401)
    f = int(flags) if (flags and flags.isdigit()) else None
    tf = int(transfer_fee) if (transfer_fee and transfer_fee.isdigit()) else None
    try:
        payload = create_nft_mint_payload(user["address"], uri, f, tf)
        return JSONResponse({"ok": True, **payload})
    except XRPLActionError as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=400)
    except Exception as e:
        return JSONResponse(
            {"ok": False, "error": f"Unexpected: {e}"},
            status_code=500,
        )


# ---------- NFT DESIGNER: IMAGE UPLOAD (logo / background) ----------
@router.post("/studio/actions/upload_image")
async def studio_upload_image(
    request: Request,
    file: UploadFile = File(...),
    kind: str = Form("generic"),
):
    """
    Accepts multipart/form-data:
      - file: image file
      - kind: "logo" | "background" | other

    Stores in backend/app/data/uploads and returns a public URL.
    """
    user = request.session.get("user")
    if not user or not user.get("address"):
        return JSONResponse({"ok": False, "error": "Login required"}, status_code=401)

    try:
        original_name = file.filename or "image"
        ts = datetime.utcnow().strftime("%Y%m%d%H%M%S")
        base = f"{ts}_{kind}_{original_name}"
        safe_name = "".join(
            ch if ch.isalnum() or ch in ("-", "_", ".") else "_"
            for ch in base
        )
        dest_path = UPLOADS_DIR / safe_name

        content = await file.read()
        dest_path.write_bytes(content)
    except Exception as e:
        return JSONResponse(
            {"ok": False, "error": f"Could not store file: {e}"},
            status_code=500,
        )

    url = f"{APP_BASE_URL.rstrip('/')}/uploads/{safe_name}"
    return JSONResponse({"ok": True, "url": url})




def _extract_canonical_ids(metadata: dict) -> dict:
    props = (metadata or {}).get("properties") or {}

    domain_id = (props.get("domain_id") or "").strip()
    template_key = (props.get("template_key") or props.get("template_id") or "").strip()

    # fallback: try attributes if needed
    if (not domain_id) or (not template_key):
        attrs = metadata.get("attributes") or []
        for a in attrs:
            if not isinstance(a, dict):
                continue
            tt = (a.get("trait_type") or "").strip()
            vv = (a.get("value") or "")
            if tt == "domain_id" and not domain_id:
                domain_id = str(vv).strip()
            if tt in ("template_key", "template_id") and not template_key:
                template_key = str(vv).strip()

    return {
        "domain_id": domain_id or None,
        "template_key": template_key or None,
    }


def _registry_domains(reg: dict) -> dict:
    """
    Registry shape can vary. Normalize to a dict of domains keyed by domain_id.
    Accepts:
      - {"domains": {...}}
      - {"registry": {"domains": {...}}}
      - {"domains": [ ... ]} (list)
      - [ ... ] (list)
      - { ... } (already a dict of domains)
    """
    if not isinstance(reg, dict):
        return {}

    if isinstance(reg.get("registry"), dict) and isinstance(reg["registry"].get("domains"), (dict, list)):
        reg = reg["registry"]

    doms = reg.get("domains")
    if isinstance(doms, dict):
        return doms

    # If domains is a list, build an index
    if isinstance(doms, list):
        out = {}
        for d in doms:
            if isinstance(d, dict):
                did = (d.get("id") or d.get("domain_id") or "").strip()
                if did:
                    out[did] = d
        return out

    # If reg itself looks like a domains dict (heuristic)
    # e.g. {"supply_chain": {...}, "regulated_rwa": {...}}
    # Keep only dict values.
    out = {}
    for k, v in reg.items():
        if isinstance(v, dict):
            out[k] = v
    return out


def _domain_templates(domain_obj: dict) -> dict:
    """
    Normalize templates collection to dict keyed by template_key.
    Accepts:
      - {"templates": {...}}
      - {"templates": [ ... ]}
      - {"template_index": {...}}
      - {"packs": {...}} etc (best-effort)
    """
    if not isinstance(domain_obj, dict):
        return {}

    # Most common keys first
    for key in ("templates", "template_index", "templateRegistry", "packs", "engine_packs"):
        t = domain_obj.get(key)
        if isinstance(t, dict):
            return t
        if isinstance(t, list):
            out = {}
            for it in t:
                if not isinstance(it, dict):
                    continue
                tk = (it.get("key") or it.get("template_key") or it.get("id") or it.get("template_id") or "").strip()
                if tk:
                    out[tk] = it
            return out

    return {}


def _find_template_in_registry(reg: dict, domain_id: str | None, template_key: str | None) -> tuple[str | None, dict | None]:
    """
    Returns (resolved_domain_id, template_obj) or (None, None).
    Enforces:
      - If domain_id is provided: template must exist inside that domain
      - Else: searches all domains for a matching template_key
    """
    domains = _registry_domains(reg)

    if not template_key:
        return (domain_id or None), None

    # If domain provided, only accept within that domain
    if domain_id:
        d = domains.get(domain_id)
        if not isinstance(d, dict):
            return domain_id, None
        tmap = _domain_templates(d)
        tmpl = tmap.get(template_key)
        if isinstance(tmpl, dict):
            return domain_id, tmpl
        return domain_id, None

    # Otherwise, search all domains
    for did, d in domains.items():
        if not isinstance(d, dict):
            continue
        tmap = _domain_templates(d)
        tmpl = tmap.get(template_key)
        if isinstance(tmpl, dict):
            return did, tmpl

    return None, None


def _template_required_keys(template_obj: dict) -> list[str]:
    """
    Best-effort extraction of required keys from template schema.
    Supports:
      - template["required"] = ["field_a", ...]
      - template["schema"]["required"] = [...]
      - template["schema"]["fields"] = [{"key": "...", "required": True}, ...]
    """
    if not isinstance(template_obj, dict):
        return []

    # explicit required list
    req = template_obj.get("required")
    if isinstance(req, list):
        return [str(x).strip() for x in req if str(x).strip()]

    schema = template_obj.get("schema")
    if isinstance(schema, dict):
        req2 = schema.get("required")
        if isinstance(req2, list):
            return [str(x).strip() for x in req2 if str(x).strip()]

        fields = schema.get("fields")
        if isinstance(fields, list):
            out = []
            for f in fields:
                if not isinstance(f, dict):
                    continue
                if f.get("required") is True:
                    k = (f.get("key") or f.get("name") or f.get("id") or "").strip()
                    if k:
                        out.append(k)
            return out

    return []


def _template_allowed_keys(template_obj: dict) -> list[str]:
    """
    Allowed keys from schema (used for rejecting unknown keys).
    Supports:
      - template["schema"]["fields"] = [{"key": "..."}]
      - template["fields"] = [...]
    """
    if not isinstance(template_obj, dict):
        return []

    schema = template_obj.get("schema")
    fields = None

    if isinstance(schema, dict) and isinstance(schema.get("fields"), list):
        fields = schema.get("fields")
    elif isinstance(template_obj.get("fields"), list):
        fields = template_obj.get("fields")

    if not isinstance(fields, list):
        return []

    out = []
    for f in fields:
        if not isinstance(f, dict):
            continue
        k = (f.get("key") or f.get("name") or f.get("id") or "").strip()
        if k:
            out.append(k)
    return out


def _extract_template_fields(meta: dict, allowed_keys: list[str]) -> dict:
    """
    Pull template field values from:
      A) meta["properties"]["template_fields"] (preferred)
      B) meta["attributes"] where trait_type matches allowed keys
    Returns dict of {key: value}
    """
    if not isinstance(meta, dict):
        return {}

    props = meta.get("properties") if isinstance(meta.get("properties"), dict) else {}
    tf = props.get("template_fields")
    if isinstance(tf, dict):
        # normalize keys to strings
        return {str(k): tf.get(k) for k in tf.keys()}

    # fallback: pull from attributes list
    out = {}
    attrs = meta.get("attributes") or []
    if isinstance(attrs, list) and allowed_keys:
        allowed = set([k for k in allowed_keys if k])
        for a in attrs:
            if not isinstance(a, dict):
                continue
            tt = (a.get("trait_type") or "").strip()
            if tt in allowed:
                out[tt] = a.get("value")
    return out


def _validate_template_truth_or_400(*, meta: dict, vertical_id: str) -> dict:
    """
    Phase C hardening (server-side):
      - Validate template_key exists
      - Validate domain_id matches registry truth (or resolve it)
      - Validate vertical_id matches template.vertical_id if present
      - Validate required fields exist & non-empty
      - Reject unknown template keys (template_fields only) if schema provides allowed keys

    Returns: {"domain_id": ..., "template_key": ..., "template": template_obj}
    Raises: HTTPException(400) with clear message
    """
    if not isinstance(meta, dict):
        raise HTTPException(status_code=400, detail="metadata must be an object")

    ids = _extract_canonical_ids(meta)
    domain_id = (ids.get("domain_id") or "").strip() or None
    template_key = (ids.get("template_key") or "").strip() or None

    # Must have template_key + domain_id (Phase C truth rails)
    if not template_key:
        raise HTTPException(status_code=400, detail="Missing template_key (properties.template_key)")

    if not domain_id:
        raise HTTPException(status_code=400, detail="Missing domain_id (properties.domain_id)")

    # Load registry + resolve template
    reg = get_registry()
    resolved_domain_id, template_obj = _find_template_in_registry(reg, domain_id, template_key)

    if not template_obj:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown template_key for domain (domain_id={domain_id}, template_key={template_key})",
        )

    if resolved_domain_id and resolved_domain_id != domain_id:
        raise HTTPException(
            status_code=400,
            detail=f"Template/domain mismatch (domain_id={domain_id} but template belongs to {resolved_domain_id})",
        )

    # Vertical match (if template declares one)
    tmpl_vertical = (
        (template_obj.get("vertical_id") or template_obj.get("vertical") or "").strip().lower()
        if isinstance(template_obj, dict)
        else ""
    )
    if tmpl_vertical and tmpl_vertical != (vertical_id or "").strip().lower():
        raise HTTPException(
            status_code=400,
            detail=f"Vertical mismatch (vertical_id={vertical_id} but template requires {tmpl_vertical})",
        )

    # Required + allowed keys
    required_keys = _template_required_keys(template_obj)
    allowed_keys = _template_allowed_keys(template_obj)

    # Pull values (prefer properties.template_fields)
    tf = _extract_template_fields(meta, allowed_keys)

    # Reject unknown keys (only when schema provides allowed keys AND template_fields is present)
    props = meta.get("properties") if isinstance(meta.get("properties"), dict) else {}
    has_template_fields_dict = isinstance(props.get("template_fields"), dict)
    if allowed_keys and has_template_fields_dict:
        allowed = set([k for k in allowed_keys if k])
        unknown = [k for k in tf.keys() if k not in allowed]
        if unknown:
            raise HTTPException(
                status_code=400,
                detail=f"Unknown template field keys: {unknown}",
            )

    # Required presence check (treat '', None as missing)
    missing = []
    for k in required_keys:
        v = tf.get(k)
        if v is None:
            missing.append(k)
            continue
        if isinstance(v, str) and not v.strip():
            missing.append(k)

    if missing:
        raise HTTPException(
            status_code=400,
            detail=f"Missing required template fields: {missing}",
        )

    return {"domain_id": domain_id, "template_key": template_key, "template": template_obj}



@router.post("/studio/actions/nft/mint_from_designer")
async def action_nft_mint_from_designer(request: Request):
    user = request.session.get("user")
    if not user or not user.get("address"):
        return JSONResponse({"ok": False, "error": "Login required"}, status_code=401)

    # ------------------------------
    # Parse JSON
    # ------------------------------
    try:
        payload_in = await request.json()
    except Exception:
        return JSONResponse({"ok": False, "error": "Invalid JSON body"}, status_code=400)

    meta = payload_in.get("metadata")
    if not isinstance(meta, dict):
        return JSONResponse({"ok": False, "error": "metadata must be an object"}, status_code=400)

    if not meta.get("name"):
        meta["name"] = "Untitled NFT"

    # ------------------------------
    # Vertical (engine pack)
    # ------------------------------
    vertical_id = ((payload_in.get("vertical_id") or payload_in.get("vertical") or "").strip().lower())

    # allow meta.properties.vertical as fallback
    try:
        props0 = meta.get("properties") if isinstance(meta.get("properties"), dict) else {}
        vertical_id = vertical_id or ((props0.get("vertical") or props0.get("vertical_id") or "").strip().lower())
    except Exception:
        pass

    if vertical_id not in ("generic", "aviation", "pharma"):
        vertical_id = "generic"

    # ensure metadata has canonical vertical stamp
    if not meta.get("properties") or not isinstance(meta.get("properties"), dict):
        meta["properties"] = {}
    meta["properties"]["vertical"] = vertical_id

    # ============================================================
    # ✅ Phase C — Server-side template truth enforcement
    # ============================================================
    try:
        _ = _validate_template_truth_or_400(meta=meta, vertical_id=vertical_id)
    except HTTPException as e:
        return JSONResponse({"ok": False, "error": e.detail}, status_code=e.status_code)

    # ---------------------------------------
    # Normalize image URLs
    # ---------------------------------------
    try:
        base_url = APP_BASE_URL.rstrip("/")

        img = meta.get("image")
        if isinstance(img, str) and img:
            if img.startswith("/"):
                meta["image"] = f"{base_url}{img}"
            elif "localhost" in img or "127.0.0.1" in img:
                if "/uploads/" in img:
                    _, tail = img.split("/uploads/", 1)
                    meta["image"] = f"{base_url}/uploads/{tail}"

        props = meta.get("properties") or {}
        visual = props.get("visual") or {}
        preview = visual.get("preview_image")

        if isinstance(preview, str) and preview:
            if preview.startswith("/"):
                visual["preview_image"] = f"{base_url}{preview}"
            elif "localhost" in preview or "127.0.0.1" in preview:
                if "/uploads/" in preview:
                    _, tail = preview.split("/uploads/", 1)
                    visual["preview_image"] = f"{base_url}/uploads/{tail}"

            meta.setdefault("properties", {})
            meta["properties"]["visual"] = visual
    except Exception:
        pass

    # ------------------------------
    # Build safe filename + URI FIRST
    # ------------------------------
    ts = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    base_name = str(meta.get("name") or "nft").replace(" ", "_")[:32] or "nft"
    filename = f"{ts}_{base_name}.json"
    safe_name = "".join(ch if ch.isalnum() or ch in ("-", "_", ".") else "_" for ch in filename)

    uri = f"{APP_BASE_URL.rstrip('/')}/meta/dyn/{safe_name}"

    # ------------------------------
    # Create DNFT pass record
    # ------------------------------
    expires_at = (
        meta.get("properties", {})
            .get("lifecycle", {})
            .get("expires_at")
    )

    try:
        pass_id = create_pass(
            creator_address=user["address"],
            metadata_uri=uri,
            expires_at=expires_at,
            metadata_json=meta,
        )
    except Exception as e:
        return JSONResponse({"ok": False, "error": f"create_pass failed: {e}"}, status_code=500)

    # ============================================================
    # 🔐 SIGNED SHARE PAYLOAD
    # ============================================================
    from backend.app.services.share_signing import generate_public_id, sign_public_id
    public_id = generate_public_id(pass_id)
    sig = sign_public_id(public_id)

    from backend.app.services.dnft_store import set_public_share
    set_public_share(pass_id, public_id, sig)

    meta.setdefault("properties", {})
    meta["properties"]["qr_payload"] = {"t": "X", "v": 1, "id": public_id, "sig": sig}

    meta["properties"]["verification"] = {
        "pass_id": pass_id,
        "public_id": public_id,
        "verify_url": f"{APP_BASE_URL.rstrip('/')}/v/{pass_id}",
        "share_url": f"{APP_BASE_URL.rstrip('/')}/s/{public_id}",
    }

    # ------------------------------
    # Store metadata JSON locally
    # ------------------------------
    try:
        path = META_DIR / safe_name
        with path.open("w", encoding="utf-8") as f:
            json.dump(meta, f, ensure_ascii=False, indent=2)
    except Exception as e:
        return JSONResponse({"ok": False, "error": f"Could not store metadata: {e}"}, status_code=500)

    from backend.app.services.dnft_store import set_metadata_json
    set_metadata_json(pass_id, meta)

    # ------------------------------
    # Create XUMM NFT.Mint payload
    # ------------------------------
    try:
        dnft_flags = 0x00000008 | 0x00000010  # tfTransferable | tfMutable

        # ---------------------------------------
        # custom_meta stamping
        # ---------------------------------------
        props = meta.get("properties") if isinstance(meta.get("properties"), dict) else {}

        custom_meta = {
            "source": "xertify",
            "kind": "nft_mint",
            "pass_id": pass_id,
            "public_id": public_id,
            "vertical_id": vertical_id,
            "custody_mode": (payload_in.get("custody_mode") or props.get("custody", {}).get("mode") or "custodial"),
            "domain_id": props.get("domain_id") or "",
            "template_key": props.get("template_key") or "",
            "template_id": props.get("template_id") or "",
        }

        payload = create_nft_mint_payload(
            account=user["address"],
            uri=uri,
            flags=dnft_flags,
            transfer_fee=None,
            custom_meta=custom_meta,
        )

        from backend.app.services.dnft_store import store_xumm_uuid
        store_xumm_uuid(pass_id, payload["uuid"])

        out = {
            "ok": True,
            "uri": uri,
            "pass_id": pass_id,
            "public_id": public_id,
            "verify_url": f"{APP_BASE_URL.rstrip('/')}/v/{pass_id}",
            "share_url": f"{APP_BASE_URL.rstrip('/')}/s/{public_id}",
            "vertical_id": vertical_id,
        }
        out.update(payload)
        return JSONResponse(out, status_code=200)

    except XRPLActionError as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=400)
    except Exception as e:
        return JSONResponse({"ok": False, "error": f"Unexpected: {e}"}, status_code=500)





def _wallet_from_seed(seed: str):
    from xrpl.wallet import Wallet

    # Newer xrpl-py
    if hasattr(Wallet, "from_seed"):
        return Wallet.from_seed(seed)

    # Older xrpl-py variants
    try:
        return Wallet(seed=seed)
    except TypeError:
        return Wallet(seed)


def _normalise_media_urls(meta: dict) -> dict:
  """
  Normalise image + visual.preview_image the same way as in
  /studio/actions/nft/mint_from_designer, so we can reuse it per-row.
  Mutates and returns meta.
  """
  try:
    base_url = APP_BASE_URL.rstrip("/")

    img = meta.get("image")
    if isinstance(img, str) and img:
      if img.startswith("/"):
        meta["image"] = f"{base_url}{img}"
      elif "127.0.0.1" in img or "localhost" in img:
        if "/uploads/" in img:
          _, tail = img.split("/uploads/", 1)
          meta["image"] = f"{base_url}/uploads/{tail}"

    props = meta.get("properties") or {}
    visual = props.get("visual") or {}
    preview = visual.get("preview_image")
    if isinstance(preview, str) and preview:
      if preview.startswith("/"):
        visual["preview_image"] = f"{base_url}{preview}"
      elif "127.0.0.1" in preview or "localhost" in preview:
        if "/uploads/" in preview:
          _, tail = preview.split("/uploads/", 1)
          visual["preview_image"] = f"{base_url}/uploads/{tail}"
      if "properties" not in meta:
        meta["properties"] = {}
      meta["properties"]["visual"] = visual
  except Exception:
    # never hard-fail batch because of URL normalisation
    pass

  return meta


def _set_attr(attrs: list, trait_type: str, value: str | None):
  """
  Upsert a single attribute in the list.
  If value is falsy, do nothing (we don't remove existing attrs).
  """
  if not value:
    return
  for a in attrs:
    if a.get("trait_type") == trait_type:
      a["value"] = value
      return
  attrs.append({"trait_type": trait_type, "value": value})





def _set_or_replace_attr(attrs: list, trait_type: str, value: Any):
    """
    Upsert attribute even if empty string; for assignment we WANT to overwrite.
    """
    for a in attrs:
        if (a.get("trait_type") or "") == trait_type:
            a["value"] = value
            return
    attrs.append({"trait_type": trait_type, "value": value})




def _extract_uuid(payload: Any) -> str:
    try:
        if isinstance(payload, dict):
            return payload.get("uuid") or payload.get("uuidv4") or payload.get("payload_uuidv4") or ""
        # xumm SDK object
        return getattr(payload, "uuid", "") or getattr(payload, "uuidv4", "") or ""
    except Exception:
        return ""


































@router.post("/studio/actions/nft/mint_batch_from_designer")
async def action_nft_mint_batch_from_designer(request: Request):
    """
    Aligned with /studio/actions/nft/mint_from_designer:
      - Creates a pass record per row
      - Generates public share payload (public_id + sig)
      - Stamps qr_payload + verification block into metadata
      - Stores metadata JSON file + DB snapshot
      - Stores xumm uuid against the pass
      - Returns items list that UI can mint one-by-one

    PLUS (Phase C hardening server-side):
      - Validate domain_id/template_key exist
      - Validate template exists in registry + domain match
      - Validate required template fields exist
      - Reject unknown template_fields keys (when schema provides allowed keys)
    """

    user = request.session.get("user")
    if not user or not user.get("address"):
        return JSONResponse({"ok": False, "error": "Login required"}, status_code=401)

    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"ok": False, "error": "Invalid JSON"}, status_code=400)

    # ---- Accept both new + legacy key names (defensive) ----
    base_meta = body.get("base_metadata") or body.get("metadata")
    rows = body.get("rows")

    if not isinstance(base_meta, dict):
        return JSONResponse({"ok": False, "error": "base_metadata must be an object"}, status_code=400)
    if not isinstance(rows, list) or not rows:
        return JSONResponse({"ok": False, "error": "rows must be a non-empty array"}, status_code=400)
    if len(rows) > 100:
        return JSONResponse({"ok": False, "error": "Too many rows (max 100)."}, status_code=400)

    # ---------------------------------------
    # Custody mode (batch-level default)
    # ---------------------------------------
    custody_mode = (body.get("custody_mode") or "custodial").strip().lower()
    if custody_mode in ("self-held", "selfheld"):
        custody_mode = "self"
    if custody_mode not in ("custodial", "self"):
        custody_mode = "custodial"

    # ------------------------------
    # Vertical (engine pack) - batch
    # ------------------------------
    vertical_id = ((body.get("vertical_id") or body.get("vertical") or "").strip().lower())
    try:
        props0 = base_meta.get("properties") if isinstance(base_meta.get("properties"), dict) else {}
        vertical_id = vertical_id or ((props0.get("vertical") or props0.get("vertical_id") or "").strip().lower())
    except Exception:
        pass
    if vertical_id not in ("generic", "aviation", "pharma"):
        vertical_id = "generic"

    base_meta.setdefault("properties", {})
    if not isinstance(base_meta["properties"], dict):
        base_meta["properties"] = {}
    base_meta["properties"]["vertical"] = vertical_id

    # safe default
    if not base_meta.get("name"):
        base_meta["name"] = "Untitled NFT"

    # ============================================================
    # ✅ Phase C — Validate template truth once at batch-level
    # (row-level overrides still allowed; base_meta must be valid)
    # ============================================================
    try:
        _ = _validate_template_truth_or_400(meta=base_meta, vertical_id=vertical_id)
    except HTTPException as e:
        return JSONResponse({"ok": False, "error": e.detail}, status_code=e.status_code)

    # dNFT flags: Transferable + Mutable
    dnft_flags = 0x00000008 | 0x00000010

    ts_prefix = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    base_name = str(base_meta.get("name") or "nft").replace(" ", "_")[:32] or "nft"

    out_items: list[dict] = []

    # Import here so the file doesn’t require these at import-time in dev
    from backend.app.services.share_signing import generate_public_id, sign_public_id
    from backend.app.services.dnft_store import set_public_share, set_metadata_json
    from backend.app.services.dnft_store import store_xumm_uuid

    for idx, row in enumerate(rows):
        if not isinstance(row, dict):
            continue

        holder_name = (row.get("holder_name") or "").strip()
        if not holder_name:
            continue

        row_address = (row.get("address") or "").strip()
        access_level = (row.get("access_level") or "Standard").strip()
        valid_until = (row.get("valid_until") or "").strip()
        notes = (row.get("notes") or "").strip()

        # If batch is self-held, each row MUST provide an address
        if custody_mode == "self" and not row_address:
            return JSONResponse(
                {"ok": False, "error": f"Row {idx}: address required for self-held custody_mode"},
                status_code=400,
            )

        # “to” address is only meaningful for UI messaging (mint is by creator)
        dest_address = row_address or user["address"]

        # ========== CLONE BASE METADATA ==========
        meta_i = deepcopy(base_meta)

        # -------------------------
        # NAME: "Holder – Program"
        # -------------------------
        base_label = (base_meta.get("name") or base_name).strip()
        if " for " in base_label:
            _before, _after = base_label.split(" for ", 1)
            if _after.strip():
                base_label = _after.strip()
        meta_i["name"] = f"{holder_name} – {base_label}"

        # -------------------------
        # DESCRIPTION rewrite
        # -------------------------
        base_desc = (base_meta.get("description") or "").strip()
        if base_desc:
            if " – " in base_desc:
                _before_desc, _suffix = base_desc.split(" – ", 1)
                meta_i["description"] = f"{meta_i['name']} – {_suffix}"
            else:
                meta_i["description"] = base_desc

        # -------------------------
        # ATTRIBUTES (row-level)
        # -------------------------
        attrs = list(meta_i.get("attributes") or [])
        _set_attr(attrs, "holder_name", holder_name)
        _set_attr(attrs, "access_level", access_level)
        _set_attr(attrs, "valid_until", valid_until)
        _set_attr(attrs, "notes", notes)
        meta_i["attributes"] = attrs

        # -------------------------
        # PROPERTIES (vertical, lifecycle, custody, batch)
        # -------------------------
        props = meta_i.get("properties") if isinstance(meta_i.get("properties"), dict) else {}
        props["vertical"] = vertical_id

        # Ensure canonical ids present per row
        base_props = base_meta.get("properties") if isinstance(base_meta.get("properties"), dict) else {}
        if base_props.get("domain_id"):
            props["domain_id"] = base_props.get("domain_id")
        if base_props.get("template_key"):
            props["template_key"] = base_props.get("template_key")
        if base_props.get("template_id") and not props.get("template_id"):
            props["template_id"] = base_props.get("template_id")

        # Ensure PD gate present per row (if present on base)
        if isinstance(base_props.get("permissioned_domain_gate"), dict):
            props["permissioned_domain_gate"] = deepcopy(base_props.get("permissioned_domain_gate"))

        # lifecycle
        lifecycle = props.get("lifecycle") if isinstance(props.get("lifecycle"), dict) else {}
        if valid_until:
            lifecycle["expires_at"] = valid_until
        props["lifecycle"] = lifecycle

        # custody
        if custody_mode == "self":
            props["custody"] = {"mode": "self", "holder_address": dest_address}
        else:
            props["custody"] = {"mode": "custodial", "holder_address": None}

        # batch info
        batch_info = props.get("batch") if isinstance(props.get("batch"), dict) else {}
        batch_info.update({"index": idx, "holder_name": holder_name, "address": dest_address})
        props["batch"] = batch_info

        meta_i["properties"] = props

        # ============================================================
        # ✅ Phase C — Validate template truth per row (safe)
        # If your templates ever allow per-row template_fields overrides,
        # this catches missing required keys early.
        # ============================================================
        try:
            _ = _validate_template_truth_or_400(meta=meta_i, vertical_id=vertical_id)
        except HTTPException as e:
            return JSONResponse({"ok": False, "error": f"Row {idx}: {e.detail}"}, status_code=e.status_code)

        # -------------------------
        # PER-ROW IMAGE OVERRIDES
        # -------------------------
        row_image = (row.get("image_url") or row.get("image") or "").strip()
        row_preview = (row.get("preview_image_url") or "").strip()

        if row_image:
            meta_i["image"] = row_image

        if row_preview:
            visual = props.get("visual") if isinstance(props.get("visual"), dict) else {}
            visual["preview_image"] = row_preview
            props["visual"] = visual
            meta_i["properties"] = props

        # Normalize URLs
        meta_i = _normalise_media_urls(meta_i)

        # ------------------------------
        # Build safe filename + URI FIRST (per row)
        # ------------------------------
        try:
            filename = f"{ts_prefix}_{base_name}_{idx}.json"
            safe_name = "".join(ch if ch.isalnum() or ch in ("-", "_", ".") else "_" for ch in filename)
            uri = f"{APP_BASE_URL.rstrip('/')}/meta/dyn/{safe_name}"
        except Exception:
            return JSONResponse({"ok": False, "error": f"Row {idx}: failed to build uri"}, status_code=500)

        # ------------------------------
        # Create DNFT pass record (per row)
        # ------------------------------
        expires_at = (meta_i.get("properties", {}) or {}).get("lifecycle", {}).get("expires_at")
        try:
            pass_id = create_pass(
                creator_address=user["address"],
                metadata_uri=uri,
                expires_at=expires_at,
                metadata_json=meta_i,
            )
        except Exception as e:
            return JSONResponse({"ok": False, "error": f"Row {idx}: create_pass failed: {e}"}, status_code=500)

        # ------------------------------
        # Signed share payload (per row)
        # ------------------------------
        public_id = generate_public_id(pass_id)
        sig = sign_public_id(public_id)
        try:
            set_public_share(pass_id, public_id, sig)
        except Exception:
            pass

        meta_i.setdefault("properties", {})
        meta_i["properties"]["qr_payload"] = {"t": "X", "v": 1, "id": public_id, "sig": sig}
        meta_i["properties"]["verification"] = {
            "pass_id": pass_id,
            "public_id": public_id,
            "verify_url": f"{APP_BASE_URL.rstrip('/')}/v/{pass_id}",
            "share_url": f"{APP_BASE_URL.rstrip('/')}/s/{public_id}",
        }

        # ------------------------------
        # Store metadata JSON locally + DB snapshot
        # ------------------------------
        try:
            path = META_DIR / safe_name
            with path.open("w", encoding="utf-8") as f:
                json.dump(meta_i, f, ensure_ascii=False, indent=2)
        except Exception as e:
            return JSONResponse(
                {"ok": False, "error": f"Row {idx}: could not store metadata file: {e}"},
                status_code=500,
            )

        try:
            set_metadata_json(pass_id, meta_i)
        except Exception:
            pass

        # ------------------------------
        # Create Mint payload (per row)
        # ------------------------------
        try:
            props_i = meta_i.get("properties") if isinstance(meta_i.get("properties"), dict) else {}

            custom_meta = {
                "source": "xertify",
                "kind": "nft_mint_batch",
                "pass_id": pass_id,
                "public_id": public_id,
                "vertical_id": vertical_id,
                "custody_mode": custody_mode,
                "domain_id": props_i.get("domain_id") or "",
                "template_key": props_i.get("template_key") or "",
                "template_id": props_i.get("template_id") or "",
                "batch_index": idx,
            }

            payload = create_nft_mint_payload(
                account=user["address"],
                uri=uri,
                flags=dnft_flags,
                transfer_fee=None,
                custom_meta=custom_meta,
            )

            uuid = _extract_uuid(payload)
            if uuid:
                try:
                    store_xumm_uuid(pass_id, uuid)
                except Exception:
                    pass

            item = {
                "index": idx,
                "uri": uri,
                "to": dest_address,
                "pass_id": pass_id,
                "public_id": public_id,
                "verify_url": f"{APP_BASE_URL.rstrip('/')}/v/{pass_id}",
                "share_url": f"{APP_BASE_URL.rstrip('/')}/s/{public_id}",
                "vertical_id": vertical_id,
            }
            item.update(payload)
            out_items.append(item)

        except XRPLActionError as e:
            return JSONResponse({"ok": False, "error": f"Row {idx}: XRPL error: {e}"}, status_code=400)
        except Exception as e:
            return JSONResponse({"ok": False, "error": f"Row {idx}: Unexpected error: {e}"}, status_code=500)

    if not out_items:
        return JSONResponse({"ok": False, "error": "No valid rows to mint."}, status_code=400)

    return JSONResponse(
        {
            "ok": True,
            "vertical_id": vertical_id,
            "custody_mode": custody_mode,
            "items": out_items,
        },
        status_code=200,
    )









@router.post("/studio/actions/xrpl/iou_payment")
def action_iou_payment(
    request: Request,
    destination: str = Form(...),
    currency: str = Form(...),
    issuer: str = Form(...),
    value: str = Form(...),
    memo: str = Form("PROJXHUB IOU Pay"),
):
    user = request.session.get("user")
    if not user or not user.get("address"):
        return JSONResponse({"ok": False, "error": "Login required"}, status_code=401)
    try:
        payload = create_iou_payment_payload(
            user["address"], destination, currency, issuer, value, memo
        )
        return JSONResponse({"ok": True, **payload})
    except XRPLActionError as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=400)
    except Exception as e:
        return JSONResponse(
            {"ok": False, "error": f"Unexpected: {e}"},
            status_code=500,
        )


@router.post("/studio/actions/xrpl/escrow_create")
def action_escrow_create(
    request: Request,
    destination: str = Form(...),
    amount_drops: str = Form(...),
    cancel_after_unix: Optional[int] = Form(None),
):
    user = request.session.get("user")
    if not user or not user.get("address"):
        return JSONResponse({"ok": False, "error": "Login required"}, status_code=401)
    try:
        payload = create_escrow_create_payload(
            user["address"], destination, int(amount_drops), cancel_after_unix
        )
        return JSONResponse({"ok": True, **payload})
    except XRPLActionError as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=400)
    except Exception as e:
        return JSONResponse(
            {"ok": False, "error": f"Unexpected: {e}"},
            status_code=500,
        )


@router.post("/studio/actions/xrpl/accountset_flag")
def action_accountset_flag(
    request: Request,
    set_flag: int = Form(...),  # e.g., 8 for DefaultRipple
):
    user = request.session.get("user")
    if not user or not user.get("address"):
        return JSONResponse({"ok": False, "error": "Login required"}, status_code=401)
    try:
        payload = create_accountset_flag_payload(
            user["address"], set_flag
        )
        return JSONResponse({"ok": True, **payload})
    except XRPLActionError as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=400)
    except Exception as e:
        return JSONResponse(
            {"ok": False, "error": f"Unexpected: {e}"},
            status_code=500,
        )


@router.post("/studio/actions/xrpl/nft_burn")
def action_nft_burn(
    request: Request,
    nft_id: str = Form(...),
):
    user = request.session.get("user")
    if not user or not user.get("address"):
        return JSONResponse({"ok": False, "error": "Login required"}, status_code=401)
    try:
        payload = create_nft_burn_payload(user["address"], nft_id)
        return JSONResponse({"ok": True, **payload})
    except XRPLActionError as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=400)
    except Exception as e:
        return JSONResponse(
            {"ok": False, "error": f"Unexpected: {e}"},
            status_code=500,
        )


@router.post("/studio/actions/xrpl/nft_offer_create")
def action_nft_offer_create(
    request: Request,
    nft_id: str = Form(...),
    amount: str = Form(...),
    destination: str = Form(""),
    sell: str = Form("true"),  # "true" / "false" from frontend
):
    user = request.session.get("user")
    if not user or not user.get("address"):
        return JSONResponse({"ok": False, "error": "Login required"}, status_code=401)

    try:
        amt = int(amount)
        if amt < 0:
            raise XRPLActionError("Amount must be >= 0")

        # frontend sends "true"/"false"
        sell_bool = (sell or "true").lower() == "true"
        dest = destination.strip() or None

        payload = create_nft_offer_payload(
            account=user["address"],
            nft_id=nft_id,
            amount_drops=amt,
            destination=dest,
            sell=sell_bool,
        )
        return JSONResponse({"ok": True, **payload})
    except ValueError:
        return JSONResponse(
            {"ok": False, "error": "Amount must be an integer (drops)"},
            status_code=400,
        )
    except XRPLActionError as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=400)
    except Exception as e:
        return JSONResponse(
            {"ok": False, "error": f"Unexpected: {e}"},
            status_code=500,
        )


@router.post("/studio/actions/xrpl/nft_send")
def action_nft_send(
    request: Request,
    nft_id: str = Form(...),
    destination: str = Form(...),
):
    """
    Used by nftSendFromModal in canvas.html
    """
    user = request.session.get("user")
    if not user or not user.get("address"):
        return JSONResponse({"ok": False, "error": "Login required"}, status_code=401)

    try:
        dest = destination.strip()
        if not dest:
            raise XRPLActionError("Destination address is required")

        payload = create_nft_send_payload(
            account=user["address"],
            nft_id=nft_id,
            destination=dest,
        )
        return JSONResponse({"ok": True, **payload})
    except XRPLActionError as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=400)
    except Exception as e:
        return JSONResponse(
            {"ok": False, "error": f"Unexpected: {e}"},
            status_code=500,
        )


@router.post("/studio/actions/xrpl/nft_offer_cancel")
def action_nft_offer_cancel(
    request: Request,
    offer_id: str = Form(...),
):
    user = request.session.get("user")
    if not user or not user.get("address"):
        return JSONResponse({"ok": False, "error": "Login required"}, status_code=401)

    try:
        payload = create_nft_cancel_offer_payload(
            account=user["address"],
            offer_id=offer_id,
        )
        return JSONResponse({"ok": True, **payload})
    except XRPLActionError as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=400)
    except Exception as e:
        return JSONResponse(
            {"ok": False, "error": f"Unexpected: {e}"},
            status_code=500,
        )


@router.post("/studio/actions/xrpl/nft_offer_accept")
def action_nft_offer_accept(
    request: Request,
    offer_id: str = Form(...),
):
    user = request.session.get("user")
    if not user or not user.get("address"):
        return JSONResponse({"ok": False, "error": "Login required"}, status_code=401)

    try:
        payload = create_nft_accept_offer_payload(
            account=user["address"],
            offer_id=offer_id,
        )
        return JSONResponse({"ok": True, **payload})
    except XRPLActionError as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=400)
    except Exception as e:
        return JSONResponse(
            {"ok": False, "error": f"Unexpected: {e}"},
            status_code=500,
        )


# ---------- NFT DESIGNER: BATCH METADATA ZIP ----------
@router.post("/studio/actions/nft/batch_metadata")
async def nft_batch_metadata(request: Request):
    """
    Accepts JSON:
      {
        "base_file_name": "ticket",
        "items": [
          { "file_name": "ticket-alice.json", "metadata": {...} },
          ...
        ]
      }

    Returns: application/zip with all metadata files.
    """
    try:
        payload = await request.json()
    except Exception as e:
        return JSONResponse({"ok": False, "error": f"Invalid JSON body: {e}"}, status_code=400)

    base_file_name = (payload.get("base_file_name") or "nft").strip() or "nft"
    items = payload.get("items") or []

    if not isinstance(items, list) or not items:
        return JSONResponse({"ok": False, "error": "No items provided"}, status_code=400)

    # Build the zip in memory
    mem_file = io.BytesIO()
    with zipfile.ZipFile(mem_file, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for idx, item in enumerate(items, start=1):
            meta = item.get("metadata")
            if meta is None:
                continue
            # Decide filename
            fn = (item.get("file_name") or "").strip()
            if not fn:
                fn = f"{base_file_name}-{idx}.json"
            # Dump JSON
            try:
                data = json.dumps(meta, indent=2, ensure_ascii=False)
            except Exception:
                continue
            zf.writestr(fn, data)

    mem_file.seek(0)

    headers = {
        "Content-Disposition": f'attachment; filename="{base_file_name}-metadata.zip"'
    }
    return StreamingResponse(
        mem_file,
        media_type="application/zip",
        headers=headers,
    )


# ---------- NFT DESIGNER: BATCH FROM CSV ----------
@router.post("/studio/actions/nft/batch_csv_metadata")
async def nft_batch_from_csv(
    template: str = Form(...),
    file: UploadFile = File(...),
):
    """
    Accepts:
      - template: JSON string for a single NFT's metadata, with tokens like {holder_name}, {seat}, etc
      - file: CSV file with columns matching those tokens.

    Returns:
      - application/zip of JSON metadata files (one per CSV row).
    """
    import csv
    import io as _io
    import json as _json

    # Parse template
    try:
        base_meta = _json.loads(template)
    except Exception as e:
        return JSONResponse({"ok": False, "error": f"Invalid template JSON: {e}"}, status_code=400)

    # Read CSV
    try:
        raw = await file.read()
        text = raw.decode("utf-8", errors="ignore")
        reader = csv.DictReader(_io.StringIO(text))
        rows = list(reader)
    except Exception as e:
        return JSONResponse({"ok": False, "error": f"Could not read CSV: {e}"}, status_code=400)

    if not rows:
        return JSONResponse({"ok": False, "error": "CSV has no rows"}, status_code=400)

    # Helper: recursively format strings in metadata with row values
    def fill_tokens(obj, row):
        if isinstance(obj, str):
            # naive .format with row keys; missing keys stay as-is
            try:
                return obj.format(**row)
            except Exception:
                return obj
        if isinstance(obj, list):
            return [fill_tokens(x, row) for x in obj]
        if isinstance(obj, dict):
            return {k: fill_tokens(v, row) for k, v in obj.items()}
        return obj

    mem_file = _io.BytesIO()
    with zipfile.ZipFile(mem_file, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for idx, row in enumerate(rows, start=1):
            meta = fill_tokens(base_meta, row)

            file_name = row.get("file_name") or row.get("id") or row.get("holder_name") or f"nft-{idx}"
            safe_name = "".join(c for c in file_name if c.isalnum() or c in ("-", "_")).strip() or f"nft-{idx}"
            fn = f"{safe_name}.json"

            try:
                data = _json.dumps(meta, indent=2, ensure_ascii=False)
            except Exception:
                continue
            zf.writestr(fn, data)

    mem_file.seek(0)
    headers = {
        "Content-Disposition": 'attachment; filename="nft-metadata-batch.zip"'
    }
    return StreamingResponse(
        mem_file,
        media_type="application/zip",
        headers=headers,
    )


@router.get("/meta/dyn/{slug}")
def dynamic_metadata(slug: str):
    """
    Dynamic metadata view for passes.

    - Reads the base JSON file from META_DIR using `slug` (same safe_name we stored).
    - If `properties.lifecycle.expires_at` is in the past, we:
        * mark status as 'expired'
        * switch to a generic 'burnt/blank' visual
        * optionally strip sensitive attributes (holder name, notes)
    - Otherwise returns the metadata unchanged.
    """
    path = META_DIR / slug

    if not path.exists():
        # Fallback: 404 or a generic "unknown pass" JSON
        return JSONResponse(
            {"ok": False, "error": "Metadata not found"},
            status_code=404,
        )

    try:
        with path.open("r", encoding="utf-8") as f:
            meta = json.load(f)
    except Exception as e:
        return JSONResponse(
            {"ok": False, "error": f"Could not read metadata: {e}"},
            status_code=500,
        )
        # --------------------------------------------------
    # STEP 4: Reflect revoked state from DB dynamically
    # --------------------------------------------------
    try:
        verification = (meta.get("properties") or {}).get("verification") or {}
        pass_id = verification.get("pass_id")

        if pass_id:
            rec = get_pass(pass_id)
            if rec and rec.get("revoked_at"):
                return JSONResponse({
                    **meta,
                    "name": f"{meta.get('name', 'Pass')} (Revoked)",
                    "attributes": (meta.get("attributes") or []) + [
                        {"trait_type": "status", "value": "revoked"}
                    ],
                    "properties": {
                        **(meta.get("properties") or {}),
                        "visual": {
                            **((meta.get("properties") or {}).get("visual") or {}),
                            "visual_state": "revoked",
                            "show_qr": False,
                            "show_badge": False,
                            "bg_image_url": "https://placehold.co/600x350?text=Revoked",
                        },
                    },
                    "image": "https://placehold.co/600x350?text=Revoked",
                })
    except Exception:
        pass


    # Safely dig into lifecycle data
    props = meta.get("properties") or {}
    lifecycle = props.get("lifecycle") or {}

    expires_at_str = (lifecycle.get("expires_at") or lifecycle.get("valid_until") or "").strip()

    # If no expiry, just return as-is
    if not expires_at_str:
        return JSONResponse(meta)

    # Parse expiry string (we expect ISO-ish or datetime-local like 2025-07-02T02:00)
    expires_at = None
    try:
        # Handle "Z" suffix if present
        if expires_at_str.endswith("Z"):
            expires_at = datetime.fromisoformat(expires_at_str.replace("Z", "+00:00"))
        else:
            # Treat as naive local -> assume UTC for simplicity
            expires_at = datetime.fromisoformat(expires_at_str)
            if expires_at.tzinfo is None:
                expires_at = expires_at.replace(tzinfo=timezone.utc)
    except Exception:
        # If parsing fails, just return as-is
        return JSONResponse(meta)

    now = datetime.now(timezone.utc)

    # If not yet expired, return unmodified
    if expires_at > now:
        return JSONResponse(meta)

    # ---------- EXPIRED: blank / burnt view ----------
    # Mark lifecycle status
    lifecycle["status"] = "expired"
    props["lifecycle"] = lifecycle
    meta["properties"] = props

    # Optional: tag status in attributes
    attrs = meta.get("attributes") or []
    # Remove sensitive bits (holder_name / notes) if you want to blank them
    filtered_attrs = []
    for a in attrs:
        t = (a.get("trait_type") or "").lower()
        if t in ("holder_name", "notes"):
            continue
        filtered_attrs.append(a)
    filtered_attrs.append({"trait_type": "status", "value": "expired"})
    meta["attributes"] = filtered_attrs

    # Adjust the visual section
    visual = props.get("visual") or {}
    visual["visual_state"] = "burnt"
    visual["show_qr"] = False
    visual["show_badge"] = False

    # Set a generic "pass burnt" image.
    # TODO: replace this with your real asset URL (e.g. from /static or CDN).
    visual["bg_image_url"] = "https://placehold.co/600x350?text=Pass+Expired"

    props["visual"] = visual
    meta["properties"] = props

    # Optionally annotate the name so it's obvious
    base_name = meta.get("name") or "Pass"
    if "(Expired)" not in base_name:
        meta["name"] = f"{base_name} (Expired)"

    return JSONResponse(meta)

from backend.app.services.dnft_store import (
    get_pass,
    log_event,
    list_events,
    compute_verdict,
    increment_usage,
)







def _try_finalize_mint_from_xumm(pass_id: str, rec: dict) -> dict:
    """
    C1: If this pass has xumm_uuid but no nft_id yet, try to fetch the payload from Xumm
    and extract the minted NFTokenID. If found, attach_nft_id() + store_mint_tx_hash().
    Always returns an (optionally) updated rec dict.
    """
    try:
        if rec.get("nft_id"):
            return rec

        xumm_uuid = rec.get("xumm_uuid")
        if not xumm_uuid:
            return rec

        # Fetch payload details from Xumm
        from backend.app.services import xumm as xumm_svc
        sdk = xumm_svc.get_sdk()
        full = sdk.payload.get(xumm_uuid)

        # normalize to dict-ish
        if isinstance(full, dict):
            full_obj = full
            response = full_obj.get("response") or {}
        else:
            response = getattr(full, "response", None) or {}
            if not isinstance(response, dict):
                try:
                    response = dict(response)
                except Exception:
                    response = {}

        # Check if signed
        signed = None
        try:
            signed = (response.get("signed") if isinstance(response, dict) else None)
        except Exception:
            signed = None

        # Many SDK payloads put signed state elsewhere; safest is to just attempt extraction
        result = response.get("result") or {}
        meta = result.get("meta") or {}

        nft_id = (
            meta.get("NFTokenID")
            or meta.get("nftoken_id")
            or result.get("NFTokenID")
            or result.get("nftoken_id")
        )

        # tx hash if present
        txid = response.get("txid") or result.get("hash") or result.get("txid")

        if nft_id:
            attach_nft_id(pass_id, nft_id)
            if txid:
                try:
                    store_mint_tx_hash(pass_id, txid)
                except Exception:
                    pass

            # refresh record after update
            rec2 = get_pass(pass_id)
            return rec2 or rec

        return rec

    except Exception:
        # never fail verification page / actions because of sync
        return rec


@router.get("/studio/domains")
def studio_domains():
    """
    Domain Registry for sidebar + UI.
    Pure data. Does not touch mint/engine logic.
    """
    reg = get_registry()
    return {"ok": True, "registry": reg}


@router.get("/v/{pass_id}", response_class=HTMLResponse)
def verify_pass(request: Request, pass_id: str):
    user = request.session.get("user")
    viewer_address = user.get("address") if user else None

    rec = get_pass(pass_id)
    if not rec:
        return templates.TemplateResponse(
            "verify/not_found.html",
            {"request": request, "pass_id": pass_id},
            status_code=404,
        )

    # self-heal mint if needed
    rec = _try_finalize_mint_from_xumm(pass_id, rec)

    # ✅ Step 2: Canonical truth comes from dnft_external_facts table
    from backend.app.services.dnft_store import get_external_facts
    external_facts = get_external_facts(pass_id) or {}

    verdict = compute_verdict(rec, external_facts=external_facts)

    # Log scan
    log_event(
        pass_id=pass_id,
        event_type="scan",
        actor_address=viewer_address,
        meta={"ua": request.headers.get("user-agent"), "verdict": verdict.get("status")},
    )

    if verdict.get("ok"):
        increment_usage(pass_id, actor_address=viewer_address)

    is_creator = (viewer_address == rec.get("creator_address"))

    # ---- map DB record -> template vars your HTML expects ----
    pass_metadata = (
        rec.get("metadata_json")
        or rec.get("pass_metadata")
        or rec.get("metadata")
        or {}
    )

    events = list_events(pass_id) or []

    return templates.TemplateResponse(
        "verify/pass.html",
        {
            "request": request,

            # existing
            "pass": rec,
            "verdict": verdict,
            "is_creator": is_creator,
            "events": events,
            "user": user,

            # ✅ these fix the "—" everywhere
            "pass_id": pass_id,
            "pass_metadata": pass_metadata,
            "verdict_status": (verdict.get("status") or ""),
            "audit_events": events,

            # ✅ Step 2: pass external facts to template for supply-chain panel
            "external_facts": external_facts,

            # lifecycle fields (top-level vars in your template)
            "state": (rec.get("state") or rec.get("status") or ""),
            "valid_until": (rec.get("expires_at") or rec.get("valid_until") or ""),
            "owner_address": (rec.get("owner_address") or rec.get("owner") or ""),
            "creator_address": (rec.get("creator_address") or rec.get("creator") or ""),
            "issuer_address": (rec.get("issuer_address") or rec.get("issuer") or ""),
            "sequence": (rec.get("sequence") or ""),
            "updated_at": (rec.get("updated_at") or ""),
            "minted_at": (rec.get("minted_at") or ""),

            # ledger refs
            "nft_id": (rec.get("nft_id") or ""),
            "mint_tx_hash": (rec.get("mint_tx_hash") or rec.get("tx_hash") or ""),
            "xumm_uuid": (rec.get("xumm_uuid") or ""),

            # anchor fields (proof + pass table)
            "anchor_tx_hash": (rec.get("anchor_tx_hash") or rec.get("anchor_tx") or ""),
            "anchor_root_hash": (rec.get("anchor_root_hash") or ""),
            "anchored_at": (rec.get("anchored_at") or ""),
        },
    )


def _template_policy_from_registry(meta: dict) -> dict:
    """
    Template-only policy source of truth.

    Reads domain_id + template_key from metadata, finds template in registry,
    and returns a policy dict (or {}).

    Expected template object can expose policy/locks in ANY of these:
      - template["policy"]
      - template["locks"]
      - template["rules"]
    """
    if not isinstance(meta, dict):
        return {}

    ids = _extract_canonical_ids(meta)
    domain_id = (ids.get("domain_id") or "").strip() or None
    template_key = (ids.get("template_key") or "").strip() or None
    if not domain_id or not template_key:
        return {}

    reg = get_registry()
    _did, template_obj = _find_template_in_registry(reg, domain_id, template_key)
    if not isinstance(template_obj, dict):
        return {}

    # Normalize where policy might live
    for k in ("policy", "locks", "rules"):
        pol = template_obj.get(k)
        if isinstance(pol, dict):
            return pol

    return {}


def _policy_bool(policy: dict, key: str, default: bool) -> bool:
    v = (policy or {}).get(key, None)
    if v is None:
        return default
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        return v.strip().lower() in ("1", "true", "yes", "on", "allow", "allowed")
    if isinstance(v, (int, float)):
        return bool(v)
    return default


def _policy_list(policy: dict, key: str) -> list:
    v = (policy or {}).get(key, None)
    if v is None:
        return []
    if isinstance(v, list):
        return v
    if isinstance(v, str):
        # allow comma-separated strings
        return [x.strip() for x in v.split(",") if x.strip()]
    return []


def _enforce_template_action_lock_or_403(*, rec: dict, action: str, payload: dict | None = None) -> None:
    """
    Enforce template-defined locks at action-time.
    This is separate from authority (who), it’s about policy (what).

    IMPORTANT: This must NOT read any bridge flags (like product_lock).
    Template policy is the only truth source.
    """
    meta = rec.get("metadata_json") or {}
    if not isinstance(meta, dict):
        meta = {}

    policy = _template_policy_from_registry(meta)
    # If a template has no policy block, default to permissive (keeps backward compat).
    # You can flip this later to default-deny once all templates ship a policy.
    default_allow = True

    # Map action -> policy keys
    allow_map = {
        "REVOKE": "allow_revoke",
        "RESET": "allow_reset",
        "REASSIGN": "allow_reassign",
        "EXTEND_EXPIRY": "allow_extend",
        "ANCHOR": "allow_anchor",

        # supply-chain style actions
        "SC_CHECKPOINT": "allow_sc_checkpoint",
        "SC_RECALL": "allow_sc_recall",
        "SC_COLD_CHAIN_POLICY": "allow_sc_cold_chain_policy",
        "SC_CUSTODY": "allow_sc_custody",

        # aviation style actions
        "CUSTODY_CHECKPOINT": "allow_av_checkpoint",
        "USAGE_REPORTED": "allow_av_usage",
        "INSPECTED": "allow_av_inspection",

        # domain action
        "PERM_DOMAIN_SET": "allow_perm_domain_set",
    }

    key = allow_map.get(action)
    if key:
        if not _policy_bool(policy, key, default_allow):
            raise HTTPException(status_code=403, detail=f"Forbidden (template policy blocks {action})")

    # Enforce custody mode constraints if present (optional)
    allowed_custody = [x.lower() for x in _policy_list(policy, "allowed_custody_modes")]
    if allowed_custody:
        # Try to infer custody_mode from payload OR metadata snapshot
        cm = None
        if isinstance(payload, dict):
            cm = payload.get("custody_mode")
        if not cm:
            try:
                cm = (((meta.get("properties") or {}).get("custody") or {}).get("mode"))
            except Exception:
                cm = None
        cm = (cm or "").strip().lower()
        if cm and cm not in allowed_custody:
            raise HTTPException(status_code=403, detail=f"Forbidden (custody mode {cm} not allowed by template)")

    # Enforce expiry requirement for EXTEND (optional)
    if action == "EXTEND_EXPIRY":
        requires = _policy_bool(policy, "expiry_extend_requires_payment_proof", False)
        # You can wire this to your chosen behavior; for now we only enforce presence of expires_at.
        if requires and (not isinstance(payload, dict) or not str(payload.get("expires_at") or "").strip()):
            raise HTTPException(status_code=400, detail="expires_at required by template policy")



from fastapi import Body
from fastapi.responses import JSONResponse
from datetime import datetime, timezone

@router.post("/v/{pass_id}/supply_chain/checkpoint")
async def supply_chain_checkpoint(
    request: Request,
    pass_id: str,
    payload: dict = Body(default={}),
):
    user = request.session.get("user")
    viewer_address = user.get("address") if user else None

    rec = get_pass(pass_id)
    if not rec:
        return JSONResponse({"ok": False, "error": "Not found", "pass_id": pass_id}, status_code=404)

    # creator-only (matches your UI intent)
        # ✅ vertical safety check
    try:
        require_vertical(rec, "pharma")
    except HTTPException as e:
        return JSONResponse({"ok": False, "error": e.detail}, status_code=e.status_code)

    # ✅ authority check (single source of truth)
    try:
        require_authority(rec, viewer_address, "SC_CHECKPOINT")
    except HTTPException as e:
        return JSONResponse(
            {"ok": False, "error": e.detail, "debug": {"viewer_address": viewer_address}},
            status_code=e.status_code,
        )


    # ---- normalize payload (strict-ish) ----
    checkpoint_type = (payload.get("checkpoint_type") or "manual").strip()
    note = (payload.get("note") or "").strip() or None

    temp_c = payload.get("temp_c", None)
    if temp_c is not None:
        try:
            temp_c = float(temp_c)
        except Exception:
            return JSONResponse({"ok": False, "error": "temp_c must be a number"}, status_code=400)

    recall_status = (payload.get("recall_status") or "").strip().lower() or None

    recalled = payload.get("recalled", None)
    if recalled is not None:
        recalled = bool(recalled)

    cold_chain_ok = payload.get("cold_chain_ok", None)
    if cold_chain_ok is not None:
        cold_chain_ok = bool(cold_chain_ok)

    custodian = (payload.get("custodian") or "").strip() or None

    ts = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    # canonical facts snapshot we store (latest truth)
    facts = {
        "ts": ts,
        "checkpoint_type": checkpoint_type,
        "note": note,
        "temp_c": temp_c,
        "recall_status": recall_status,
        "recalled": recalled,
        "cold_chain_ok": cold_chain_ok,
        "custodian": custodian,
    }

    # remove nulls (keeps facts clean)
    facts = {k: v for k, v in facts.items() if v is not None}

    from backend.app.services.dnft_store import (
        set_external_facts,
        get_external_facts,
        log_event,
        list_events,
        get_latest_event_by_type,
        get_pass as get_pass_fresh,
    )

    # ✅ persist canonical truth
    set_external_facts(pass_id, facts)

    # ✅ Append-only audit event
    log_event(
        pass_id=pass_id,
        event_type="sc_checkpoint",
        actor_address=viewer_address,
        meta=facts,
    )

    # ✅ Canonical truth used for verdict (NOT events)
    external_facts = get_external_facts(pass_id) or {}
    verdict = compute_verdict(rec, external_facts=external_facts)

    # ✅ IMPORTANT: run auto-anchor inline so the trail updates immediately
    # NOTE: this can add 1–3s latency depending on XRPL submit_and_wait/validation.
    anchor_out = await maybe_request_anchor_for_pass(
        pass_id=pass_id,
        actor_address=viewer_address,
        reason="sc_checkpoint",
        force=False,
    )

    # ✅ Build fresh proof bundle NOW (no refresh needed)
    fresh = get_pass_fresh(pass_id) or rec
    events = list_events(pass_id) or []
    latest_cp = get_latest_event_by_type(pass_id, "sc_checkpoint") or None

    pass_metadata = (
        fresh.get("metadata_json")
        or fresh.get("pass_metadata")
        or fresh.get("metadata")
        or {}
    )

    bundle = {
        "ok": True,
        "type": "xertify_proof_bundle",
        "version": 1,
        "pass_id": pass_id,

        # identity
        "issuer_address": fresh.get("issuer_address") or fresh.get("issuer") or "",
        "creator_address": fresh.get("creator_address") or fresh.get("creator") or "",
        "owner_address": fresh.get("owner_address") or fresh.get("owner") or "",
        "viewer_address": viewer_address or "",

        # ledger refs
        "nft_id": fresh.get("nft_id") or "",
        "mint_tx_hash": fresh.get("mint_tx_hash") or fresh.get("tx_hash") or "",
        "metadata_uri": fresh.get("metadata_uri") or "",
        "xumm_uuid": fresh.get("xumm_uuid") or "",

        # state/verdict
        "state": (fresh.get("state") or fresh.get("status") or ""),
        "valid_until": (fresh.get("expires_at") or fresh.get("valid_until") or ""),
        "verdict": verdict,

        # debug
        "latest_checkpoint": latest_cp,
        "external_facts": external_facts or {},

        # audit trail
        "events": events,

        # snapshot + hash
        "metadata_snapshot": pass_metadata,
        "metadata_sha256": _stable_json_hash(pass_metadata),

        # anchoring
        "anchor": {
            "anchored": bool(fresh.get("anchor_tx_hash") or fresh.get("anchor_tx")),
            "tx_hash": fresh.get("anchor_tx_hash") or fresh.get("anchor_tx") or "",
            "root_hash": fresh.get("anchor_root_hash") or "",
            "anchored_at": fresh.get("anchored_at") or "",
        },

        # extra: include what the checkpoint saved + what anchoring returned
        "checkpoint_saved": facts,
        "anchor_result": anchor_out,
    }

    return JSONResponse(bundle, status_code=200)
















@router.post("/v/{pass_id}/revoke")
async def revoke_pass_action(request: Request, pass_id: str):
    user = request.session.get("user")
    if not user:
        return JSONResponse({"ok": False, "error": "Not logged in"}, status_code=401)

    rec = get_pass(pass_id)
    if not rec:
        return JSONResponse({"ok": False, "error": "Not found"}, status_code=404)

    viewer_address = user.get("address")

    # 🔐 single gate (creator override happens inside enforce_authority if enabled)
    try:
        require_authority(rec, viewer_address, "REVOKE")
            # ✅ Template policy lock
        _enforce_template_action_lock_or_403(rec=rec, action="REVOKE", payload=None)

    except HTTPException as e:
        return JSONResponse({"ok": False, "error": e.detail}, status_code=e.status_code)


    def _payload_to_dict(p):
        if isinstance(p, dict):
            return p
        for attr in ("to_dict", "dict"):
            if hasattr(p, attr):
                try:
                    out = getattr(p, attr)()
                    if isinstance(out, dict):
                        return out
                except Exception:
                    pass
        try:
            return dict(p)
        except Exception:
            pass
        try:
            return dict(getattr(p, "__dict__", {}) or {})
        except Exception:
            return {"_raw": str(p)}

    def _extract_uuid(d: dict) -> str:
        if not isinstance(d, dict):
            return ""
        return (
            d.get("uuid")
            or d.get("uuidv4")
            or d.get("payload_uuidv4")
            or (d.get("payload") or {}).get("uuidv4")
            or ""
        )

    nft_id = rec.get("nft_id")

    from backend.app.services.dnft_store import (
        store_action_uuid,
        revoke_pass,
        mark_action_requested,
    )

    # -------------------------
    # DB-only revoke (no nft_id)
    # -------------------------
    if not nft_id:
        revoke_pass(
            pass_id,
            actor_address=viewer_address,
            reason="revoked_by_creator_no_nft_id",
        )

        mark_action_requested(
            pass_id=pass_id,
            action_type="revoke",
            actor_address=viewer_address,
            xumm_uuid=None,
            target_state=None,
            uri=None,
            note="No nft_id yet; DB-only revoke",
        )

        anchor_out = None
        try:
            anchor_out = await maybe_request_anchor_for_pass(
                pass_id=pass_id,
                actor_address=viewer_address,
                reason="revoke",
                force=False,
            )
        except Exception as e:
            anchor_out = {"ok": False, "error": str(e)}

        bundle = build_proof_bundle(pass_id=pass_id, viewer_address=viewer_address)
        if not bundle:
            return JSONResponse({"ok": False, "error": "Not found", "pass_id": pass_id}, status_code=404)

        bundle["action_result"] = {
            "action": "revoke",
            "mode": "db_only",
            "warning": "No nft_id yet, revoked in DB only.",
            "anchor_result": anchor_out,
        }

        return JSONResponse(bundle, status_code=200)

    # -------------------------
    # XRPL revoke (nft_id exists)
    # -------------------------
    new_uri = f"{APP_BASE_URL.rstrip('/')}/meta/state/revoked/{pass_id}"

    try:
        xrpl_payload = create_nft_modify_payload(
            account=viewer_address,
            nft_id=nft_id,
            new_uri=new_uri,
            memo=f"XERTIFY: revoke {pass_id}",
        )

        xrpl_payload = _payload_to_dict(xrpl_payload)
        uuid = _extract_uuid(xrpl_payload)

        if not uuid:
            return JSONResponse(
                {"ok": False, "error": "XUMM payload created but uuid missing", "xrpl": xrpl_payload},
                status_code=500,
            )

        store_action_uuid(uuid, pass_id, "revoke")

        mark_action_requested(
            pass_id=pass_id,
            action_type="revoke",
            actor_address=viewer_address,
            xumm_uuid=uuid,
            target_state=None,
            uri=new_uri,
            note="XRPL revoke payload created",
        )

        anchor_out = None
        try:
            anchor_out = await maybe_request_anchor_for_pass(
                pass_id=pass_id,
                actor_address=viewer_address,
                reason="revoke",
                force=False,
            )
        except Exception as e:
            anchor_out = {"ok": False, "error": str(e)}

        bundle = build_proof_bundle(pass_id=pass_id, viewer_address=viewer_address)
        if not bundle:
            return JSONResponse({"ok": False, "error": "Not found", "pass_id": pass_id}, status_code=404)

        bundle["action_result"] = {
            "action": "revoke",
            "mode": "xrpl_payload",
            "uuid": uuid,
            "xrpl": xrpl_payload,
            "new_uri": new_uri,
            "anchor_result": anchor_out,
        }

        return JSONResponse(bundle, status_code=200)

    except XRPLActionError as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=400)
    except Exception as e:
        return JSONResponse({"ok": False, "error": f"Unexpected: {e}"}, status_code=500)


@router.post("/v/{pass_id}/reset")
async def reset_pass_action(request: Request, pass_id: str):
    user = request.session.get("user")
    if not user:
        return JSONResponse({"ok": False, "error": "Not logged in"}, status_code=401)

    rec = get_pass(pass_id)
    if not rec:
        return JSONResponse({"ok": False, "error": "Not found"}, status_code=404)

    viewer_address = user.get("address")

    # 🔐 single gate
    try:
        require_authority(rec, viewer_address, "RESET")
            # ✅ Template policy lock
        _enforce_template_action_lock_or_403(rec=rec, action="RESET", payload=None)

    except HTTPException as e:
        return JSONResponse({"ok": False, "error": e.detail}, status_code=e.status_code)


    # 🔐 single source of truth gate (no bypass)


    def _payload_to_dict(p):
        if isinstance(p, dict):
            return p
        for attr in ("to_dict", "dict"):
            if hasattr(p, attr):
                try:
                    out = getattr(p, attr)()
                    if isinstance(out, dict):
                        return out
                except Exception:
                    pass
        try:
            return dict(p)
        except Exception:
            pass
        try:
            return dict(getattr(p, "__dict__", {}) or {})
        except Exception:
            return {"_raw": str(p)}

    def _extract_uuid(d: dict) -> str:
        if not isinstance(d, dict):
            return ""
        return (
            d.get("uuid")
            or d.get("uuidv4")
            or d.get("payload_uuidv4")
            or (d.get("payload") or {}).get("uuidv4")
            or ""
        )

    nft_id = rec.get("nft_id")

    from backend.app.services.dnft_store import (
        store_action_uuid,
        reset_pass,
        mark_action_requested,
    )

    # -------------------------
    # DB-only reset (no nft_id)
    # -------------------------
    if not nft_id:
        reset_pass(pass_id, actor_address=viewer_address)

        mark_action_requested(
            pass_id=pass_id,
            action_type="reset",
            actor_address=viewer_address,
            xumm_uuid=None,
            target_state=None,
            uri=None,
            note="No nft_id yet; DB-only reset",
        )

        anchor_out = None
        try:
            anchor_out = await maybe_request_anchor_for_pass(
                pass_id=pass_id,
                actor_address=viewer_address,
                reason="reset",
                force=False,
            )
        except Exception as e:
            anchor_out = {"ok": False, "error": str(e)}

        bundle = build_proof_bundle(pass_id=pass_id, viewer_address=viewer_address)
        if not bundle:
            return JSONResponse({"ok": False, "error": "Not found", "pass_id": pass_id}, status_code=404)

        bundle["action_result"] = {
            "action": "reset",
            "mode": "db_only",
            "warning": "No nft_id yet, reset in DB only.",
            "anchor_result": anchor_out,
        }

        return JSONResponse(bundle, status_code=200)

    # -------------------------
    # XRPL reset (nft_id exists)
    # -------------------------
    new_uri = f"{APP_BASE_URL.rstrip('/')}/meta/state/ready/{pass_id}"

    try:
        xrpl_payload = create_nft_modify_payload(
            account=viewer_address,
            nft_id=nft_id,
            new_uri=new_uri,
            memo=f"XERTIFY: reset {pass_id}",
        )

        xrpl_payload = _payload_to_dict(xrpl_payload)
        uuid = _extract_uuid(xrpl_payload)

        if not uuid:
            return JSONResponse(
                {"ok": False, "error": "XUMM payload created but uuid missing", "xrpl": xrpl_payload},
                status_code=500,
            )

        store_action_uuid(uuid, pass_id, "reset")

        mark_action_requested(
            pass_id=pass_id,
            action_type="reset",
            actor_address=viewer_address,
            xumm_uuid=uuid,
            target_state=None,
            uri=new_uri,
            note="XRPL reset payload created",
        )

        anchor_out = None
        try:
            anchor_out = await maybe_request_anchor_for_pass(
                pass_id=pass_id,
                actor_address=viewer_address,
                reason="reset",
                force=False,
            )
        except Exception as e:
            anchor_out = {"ok": False, "error": str(e)}

        bundle = build_proof_bundle(pass_id=pass_id, viewer_address=viewer_address)
        if not bundle:
            return JSONResponse({"ok": False, "error": "Not found", "pass_id": pass_id}, status_code=404)

        bundle["action_result"] = {
            "action": "reset",
            "mode": "xrpl_payload",
            "uuid": uuid,
            "xrpl": xrpl_payload,
            "new_uri": new_uri,
            "anchor_result": anchor_out,
        }

        return JSONResponse(bundle, status_code=200)

    except XRPLActionError as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=400)
    except Exception as e:
        return JSONResponse({"ok": False, "error": f"Unexpected: {e}"}, status_code=500)




@router.post("/studio/actions/authority/grant")
async def studio_grant_role(req: RoleGrantRequest, user=Depends(get_current_user)):
    # Creator/issuer only (same “admin” posture you use elsewhere)
    require_creator_id(user)

    creator = user.get("address") or user.get("account") or user.get("wallet")
    if not creator:
        raise HTTPException(status_code=401, detail="Missing creator address")

    # Minimal hygiene
    if not req.actor_address or not req.role:
        raise HTTPException(status_code=400, detail="actor_address and role required")

    res = grant_role(
        vertical_id=req.vertical_id,
        actor_address=req.actor_address,
        role=req.role,
        granted_by=creator,
        scope=req.scope,
    )
    return res


@router.post("/studio/actions/authority/revoke")
async def studio_revoke_role(req: RoleRevokeRequest, user=Depends(get_current_user)):
    require_creator_id(user)

    creator = user.get("address") or user.get("account") or user.get("wallet")
    if not creator:
        raise HTTPException(status_code=401, detail="Missing creator address")

    res = revoke_role(
        vertical_id=req.vertical_id,
        actor_address=req.actor_address,
        role=req.role,
        revoked_by=creator,
    )
    return res


@router.get("/studio/actions/authority/me")
async def studio_my_roles(vertical_id: str = "aviation", user=Depends(get_current_user)):
    actor = user.get("address") or user.get("account") or user.get("wallet")
    if not actor:
        raise HTTPException(status_code=401, detail="Missing actor address")

    roles = list_roles_for_actor(vertical_id, actor) or []

    # "active" = not revoked
    active_roles = []
    for r in roles:
        # roles are dict rows in your current system
        if isinstance(r, dict):
            if r.get("revoked_at") is None:
                active_roles.append(r)
        else:
            # if it ever returns strings, treat as active
            active_roles.append(r)

    return {
        "ok": True,
        "vertical_id": vertical_id,
        "actor_address": actor,
        "active_roles": active_roles,   # ✅ what your UI/tests should look at
        "roles": roles,                 # ✅ keep history for audits/debugging
    }



@router.post("/v/{pass_id}/reassign")
async def reassign_pass_action(request: Request, pass_id: str):
    """
    XRPL-backed reassignment.
    Requires pass to be READY.
    UI submits JSON.
    """
    user = request.session.get("user")
    if not user or not user.get("address"):
        return JSONResponse({"ok": False, "error": "Not logged in"}, status_code=401)

    rec = get_pass(pass_id)
    if not rec:
        return JSONResponse({"ok": False, "error": "Not found"}, status_code=404)

    viewer_address = user["address"]

    # Must be minted
    nft_id = rec.get("nft_id")
    if not nft_id:
        return JSONResponse({"ok": False, "error": "NFT not minted yet"}, status_code=400)

    # ✅ IMPORTANT: accept READY from either state or status
    current_state = (rec.get("state") or rec.get("status") or "").strip().lower()
    if current_state != "ready":
        return JSONResponse(
            {
                "ok": False,
                "error": "Pass must be reset to READY before reassignment",
                "debug": {"state": rec.get("state"), "status": rec.get("status")},
            },
            status_code=400,
        )

    # ✅ Parse JSON FIRST
    try:
        body = await request.json()
        if not isinstance(body, dict):
            return JSONResponse({"ok": False, "error": "Invalid JSON (object expected)"}, status_code=400)
    except Exception:
        return JSONResponse({"ok": False, "error": "Invalid JSON"}, status_code=400)

    # 🔐 SINGLE GATE: authority + template policy lock (NO BYPASS)
    try:
        require_authority(rec, viewer_address, "REASSIGN")
        _enforce_template_action_lock_or_403(rec=rec, action="REASSIGN", payload=body)
    except HTTPException as e:
        return JSONResponse({"ok": False, "error": e.detail}, status_code=e.status_code)

    holder_name = (body.get("holder_name") or "").strip()
    destination = (body.get("destination") or "").strip()
    access_level = (body.get("access_level") or "").strip()
    valid_until = (body.get("valid_until") or "").strip()
    notes = (body.get("notes") or "").strip()

    if not holder_name:
        return JSONResponse({"ok": False, "error": "holder_name required"}, status_code=400)

    # ---------- Load metadata snapshot ----------
    meta = rec.get("metadata_json") or {}
    if not isinstance(meta, dict):
        meta = {}

    meta.setdefault("properties", {})
    meta["properties"].setdefault("lifecycle", {})
    meta.setdefault("attributes", [])

    # ----------------------------
    # Custody mode
    # ----------------------------
    custody_mode = (body.get("custody_mode") if isinstance(body, dict) else None)
    custody_mode = (custody_mode or "").strip().lower()

    meta["properties"].setdefault("custody", {})

    # Default inference
    if destination:
        meta["properties"]["custody"] = {"mode": "self", "holder_address": destination}
    else:
        meta["properties"]["custody"] = {"mode": "custodial", "holder_address": None}

    # If UI explicitly set mode, respect it
    if custody_mode in ("self", "self-held", "selfheld"):
        if not destination:
            return JSONResponse(
                {"ok": False, "error": "destination required for self-held"},
                status_code=400,
            )
        meta["properties"]["custody"]["mode"] = "self"
        meta["properties"]["custody"]["holder_address"] = destination

    if custody_mode in ("custodial", "custody"):
        meta["properties"]["custody"]["mode"] = "custodial"
        meta["properties"]["custody"]["holder_address"] = None

    attrs = list(meta.get("attributes") or [])

    def upsert(trait, value):
        for a in attrs:
            if a.get("trait_type") == trait:
                a["value"] = value
                return
        attrs.append({"trait_type": trait, "value": value})

    # ---------- Apply new owner data ----------
    upsert("holder_name", holder_name)
    if access_level:
        upsert("access_level", access_level)
    if notes:
        upsert("notes", notes)

    if valid_until:
        upsert("valid_until", valid_until)
        meta["properties"]["lifecycle"]["expires_at"] = valid_until

    upsert("status", "live")
    meta["attributes"] = attrs

    from backend.app.services.dnft_store import (
        log_event,
        store_action_uuid,
        mark_action_requested,
    )

    uri = rec.get("metadata_uri") or ""
    slug = None
    if "/meta/dyn/" in uri:
        slug = uri.split("/meta/dyn/", 1)[1].split("?", 1)[0]

    # ---------- XRPL: point NFT back to dynamic metadata ----------
    def _payload_to_dict(p):
        if isinstance(p, dict):
            return p
        for attr in ("to_dict", "dict"):
            if hasattr(p, attr):
                try:
                    out = getattr(p, attr)()
                    if isinstance(out, dict):
                        return out
                except Exception:
                    pass
        try:
            return dict(p)
        except Exception:
            pass
        try:
            return dict(getattr(p, "__dict__", {}) or {})
        except Exception:
            return {"_raw": str(p)}

    def _extract_uuid(d: dict) -> str:
        if not isinstance(d, dict):
            return ""
        return (
            d.get("uuid")
            or d.get("uuidv4")
            or d.get("payload_uuidv4")
            or (d.get("payload") or {}).get("uuidv4")
            or ""
        )

    try:
        xrpl_payload = create_nft_modify_payload(
            account=viewer_address,
            nft_id=nft_id,
            new_uri=rec.get("metadata_uri"),
            memo=f"XERTIFY: reassign {pass_id}",
        )

        xrpl_payload = _payload_to_dict(xrpl_payload)
        uuid = _extract_uuid(xrpl_payload)

        if not uuid:
            return JSONResponse(
                {"ok": False, "error": "XUMM payload created but uuid missing", "xrpl": xrpl_payload},
                status_code=500,
            )

        store_action_uuid(
            uuid,
            pass_id,
            "reassign",
            meta={
                "metadata_json": meta,
                "metadata_slug": slug,
                "destination": destination or None,
                "custody_mode": meta.get("properties", {}).get("custody", {}).get("mode"),
                "holder_name": holder_name,
                "valid_until": valid_until or None,
            },
        )

        mark_action_requested(
            pass_id=pass_id,
            action_type="reassign",
            actor_address=viewer_address,
            xumm_uuid=uuid,
            target_state=None,
            uri=rec.get("metadata_uri"),
            note="XRPL reassign payload created",
        )

        log_event(
            pass_id=pass_id,
            event_type="reassign_requested",
            actor_address=viewer_address,
            meta={
                "uuid": uuid,
                "destination": destination or None,
                "custody_mode": meta.get("properties", {}).get("custody", {}).get("mode"),
                "holder_name": holder_name,
                "valid_until": valid_until or None,
            },
        )

        anchor_out = None
        try:
            anchor_out = await maybe_request_anchor_for_pass(
                pass_id=pass_id,
                actor_address=viewer_address,
                reason="reassign",
                force=False,
            )
        except Exception as e:
            anchor_out = {"ok": False, "error": str(e)}

        bundle = build_proof_bundle(pass_id=pass_id, viewer_address=viewer_address)
        if not bundle:
            return JSONResponse({"ok": False, "error": "Not found", "pass_id": pass_id}, status_code=404)

        bundle["action_result"] = {
            "action": "reassign",
            "mode": "xrpl_payload",
            "uuid": uuid,
            "xrpl": xrpl_payload,
            "destination": destination or None,
            "custody_mode": meta.get("properties", {}).get("custody", {}).get("mode"),
            "holder_name": holder_name,
            "valid_until": valid_until or None,
            "anchor_result": anchor_out,
        }

        return JSONResponse(bundle, status_code=200)

    except XRPLActionError as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=400)
    except Exception as e:
        return JSONResponse({"ok": False, "error": f"Unexpected: {e}"}, status_code=500)


@router.post("/v/{pass_id}/assign")
async def assign_pass_action(request: Request, pass_id: str):
    """
    Reassign a pass to a new holder and make it LIVE again.
    Backend-only (no XRPL touch in v1).
    """
    user = request.session.get("user")
    if not user or not user.get("address"):
        return JSONResponse({"ok": False, "error": "Not logged in"}, status_code=401)

    viewer_address = user["address"]

    rec = get_pass(pass_id)
    if not rec:
        return JSONResponse({"ok": False, "error": "Not found"}, status_code=404)

    # ✅ Parse JSON FIRST
    try:
        body = await request.json()
        if not isinstance(body, dict):
            return JSONResponse({"ok": False, "error": "Invalid JSON (object expected)"}, status_code=400)
    except Exception:
        return JSONResponse({"ok": False, "error": "Invalid JSON"}, status_code=400)

    # 🔐 SINGLE GATE: authority + template policy lock (NO BYPASS)
    # creator-only for now (policy gate uses RESET role)
    try:
        require_authority(rec, viewer_address, "RESET")
        _enforce_template_action_lock_or_403(rec=rec, action="ASSIGN", payload=body)
    except HTTPException as e:
        return JSONResponse({"ok": False, "error": e.detail}, status_code=e.status_code)

    holder_name = (body.get("holder_name") or "").strip()
    if not holder_name:
        return JSONResponse({"ok": False, "error": "holder_name required"}, status_code=400)

    expires_at = (body.get("expires_at") or "").strip() or None
    notes = (body.get("notes") or "").strip() or None

    from backend.app.services.dnft_store import (
        set_metadata_json,
        log_event,
        reset_pass,
        set_state,
        extend_expiry,
        mark_action_requested,
    )

    # 1) DB: reset usage + clear revoked, and set LIVE state
    reset_pass(pass_id, actor_address=viewer_address)
    set_state(pass_id, "live", reason="assigned", actor_address=viewer_address)

    if expires_at:
        extend_expiry(pass_id, expires_at, actor_address=viewer_address)

    # ✅ Option B parity: mark requested + flip DB state immediately
    mark_action_requested(
        pass_id=pass_id,
        action_type="assign_db",
        actor_address=viewer_address,
        xumm_uuid=None,
        target_state=None,
        uri=rec.get("metadata_uri") or None,
        note="DB-only assign",
    )

    # 2) Load metadata snapshot (prefer DB copy)
    meta = rec.get("metadata_json")
    if not isinstance(meta, dict):
        meta = {}

    # Ensure structure
    meta.setdefault("properties", {})
    meta["properties"].setdefault("lifecycle", {})
    meta.setdefault("attributes", [])

    # 3) Update metadata fields for new owner
    attrs = list(meta.get("attributes") or [])
    _set_or_replace_attr(attrs, "holder_name", holder_name)
    if expires_at:
        _set_or_replace_attr(attrs, "valid_until", expires_at)
        meta["properties"]["lifecycle"]["expires_at"] = expires_at
    if notes:
        _set_or_replace_attr(attrs, "notes", notes)
    _set_or_replace_attr(attrs, "status", "live")
    meta["attributes"] = attrs

    # 4) Persist metadata snapshot in DB
    set_metadata_json(pass_id, meta)

    # 5) Persist metadata JSON file
    uri = rec.get("metadata_uri") or ""
    slug = None
    if "/meta/dyn/" in uri:
        slug = uri.split("/meta/dyn/", 1)[1].strip()
        if "?" in slug:
            slug = slug.split("?", 1)[0]

    if slug:
        try:
            path = META_DIR / slug
            with path.open("w", encoding="utf-8") as f:
                json.dump(meta, f, ensure_ascii=False, indent=2)
        except Exception as e:
            return JSONResponse(
                {"ok": False, "error": f"Could not store metadata file: {e}"},
                status_code=500,
            )

    # ✅ audit log (DB-only assign)
    log_event(
        pass_id=pass_id,
        event_type="assign_db",
        actor_address=viewer_address,
        meta={"holder_name": holder_name, "expires_at": expires_at, "notes": notes},
    )

    # ✅ AUTO-ANCHOR inline so UI updates immediately
    anchor_out = None
    try:
        anchor_out = await maybe_request_anchor_for_pass(
            pass_id=pass_id,
            actor_address=viewer_address,
            reason="assign_db",
            force=False,
        )
    except Exception as e:
        anchor_out = {"ok": False, "error": str(e)}

    # ✅ Return fresh bundle NOW (no refresh needed)
    bundle = build_proof_bundle(pass_id=pass_id, viewer_address=viewer_address)
    if not bundle:
        return JSONResponse({"ok": False, "error": "Not found", "pass_id": pass_id}, status_code=404)

    bundle["action_result"] = {
        "action": "assign_db",
        "mode": "db_only",
        "holder_name": holder_name,
        "expires_at": expires_at,
        "notes": notes,
        "anchor_result": anchor_out,
    }

    return JSONResponse(bundle, status_code=200)




@router.post("/v/{pass_id}/extend")
async def extend_pass_action(
    request: Request,
    pass_id: str,
    expires_at: str = Form(...),
):
    user = request.session.get("user")
    if not user or not user.get("address"):
        return JSONResponse({"ok": False, "error": "Not logged in"}, status_code=401)

    viewer_address = user["address"]

    rec = get_pass(pass_id)
    if not rec:
        return JSONResponse({"ok": False, "error": "Not found", "pass_id": pass_id}, status_code=404)

    # ✅ Normalize first (so lock sees canonical value)
    expires_at = (expires_at or "").strip()
    if not expires_at:
        return JSONResponse({"ok": False, "error": "expires_at required"}, status_code=400)

    # 🔐 SINGLE GATE: authority + template policy lock (NO BYPASS)
    try:
        require_authority(rec, viewer_address, "EXTEND_EXPIRY")
        _enforce_template_action_lock_or_403(
            rec=rec,
            action="EXTEND_EXPIRY",
            payload={"expires_at": expires_at, "custody_mode": None},
        )
    except HTTPException as e:
        return JSONResponse({"ok": False, "error": e.detail}, status_code=e.status_code)

    memo = f"XERTIFY extend {pass_id} expires_at={expires_at}"

    # pick a proof destination that is NOT the sender
    PROOF_DEST = (
        request.app.state.settings.EXTEND_PROOF_DESTINATION
        if hasattr(request.app.state, "settings")
        else None
    )

    import os
    PROOF_DEST = (PROOF_DEST or os.getenv("EXTEND_PROOF_DESTINATION", "")).strip()

    if not PROOF_DEST or PROOF_DEST == viewer_address:
        return JSONResponse(
            {"ok": False, "error": "Server EXTEND_PROOF_DESTINATION not set (must be a different XRPL address)."},
            status_code=500,
        )

    xrpl_payload = create_simple_xrp_payment_payload(
        account=viewer_address,
        destination=PROOF_DEST,   # ✅ NOT same as sender
        drops=1,
        memo=memo,
        webhook_url=f"{APP_BASE_URL.rstrip('/')}/studio/actions/xrpl/xumm/webhook",
    )

    def _payload_to_dict(p):
        if isinstance(p, dict):
            return p
        for attr in ("to_dict", "dict"):
            if hasattr(p, attr):
                try:
                    out = getattr(p, attr)()
                    if isinstance(out, dict):
                        return out
                except Exception:
                    pass
        try:
            return dict(p)
        except Exception:
            pass
        try:
            return dict(getattr(p, "__dict__", {}) or {})
        except Exception:
            return {"_raw": str(p)}

    def _extract_uuid(d: dict) -> str:
        if not isinstance(d, dict):
            return ""
        return (
            d.get("uuid")
            or d.get("uuidv4")
            or d.get("payload_uuidv4")
            or (d.get("payload") or {}).get("uuidv4")
            or ""
        )

    xrpl_payload = _payload_to_dict(xrpl_payload)
    uuid = _extract_uuid(xrpl_payload)

    if not uuid:
        return JSONResponse(
            {"ok": False, "error": "XUMM payload uuid missing", "xrpl": xrpl_payload},
            status_code=500,
        )

    from backend.app.services.dnft_store import (
        store_action_uuid,
        log_event,
        mark_action_requested,
    )

    store_action_uuid(uuid, pass_id, "extend", meta={"expires_at": expires_at})

    mark_action_requested(
        pass_id=pass_id,
        action_type="extend",
        actor_address=viewer_address,
        xumm_uuid=uuid,
        target_state=None,
        uri=rec.get("metadata_uri") or None,
        note=f"Extend requested expires_at={expires_at}",
    )

    log_event(
        pass_id=pass_id,
        event_type="extend_requested",
        actor_address=viewer_address,
        meta={"uuid": uuid, "expires_at": expires_at},
    )

    anchor_out = None
    try:
        anchor_out = await maybe_request_anchor_for_pass(
            pass_id=pass_id,
            actor_address=viewer_address,
            reason="extend",
            force=False,
        )
    except Exception as e:
        anchor_out = {"ok": False, "error": str(e)}

    bundle = build_proof_bundle(pass_id=pass_id, viewer_address=viewer_address)
    if not bundle:
        return JSONResponse({"ok": False, "error": "Not found", "pass_id": pass_id}, status_code=404)

    bundle["action_result"] = {
        "action": "extend",
        "mode": "xumm_payment_proof",
        "uuid": uuid,
        "expires_at": expires_at,
        "xrpl": xrpl_payload,
        "anchor_result": anchor_out,
    }

    return JSONResponse(bundle, status_code=200)



@router.post("/v/{pass_id}/supply_chain/recall")
async def supply_chain_recall(
    request: Request,
    pass_id: str,
):
    user = request.session.get("user")
    if not user or not user.get("address"):
        return JSONResponse({"ok": False, "error": "Not logged in"}, status_code=401)

    viewer_address = user["address"]

    rec = get_pass(pass_id)
    if not rec:
        return JSONResponse({"ok": False, "error": "Not found"}, status_code=404)

        # ✅ vertical safety check
    try:
        require_vertical(rec, "pharma")
    except HTTPException as e:
        return JSONResponse({"ok": False, "error": e.detail}, status_code=e.status_code)

   


    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"ok": False, "error": "Invalid JSON"}, status_code=400)
    
    try:
        require_authority(rec, viewer_address, "SC_RECALL")
        _enforce_template_action_lock_or_403(rec=rec, action="SC_RECALL", payload=body)
    except HTTPException as e:
        return JSONResponse(
            {"ok": False, "error": e.detail, "debug": {"viewer_address": viewer_address}},
            status_code=e.status_code,
        )


    recalled = bool(body.get("recalled"))
    reason_txt = (body.get("reason") or "").strip() or None
    recall_id = (body.get("recall_id") or "").strip() or None

    facts_update = {
        "recalled": recalled,
        "recall_reason": reason_txt,
        "recall_id": recall_id,
        "recall_ts": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }

    from backend.app.services.dnft_store import (
        set_external_facts,
        get_external_facts,
        log_event,
        mark_action_requested,
    )

    current = get_external_facts(pass_id) or {}
    merged = {**current, **facts_update}
    # remove nulls to keep canonical facts clean
    merged = {k: v for k, v in merged.items() if v is not None}

    set_external_facts(pass_id, merged)

    log_event(
        pass_id=pass_id,
        event_type="sc_recall" if recalled else "sc_recall_cleared",
        actor_address=viewer_address,
        meta=merged,
    )

    # ✅ Option B parity: record that a recall action happened (and intended effect)
    mark_action_requested(
        pass_id=pass_id,
        action_type="sc_recall" if recalled else "sc_recall_cleared",
        actor_address=viewer_address,
        xumm_uuid=None,
        target_state=None,
        uri=rec.get("metadata_uri") or None,
        note=("Recall set" if recalled else "Recall cleared"),
    )

    # ✅ AUTO-ANCHOR inline so the trail updates immediately
    # If recalled=True, treat as critical (force anchor)
    anchor_out = None
    try:
        anchor_out = await maybe_request_anchor_for_pass(
            pass_id=pass_id,
            actor_address=viewer_address,
            reason="sc_recall" if recalled else "sc_recall_cleared",
            force=bool(recalled),
        )
    except Exception as e:
        anchor_out = {"ok": False, "error": str(e)}

    # ✅ Return fresh proof bundle NOW (no refresh needed)
    bundle = build_proof_bundle(pass_id=pass_id, viewer_address=viewer_address)
    if not bundle:
        return JSONResponse({"ok": False, "error": "Not found", "pass_id": pass_id}, status_code=404)

    bundle["action_result"] = {
        "action": "sc_recall" if recalled else "sc_recall_cleared",
        "mode": "facts_update",
        "recalled": recalled,
        "recall_reason": reason_txt,
        "recall_id": recall_id,
        "external_facts": merged,
        "anchor_result": anchor_out,
    }

    return JSONResponse(bundle, status_code=200)












@router.post("/v/{pass_id}/supply_chain/cold_chain_policy")
async def supply_chain_cold_chain_policy(
    request: Request,
    pass_id: str,
):
    user = request.session.get("user")
    if not user or not user.get("address"):
        return JSONResponse({"ok": False, "error": "Not logged in"}, status_code=401)

    viewer_address = user["address"]

    rec = get_pass(pass_id)
    if not rec:
        return JSONResponse({"ok": False, "error": "Not found"}, status_code=404)

        # ✅ vertical safety check
    try:
        require_vertical(rec, "pharma")
    except HTTPException as e:
        return JSONResponse({"ok": False, "error": e.detail}, status_code=e.status_code)

    


    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"ok": False, "error": "Invalid JSON"}, status_code=400)
    
    try:
        require_authority(rec, viewer_address, "SC_COLD_CHAIN_POLICY")
        _enforce_template_action_lock_or_403(rec=rec, action="SC_COLD_CHAIN_POLICY", payload=body)
    except HTTPException as e:
        return JSONResponse(
            {"ok": False, "error": e.detail, "debug": {"viewer_address": viewer_address}},
            status_code=e.status_code,
        )


    required = bool(body.get("required", True))
    temp_min = body.get("temp_min")
    temp_max = body.get("temp_max")

    # Optional: validate numeric if provided
    if temp_min is not None:
        try:
            temp_min = float(temp_min)
        except Exception:
            return JSONResponse({"ok": False, "error": "temp_min must be a number"}, status_code=400)

    if temp_max is not None:
        try:
            temp_max = float(temp_max)
        except Exception:
            return JSONResponse({"ok": False, "error": "temp_max must be a number"}, status_code=400)

    if required and (temp_min is None or temp_max is None):
        return JSONResponse(
            {"ok": False, "error": "temp_min and temp_max required when required=true"},
            status_code=400,
        )

    facts_update = {
        "cold_chain_required": required,
        "temp_min": temp_min,
        "temp_max": temp_max,
        "cold_chain_policy_ts": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }

    from backend.app.services.dnft_store import (
        set_external_facts,
        get_external_facts,
        log_event,
        mark_action_requested,
    )

    current = get_external_facts(pass_id) or {}
    merged = {**current, **facts_update}
    # remove nulls (keeps canonical facts clean)
    merged = {k: v for k, v in merged.items() if v is not None}

    set_external_facts(pass_id, merged)

    log_event(
        pass_id=pass_id,
        event_type="sc_cold_chain_policy",
        actor_address=viewer_address,
        meta=merged,
    )

    # ✅ Option B parity: record action + intent
    mark_action_requested(
        pass_id=pass_id,
        action_type="sc_cold_chain_policy",
        actor_address=viewer_address,
        xumm_uuid=None,
        target_state=None,
        uri=rec.get("metadata_uri") or None,
        note=f"Cold-chain policy set required={required}",
    )

    # ✅ AUTO-ANCHOR inline so the trail updates immediately
    anchor_out = None
    try:
        anchor_out = await maybe_request_anchor_for_pass(
            pass_id=pass_id,
            actor_address=viewer_address,
            reason="sc_cold_chain_policy",
            force=False,
        )
    except Exception as e:
        anchor_out = {"ok": False, "error": str(e)}

    # ✅ Return fresh proof bundle NOW (no refresh needed)
    bundle = build_proof_bundle(pass_id=pass_id, viewer_address=viewer_address)
    if not bundle:
        return JSONResponse({"ok": False, "error": "Not found", "pass_id": pass_id}, status_code=404)

    bundle["action_result"] = {
        "action": "sc_cold_chain_policy",
        "mode": "facts_update",
        "external_facts": merged,
        "anchor_result": anchor_out,
    }

    return JSONResponse(bundle, status_code=200)





@router.post("/v/{pass_id}/supply_chain/custody")
async def supply_chain_custody(
    request: Request,
    pass_id: str,
):
    user = request.session.get("user")
    if not user or not user.get("address"):
        return JSONResponse({"ok": False, "error": "Not logged in"}, status_code=401)

    viewer_address = user["address"]

    rec = get_pass(pass_id)
    if not rec:
        return JSONResponse({"ok": False, "error": "Not found"}, status_code=404)

        # ✅ vertical safety check
    try:
        require_vertical(rec, "pharma")
    except HTTPException as e:
        return JSONResponse({"ok": False, "error": e.detail}, status_code=e.status_code)

    


    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"ok": False, "error": "Invalid JSON"}, status_code=400)
    

    try:
        require_authority(rec, viewer_address, "SC_CUSTODY")
        _enforce_template_action_lock_or_403(rec=rec, action="SC_CUSTODY", payload=body)
    except HTTPException as e:
        return JSONResponse(
            {"ok": False, "error": e.detail, "debug": {"viewer_address": viewer_address}},
            status_code=e.status_code,
        )


    custodian = (body.get("custodian") or "").strip() or None
    custodian_address = (body.get("custodian_address") or "").strip() or None
    notes = (body.get("notes") or "").strip() or None

    facts_update = {
        "custodian": custodian,
        "custodian_address": custodian_address,
        "custody_notes": notes,
        "custody_ts": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }

    from backend.app.services.dnft_store import (
        set_external_facts,
        get_external_facts,
        log_event,
        mark_action_requested,
    )

    current = get_external_facts(pass_id) or {}
    merged = {**current, **facts_update}
    # remove nulls (keeps canonical facts clean)
    merged = {k: v for k, v in merged.items() if v is not None}

    set_external_facts(pass_id, merged)

    log_event(
        pass_id=pass_id,
        event_type="sc_custody",
        actor_address=viewer_address,
        meta=merged,
    )

    # ✅ Option B parity: record action + intent
    mark_action_requested(
        pass_id=pass_id,
        action_type="sc_custody",
        actor_address=viewer_address,
        xumm_uuid=None,
        target_state=None,
        uri=rec.get("metadata_uri") or None,
        note=f"Custody updated custodian={custodian or ''}".strip(),
    )

    # ✅ AUTO-ANCHOR inline so the trail updates immediately
    anchor_out = None
    try:
        anchor_out = await maybe_request_anchor_for_pass(
            pass_id=pass_id,
            actor_address=viewer_address,
            reason="sc_custody",
            force=False,
        )
    except Exception as e:
        anchor_out = {"ok": False, "error": str(e)}

    # ✅ Return fresh proof bundle NOW (no refresh needed)
    bundle = build_proof_bundle(pass_id=pass_id, viewer_address=viewer_address)
    if not bundle:
        return JSONResponse({"ok": False, "error": "Not found", "pass_id": pass_id}, status_code=404)

    bundle["action_result"] = {
        "action": "sc_custody",
        "mode": "facts_update",
        "external_facts": merged,
        "anchor_result": anchor_out,
    }

    return JSONResponse(bundle, status_code=200)





@router.post("/v/{pass_id}/aviation/checkpoint")
async def aviation_checkpoint(
    request: Request,
    pass_id: str,
    payload: dict = Body(default={}),
):
    user = request.session.get("user")
    viewer_address = user.get("address") if user else None

    rec = get_pass(pass_id)
    if not rec:
        return JSONResponse({"ok": False, "error": "Not found", "pass_id": pass_id}, status_code=404)

    # ✅ vertical safety check
    try:
        require_vertical(rec, "aviation")
    except HTTPException as e:
        return JSONResponse({"ok": False, "error": e.detail}, status_code=e.status_code)

    try:
        require_authority(rec, viewer_address, "CUSTODY_CHECKPOINT")
        _enforce_template_action_lock_or_403(rec=rec, action="CUSTODY_CHECKPOINT", payload=payload)
    except HTTPException as e:
        return JSONResponse(
            {"ok": False, "error": e.detail, "debug": {"viewer_address": viewer_address}},
            status_code=e.status_code,
        )




    ts = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    event_type = (payload.get("event_type") or "checkpoint").strip()
    station = (payload.get("station") or "").strip() or None
    custodian = (payload.get("custodian") or "").strip() or None
    custodian_address = (payload.get("custodian_address") or "").strip() or None
    note = (payload.get("note") or "").strip() or None

    facts_update = {
        "ts": ts,
        "event_type": event_type,
        "station": station,
        "custodian": custodian,
        "custodian_address": custodian_address,
        "note": note,
        "aviation_last_checkpoint_ts": ts,
    }
    facts_update = {k: v for k, v in facts_update.items() if v is not None}

    from backend.app.services.dnft_store import (
        set_external_facts,
        get_external_facts,
        log_event,
        mark_action_requested,
    )

    current = get_external_facts(pass_id) or {}
    merged = {**current, **facts_update}
    set_external_facts(pass_id, merged)

    log_event(
        pass_id=pass_id,
        event_type="av_checkpoint",
        actor_address=viewer_address,
        meta=facts_update,
    )

    mark_action_requested(
    pass_id=pass_id,
    action_type="av_checkpoint",
    actor_address=viewer_address,
    xumm_uuid=None,
    target_state=None,
    uri=rec.get("metadata_uri") or None,
    note=f"station={station or ''} custodian={custodian or ''}".strip(),
)


    verdict = compute_verdict(rec, external_facts=merged)

    anchor_out = await maybe_request_anchor_for_pass(
        pass_id=pass_id,
        actor_address=viewer_address,
        reason="av_checkpoint",
        force=False,
    )

    bundle = build_proof_bundle(pass_id=pass_id, viewer_address=viewer_address)
    if not bundle:
        return JSONResponse({"ok": False, "error": "Could not build proof bundle"}, status_code=500)

    bundle["anchor_result"] = anchor_out
    bundle["checkpoint_saved"] = facts_update
    return JSONResponse(bundle, status_code=200)







@router.post("/v/{pass_id}/aviation/usage")
async def aviation_usage_update(
    request: Request,
    pass_id: str,
    payload: dict = Body(default={}),
):
    user = request.session.get("user")
    viewer_address = user.get("address") if user else None

    rec = get_pass(pass_id)
    if not rec:
        return JSONResponse({"ok": False, "error": "Not found", "pass_id": pass_id}, status_code=404)

    # ✅ vertical safety check
    try:
        require_vertical(rec, "aviation")
    except HTTPException as e:
        return JSONResponse({"ok": False, "error": e.detail}, status_code=e.status_code)

    # ✅ authority check (ONLY ONCE)
        # ✅ authority check (single source of truth)
    try:
        require_authority(rec, viewer_address, "USAGE_REPORTED")
        _enforce_template_action_lock_or_403(rec=rec, action="USAGE_REPORTED", payload=payload)
    except HTTPException as e:
        return JSONResponse(
            {"ok": False, "error": e.detail, "debug": {"viewer_address": viewer_address}},
            status_code=e.status_code,
        )




    ts = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    def _to_number(x):
        try:
            return float(x)
        except Exception:
            return None

    cycles = _to_number(payload.get("current_cycles"))
    hours = _to_number(payload.get("current_hours"))

    if cycles is None and hours is None:
        return JSONResponse({"ok": False, "error": "Provide current_cycles and/or current_hours"}, status_code=400)

    facts_update = {"ts": ts}
    if cycles is not None:
        facts_update["current_cycles"] = int(cycles)
    if hours is not None:
        facts_update["current_hours"] = float(hours)

    from backend.app.services.dnft_store import (
        set_external_facts,
        get_external_facts,
        log_event,
        mark_action_requested,
    )

    current = get_external_facts(pass_id) or {}
    merged = {**current, **facts_update}
    set_external_facts(pass_id, merged)

    log_event(
        pass_id=pass_id,
        event_type="av_usage",
        actor_address=viewer_address,
        meta=facts_update,
    )

    mark_action_requested(
    pass_id=pass_id,
    action_type="av_usage",
    actor_address=viewer_address,
    xumm_uuid=None,
    target_state=None,
    uri=rec.get("metadata_uri") or None,
    note=f"cycles={facts_update.get('current_cycles','')} hours={facts_update.get('current_hours','')}".strip(),
)


    verdict = compute_verdict(rec, external_facts=merged)

    anchor_out = await maybe_request_anchor_for_pass(
        pass_id=pass_id,
        actor_address=viewer_address,
        reason="av_usage",
        force=(verdict.get("status") in ("life_limit_exceeded", "not_airworthy", "incident_flagged")),
    )

    bundle = build_proof_bundle(pass_id=pass_id, viewer_address=viewer_address)
    bundle["anchor_result"] = anchor_out
    bundle["usage_saved"] = facts_update
    return JSONResponse(bundle, status_code=200)



@router.post("/v/{pass_id}/aviation/inspection")
async def aviation_inspection_update(
    request: Request,
    pass_id: str,
    payload: dict = Body(default={}),
):
    user = request.session.get("user")
    viewer_address = user.get("address") if user else None

    rec = get_pass(pass_id)
    if not rec:
        return JSONResponse({"ok": False, "error": "Not found", "pass_id": pass_id}, status_code=404)
    


        # ✅ vertical safety check
    try:
        require_vertical(rec, "aviation")
    except HTTPException as e:
        return JSONResponse({"ok": False, "error": e.detail}, status_code=e.status_code)


        # ✅ authority check (single source of truth)
    try:
        require_authority(rec, viewer_address, "INSPECTED")
        _enforce_template_action_lock_or_403(rec=rec, action="INSPECTED", payload=payload)
    except HTTPException as e:
        return JSONResponse(
            {"ok": False, "error": e.detail, "debug": {"viewer_address": viewer_address}},
            status_code=e.status_code,
        )



    ts = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    status = (payload.get("inspection_status") or "").strip().lower()
    if status not in ("pass", "fail"):
        return JSONResponse({"ok": False, "error": "inspection_status must be pass or fail"}, status_code=400)

    inspector = (payload.get("inspector") or "").strip() or None
    note = (payload.get("note") or "").strip() or None

    facts_update = {
        "last_inspection_ts": ts,
        "inspection_status": status,
        "inspection_inspector": inspector,
        "inspection_note": note,
    }
    facts_update = {k: v for k, v in facts_update.items() if v is not None}

    from backend.app.services.dnft_store import (
        set_external_facts,
        get_external_facts,
        log_event,
        mark_action_requested,
    )

    current = get_external_facts(pass_id) or {}
    merged = {**current, **facts_update}
    set_external_facts(pass_id, merged)

    log_event(
        pass_id=pass_id,
        event_type="av_inspection",
        actor_address=viewer_address,
        meta=facts_update,
    )
    mark_action_requested(
    pass_id=pass_id,
    action_type="av_inspection",
    actor_address=viewer_address,
    xumm_uuid=None,
    target_state=None,
    uri=rec.get("metadata_uri") or None,
    note=f"status={status} inspector={inspector or ''}".strip(),
)


    verdict = compute_verdict(rec, external_facts=merged)

    anchor_out = await maybe_request_anchor_for_pass(
        pass_id=pass_id,
        actor_address=viewer_address,
        reason="av_inspection",
        force=(status == "fail"),
    )

    bundle = build_proof_bundle(pass_id=pass_id, viewer_address=viewer_address)
    bundle["anchor_result"] = anchor_out
    bundle["inspection_saved"] = facts_update
    return JSONResponse(bundle, status_code=200)
















@router.get("/meta/state/ready/{pass_id}")
def meta_ready(pass_id: str):
    return JSONResponse({
        "name": "Ready Pass",
        "description": "Ready for reassignment.",
        "attributes": [{"trait_type": "status", "value": "ready"}],
        "properties": {
            "visual": {
                "visual_state": "ready",
                "show_qr": False,
                "show_badge": False,
                "bg_image_url": "https://placehold.co/600x350?text=Ready"
            }
        },
        "image": "https://placehold.co/600x350?text=Ready"
    })


@router.get("/meta/state/revoked/{pass_id}")
def meta_revoked(pass_id: str):
    return JSONResponse({
        "name": "Revoked Pass",
        "description": "This pass has been revoked.",
        "attributes": [{"trait_type": "status", "value": "revoked"}],
        "properties": {
            "visual": {
                "visual_state": "revoked",
                "show_qr": False,
                "show_badge": False,
                "bg_image_url": "https://placehold.co/600x350?text=Revoked"
            }
        },
        "image": "https://placehold.co/600x350?text=Revoked"
    })


def _stable_json_hash(obj) -> str:
    """Deterministic hash of a dict/list for audit."""
    try:
        raw = json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    except Exception:
        raw = str(obj)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()







def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

@router.get("/v/{pass_id}/proof.json")
def pass_proof_bundle(request: Request, pass_id: str):
    user = request.session.get("user")
    viewer_address = user.get("address") if user else None

    rec = get_pass(pass_id)
    if not rec:
        return JSONResponse({"ok": False, "error": "Not found", "pass_id": pass_id}, status_code=404)

    try:
        rec = _try_finalize_mint_from_xumm(pass_id, rec)
    except Exception:
        pass

    evaluated_at = _iso_now()
    bundle = build_proof_bundle(pass_id=pass_id, viewer_address=viewer_address, evaluated_at=evaluated_at)
    if not bundle:
        return JSONResponse({"ok": False, "error": "Not found", "pass_id": pass_id}, status_code=404)

    return JSONResponse(bundle, status_code=200)





@router.get("/spec/verticals")
def spec_verticals():
    from backend.app.services.verticals import VERTICAL_PACKS
    # light index for discovery (don’t dump everything)
    out = []
    for k, v in VERTICAL_PACKS.items():
        out.append({
            "id": v.get("id") or k,
            "name": v.get("name") or k,
            "version": v.get("version") or 1,
            "policy_id": (v.get("policy") or {}).get("id") or "",
            "event_vocab": v.get("event_vocab") or [],
        })
    return JSONResponse({"ok": True, "verticals": out})


@router.get("/spec/aviation")
def spec_aviation():
    from backend.app.services.verticals import get_vertical_pack
    pack = get_vertical_pack("aviation")
    return JSONResponse({"ok": True, "aviation": pack})




def build_proof_bundle(*, pass_id: str, viewer_address: str | None, evaluated_at: str | None = None):
    from backend.app.services.dnft_store import (
        get_pass,
        get_external_facts,
        list_events,
        get_latest_event_by_type,
        get_latest_state_proof,
        compute_anchor_root,
    )

    rec = get_pass(pass_id)
    if not rec:
        return None

    external_facts = get_external_facts(pass_id) or {}
    events = list_events(pass_id) or []
    latest_cp = get_latest_event_by_type(pass_id, "sc_checkpoint") or None

    pass_metadata = (
        rec.get("metadata_json")
        or rec.get("pass_metadata")
        or rec.get("metadata")
        or {}
    )

    from backend.app.services.verticals import get_policy_sha256, get_schema_obj, get_vertical_pack

    def _pass_vertical_id_from_meta(rec: Dict[str, Any]) -> str:
        meta = rec.get("metadata_json") or rec.get("metadata") or {}
        if isinstance(meta, dict):
            props = meta.get("properties") or {}
            if isinstance(props, dict):
                v = (props.get("vertical") or props.get("vertical_id") or "").strip().lower()
                if v:
                    return v
        return "generic"

    vertical_id = _pass_vertical_id_from_meta(rec)
    policy_sha256 = get_policy_sha256(vertical_id)
    vertical_pack = get_vertical_pack(vertical_id)
    vertical_schema = get_schema_obj(vertical_id)


    # ---- Proof chain summary (truth spine) ----
    latest_proof = get_latest_state_proof(pass_id) or {}
    root_info = compute_anchor_root(pass_id)

    root_hash, proof_count, proof_tip = "", 0, ""
    if isinstance(root_info, (tuple, list)):
        root_hash = (root_info[0] or "").strip() if len(root_info) >= 1 else ""
        proof_count = int(root_info[1] or 0) if len(root_info) >= 2 else 0
        proof_tip = (root_info[2] or "").strip() if len(root_info) >= 3 else ""
    elif isinstance(root_info, dict):
        root_hash = (root_info.get("root_hash") or root_info.get("anchor_root_hash") or "").strip()
        proof_count = int(root_info.get("count") or 0)
        proof_tip = (root_info.get("tip") or root_info.get("proof_tip") or "").strip()

    # ---- Deterministic evaluation time (passed in by route) ----
    # IMPORTANT: Do NOT set evaluated_at from "now" here, or determinism breaks.
    # If not provided, we fallback to latest event timestamp; final fallback empty string.
    if not evaluated_at:
        # try to derive from latest event for stability
        try:
            if events:
                # pick latest by occurred_at then id (string safe)
                def _key(e):
                    return (e.get("occurred_at") or "", e.get("id") or "")
                last = sorted(events, key=_key)[-1]
                evaluated_at = last.get("occurred_at") or ""
            else:
                evaluated_at = ""
        except Exception:
            evaluated_at = ""

    # ---- Verdict computation (pure) ----
    raw = compute_verdict(rec, external_facts=external_facts) or {}
    verdict = normalize_verdict(raw, evaluated_at)

    # Optional: keep legacy ts for backward compatibility
    if "ts" not in verdict:
        verdict["ts"] = evaluated_at

    # ---- Canonical URLs (do not guess routes) ----
    canonical = {
        "verdict_url": f"/v/{pass_id}/verdict",
        "proof_url": f"/v/{pass_id}/proof.json",
        "verify_url": f"/v/{pass_id}",
        "spec_url": "/spec/proof-bundle-v1",
    }

    # ---- Hash inputs (deterministic) ----
    metadata_sha256 = _sha256_hex(_stable_dumps(pass_metadata))
    external_facts_sha256 = _sha256_hex(_stable_dumps(external_facts))

    # Verdict inputs = reproducibility contract
    verdict_inputs = {
        "evaluated_at": evaluated_at or "",
        "state": (rec.get("state") or rec.get("status") or ""),
        "valid_until": (rec.get("expires_at") or rec.get("valid_until") or ""),
        "metadata_sha256": metadata_sha256,
        "external_facts_sha256": external_facts_sha256,
        "proof_tip_hash": proof_tip or "",
        "anchor_root_hash": root_hash or (rec.get("anchor_root_hash") or ""),
        "policy_sha256": policy_sha256,
        "vertical_id": vertical_id,

    }
    verdict_inputs_sha256 = _sha256_hex(_stable_dumps(verdict_inputs))

    # ---- Viewer context is NOT truth (excluded from canonical hash) ----
    viewer_ctx = {"viewer_address": viewer_address or ""}

    # ---- Canonical hashable payload (truth artifact) ----
    canonical_payload = {
        "type": "xertify_proof_bundle",
        "version": 1,
        "pass_id": pass_id,

        "issuer_address": rec.get("issuer_address") or rec.get("issuer") or "",
        "creator_address": rec.get("creator_address") or rec.get("creator") or "",
        "owner_address": rec.get("owner_address") or rec.get("owner") or "",

        "nft_id": rec.get("nft_id") or "",
        "mint_tx_hash": rec.get("mint_tx_hash") or rec.get("tx_hash") or "",
        "metadata_uri": rec.get("metadata_uri") or "",
        "xumm_uuid": rec.get("xumm_uuid") or "",

        "state": (rec.get("state") or rec.get("status") or ""),
        "valid_until": (rec.get("expires_at") or rec.get("valid_until") or ""),

        "verdict": verdict,
        "verdict_inputs": verdict_inputs,
        "verdict_inputs_sha256": verdict_inputs_sha256,

        "latest_checkpoint": latest_cp,
        "external_facts": external_facts,
        "events": events,

        "metadata_snapshot": pass_metadata,
        "metadata_sha256": metadata_sha256,

        "proof_chain": {
            "latest_proof_hash": latest_proof.get("proof_hash") or rec.get("proof_hash") or "",
            "latest_prev_hash": latest_proof.get("prev_hash") or "",
            "anchor_root_hash": root_hash or (rec.get("anchor_root_hash") or ""),
            "proof_count": proof_count,
            "proof_tip_hash": proof_tip or "",
        },

        "anchor": {
            "anchored": bool(rec.get("anchor_tx_hash") or rec.get("anchor_tx")),
            "tx_hash": rec.get("anchor_tx_hash") or rec.get("anchor_tx") or "",
            "root_hash": rec.get("anchor_root_hash") or root_hash or "",
            "anchored_at": rec.get("anchored_at") or "",
        },

        "canonical": canonical,

                "vertical": {
            "id": vertical_id,
            "name": vertical_pack.get("name") or "",
            "version": vertical_pack.get("version") or 1,
        },
        "policy": {
            "id": (vertical_pack.get("policy") or {}).get("id") or "",
            "sha256": policy_sha256,
        },
        "schema": {
            "id": f"{vertical_id}/v{vertical_pack.get('version') or 1}",
            "object": vertical_schema,  # included for auditors/partners
        },
    }

    bundle_sha256 = _sha256_hex(_stable_dumps(canonical_payload))

    # Final bundle response: canonical + hash + viewer context
    return {
        "ok": True,
        **canonical_payload,
        "bundle_sha256": bundle_sha256,
        "viewer": viewer_ctx,
    }











from backend.app.services.dnft_store import get_event

@router.get("/v/{pass_id}/events/{event_id}")
def get_pass_event(request: Request, pass_id: str, event_id: int):
    rec = get_pass(pass_id)
    if not rec:
        return JSONResponse({"ok": False, "error": "Not found"}, status_code=404)

    ev = get_event(pass_id, event_id)
    if not ev:
        return JSONResponse({"ok": False, "error": "Event not found"}, status_code=404)

    return JSONResponse({"ok": True, "event": ev}, status_code=200)


@router.get("/actions/xrpl/xumm/payload/{uuid}")
async def xumm_payload_status(uuid: str):
    """
    Frontend polling endpoint.
    IMPORTANT:
      - "signed" is NOT enough anymore.
      - We only consider the action "done" when webhook applied it:
          dnft_actions.meta_json.applied == true
    """
    try:
        from backend.app.services.dnft_store import get_action_by_uuid

        act = get_action_by_uuid(uuid)
        if not act:
            return {"ok": True, "found": False, "uuid": uuid}

        meta = act.get("meta") or {}
        applied = bool(meta.get("applied"))

        return {
            "ok": True,
            "found": True,
            "uuid": uuid,
            "pass_id": act.get("pass_id"),
            "action_type": act.get("action_type"),
            "signed": bool(act.get("signed")),
            "txid": act.get("txid"),
            "signed_at": act.get("signed_at"),

            # ✅ webhook-applied signal (this is what step 8 needs)
            "applied": applied,
            "applied_at": meta.get("applied_at"),
            "applied_action": meta.get("applied_action"),
            "applied_txid": meta.get("applied_txid"),
        }

    except Exception as e:
        return {"ok": True, "found": False, "uuid": uuid, "error": str(e)}




@router.post("/v/{pass_id}/permissioned_domain/create")
async def create_permissioned_domain_action(request: Request, pass_id: str):
    user = request.session.get("user")
    if not user or not user.get("address"):
        return JSONResponse({"ok": False, "error": "Not logged in"}, status_code=401)

    viewer_address = user["address"]

    rec = get_pass(pass_id)
    if not rec:
        return JSONResponse({"ok": False, "error": "Not found"}, status_code=404)

   


    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"ok": False, "error": "Invalid JSON"}, status_code=400)
    
    try:
        require_authority(rec, viewer_address, "PERM_DOMAIN_SET")
        _enforce_template_action_lock_or_403(rec=rec, action="PERM_DOMAIN_SET", payload=body)
    except HTTPException as e:
        return JSONResponse({"ok": False, "error": e.detail}, status_code=e.status_code)


    domain_label = (body.get("domain_label") or "").strip()
    domain_purpose = (body.get("domain_purpose") or "").strip()  # supply_chain / regulated_rwa
    credential_issuer = (body.get("credential_issuer") or "").strip()
    credential_type = (body.get("credential_type") or "").strip()
    domain_mode = (body.get("domain_mode") or "create_new").strip()
    existing_domain_id = (body.get("existing_domain_id") or "").strip()

    if not domain_label:
        return JSONResponse({"ok": False, "error": "domain_label required"}, status_code=400)
    if domain_purpose not in ("supply_chain", "regulated_rwa"):
        return JSONResponse(
            {"ok": False, "error": "domain_purpose must be supply_chain or regulated_rwa"},
            status_code=400,
        )
    if not credential_issuer or not credential_type:
        return JSONResponse(
            {"ok": False, "error": "credential_issuer and credential_type required"},
            status_code=400,
        )
    if domain_mode == "use_existing" and not existing_domain_id:
        return JSONResponse(
            {"ok": False, "error": "existing_domain_id required when domain_mode=use_existing"},
            status_code=400,
        )

    # Build XRPL tx for Xumm
    tx = {
        "TransactionType": "PermissionedDomainSet",
        "Account": viewer_address,
        "AcceptedCredentials": [
            {
                "Credential": {
                    "Issuer": credential_issuer,
                    "CredentialType": credential_type,
                }
            }
        ],
    }
    if domain_mode == "use_existing":
        tx["DomainID"] = existing_domain_id

    # Create XUMM payload
    try:
        from backend.app.services import xumm as xumm_svc
        sdk = xumm_svc.get_sdk()

        created = sdk.payload.create(
            {
                "txjson": tx,
                "custom_meta": {
                    "instruction": f"XERTIFY: Permissioned Domain ({domain_purpose}) for pass {pass_id}",
                    "blob": {"pass_id": pass_id, "action": "perm_domain_set"},
                },
            }
        )

        # Extract uuid reliably
        uuid = None
        if isinstance(created, dict):
            uuid = created.get("uuid") or created.get("uuidv4")
        else:
            uuid = getattr(created, "uuid", None) or getattr(created, "uuidv4", None)

        if not uuid:
            return JSONResponse(
                {"ok": False, "error": "XUMM payload create failed (no uuid)"},
                status_code=500,
            )

        from backend.app.services.dnft_store import (
            store_action_uuid,
            log_event,
            mark_action_requested,
        )

        # Store the action WITH meta so webhook can finalize domain creation
        store_action_uuid(
            uuid,
            pass_id,
            "perm_domain_set",
            meta={
                "domain_label": domain_label,
                "domain_purpose": domain_purpose,
                "credential_issuer": credential_issuer,
                "credential_type": credential_type,
                "domain_mode": domain_mode,
                "existing_domain_id": existing_domain_id or None,
                "accepted_credentials": [
                    {"issuer": credential_issuer, "credential_type": credential_type}
                ],
            },
        )

        # ✅ Option B parity (shows in "actions" table immediately)
        mark_action_requested(
            pass_id=pass_id,
            action_type="perm_domain_set",
            actor_address=viewer_address,
            xumm_uuid=uuid,
            target_state=None,
            uri=rec.get("metadata_uri") or None,
            note=f"Permissioned domain set requested ({domain_purpose})",
        )

        # Audit log
        log_event(
            pass_id=pass_id,
            event_type="perm_domain_set_requested",
            actor_address=viewer_address,
            meta={
                "uuid": uuid,
                "domain_label": domain_label,
                "domain_purpose": domain_purpose,
                "domain_mode": domain_mode,
                "existing_domain_id": existing_domain_id or None,
                "credential_issuer": credential_issuer,
                "credential_type": credential_type,
            },
        )

        # ✅ Inline anchor so trail updates immediately (anchor_requested / confirmed appears)
        anchor_out = None
        try:
            anchor_out = await maybe_request_anchor_for_pass(
                pass_id=pass_id,
                actor_address=viewer_address,
                reason="perm_domain_set",
                force=False,
            )
        except Exception as e:
            anchor_out = {"ok": False, "error": str(e)}

        # ✅ Return fresh proof bundle NOW (no refresh needed)
        bundle = build_proof_bundle(pass_id=pass_id, viewer_address=viewer_address)
        if not bundle:
            return JSONResponse({"ok": False, "error": "Not found", "pass_id": pass_id}, status_code=404)

        bundle["action_result"] = {
            "action": "perm_domain_set",
            "mode": "xumm_payload",
            "uuid": uuid,
            "xrpl": created,
            "anchor_result": anchor_out,
        }

        return JSONResponse(bundle, status_code=200)

    except Exception as e:
        return JSONResponse({"ok": False, "error": f"Unexpected: {e}"}, status_code=500)





# ============================================================
# PHASE 3.1 — ANCHOR (REFactor for seamless auto-anchoring)
# Drop this into your studio.py (same router file).
#
# What this does:
# - Keeps /v/{pass_id}/anchor as a "manual" endpoint (for debugging/admin),
#   but moves all logic into a reusable helper.
# - You can call maybe_request_anchor_for_pass(...) automatically from:
#     • supply_chain_checkpoint
#     • supply_chain_recall
#     • supply_chain_custody
#     • AND inside revoke/reset/reassign/extend routes after you log/flip state
#
# ZERO new buttons needed — the backend can request anchoring silently.
# ============================================================



# ---------------------------
# Small helpers
# ---------------------------

def _utc_now_iso_z() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

def _stable_json_hash(obj) -> str:
    try:
        raw = json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    except Exception:
        raw = str(obj)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()

def _build_anchor_tx_json(account: str, destination: str, memo_type: str, memo_data: str) -> dict:
    """
    Build a minimal XRPL Payment tx carrying a memo.

    NOTE:
    - Some tooling/wallets disallow Account == Destination. So we always allow a distinct destination.
    - Amount is 1 drop (tiny) just to get an on-ledger tx with the memo.
    """
    def _to_hex(s: str) -> str:
        return (s or "").encode("utf-8").hex()

    return {
        "TransactionType": "Payment",
        "Account": account,
        "Destination": destination,
        "Amount": "1",  # 1 drop
        "Memos": [
            {
                "Memo": {
                    "MemoType": _to_hex(memo_type),
                    "MemoData": _to_hex(memo_data),
                }
            }
        ],
    }








async def _submit_anchor_payment_server_signed(
    *,
    rpc_url: str,
    seed: str,
    destination: str,
    memo_type: str,
    memo_data: str,
) -> dict:
    """
    Server-signed anchor tx (Option A): sign + autofill + submit directly to XRPL (async).
    Compatible with xrpl-py versions where:
      - Wallet must be created via Wallet.from_seed(...)
      - safe_sign_and_autofill_transaction is NOT available in xrpl.asyncio.transaction
    """
    try:
        from xrpl.asyncio.clients import AsyncJsonRpcClient
        from xrpl.wallet import Wallet
        from xrpl.models.transactions import Payment
        from xrpl.models.transactions import Memo as XrplMemo
        from xrpl.asyncio.transaction import submit_and_wait

        client = AsyncJsonRpcClient(rpc_url)
        wallet = Wallet.from_seed(seed)

        try:
            tx = Payment(
                account=wallet.classic_address,
                amount="1",  # 1 drop
                destination=destination,
                memos=[
                    XrplMemo(
                        memo_type=memo_type.encode("utf-8").hex(),
                        memo_data=memo_data.encode("utf-8").hex(),
                    )
                ],
            )

            # submit_and_wait will autofill + sign (with provided wallet) + submit
            resp = await submit_and_wait(tx, client, wallet)

            out = resp.result if hasattr(resp, "result") else (resp or {})
            if not isinstance(out, dict):
                out = {"raw": out}

            # Common fields
            engine_result = out.get("engine_result")
            engine_result_message = out.get("engine_result_message")
            validated = bool(out.get("validated")) if "validated" in out else None

            # Hash may live in a few places depending on xrpl-py / rippled response
            txid = (
                (out.get("tx_json") or {}).get("hash")
                or out.get("hash")
                or ((out.get("result") or {}).get("tx_json") or {}).get("hash")
                or ""
            )

            ok = (engine_result == "tesSUCCESS") or (validated is True)

            return {
                "ok": bool(ok),
                "txid": txid,
                "engine_result": engine_result,
                "engine_result_message": engine_result_message,
                "validated": validated,
                "result": out,
            }

        finally:
            try:
                await client.close()
            except Exception:
                pass

    except Exception as e:
        return {"ok": False, "error": str(e)}

















async def maybe_request_anchor_for_pass(
    *,
    pass_id: str,
    actor_address: str,
    reason: str,
    force: bool = False,
) -> dict:
    """
    Auto-anchor helper:
      - Option A: server-signed anchor tx (no user signature)
      - Fallback: XUMM payload (user signature)

    Anchors when:
      - pharma critical verdict forces it
      - force=True
      - or reason is important AND root changed
    """
    import os

    from backend.app.services.dnft_store import (
        get_pass,
        get_external_facts,
        compute_anchor_root,
        append_state_proof,
        get_latest_state_proof,
        log_event,
        store_anchor_result,
        compute_identity_hash,
    )

    rec = get_pass(pass_id)
    if not rec:
        return {"ok": False, "error": "Not found"}

    external_facts = get_external_facts(pass_id) or {}

    verdict = compute_verdict(rec, external_facts=external_facts)
    v_status = (verdict.get("status") or "").strip().lower()

    pharma_force_statuses = {"recalled", "cold_chain_failed", "temp_breach"}
    is_pharma_critical = v_status in pharma_force_statuses

    important_reasons = {
        "revoke",
        "reset",
        "reassign",
        "extend",
        "assign_db",
        "perm_domain_set",
        "permissioned_domain",
        "sc_recall",
        "sc_checkpoint",
        "sc_custody",
        "sc_cold_chain_policy",
        "manual_anchor",
    }

    if not force and (reason not in important_reasons) and not is_pharma_critical:
        return {"ok": True, "skipped": True, "why": "reason_not_important"}

    # ---- Append proof entry (feeds Merkle root) ----
    now = _utc_now_iso_z()
    identity_hash = rec.get("identity_hash") or compute_identity_hash(
        rec.get("id") or pass_id,
        rec.get("creator_address") or "",
        rec.get("created_at") or now,
    )

    last_proof = get_latest_state_proof(pass_id) or {}
    prev_state = (last_proof.get("state") or rec.get("state") or rec.get("status") or "live")
    current_state = (rec.get("state") or rec.get("status") or "live")

    facts_hash = _stable_json_hash(external_facts)
    verdict_hash = _stable_json_hash(
        {"status": verdict.get("status"), "ok": verdict.get("ok"), "reason": verdict.get("reason")}
    )

    append_state_proof(
        pass_id=pass_id,
        identity_hash=identity_hash,
        state=current_state,
        prev_state=prev_state,
        actor_address=actor_address,
        reason=f"{reason}|verdict={v_status}|facts={facts_hash[:12]}|vh={verdict_hash[:12]}",
        created_at=now,
    )

    # ---- Compute anchor root (dict OR tuple) ----
    root_info = compute_anchor_root(pass_id)
    root_hash = ""
    proof_count = 0
    proof_tip = ""

    if isinstance(root_info, (tuple, list)):
        if len(root_info) >= 1:
            root_hash = (root_info[0] or "").strip()
        if len(root_info) >= 2:
            try:
                proof_count = int(root_info[1] or 0)
            except Exception:
                proof_count = 0
        if len(root_info) >= 3:
            proof_tip = (root_info[2] or "").strip()

    elif isinstance(root_info, dict):
        root_hash = (root_info.get("anchor_root_hash") or root_info.get("root_hash") or "").strip()
        try:
            proof_count = int(root_info.get("count") or root_info.get("proof_count") or 0)
        except Exception:
            proof_count = 0
        proof_tip = (root_info.get("proof_tip_hash") or root_info.get("proof_tip") or "").strip()

    if not root_hash:
        return {"ok": False, "error": "Failed to compute anchor root"}

    existing_root = (rec.get("anchor_root_hash") or "").strip()
    existing_tx = (rec.get("anchor_tx_hash") or rec.get("anchor_tx") or "").strip()

    if not force and existing_root and existing_root == root_hash and existing_tx:
        return {"ok": True, "skipped": True, "why": "already_anchored_same_root", "root_hash": root_hash}

    if not force and (not is_pharma_critical) and existing_root and existing_root == root_hash:
        return {"ok": True, "skipped": True, "why": "root_unchanged", "root_hash": root_hash}

    memo_type = "XERTIFY_PROOF_ROOT"
    memo_data = f"{pass_id}:{root_hash}:{proof_tip}:{proof_count}"

    # ============================================================
    # OPTION A: server-signed anchor tx (no user signature)
    # ============================================================
    anchor_seed = (os.getenv("ANCHOR_WALLET_SEED") or "").strip().strip('"').strip("'")
    xrpl_rpc_url = (os.getenv("XRPL_RPC_URL") or os.getenv("XRPL_JSON_RPC_URL") or "").strip()
    anchor_dest = (os.getenv("ANCHOR_PROOF_DESTINATION") or "").strip().strip('"').strip("'")

    if anchor_seed and xrpl_rpc_url and anchor_dest:
        try:
            # ✅ FIX: xrpl-py expects Wallet.from_seed(...)
            from xrpl.wallet import Wallet
            sender_addr = Wallet.from_seed(anchor_seed).classic_address

            if anchor_dest == sender_addr:
                return {"ok": False, "error": "ANCHOR_PROOF_DESTINATION must NOT equal the anchor wallet address."}

            submit_out = await _submit_anchor_payment_server_signed(
                rpc_url=xrpl_rpc_url,
                seed=anchor_seed,
                destination=anchor_dest,
                memo_type=memo_type,
                memo_data=memo_data,
            )

            if submit_out.get("ok") and (submit_out.get("txid") or "").strip():
                txid = (submit_out.get("txid") or "").strip()

                store_anchor_result(
                    pass_id=pass_id,
                    anchor_tx_hash=txid,
                    anchor_root_hash=root_hash,
                    anchored_at=None,
                )

                log_event(
                    pass_id=pass_id,
                    event_type="anchor_confirmed",
                    actor_address=actor_address,
                    meta={
                        "mode": "server_signed",
                        "reason": reason,
                        "txid": txid,
                        "root_hash": root_hash,
                        "proof_tip": proof_tip,
                        "proof_count": proof_count,
                        "verdict_status": v_status,
                        "engine_result": submit_out.get("engine_result"),
                        "engine_result_message": submit_out.get("engine_result_message"),
                        "validated": submit_out.get("validated"),
                    },
                )

                return {
                    "ok": True,
                    "anchored": True,
                    "mode": "server_signed",
                    "txid": txid,
                    "root_hash": root_hash,
                    "proof_tip": proof_tip,
                    "proof_count": proof_count,
                    "verdict_status": v_status,
                }

            # If it didn’t succeed, log why, then fall through to XUMM fallback
            log_event(
                pass_id=pass_id,
                event_type="anchor_server_signed_failed",
                actor_address=actor_address,
                meta={
                    "reason": reason,
                    "root_hash": root_hash,
                    "error": submit_out.get("error"),
                    "engine_result": submit_out.get("engine_result"),
                    "engine_result_message": submit_out.get("engine_result_message"),
                    "validated": submit_out.get("validated"),
                },
            )

        except Exception as e:
            log_event(
                pass_id=pass_id,
                event_type="anchor_server_signed_failed",
                actor_address=actor_address,
                meta={"reason": reason, "error": str(e), "root_hash": root_hash},
            )

    # ============================================================
    # FALLBACK: XUMM payload (requires user signature)
    # ============================================================
    try:
        from backend.app.services import xumm as xumm_svc
        from backend.app.services.dnft_store import store_action_uuid

        sdk = xumm_svc.get_sdk()

        xumm_dest = (os.getenv("ANCHOR_PROOF_DESTINATION") or "").strip().strip('"').strip("'")
        if not xumm_dest:
            xumm_dest = (rec.get("issuer_address") or rec.get("creator_address") or "").strip()

        if not xumm_dest or xumm_dest == actor_address:
            return {"ok": False, "error": "ANCHOR_PROOF_DESTINATION not set (must be a different XRPL address)."}

        txjson = _build_anchor_tx_json(
            account=actor_address,
            destination=xumm_dest,
            memo_type=memo_type,
            memo_data=memo_data,
        )

        created = sdk.payload.create(
            {
                "txjson": txjson,
                "custom_meta": {
                    "instruction": f"XERTIFY: Anchor proof root for pass {pass_id}",
                    "blob": {
                        "pass_id": pass_id,
                        "action": "anchor_proof",
                        "reason": reason,
                        "root_hash": root_hash,
                        "proof_tip": proof_tip,
                        "proof_count": proof_count,
                    },
                },
            }
        )

        uuid = None
        if isinstance(created, dict):
            uuid = created.get("uuid") or created.get("uuidv4")
        else:
            uuid = getattr(created, "uuid", None) or getattr(created, "uuidv4", None)

        if not uuid:
            return {"ok": False, "error": "XUMM payload create failed (no uuid)"}

        store_action_uuid(
            uuid,
            pass_id,
            "anchor_proof",
            meta={
                "reason": reason,
                "root_hash": root_hash,
                "proof_count": proof_count,
                "proof_tip": proof_tip,
                "verdict_status": v_status,
                "facts_hash": facts_hash,
                "verdict_hash": verdict_hash,
            },
        )

        log_event(
            pass_id=pass_id,
            event_type="anchor_requested",
            actor_address=actor_address,
            meta={
                "uuid": uuid,
                "reason": reason,
                "root_hash": root_hash,
                "proof_count": proof_count,
                "proof_tip": proof_tip,
                "verdict_status": v_status,
            },
        )

        return {
            "ok": True,
            "requested": True,
            "mode": "xumm",
            "uuid": uuid,
            "xrpl": created,
            "root_hash": root_hash,
            "proof_tip": proof_tip,
            "proof_count": proof_count,
            "verdict_status": v_status,
        }

    except Exception as e:
        return {"ok": False, "error": f"Unexpected: {e}"}




def _stable_dumps(obj: Any) -> str:
    """
    Deterministic JSON serialization (stable across machines).
    """
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)

def _sha256_hex(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


# ============================================================
# 3.1 Route (manual/admin/debug)
# Keep it, but thin wrapper around helper
# ============================================================

@router.post("/v/{pass_id}/anchor")
async def anchor_proof_root(request: Request, pass_id: str):
    """
    Manual/admin anchor trigger.
    In production, you won't expose this in UI — auto-calls will handle it.
    """
    user = request.session.get("user")
    if not user or not user.get("address"):
        return JSONResponse({"ok": False, "error": "Not logged in"}, status_code=401)

    rec = get_pass(pass_id)
    if not rec:
        return JSONResponse({"ok": False, "error": "Not found"}, status_code=404)

    viewer_address = user["address"]

    # 🔐 single gate (no bypass)
    try:
        require_authority(rec, viewer_address, "ANCHOR")
    except HTTPException as e:
        return JSONResponse({"ok": False, "error": e.detail}, status_code=e.status_code)


    out = await maybe_request_anchor_for_pass(
    pass_id=pass_id,
    actor_address=viewer_address,
    reason="manual_anchor",
    force=True,
)


    code = 200 if out.get("ok") else 500
    return JSONResponse(out, status_code=code)




def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

@router.get("/v/{pass_id}/verdict")
def pass_verdict(request: Request, pass_id: str):
    from backend.app.services.dnft_store import get_pass, get_external_facts

    rec = get_pass(pass_id)
    if not rec:
        return JSONResponse({"ok": False, "error": "Not found", "pass_id": pass_id}, status_code=404)

    external_facts = get_external_facts(pass_id) or {}

    # PURE: no self-heal, no writes, no finalize
    raw = compute_verdict(rec, external_facts=external_facts) or {}

    evaluated_at = _iso_now()
    verdict = normalize_verdict(raw, evaluated_at)

    out = {
        "ok": verdict["ok"],
        "type": "xertify_verdict",
        "version": 1,
        "pass_id": pass_id,
        "status": verdict["status"],
        "reason": verdict["reason"],
        "evaluated_at": verdict["evaluated_at"],
    }

    # Optional details (non-contractual)
    if isinstance(verdict, dict) and "details" in verdict:
        out["details"] = verdict["details"]

    return JSONResponse(out, status_code=200)









PROOF_BUNDLE_V1_SCHEMA = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$id": "xertify://schemas/proof-bundle/1.0",
    "title": "XERTIFY Proof Bundle",
    "type": "object",
    "required": [
        "ok",
        "type",
        "version",
        "pass_id",
        "state",
        "verdict",
        "events",
        "external_facts",
        "metadata_snapshot",
        "metadata_sha256",
        "proof_chain",
        "anchor",
        "canonical",
        "bundle_sha256",
        "viewer"
    ],
    "properties": {
        "ok": {"type": "boolean"},
        "type": {"type": "string", "const": "xertify_proof_bundle"},
        "version": {"type": "integer", "const": 1},
        "pass_id": {"type": "string"},

        "issuer_address": {"type": "string"},
        "creator_address": {"type": "string"},
        "owner_address": {"type": "string"},

        "nft_id": {"type": "string"},
        "mint_tx_hash": {"type": "string"},
        "metadata_uri": {"type": "string"},
        "xumm_uuid": {"type": "string"},

        "state": {"type": "string"},
        "valid_until": {"type": "string"},

        "verdict": {
            "type": "object",
            "required": ["ok", "status", "reason"],
            "properties": {
                "ok": {"type": "boolean"},
                "status": {"type": "string"},
                "reason": {"type": "string"},
                "ts": {"type": "string"}
            },
            "additionalProperties": True
        },

        "latest_checkpoint": {"type": ["object", "null"], "additionalProperties": True},
        "external_facts": {"type": "object", "additionalProperties": True},
        "events": {"type": "array", "items": {"type": "object", "additionalProperties": True}},

        "metadata_snapshot": {"type": "object", "additionalProperties": True},
        "metadata_sha256": {"type": "string"},

        "proof_chain": {
            "type": "object",
            "required": ["anchor_root_hash", "proof_count"],
            "properties": {
                "latest_proof_hash": {"type": "string"},
                "latest_prev_hash": {"type": "string"},
                "anchor_root_hash": {"type": "string"},
                "proof_count": {"type": "integer"},
                "proof_tip_hash": {"type": "string"},
            },
            "additionalProperties": False
        },

        "anchor": {
            "type": "object",
            "required": ["anchored", "tx_hash", "root_hash", "anchored_at"],
            "properties": {
                "anchored": {"type": "boolean"},
                "tx_hash": {"type": "string"},
                "root_hash": {"type": "string"},
                "anchored_at": {"type": "string"},
            },
            "additionalProperties": False
        },

        "canonical": {
            "type": "object",
            "required": ["verdict_url", "proof_url", "verify_url"],
            "properties": {
                "verdict_url": {"type": "string"},
                "proof_url": {"type": "string"},
                "verify_url": {"type": "string"},
            },
            "additionalProperties": False
        },

        "bundle_sha256": {"type": "string"},

        "viewer": {
            "type": "object",
            "required": ["viewer_address"],
            "properties": {"viewer_address": {"type": "string"}},
            "additionalProperties": False
        }
    },
    "additionalProperties": True
}

def _example_proof_bundle(pass_id: str = "EXAMPLE-001"):
    now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    example = {
        "ok": True,
        "type": "xertify_proof_bundle",
        "version": 1,
        "pass_id": pass_id,
        "issuer_address": "ORG:EXAMPLE",
        "creator_address": "ORG:EXAMPLE",
        "owner_address": "ADDR:OWNER",
        "nft_id": "",
        "mint_tx_hash": "",
        "metadata_uri": "",
        "xumm_uuid": "",
        "state": "active",
        "valid_until": "",
        "verdict": {"ok": True, "status": "ok", "reason": "No violations found.", "ts": now},
        "latest_checkpoint": None,
        "external_facts": {"sample_fact": True},
        "events": [
            {
                "id": "EVT-0001",
                "type": "ISSUED",
                "occurred_at": now,
                "actor": {"id": "ORG:EXAMPLE", "role": "issuer", "method": "signature"},
                "inputs": {},
                "result": {"status": "active", "notes": None}
            }
        ],
        "metadata_snapshot": {"name": "Example Passport"},
        "metadata_sha256": _sha256_hex(_stable_dumps({"name": "Example Passport"})),
        "proof_chain": {
            "latest_proof_hash": "",
            "latest_prev_hash": "",
            "anchor_root_hash": "",
            "proof_count": 0,
            "proof_tip_hash": ""
        },
        "anchor": {"anchored": False, "tx_hash": "", "root_hash": "", "anchored_at": ""},
        "canonical": {
            "verdict_url": f"/v/{pass_id}/verdict",
            "proof_url": f"/v/{pass_id}/proof.json",
            "verify_url": f"/v/{pass_id}",
        },
        "viewer": {"viewer_address": ""},
    }
    # bundle_sha256 is over canonical payload: mirror your build_proof_bundle canonical_payload logic
    canonical_payload = {k: v for k, v in example.items() if k not in {"ok", "bundle_sha256", "viewer"}}
    example["bundle_sha256"] = _sha256_hex(_stable_dumps(canonical_payload))
    return example


@router.get("/spec/proof-bundle-v1")
def spec_proof_bundle_v1(request: Request):
    """
    Canonical public contract for integrations.
    This is how you stop being 'an NFT thing' and become infrastructure.
    """
    return JSONResponse({
        "ok": True,
        "spec": {
            "name": "XERTIFY Proof Bundle",
            "version": "1.0",
            "schema": PROOF_BUNDLE_V1_SCHEMA,
            "example": _example_proof_bundle(),
            "invariants": [
                "bundle_sha256 is computed over the canonical payload (excluding viewer context).",
                "viewer context must never affect canonical truth or hashes.",
                "proof bundle must be reproducible given the same stored facts/events/policy inputs.",
                "verdict is a pure evaluation (no side-effects)."
            ],
            "vocabulary": {
                "object/subject": "Thing whose truth is tracked.",
                "event": "Only way truth changes.",
                "authority": "Who is allowed to emit events.",
                "policy": "Rules used to evaluate truth.",
                "verdict": "Machine-readable decision output.",
                "proof bundle": "Canonical truth artifact."
            }
        }
    }, status_code=200)














@router.get("/spec")
def spec_index(request: Request):
    return JSONResponse({
        "ok": True,
        "specs": {
            "proof_bundle_v1": "/spec/proof-bundle-v1"
        }
    }, status_code=200)



@router.get("/health/truth")
def health_truth(request: Request):
    """
    Truth engine invariants.
    This endpoint exists ONLY to prove determinism + purity.
    """
    test_pass_id = request.query_params.get("pass_id") or ""
    if not test_pass_id:
        return JSONResponse({
            "ok": False,
            "error": "pass_id query param required",
            "example": "/health/truth?pass_id=<PASS_ID>"
        }, status_code=400)

    # Use one deterministic evaluation time
    evaluated_at = _iso_now()

    bundle_a = build_proof_bundle(
        pass_id=test_pass_id,
        viewer_address="VIEWER_A",
        evaluated_at=evaluated_at
    )
    bundle_b = build_proof_bundle(
        pass_id=test_pass_id,
        viewer_address="VIEWER_B",
        evaluated_at=evaluated_at
    )

    if not bundle_a or not bundle_b:
        return JSONResponse(
            {"ok": False, "error": "Not found", "pass_id": test_pass_id},
            status_code=404
        )

    def _rehash(bundle: dict) -> str:
        canonical_payload = {
            k: v for k, v in bundle.items()
            if k not in {"ok", "bundle_sha256", "viewer"}
        }
        return _sha256_hex(_stable_dumps(canonical_payload))

    same_bundle_hash = (bundle_a.get("bundle_sha256") == bundle_b.get("bundle_sha256"))
    recomputed_a = _rehash(bundle_a)
    recomputed_b = _rehash(bundle_b)
    rehash_matches = (
        recomputed_a == bundle_a.get("bundle_sha256")
        and recomputed_b == bundle_b.get("bundle_sha256")
    )

    # Required truth fields
    missing = []
    if not (bundle_a.get("verdict") or {}).get("evaluated_at"):
        missing.append("verdict.evaluated_at")
    if "verdict_inputs" not in bundle_a:
        missing.append("verdict_inputs")
    if not bundle_a.get("verdict_inputs_sha256"):
        missing.append("verdict_inputs_sha256")

    ok = same_bundle_hash and rehash_matches and not missing

    return JSONResponse({
        "ok": ok,
        "pass_id": test_pass_id,
        "evaluated_at_used": evaluated_at,
        "determinism": {
            "viewer_independent_hash": same_bundle_hash,
            "rehash_matches_stored": rehash_matches,
            "bundle_sha256_a": bundle_a.get("bundle_sha256"),
            "bundle_sha256_b": bundle_b.get("bundle_sha256"),
            "recomputed_a": recomputed_a,
            "recomputed_b": recomputed_b,
        },
        "required_fields": {
            "missing": missing
        },
        "notes": [
            "viewer context must not affect bundle_sha256",
            "bundle_sha256 must match canonical re-hash",
            "verdict must include evaluated_at",
            "verdict_inputs + verdict_inputs_sha256 must exist"
        ]
    }, status_code=200 if ok else 500)



@router.get("/spec/verdict-v1")
def spec_verdict_v1(request: Request):
    return JSONResponse({
        "ok": True,
        "type": "xertify_verdict",
        "version": 1,
        "status_enum": sorted(list(CANONICAL_VERDICT_STATUSES)),
        "contract": {
            "ok": "boolean",
            "status": "string (enum)",
            "reason": "string",
            "evaluated_at": "ISO-8601"
        }
    }, status_code=200)




