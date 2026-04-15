from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
import json
from pathlib import Path

from backend.app.core.templating import get_templates


router = APIRouter()
templates = get_templates()

@router.get("/profile", response_class=HTMLResponse)
def profile_page(request: Request):
    user = request.session.get("user")
    return templates.TemplateResponse(
        "profile.html",
        {
            "request": request,
            "title": "Profile",
            "user": user,
            "profile": {"nickname": None, "bio": "", "website": "", "twitter": "", "discord": ""},
            "achievements": [],
        },
    )






@router.get("/xertify", response_class=HTMLResponse)
def xertify_page(request: Request):
    user = request.session.get("user")

    # ===============================
    # Load template registry (legacy)
    # ===============================
    HERE = Path(__file__).resolve()
    APP_DIR = HERE.parents[1]        # .../backend/app
    templates_json_path = APP_DIR / "templates" / "templates.json"

    try:
        template_registry = json.loads(templates_json_path.read_text(encoding="utf-8"))
    except Exception as e:
        print("[XERTIFY] templates.json load failed:", templates_json_path, "err:", e)
        template_registry = {}

    # ===============================
    # Load domain registry (new)
    # ===============================
    try:
        from backend.app.services.domains.registry import get_registry
        domain_registry = get_registry()
    except Exception as e:
        print("[XERTIFY] domain registry load failed:", "err:", e)
        domain_registry = {}

    return templates.TemplateResponse(
        "xertify.html",
        {
            "request": request,
            "title": "Xertify • DNFT Studio",
            "user": user,

            # legacy (keep for now)
            "template_registry": template_registry,

            # new (sidebar upgrade uses this)
            "domain_registry": domain_registry,
        }
    )




@router.get("/verify", response_class=HTMLResponse)
def verify_home(request: Request):
    user = request.session.get("user")  # ✅ keeps navbar logged-in state
    return templates.TemplateResponse(
        "verify/index.html",
        {
            "request": request,
            "user": user,
        },
    )



from fastapi.responses import RedirectResponse
from fastapi import HTTPException

@router.get("/s/{public_id}")
def share_redirect(public_id: str):
    from backend.app.services.dnft_store import get_pass_by_public_id
    p = get_pass_by_public_id(public_id)
    if not p:
        raise HTTPException(status_code=404, detail="Not found")
    return RedirectResponse(url=f"/v/{p['id']}", status_code=302)
