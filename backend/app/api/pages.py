from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

templates = Jinja2Templates(directory="backend/app/templates")
router = APIRouter()

@router.get("/build", response_class=HTMLResponse)
def build_page(request: Request):
    return templates.TemplateResponse("stub.html", {"request": request, "title": "Build", "label": "Build"})

@router.get("/learn", response_class=HTMLResponse)
def learn_page(request: Request):
    return templates.TemplateResponse("stub.html", {"request": request, "title": "Learn", "label": "Learn"})

@router.get("/trade", response_class=HTMLResponse)
def trade_page(request: Request):
    return templates.TemplateResponse("stub.html", {"request": request, "title": "Trade", "label": "Trade"})

@router.get("/predict", response_class=HTMLResponse)
def predict_page(request: Request):
    return templates.TemplateResponse("stub.html", {"request": request, "title": "Predict", "label": "Predict"})

@router.get("/marketplace", response_class=HTMLResponse)
def marketplace_page(request: Request):
    return templates.TemplateResponse("stub.html", {"request": request, "title": "Marketplace", "label": "Marketplace"})

@router.get("/profile", response_class=HTMLResponse)
def profile_page(request: Request):
    return templates.TemplateResponse("stub.html", {"request": request, "title": "Profile", "label": "Profile"})
