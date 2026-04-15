# backend/app/core/templating.py
from functools import lru_cache
from pathlib import Path
from fastapi.templating import Jinja2Templates

BASE_DIR = Path(__file__).resolve().parents[1]  # backend/app

@lru_cache
def get_templates() -> Jinja2Templates:
    templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

    # Safe nickname helper (store.py removed)
    def get_nickname(_addr: str):
        return None

    templates.env.globals["get_nickname"] = get_nickname

    return templates
