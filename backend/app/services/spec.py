# backend/app/services/spec.py

import json
from pathlib import Path

PACKS_DIR = Path(__file__).resolve().parent / "verticals" / "packs"

def load_authority_model(vertical_id: str) -> dict:
    """
    Loads authority.model.json for a vertical.
    Returns {} if none exists.
    """
    p = PACKS_DIR / vertical_id / "authority.model.json"
    if not p.exists():
        return {}
    return json.loads(p.read_text())
