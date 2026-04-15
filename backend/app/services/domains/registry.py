from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

REGISTRY_PATH = Path(__file__).resolve().parent / "domain_registry.json"

class RegistryError(RuntimeError):
    pass

_cached: Optional[dict] = None

def _load_raw() -> dict:
    if not REGISTRY_PATH.exists():
        raise RegistryError(f"Domain registry missing: {REGISTRY_PATH}")
    try:
        return json.loads(REGISTRY_PATH.read_text(encoding="utf-8"))
    except Exception as e:
        raise RegistryError(f"Domain registry JSON parse error: {e}")

def _validate(reg: dict) -> None:
    if not isinstance(reg, dict):
        raise RegistryError("Registry must be a JSON object")
    if "domains" not in reg or not isinstance(reg["domains"], list):
        raise RegistryError("Registry must contain domains[]")

    seen_domains = set()
    seen_templates = set()

    for d in reg["domains"]:
        if not isinstance(d, dict):
            raise RegistryError("Each domain must be an object")
        domain_id = d.get("domain_id")
        if not domain_id or not isinstance(domain_id, str):
            raise RegistryError("Domain missing domain_id")
        if domain_id in seen_domains:
            raise RegistryError(f"Duplicate domain_id: {domain_id}")
        seen_domains.add(domain_id)

        templates = d.get("templates", [])
        if not isinstance(templates, list):
            raise RegistryError(f"Domain {domain_id} templates must be a list")

        for t in templates:
            if not isinstance(t, dict):
                raise RegistryError(f"Domain {domain_id} template must be an object")
            template_id = t.get("template_id")
            vertical_id = t.get("vertical_id")
            if not template_id or not isinstance(template_id, str):
                raise RegistryError(f"Domain {domain_id} has template missing template_id")
            if template_id in seen_templates:
                raise RegistryError(f"Duplicate template_id: {template_id}")
            seen_templates.add(template_id)

            if not vertical_id or not isinstance(vertical_id, str):
                raise RegistryError(f"Template {template_id} missing vertical_id")

def get_registry(force_reload: bool = False) -> dict:
    global _cached
    if _cached is None or force_reload:
        reg = _load_raw()
        _validate(reg)
        _cached = reg
    return _cached

def list_domains() -> List[dict]:
    reg = get_registry()
    return reg["domains"]

def find_template(template_id: str) -> Optional[Tuple[dict, dict]]:
    reg = get_registry()
    for d in reg["domains"]:
        for t in d.get("templates", []):
            if t.get("template_id") == template_id:
                return d, t
    return None
