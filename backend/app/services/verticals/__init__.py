from pathlib import Path
from .registry import VerticalRegistry

BASE_DIR = Path(__file__).parent / "packs"

_registry = VerticalRegistry(BASE_DIR)
_registry.load_all()

def get_vertical_pack(vertical_id: str):
    return _registry.get_pack(vertical_id)

def get_policy_sha256(vertical_id: str) -> str:
    return _registry.get_policy_sha256(vertical_id)

def get_schema_obj(vertical_id: str):
    return _registry.get_object_schema(vertical_id)

def get_events_schema_obj(vertical_id: str):
    return _registry.get_events_schema(vertical_id)

def get_policies_obj(vertical_id: str):
    return _registry.get_policies(vertical_id)
