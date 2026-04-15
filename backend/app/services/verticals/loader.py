from __future__ import annotations
import json
from pathlib import Path
from typing import Any, Dict

from .types import VerticalSpec


def _read_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def load_vertical_from_dir(dir_path: Path) -> VerticalSpec:
    """
    Loads a vertical from:
      vertical.json
      object.schema.json
      events.schema.json
      policies.json
    """
    vertical_meta = _read_json(dir_path / "vertical.json")
    object_schema = _read_json(dir_path / "object.schema.json")
    events_schema = _read_json(dir_path / "events.schema.json")
    policies = _read_json(dir_path / "policies.json")

    return VerticalSpec(
        vertical_id=vertical_meta["vertical_id"],
        name=vertical_meta.get("name", vertical_meta["vertical_id"]),
        version=str(vertical_meta.get("version", "1.0.0")),
        object_schema=object_schema,
        events_schema=events_schema,
        policies=policies,
        labels=vertical_meta.get("labels", {}),
    )
