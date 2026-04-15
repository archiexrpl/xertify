from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Dict, Optional

from .loader import load_vertical_from_dir
from .types import VerticalSpec


class VerticalRegistry:
    """
    Simple in-process registry.
    Verticals are pure data.
    No engine logic lives here.
    """

    def __init__(self, base_dir: Path):
        self.base_dir = base_dir
        self._by_id: Dict[str, VerticalSpec] = {}

    def load_all(self) -> None:
        if not self.base_dir.exists():
            return

        for child in self.base_dir.iterdir():
            if child.is_dir() and (child / "vertical.json").exists():
                spec = load_vertical_from_dir(child)
                self._by_id[spec.vertical_id] = spec

    # ---------- Core access ----------

    def get(self, vertical_id: str) -> Optional[VerticalSpec]:
        return self._by_id.get((vertical_id or "").strip())

    def all_ids(self):
        return sorted(self._by_id.keys())

    # ---------- Spec accessors (Day 4) ----------

    def get_pack(self, vertical_id: str) -> Dict:
        spec = self.get(vertical_id)
        if not spec:
            return {}
        return {
            "id": spec.vertical_id,
            "name": spec.name,
            "version": spec.version,
            "policy": {"id": f"{spec.vertical_id}/v1"},
        }

    def get_object_schema(self, vertical_id: str) -> Dict:
        spec = self.get(vertical_id)
        return spec.object_schema if spec else {}

    def get_events_schema(self, vertical_id: str) -> Dict:
        spec = self.get(vertical_id)
        return spec.events_schema if spec else {}

    def get_policies(self, vertical_id: str) -> Dict:
        spec = self.get(vertical_id)
        return spec.policies if spec else {}

    def get_policy_sha256(self, vertical_id: str) -> str:
        """
        Canonical policy hash (stable JSON dump → sha256)
        """
        spec = self.get(vertical_id)
        if not spec:
            return ""

        raw = json.dumps(spec.policies, sort_keys=True, separators=(",", ":")).encode()
        return hashlib.sha256(raw).hexdigest()
