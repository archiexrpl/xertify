# backend/app/services/verticals.py
from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, List, Optional
import hashlib
import json


def _stable_dumps(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _sha256_hex(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


# ============================================================
# Vertical Packs (schemas + policies + event vocab)
# Skins, not engines.
# ============================================================

VERTICAL_PACKS: Dict[str, Dict[str, Any]] = {
    "generic": {
        "id": "generic",
        "name": "Generic",
        "version": 1,
        "schema": {
            "type": "object",
            "required": [],
            "properties": {},
            "additionalProperties": True,
        },
        "event_vocab": [
            "ISSUED",
            "INSPECTED",
            "TRANSFERRED",
            "REVOKED",
            "RESET",
            "REASSIGNED",
            "EXTENDED",
            "ANCHOR",
        ],
        "policy": {
            "id": "generic/v1",
            "vertical": "generic",
            "version": 1,
            "rules": [
                # NOTE: your core engine already handles revoked/expired/usage/etc.
                # This pack exists to provide an explicit "policy object" + hash.
                {"if": "always", "status": "valid", "ok": True, "reason": "Valid"},
            ],
        },
    },

    # ========================================================
    # AVIATION (Flagship Spec) — Step 10 foundation
    # ========================================================
    "aviation": {
        "id": "aviation",
        "name": "Aviation",
        "version": 1,
        "schema": {
            "type": "object",
            "required": ["properties"],
            "properties": {
                "properties": {
                    "type": "object",
                    "required": ["aviation"],
                    "properties": {
                        "aviation": {
                            "type": "object",
                            "required": ["part_number", "serial_number"],
                            "properties": {
                                "part_number": {"type": "string"},
                                "serial_number": {"type": "string"},
                                "oem": {"type": "string"},
                                "mro": {"type": "string"},
                                "aircraft_tail": {"type": "string"},
                                "installed_at": {"type": "string"},  # ISO
                                "last_inspected_at": {"type": "string"},  # ISO
                                "inspection_interval_days": {"type": "number"},
                            },
                            "additionalProperties": True,
                        }
                    },
                    "additionalProperties": True,
                }
            },
            "additionalProperties": True,
        },
        "event_vocab": [
            "ISSUED",
            "INSTALL",
            "INSPECT",
            "REMOVE",
            "TRANSFERRED",
            "REVOKED",
            "MAINTENANCE_NOTE",
        ],
        "authority_roles": [
            "OEM",
            "MRO",
            "OPERATOR",
        ],
        # This policy is intentionally LIGHT — it declares aviation semantics
        # without changing engine logic. Engine remains generic.
        "policy": {
            "id": "aviation/v1",
            "vertical": "aviation",
            "version": 1,
            "defaults": {
                "inspection_interval_days": 365,
            },
            "rules": [
                # If last inspection exists and is too old -> maintenance_overdue
                # The engine will implement this by reading external_facts/metadata props.
                {"when": "inspection_overdue", "status": "maintenance_overdue", "ok": False, "reason": "Inspection window expired"},
                {"if": "otherwise", "status": "valid", "ok": True, "reason": "Valid"},
            ],
        },
    },
}


def get_vertical_pack(vertical_id: Optional[str]) -> Dict[str, Any]:
    vid = (vertical_id or "").strip().lower() or "generic"
    pack = VERTICAL_PACKS.get(vid) or VERTICAL_PACKS["generic"]
    return deepcopy(pack)


def get_policy_obj(vertical_id: Optional[str]) -> Dict[str, Any]:
    pack = get_vertical_pack(vertical_id)
    pol = pack.get("policy") or {}
    return deepcopy(pol)


def get_policy_sha256(vertical_id: Optional[str]) -> str:
    pol = get_policy_obj(vertical_id)
    return _sha256_hex(_stable_dumps(pol))


def get_schema_obj(vertical_id: Optional[str]) -> Dict[str, Any]:
    pack = get_vertical_pack(vertical_id)
    sch = pack.get("schema") or {}
    return deepcopy(sch)
