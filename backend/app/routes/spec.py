from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

from fastapi import APIRouter, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse

from backend.app.services.verticals import (
    get_vertical_pack,
    get_policy_sha256,
    get_schema_obj,
    get_events_schema_obj,
    get_policies_obj,
)

router = APIRouter()

# ✅ Correct path:
# routes/spec.py -> backend/app/routes
# parents[1] == backend/app
# so packs live at: backend/app/services/verticals/packs
PACKS_DIR = Path(__file__).resolve().parents[1] / "services" / "verticals" / "packs"


def _list_proof_examples(vertical_id: str) -> List[Dict[str, str]]:
    vdir = PACKS_DIR / vertical_id
    if not vdir.exists():
        return []

    out = []
    for p in sorted(vdir.glob("proof_example*.json")):
        out.append({"file": p.name, "url": f"/spec/{vertical_id}/examples/{p.name}"})
    return out


def _offline_verify_steps(vertical_id: str) -> List[str]:
    return [
        "1) Fetch the proof bundle JSON (proof_url) and keep it as a file.",
        "2) Verify bundle_sha256: stable-dump canonical JSON and SHA-256 it. Must match bundle_sha256.",
        "3) Validate metadata_snapshot against the vertical object schema (schemas.object).",
        "4) Validate the events list against schemas.events (structure + allowed event types).",
        "5) Recompute verdict_inputs_sha256 from verdict_inputs and confirm it matches.",
        "6) Confirm policy_sha256 equals the SHA-256 of policies.json (from /spec/{vertical}.json).",
        "7) Optionally verify proof_chain linkage (latest_proof_hash -> prev_hash -> …).",
        "8) If anchors are enabled later: verify anchor_root_hash was committed on-chain.",
    ]


def _verdict_statuses(vertical_id: str) -> List[Dict[str, Any]]:
    if vertical_id == "aviation":
        return [
            {"status": "ok", "ok": True, "meaning": "Airworthy at evaluated_at."},
            {"status": "maintenance_overdue", "ok": False, "meaning": "Inspection window expired at evaluated_at."},
            {"status": "revoked", "ok": False, "meaning": "Explicitly revoked (hard fail)."},
            {"status": "expired", "ok": False, "meaning": "Lifecycle expired (hard fail)."},
            {"status": "not_found", "ok": False, "meaning": "No such object/pass id."},
        ]

    if vertical_id == "pharma":
        return [
            {"status": "ok", "ok": True, "meaning": "Compliant at evaluated_at."},
            {"status": "recalled", "ok": False, "meaning": "Batch is recalled (hard fail)."},
            {"status": "cold_chain_failed", "ok": False, "meaning": "Cold chain compliance failed (hard fail)."},
            {"status": "temp_breach", "ok": False, "meaning": "Temperature outside allowed min/max (hard fail)."},
            {"status": "revoked", "ok": False, "meaning": "Explicitly revoked (hard fail)."},
            {"status": "expired", "ok": False, "meaning": "Lifecycle expired (hard fail)."},
            {"status": "not_found", "ok": False, "meaning": "No such object/pass id."},
        ]

    return [
        {"status": "ok", "ok": True, "meaning": "Valid at evaluated_at."},
        {"status": "revoked", "ok": False, "meaning": "Revoked (hard fail)."},
        {"status": "expired", "ok": False, "meaning": "Expired (hard fail)."},
        {"status": "not_found", "ok": False, "meaning": "No such object/pass id."},
    ]


@router.get("/spec/{vertical_id}.json")
def spec_json(vertical_id: str):
    vertical_id = (vertical_id or "").strip().lower()
    if vertical_id not in ("generic", "aviation", "pharma"):
        raise HTTPException(status_code=404, detail="Unknown vertical")

    pack = get_vertical_pack(vertical_id) or {}
    policy_sha256 = get_policy_sha256(vertical_id) or ""
    obj_schema = get_schema_obj(vertical_id) or {}
    evt_schema = get_events_schema_obj(vertical_id) or {}
    policies = get_policies_obj(vertical_id) or {}
    examples = _list_proof_examples(vertical_id)

    return JSONResponse(
        {
            "ok": True,
            "type": "xertify_vertical_spec",
            "version": 1,
            "vertical": {
                "id": pack.get("id", vertical_id),
                "name": pack.get("name", ""),
                "version": pack.get("version", 1),
            },
            "policy": {
                "id": (pack.get("policy") or {}).get("id", ""),
                "sha256": policy_sha256,
                "doc": policies,
            },
            "schemas": {
                "object": obj_schema,
                "events": evt_schema,
            },
            "verdict_statuses": _verdict_statuses(vertical_id),
            "examples": examples,
            "offline_verification": {
                "why": "Allows a partner to verify truth without trusting XERTIFY or needing the UI online.",
                "steps": _offline_verify_steps(vertical_id),
            },
            "canonical_urls": {
                "spec_html": f"/spec/{vertical_id}",
                "spec_json": f"/spec/{vertical_id}.json",
            },
        }
    )


@router.get("/spec/{vertical_id}/examples/{filename}")
def spec_example(vertical_id: str, filename: str):
    vertical_id = (vertical_id or "").strip().lower()
    if vertical_id not in ("generic", "aviation", "pharma"):
        raise HTTPException(status_code=404, detail="Unknown vertical")

    vdir = PACKS_DIR / vertical_id
    path = vdir / filename

    if not vdir.exists() or not path.exists():
        raise HTTPException(status_code=404, detail="Example not found")

    # Must be valid JSON file
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Invalid JSON in example: {e}")

    return JSONResponse(data)


@router.get("/spec/{vertical_id}", response_class=HTMLResponse)
def spec_html(vertical_id: str):
    vertical_id = (vertical_id or "").strip().lower()
    if vertical_id not in ("generic", "aviation", "pharma"):
        raise HTTPException(status_code=404, detail="Unknown vertical")

    return HTMLResponse(
        f"""
<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <title>XERTIFY Spec — {vertical_id}</title>
  <style>
    body {{ font-family: ui-sans-serif, system-ui; margin: 24px; max-width: 920px; }}
    code, pre {{ background:#0b1220; color:#d6e4ff; padding: 10px; border-radius: 10px; overflow:auto; }}
    .card {{ border: 1px solid #e5e7eb; border-radius: 16px; padding: 16px; margin: 12px 0; }}
    a {{ color: #2563eb; text-decoration: none; }}
    a:hover {{ text-decoration: underline; }}
  </style>
</head>
<body>
  <h1>XERTIFY Vertical Spec: {vertical_id}</h1>

  <div class="card">
    <p>This page describes the object schema, event vocabulary, policy hash, verdict statuses, and real proof examples.</p>
    <p><b>Machine-readable spec:</b> <a href="/spec/{vertical_id}.json">/spec/{vertical_id}.json</a></p>
  </div>

  <div class="card">
    <h2>Proof examples</h2>
    <p>See <code>/spec/{vertical_id}.json</code> → <code>examples</code> list for links.</p>
  </div>

  <div class="card">
    <h2>Offline verification</h2>
    <p>See <code>/spec/{vertical_id}.json</code> → <code>offline_verification</code>.</p>
  </div>
</body>
</html>
"""
    )
