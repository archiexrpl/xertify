# backend/app/api/studio_xrpl.py
from fastapi import APIRouter, Query, Request
from fastapi.responses import JSONResponse

from backend.app.services.xrpl_account import account_overview
import os
import json
import httpx
from datetime import datetime, timezone


from pathlib import Path

# Ensure META_DIR exists in THIS module (webhook module)
BASE_DIR = Path(__file__).resolve().parent.parent   # backend/app/api -> backend/app
META_DIR = BASE_DIR / "static" / "meta" / "dyn"
META_DIR.mkdir(parents=True, exist_ok=True)


# IMPORTANT:
# This prefix is applied to ALL routes in this file.
router = APIRouter(
    prefix="/studio/actions/xrpl",
    tags=["studio_xrpl"],
)


import httpx
from backend.app.core.settings import XRPL_RPC_URL

async def _get_nftoken_id_from_txid(txid: str) -> str | None:
    """
    Look up an XRPL transaction by hash and extract NFTokenID from meta.
    Works in async FastAPI routes (no asyncio.run).
    """
    if not txid:
        return None

    payload = {
        "method": "tx",
        "params": [{
            "transaction": txid,
            "binary": False
        }]
    }

    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.post(XRPL_RPC_URL, json=payload)
        r.raise_for_status()
        data = r.json()

    result = data.get("result") or {}
    meta = result.get("meta") or {}

    # XRPL NFTokenMint puts the minted token in meta.nftoken_id (most common)
    nft_id = meta.get("nftoken_id") or meta.get("NFTokenID")
    if nft_id:
        return nft_id

    # Fallback: scan affected nodes for NFTokenID-ish fields
    affected = meta.get("AffectedNodes") or []
    for node in affected:
        inner = next(iter(node.values()), {}) if isinstance(node, dict) else {}
        final_fields = inner.get("FinalFields") or {}
        new_fields = inner.get("NewFields") or {}
        for fields in (final_fields, new_fields):
            cand = fields.get("NFTokenID") or fields.get("nftoken_id")
            if cand:
                return cand

    return None


async def _get_permissioned_domain_id_from_txid(txid: str):
    """
    Extract Permissioned Domain ID (LedgerIndex) from a PermissionedDomainSet tx meta.
    Created ledger object type is PermissionedDomain. :contentReference[oaicite:4]{index=4}
    """
    if not txid:
        return None

    try:
        from backend.app.services.xrpl_tx import get_tx  # or whatever your tx fetch helper is
    except Exception:
        get_tx = None

    if not get_tx:
        return None

    tx = await get_tx(txid)
    if not isinstance(tx, dict):
        return None

    meta = tx.get("meta") or tx.get("metaData") or {}
    nodes = meta.get("AffectedNodes") or []
    for n in nodes:
        created = n.get("CreatedNode")
        if not created:
            continue
        if (created.get("LedgerEntryType") or "") == "PermissionedDomain":
            return created.get("LedgerIndex")

    return None



async def maybe_request_anchor_for_pass(
    pass_id: str,
    actor_address: str | None,
    reason: str,
    force: bool = False,
):
    """
    Auto-anchor pipeline (Option A):
      - read canonical truth from dnft_external_facts
      - compute verdict
      - append a state proof (append-only)
      - compute Merkle root / anchor root
      - if policy says YES, submit an anchor tx automatically (Step 5),
        then persist anchor result in DB.

    This should never require extra UI/buttons.
    """
    # feature flag
    enabled = (os.getenv("AUTO_ANCHOR_ENABLED", "true").strip().lower() == "true")
    if not enabled and not force:
        return

    from backend.app.services.dnft_store import (
        get_pass,
        get_external_facts,
        append_state_proof,     # ✅ ensure this exists in dnft_store.py
        compute_anchor_root,    # ✅ returns dict
        store_anchor_result,    # ✅ you added in step 3.x
        log_event,
    )

    rec = get_pass(pass_id)
    if not rec:
        return

    external_facts = get_external_facts(pass_id) or {}

    # compute verdict using canonical facts
    verdict = compute_verdict(rec, external_facts=external_facts)

    # append proof (append-only)
    try:
        proof_hash = append_state_proof(
            pass_id=pass_id,
            pass_record=rec,
            verdict=verdict,
            external_facts=external_facts,
        )
    except TypeError:
        # if your append_state_proof signature differs, keep it consistent with your dnft_store.py
        proof_hash = append_state_proof(pass_id, rec, verdict, external_facts)

    # compute current anchor root status
    root_info = compute_anchor_root(pass_id) or {}
    root_hash = (root_info.get("anchor_root_hash") or "").strip()
    unanchored_count = int(root_info.get("unanchored_count") or 0)

    if not root_hash:
        return

    # already anchored to this root?
    already = (rec.get("anchor_root_hash") or rec.get("anchor_root") or "").strip()
    if already and already == root_hash and not force:
        return

    # policy threshold
    min_unanchored = int(os.getenv("AUTO_ANCHOR_MIN_UNANCHORED", "1"))
    if (unanchored_count < min_unanchored) and not force:
        return

    # ---- Step 5 submits the actual XRPL tx (server-signed) ----
    try:
        from backend.app.services.xrpl_tx import submit_anchor_root_server_signed
    except Exception:
        # if you haven't added Step 5 yet, just log and exit (don’t break routes)
        log_event(
            pass_id=pass_id,
            event_type="anchor_skipped_missing_submitter",
            actor_address=actor_address,
            meta={"reason": reason, "root_hash": root_hash, "unanchored_count": unanchored_count},
        )
        return

    try:
        txid = await submit_anchor_root_server_signed(
            root_hash=root_hash,
            memo_type="XERTIFY_PROOF_ROOT",
            memo_data=f"{pass_id}:{root_hash}",
        )
    except Exception as e:
        log_event(
            pass_id=pass_id,
            event_type="anchor_submit_failed",
            actor_address=actor_address,
            meta={"reason": reason, "root_hash": root_hash, "error": str(e)},
        )
        return

    # persist anchor result
    store_anchor_result(
        pass_id=pass_id,
        anchor_tx_hash=txid,
        anchor_root_hash=root_hash,
        anchored_at=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    )

    log_event(
        pass_id=pass_id,
        event_type="anchor_auto_confirmed",
        actor_address=actor_address,
        meta={"reason": reason, "root_hash": root_hash, "txid": txid, "unanchored_count": unanchored_count, "proof_hash": proof_hash},
    )



@router.get("/account_balance", response_class=JSONResponse)
def account_balance(address: str = Query(..., description="XRPL account address")):
    if not address:
        return JSONResponse({"ok": False, "error": "missing address"}, status_code=400)

    try:
        overview = account_overview(address)
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)

    xrp_info = overview.get("xrp", {}) or {}
    return JSONResponse(
        {
            "ok": True,
            "account": address,
            "xrp": xrp_info.get("total", 0.0),
            "available": xrp_info.get("available", 0.0),
            "reserve": xrp_info.get("reserve", 0.0),
            "funded": overview.get("funded", False),
        },
        status_code=200,
    )





XRPL_RPC_URL = os.getenv("XRPL_RPC_URL", "https://s1.ripple.com:51234")





def _hex_to_str(h: str) -> str:
    try:
        return bytes.fromhex(h).decode("utf-8")
    except Exception:
        return ""

async def _xrpl_tx(txid: str) -> dict | None:
    """
    Fetch transaction from XRPL JSON-RPC.
    Returns the full tx result dict (including meta/ledger_index) or None.
    """
    try:
        async with httpx.AsyncClient(timeout=12) as client:
            r = await client.post(
                XRPL_RPC_URL,
                json={
                    "method": "tx",
                    "params": [{"transaction": txid, "binary": False}],
                },
            )
            j = r.json()
            return (j.get("result") or None) if isinstance(j, dict) else None
    except Exception:
        return None

def _extract_anchor_memo(tx_result: dict) -> dict | None:
    """
    Look for MemoType == 'XERTIFY_ANCHOR' and parse MemoData JSON.
    Returns parsed dict or None.
    """
    try:
        tx = tx_result or {}
        memos = tx.get("Memos") or []
        for m in memos:
            memo = (m or {}).get("Memo") or {}
            mt = _hex_to_str(memo.get("MemoType") or "")
            if mt != "XERTIFY_ANCHOR":
                continue
            md = _hex_to_str(memo.get("MemoData") or "")
            if not md:
                continue
            return json.loads(md)
    except Exception:
        return None
    return None

    

import os
import json
from typing import Any, Dict, Optional, Tuple

from fastapi import Request, HTTPException
from xrpl.clients import JsonRpcClient
from xrpl.models.requests import Tx

from backend.app.core.settings import XRPL_RPC_URL
from backend.app.services import xumm

# ============================================================
# XUMM WEBHOOK (MINT + STATE FINALIZATION + DOMAIN FINALIZE + ANCHOR FINALIZE)
# URL (because router prefix already includes /studio/actions/xrpl):
#   /studio/actions/xrpl/xumm/webhook
# ============================================================

def _env(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()

def _safe_get(obj: Any, *keys: str) -> Any:
    cur = obj
    for k in keys:
        if cur is None:
            return None
        if isinstance(cur, dict):
            cur = cur.get(k)
        else:
            cur = getattr(cur, k, None)
    return cur

def _extract_uuid(payload: Dict[str, Any]) -> Optional[str]:
    # XUMM commonly sends payloadResponse; sometimes payload
    pr = payload.get("payloadResponse") or payload.get("payload") or {}

    return (
        pr.get("payload_uuidv4")
        or pr.get("payload_uuid")
        or pr.get("uuidv4")
        or pr.get("uuid")
        or payload.get("payload_uuidv4")
        or payload.get("uuid")
    )

def _payload_flags(details: Any) -> Tuple[bool, bool, bool]:
    """
    Returns (resolved, signed, cancelled) from XUMM payload details.
    """
    resolved = bool(_safe_get(details, "meta", "resolved"))
    signed = bool(_safe_get(details, "meta", "signed"))
    cancelled = bool(_safe_get(details, "meta", "cancelled"))
    return resolved, signed, cancelled

def _payload_expired(details: Any) -> bool:
    return bool(_safe_get(details, "meta", "expired"))

def _payload_txid(details: Any) -> Optional[str]:
    txid = _safe_get(details, "response", "txid")
    if isinstance(txid, str) and txid.strip():
        return txid.strip()
    return None

def _ledger_validated(tx_hash: str) -> Tuple[bool, Optional[str], Optional[str], Optional[Dict[str, Any]]]:
    """
    Returns (validated_ok, engine_result, engine_result_message, raw_tx_result)
    """
    client = JsonRpcClient(XRPL_RPC_URL)
    resp = client.request(Tx(transaction=tx_hash))
    result = resp.result or {}

    validated = bool(result.get("validated"))
    meta = result.get("meta") or {}

    # Most reliable result code is meta.TransactionResult when available
    engine_result = meta.get("TransactionResult") or result.get("engine_result")
    engine_result_message = meta.get("TransactionResult") or result.get("engine_result_message")

    if validated:
        return True, engine_result, engine_result_message, result

    return False, engine_result, engine_result_message, result

from fastapi import Request, HTTPException
from datetime import datetime, timezone
import json
@router.post("/xumm/webhook")
async def xumm_webhook(request: Request):
    # 0) Optional shared-secret (recommended)
    expected_token = _env("XUMM_WEBHOOK_TOKEN")
    if expected_token:
        token = (request.query_params.get("token") or "").strip()
        if token != expected_token:
            raise HTTPException(status_code=401, detail="Invalid webhook token")

    payload = await request.json()

    # 1) Identify uuid from webhook body (best-effort)
    uuid = _extract_uuid(payload)

    print("✅ XUMM WEBHOOK HIT (raw):", uuid)

    # Always acknowledge webhook (XUMM retries if you error)
    if not uuid:
        return {"ok": True, "ignored": True, "reason": "no uuid"}

    # 2) Fetch authoritative payload details from XUMM (tightening step)
    sdk = xumm.get_sdk()
    try:
        details = sdk.payload.get(uuid)
    except Exception as e:
        # Don’t finalize anything if we can’t fetch details
        print("⚠️ webhook: sdk.payload.get failed:", str(e))
        return {"ok": True, "pending": True, "reason": "payload fetch failed"}

    resolved, signed, cancelled = _payload_flags(details)
    expired = _payload_expired(details)

    # Prefer txid from authoritative details
    txid = _payload_txid(details)

    # Optional: keep fallback txid extraction from webhook body
    if not txid:
        pr = payload.get("payloadResponse") or payload.get("payload") or {}
        txid = (
            pr.get("txid")
            or pr.get("transaction_hash")
            or pr.get("tx_hash")
            or payload.get("txid")
            or payload.get("transaction_hash")
            or payload.get("tx_hash")
        )

    # 2.1) Extract custom_meta (authoritative) — NEW
    custom_meta = {}
    try:
        cm = _safe_get(details, "custom_meta")
        if isinstance(cm, dict):
            custom_meta = cm
    except Exception:
        custom_meta = {}

    cm_kind = (custom_meta.get("kind") or "").strip().lower()
    cm_pass_id = custom_meta.get("pass_id")
    cm_public_id = custom_meta.get("public_id")
    cm_vertical_id = (custom_meta.get("vertical_id") or "").strip().lower()
    cm_domain_id = (custom_meta.get("domain_id") or "").strip()
    cm_template_key = (custom_meta.get("template_key") or "").strip()
    cm_template_id = (custom_meta.get("template_id") or "").strip()
    cm_custody_mode = (custom_meta.get("custody_mode") or "").strip().lower()

    print(
        "✅ XUMM WEBHOOK (authoritative):",
        uuid,
        "resolved:",
        resolved,
        "signed:",
        signed,
        "cancelled:",
        cancelled,
        "expired:",
        expired,
        "txid:",
        txid,
        "custom_kind:",
        cm_kind,
        "custom_pass_id:",
        cm_pass_id,
    )

    # 3) Fast exit states (no “signed == final” anymore)
    if cancelled:
        return {"ok": True, "final": True, "status": "CANCELLED"}

    if expired:
        return {"ok": True, "final": True, "status": "EXPIRED"}

    if not resolved:
        return {"ok": True, "final": False, "status": "PENDING_RESOLUTION"}

    if resolved and not signed:
        return {"ok": True, "final": True, "status": "FAILED", "reason": "not signed"}

    # 4) Signed but no txid yet => pending ledger (don’t finalize)
    if not txid:
        return {"ok": True, "final": False, "status": "PENDING_LEDGER", "reason": "no txid yet"}

    # 5) Ledger validation tightening: only finalize after validated
    validated_ok, engine_result, engine_msg, raw_tx = _ledger_validated(txid)

    if not validated_ok:
        # Not final yet. Keep pending and let front-end/poller re-check later.
        return {
            "ok": True,
            "final": False,
            "status": "PENDING_LEDGER",
            "txid": txid,
            "engine_result": engine_result,
        }

    # If validated but failed on-ledger
    if engine_result and engine_result != "tesSUCCESS":
        return {
            "ok": True,
            "final": True,
            "status": "FAILED",
            "txid": txid,
            "engine_result": engine_result,
            "engine_result_message": engine_msg,
        }

    # ============================================================
    # ✅ From here down: VALIDATED SUCCESS ONLY
    # ============================================================

    # ------------------------------------------------------------
    # A) ACTION FINALIZATION (revoke / reset / reassign / extend / domain / anchor)
    # ------------------------------------------------------------
    try:
        from backend.app.services.dnft_store import (
            get_action_by_uuid,
            revoke_pass,
            reset_pass,
            set_state,
            log_event,
            get_pass,
            mark_action_signed,
            extend_expiry,
            set_metadata_json,
            set_permissioned_domain_fields,
            store_anchor_result,
            set_action_meta,
        )

        def _mark_applied(action_uuid: str, action_type: str):
            # frontend waits for meta.applied == True
            try:
                set_action_meta(
                    action_uuid,
                    {
                        "applied": True,
                        "applied_action": action_type,
                        "applied_txid": txid,
                        "applied_at": datetime.now(timezone.utc).isoformat(),
                    },
                )
            except Exception as e:
                print("⚠️ webhook: set_action_meta(applied) failed:", str(e))

        act = get_action_by_uuid(uuid)
        if act:
            pass_id = act.get("pass_id")
            action_type = (act.get("action_type") or "").strip().lower()
            meta_in = act.get("meta") or {}
            if not isinstance(meta_in, dict):
                meta_in = {}

            print("✅ webhook: action match:", action_type, "pass:", pass_id)

            if not pass_id:
                # still mark applied for action-only uuids if desired
                _mark_applied(uuid, action_type or "unknown")
                return {"ok": True, "final": True, "status": "SUCCESS", "txid": txid}

            # ✅ record signed + txid on the action row (idempotent is ideal)
            try:
                mark_action_signed(uuid, txid=txid)
            except Exception as e:
                print("⚠️ webhook: mark_action_signed failed:", str(e))

            # -------- revoke --------
            if action_type == "revoke":
                revoke_pass(pass_id, actor_address=None, reason="xrpl_revoke_validated")
                set_state(pass_id, "revoked", reason="xrpl_revoke_validated", actor_address=None)
                log_event(pass_id, "revoke_validated", None, {"uuid": uuid, "txid": txid})
                _mark_applied(uuid, action_type)
                return {"ok": True, "final": True, "status": "SUCCESS", "txid": txid}

            # -------- reset --------
            if action_type == "reset":
                reset_pass(pass_id, actor_address=None)
                set_state(pass_id, "ready", reason="xrpl_reset_validated", actor_address=None)
                log_event(pass_id, "reset_validated", None, {"uuid": uuid, "txid": txid})
                _mark_applied(uuid, action_type)
                return {"ok": True, "final": True, "status": "SUCCESS", "txid": txid}

            # -------- reassign --------
            if action_type == "reassign":
                # 1) Apply stored metadata intent ONLY now, after validated success
                try:
                    new_meta = meta_in.get("metadata_json")
                    slug = (meta_in.get("metadata_slug") or "").strip()

                    if isinstance(new_meta, dict) and new_meta:
                        set_metadata_json(pass_id, new_meta)

                        # optional: write /meta/dyn/<slug> for filesystem verifier
                        if slug:
                            try:
                                META_DIR.mkdir(parents=True, exist_ok=True)
                                path = META_DIR / slug
                                with path.open("w", encoding="utf-8") as f:
                                    json.dump(new_meta, f, ensure_ascii=False, indent=2)
                            except Exception as e:
                                print("⚠️ webhook: could not write reassign metadata file:", str(e))
                    else:
                        print("⚠️ webhook: reassign validated but no metadata_json stored on action meta")
                except Exception as e:
                    print("⚠️ webhook: reassign metadata apply failed:", str(e))

                # 2) finalize lifecycle
                reset_pass(pass_id, actor_address=None)
                set_state(pass_id, "live", reason="xrpl_reassign_validated", actor_address=None)

                # 3) Log with holder_name from stored intent (preferred)
                holder_name = meta_in.get("holder_name") or None
                log_event(
                    pass_id,
                    "reassign_validated",
                    None,
                    {
                        "uuid": uuid,
                        "txid": txid,
                        "holder_name": holder_name,
                        "destination": meta_in.get("destination"),
                        "custody_mode": meta_in.get("custody_mode"),
                        "valid_until": meta_in.get("valid_until"),
                    },
                )

                _mark_applied(uuid, action_type)
                return {"ok": True, "final": True, "status": "SUCCESS", "txid": txid}

            # -------- extend --------
            if action_type == "extend":
                expires_at = (meta_in.get("expires_at") or meta_in.get("valid_until") or "").strip()

                if not expires_at:
                    print("⚠️ webhook: extend validated but no expires_at stored on action meta")
                    log_event(pass_id, "extend_validated_missing_expires_at", None, {"uuid": uuid, "txid": txid})
                    _mark_applied(uuid, action_type)
                    return {"ok": True, "final": True, "status": "SUCCESS", "txid": txid}

                # 1) DB update expiry
                try:
                    extend_expiry(pass_id, expires_at, actor_address=None)
                except Exception as e:
                    print("⚠️ webhook: extend_expiry failed:", str(e))
                    log_event(pass_id, "extend_apply_failed", None, {"uuid": uuid, "txid": txid, "error": str(e)})
                    _mark_applied(uuid, action_type)
                    return {"ok": True, "final": True, "status": "SUCCESS", "txid": txid}

                # 2) Update metadata snapshot for UI/proofs
                try:
                    rec2 = get_pass(pass_id) or {}
                    meta2 = rec2.get("metadata_json") or {}
                    if not isinstance(meta2, dict):
                        meta2 = {}

                    meta2.setdefault("properties", {})
                    meta2["properties"].setdefault("lifecycle", {})
                    meta2["properties"]["lifecycle"]["expires_at"] = expires_at

                    attrs = list(meta2.get("attributes") or [])

                    def upsert_attr(trait_type: str, value: str):
                        for a in attrs:
                            if (a.get("trait_type") or "") == trait_type:
                                a["value"] = value
                                return
                        attrs.append({"trait_type": trait_type, "value": value})

                    upsert_attr("valid_until", expires_at)
                    meta2["attributes"] = attrs

                    set_metadata_json(pass_id, meta2)

                    # optional: write /meta/dyn/<slug> if your file system uses it
                    uri2 = rec2.get("metadata_uri") or ""
                    slug2 = None
                    if "/meta/dyn/" in uri2:
                        slug2 = uri2.split("/meta/dyn/", 1)[1].split("?", 1)[0]

                    if slug2:
                        try:
                            META_DIR.mkdir(parents=True, exist_ok=True)
                            path = META_DIR / slug2
                            with path.open("w", encoding="utf-8") as f:
                                json.dump(meta2, f, ensure_ascii=False, indent=2)
                        except Exception as e:
                            print("⚠️ webhook: could not write metadata file:", str(e))

                    log_event(pass_id, "extend_validated", None, {"uuid": uuid, "txid": txid, "expires_at": expires_at})
                except Exception as e:
                    print("⚠️ webhook: extend metadata update failed:", str(e))
                    log_event(
                        pass_id,
                        "extend_validated_metadata_update_failed",
                        None,
                        {"uuid": uuid, "txid": txid, "error": str(e)},
                    )

                _mark_applied(uuid, action_type)
                return {"ok": True, "final": True, "status": "SUCCESS", "txid": txid}

            # -------- permissioned domain --------
            if action_type in (
                "permissioned_domain_set",
                "permissioneddomainset",
                "perm_domain_set",
                "domain_create",
            ):
                domain_mode = (meta_in.get("domain_mode") or "create_new").strip().lower()
                existing_domain_id = (meta_in.get("existing_domain_id") or "").strip()

                domain_purpose = (meta_in.get("domain_purpose") or meta_in.get("purpose") or "").strip()
                credential_issuer = (meta_in.get("credential_issuer") or meta_in.get("issuer") or "").strip()
                credential_type = (meta_in.get("credential_type") or "").strip()
                domain_label = (meta_in.get("domain_label") or "").strip()
                accepted_credentials = meta_in.get("accepted_credentials") or []

                domain_id = ""
                if domain_mode == "use_existing" and existing_domain_id:
                    domain_id = existing_domain_id
                else:
                    try:
                        domain_id = await _get_permissioned_domain_id_from_txid(txid)
                    except Exception as e:
                        print("⚠️ webhook: domain tx lookup failed:", str(e))

                if not domain_id:
                    print("⚠️ webhook: domain validated but could not resolve domain_id")
                    log_event(pass_id, "perm_domain_validated_missing_domain_id", None, {"uuid": uuid, "txid": txid})
                    _mark_applied(uuid, action_type)
                    return {"ok": True, "final": True, "status": "SUCCESS", "txid": txid}

                try:
                    set_permissioned_domain_fields(
                        pass_id,
                        permissioned_domain_id=domain_id,
                        permissioned_domain_purpose=domain_purpose or None,
                        credential_issuer=credential_issuer or None,
                        credential_type=credential_type or None,
                        compliance_profile={
                            "enabled": True,
                            "domain_label": domain_label,
                            "accepted_credentials": accepted_credentials,
                            "domain_mode": domain_mode,
                        },
                    )
                except Exception as e:
                    print("⚠️ webhook: set_permissioned_domain_fields failed:", str(e))

                try:
                    rec3 = get_pass(pass_id) or {}
                    meta3 = rec3.get("metadata_json") or {}
                    if not isinstance(meta3, dict):
                        meta3 = {}

                    meta3.setdefault("properties", {})
                    meta3["properties"]["permissioned_domain"] = {
                        "enabled": True,
                        "domain_id": domain_id,
                        "domain_purpose": domain_purpose,
                        "domain_label": domain_label,
                        "accepted_credentials": accepted_credentials,
                        "credential_issuer": credential_issuer,
                        "credential_type": credential_type,
                        "domain_mode": domain_mode,
                    }

                    set_metadata_json(pass_id, meta3)

                    uri3 = rec3.get("metadata_uri") or ""
                    slug3 = None
                    if "/meta/dyn/" in uri3:
                        slug3 = uri3.split("/meta/dyn/", 1)[1].split("?", 1)[0]

                    if slug3:
                        try:
                            META_DIR.mkdir(parents=True, exist_ok=True)
                            path = META_DIR / slug3
                            with path.open("w", encoding="utf-8") as f:
                                json.dump(meta3, f, ensure_ascii=False, indent=2)
                        except Exception as e:
                            print("⚠️ webhook: could not write domain metadata file:", str(e))

                    log_event(pass_id, "perm_domain_validated", None, {"uuid": uuid, "txid": txid, "domain_id": domain_id})
                except Exception as e:
                    print("⚠️ webhook: domain metadata update failed:", str(e))
                    log_event(
                        pass_id,
                        "perm_domain_validated_metadata_update_failed",
                        None,
                        {"uuid": uuid, "txid": txid, "error": str(e)},
                    )

                _mark_applied(uuid, action_type)
                return {"ok": True, "final": True, "status": "SUCCESS", "txid": txid}

            # -------- anchor --------
            if action_type in ("anchor_proof", "anchor", "anchor_root"):
                root_hash = (
                    (meta_in.get("root_hash") or "").strip()
                    or (meta_in.get("anchor_root_hash") or "").strip()
                    or (meta_in.get("root") or "").strip()
                )

                try:
                    store_anchor_result(
                        pass_id=pass_id,
                        anchor_tx_hash=txid,
                        anchor_root_hash=root_hash or None,
                        anchored_at=None,
                    )
                except Exception as e:
                    print("⚠️ webhook: store_anchor_result failed:", str(e))
                    log_event(pass_id, "anchor_apply_failed", None, {"uuid": uuid, "txid": txid, "error": str(e)})
                    _mark_applied(uuid, action_type)
                    return {"ok": True, "final": True, "status": "SUCCESS", "txid": txid}

                if not root_hash:
                    log_event(pass_id, "anchor_confirmed_missing_root_hash", None, {"uuid": uuid, "txid": txid})

                log_event(pass_id, "anchor_confirmed", None, {"uuid": uuid, "txid": txid, "root_hash": root_hash})
                _mark_applied(uuid, action_type)
                return {"ok": True, "final": True, "status": "SUCCESS", "txid": txid}

            # Action uuid matched but not one of the above types
            _mark_applied(uuid, action_type or "unknown")
            return {"ok": True, "final": True, "status": "SUCCESS", "txid": txid}

    except Exception as e:
        print("⚠️ webhook: action handling failed:", str(e))

    # ------------------------------------------------------------
    # B) MINT FINALIZATION (uuid -> pass OR custom_meta.pass_id)
    #    (VALIDATED SUCCESS ONLY)
    # ------------------------------------------------------------
    try:
        from backend.app.services.dnft_store import (
            get_pass_by_uuid,
            get_pass,
            attach_nft_id,
            store_mint_tx_hash,
            set_state,
            log_event,
            set_metadata_json,
        )

        rec = get_pass_by_uuid(uuid)

        # NEW: fallback to custom_meta.pass_id when uuid->pass not found
        if not rec and cm_pass_id:
            try:
                rec = get_pass(cm_pass_id)
            except Exception:
                rec = None

        if not rec:
            print("ℹ️ webhook: ignored non-pass uuid:", uuid, "custom_pass_id:", cm_pass_id)
            return {"ok": True, "final": True, "status": "SUCCESS", "txid": txid}

        pass_id = rec.get("id")

        # Store tx hash (validated)
        try:
            store_mint_tx_hash(pass_id, txid)
        except Exception:
            pass

        # Resolve NFTokenID by tx lookup (preferred)
        nft_id = None
        try:
            nft_id = await _get_nftoken_id_from_txid(txid)
            if nft_id:
                attach_nft_id(pass_id, nft_id)
                print("✅ webhook: NFTokenID resolved:", nft_id)
        except Exception as e:
            print("⚠️ webhook: tx lookup failed:", str(e))

        # Fallback: try extracting from payload details response if present
        if not nft_id:
            try:
                response = _safe_get(details, "response") or {}
                result = response.get("result") if isinstance(response, dict) else _safe_get(response, "result") or {}
                meta_r = result.get("meta") if isinstance(result, dict) else _safe_get(result, "meta") or {}

                nft_id = (
                    (meta_r.get("NFTokenID") if isinstance(meta_r, dict) else None)
                    or (meta_r.get("nftoken_id") if isinstance(meta_r, dict) else None)
                    or (result.get("NFTokenID") if isinstance(result, dict) else None)
                    or (result.get("nftoken_id") if isinstance(result, dict) else None)
                )
                if nft_id:
                    attach_nft_id(pass_id, nft_id)
                    print("✅ webhook: NFTokenID extracted from payload details:", nft_id)
            except Exception:
                pass

        # NEW: mark pass state + stamp metadata_json (idempotent-safe)
        try:
            meta_j = rec.get("metadata_json") or {}
            if not isinstance(meta_j, dict):
                meta_j = {}

            meta_j.setdefault("properties", {})
            meta_j["properties"].setdefault("mint", {})
            meta_j["properties"]["mint"].update(
                {
                    "status": "validated_success",
                    "uuid": uuid,
                    "txid": txid,
                    "minted_at": datetime.now(timezone.utc).isoformat(),
                }
            )

            # keep canonical identifiers close to the mint proof
            meta_j["properties"]["mint"]["public_id"] = cm_public_id or meta_j.get("properties", {}).get("verification", {}).get("public_id")
            meta_j["properties"]["mint"]["vertical_id"] = cm_vertical_id or meta_j.get("properties", {}).get("vertical")
            meta_j["properties"]["mint"]["domain_id"] = cm_domain_id or meta_j.get("properties", {}).get("domain_id")
            meta_j["properties"]["mint"]["template_key"] = cm_template_key or meta_j.get("properties", {}).get("template_key")
            meta_j["properties"]["mint"]["template_id"] = cm_template_id or meta_j.get("properties", {}).get("template_id")
            meta_j["properties"]["mint"]["custody_mode"] = cm_custody_mode or meta_j.get("properties", {}).get("custody", {}).get("mode")

            if nft_id:
                meta_j["properties"]["mint"]["nftoken_id"] = nft_id

            set_metadata_json(pass_id, meta_j)
        except Exception as e:
            print("⚠️ webhook: mint metadata stamp failed:", str(e))

        try:
            # safest final state label (use your own if you have a canonical one)
            set_state(pass_id, "live", reason="xrpl_mint_validated", actor_address=None)
        except Exception:
            pass

        try:
            log_event(
                pass_id,
                "mint_validated",
                None,
                {
                    "uuid": uuid,
                    "txid": txid,
                    "nftoken_id": nft_id,
                    "domain_id": cm_domain_id,
                    "template_key": cm_template_key,
                    "vertical_id": cm_vertical_id,
                },
            )
        except Exception:
            pass

    except Exception as e:
        print("⚠️ webhook: mint finalization failed:", str(e))

    return {"ok": True, "final": True, "status": "SUCCESS", "txid": txid}
