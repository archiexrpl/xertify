# backend/app/services/dnft_store.py
from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import hashlib

from datetime import datetime, timezone
from typing import Any, Dict
from fastapi import HTTPException


import hashlib
import json
from datetime import datetime, timezone

def _utcnow_z() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

def _stable_dumps(obj) -> str:
    # deterministic JSON for hashing
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)

def _sha256_hex(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()



BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

DB_PATH = DATA_DIR / "xertify.db"

# -----------------------------
# Policy v1 + State (simple)
# -----------------------------

DEFAULT_POLICY_V1: Dict[str, Any] = {
    "version": 1,
    "type": "generic",
    "rules": {
        # creator powers
        "creator_can_revoke": True,
        "creator_can_reset": True,
        "creator_can_extend_expiry": True,

        # holder powers (future)
        "holder_can_transfer": True,

        # lifecycle
        "reassignable": True,  # allows "ready" reuse
        "treat_expired_as_inactive": True,
    },
}




def compute_identity_hash(pass_id: str, creator_address: str, created_at: str) -> str:
    # Immutable identity fingerprint
    return _sha256_hex(f"{pass_id}:{creator_address}:{created_at}")


def _json_dumps(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False)


def _json_loads(s: str) -> Any:
    try:
        return json.loads(s)
    except Exception:
        return None


def set_policy(pass_id: str, policy: Dict[str, Any]):
    with _conn() as c:
        c.execute(
            "UPDATE dnft_passes SET policy_json=? WHERE id=?",
            (_json_dumps(policy), pass_id),
        )


def get_policy(rec: Dict[str, Any]) -> Dict[str, Any]:
    raw = rec.get("policy_json")
    if isinstance(raw, str) and raw.strip():
        obj = _json_loads(raw)
        if isinstance(obj, dict):
            return obj
    return DEFAULT_POLICY_V1


def set_state(
    pass_id: str,
    state: str,
    reason: Optional[str] = None,
    actor_address: Optional[str] = None,
):
    st = (state or "").strip().lower() or "live"

    rec = get_pass(pass_id)
    if not rec:
        return

    prev_state = (rec.get("state") or "live").strip().lower()

    identity_hash = rec.get("identity_hash") or compute_identity_hash(
        rec["id"], rec["creator_address"], rec["created_at"]
    )

    now = datetime.now(timezone.utc).isoformat()

    # Keep revoked_at aligned with state
    revoked_at_value = rec.get("revoked_at")

    if st == "revoked":
        revoked_at_value = revoked_at_value or now
    elif st in ("ready", "live"):
        revoked_at_value = None

    with _conn() as c:
        c.execute(
            """
            UPDATE dnft_passes
            SET
              state = ?,
              state_reason = ?,
              identity_hash = ?,
              revoked_at = ?
            WHERE id = ?
            """,
            (st, reason, identity_hash, revoked_at_value, pass_id),
        )

    log_event(pass_id, "state_changed", actor_address, {"state": st, "reason": reason})

    append_state_proof(
        pass_id=pass_id,
        identity_hash=identity_hash,
        state=st,
        prev_state=prev_state,
        actor_address=actor_address,
        reason=reason,
        created_at=now,
    )





def can_actor(rec: Dict[str, Any], actor_address: Optional[str], action: str) -> bool:
    """
    Central permission gate for UI + legacy checks.

    Rule:
    - If the pass has a vertical_id AND the authority model defines a rule for this event (or alias),
      then ONLY enforce_authority decides.
    - Otherwise, fall back to creator-only policy rules for generic actions (revoke/reset/etc).
    """
    # Basic identity
    creator = rec.get("creator_address")
    is_creator = bool(actor_address) and actor_address == creator

    # ---- 1) Vertical authority path (ONE PATH ONLY) ----
    if action:
        # match studio.py vertical resolution
        meta = rec.get("metadata_json") or {}
        props = meta.get("properties") if isinstance(meta, dict) else {}

        vertical_id = (
            (rec.get("vertical_id") or "").strip().lower()
            or (meta.get("vertical_id") or "").strip().lower()
            or (
                ((props.get("vertical") or props.get("vertical_id") or "").strip().lower())
                if isinstance(props, dict)
                else ""
            )
        )

        if vertical_id:
            try:
                model = _load_authority_model(vertical_id)
                norm = _normalize_event_type(action, model)

                # If the model has a rule for this event, authority decides (no bypass).
                event_auth = model.get("event_authority", {}) or {}
                has_rule = norm in event_auth

                if has_rule:
                    allowed, _reason = enforce_authority(
                        pass_rec=rec,
                        pass_id=rec.get("id") or "",
                        actor_address=actor_address or "",
                        vertical_id=vertical_id,
                        event_type=action,
                    )
                    return bool(allowed)

            except Exception:
                # safest default if packs missing / broken
                return False

    # ---- 2) Creator-only legacy actions (non-vertical) ----
    policy = get_policy(rec)
    rules = (policy.get("rules") or {}) if isinstance(policy, dict) else {}

    if action == "revoke":
        return is_creator and bool(rules.get("creator_can_revoke", True))

    if action == "reset":
        return is_creator and bool(rules.get("creator_can_reset", True))

    if action == "extend_expiry":
        return is_creator and bool(rules.get("creator_can_extend_expiry", True))

    # default deny
    return False







def _ensure_column(table: str, col: str, coltype: str):
    """
    Adds a column if it doesn't exist yet (SQLite safe migration).
    """
    try:
        with _conn() as c:
            cols = [r[1] for r in c.execute(f"PRAGMA table_info({table})").fetchall()]
            if col not in cols:
                c.execute(f"ALTER TABLE {table} ADD COLUMN {col} {coltype}")
    except Exception:
        # Never hard-fail startup for a best-effort migration
        pass



def _conn():
    c = sqlite3.connect(DB_PATH)
    c.row_factory = sqlite3.Row
    return c

def _ensure_columns():
    """
    Lightweight migrations: add columns if missing.
    Safe to run on every startup.
    """
    with _conn() as c:
        cols = {row["name"] for row in c.execute("PRAGMA table_info(dnft_passes)").fetchall()}

        if "metadata_json" not in cols:
            c.execute("ALTER TABLE dnft_passes ADD COLUMN metadata_json TEXT")

        if "xumm_uuid" not in cols:
            c.execute("ALTER TABLE dnft_passes ADD COLUMN xumm_uuid TEXT")

        if "mint_tx_hash" not in cols:
            c.execute("ALTER TABLE dnft_passes ADD COLUMN mint_tx_hash TEXT")

        # --- lifecycle / policy ---
        if "state" not in cols:
            c.execute("ALTER TABLE dnft_passes ADD COLUMN state TEXT DEFAULT 'live'")

        if "state_reason" not in cols:
            c.execute("ALTER TABLE dnft_passes ADD COLUMN state_reason TEXT")

        if "policy_json" not in cols:
            c.execute("ALTER TABLE dnft_passes ADD COLUMN policy_json TEXT")

        if "public_id" not in cols:
            c.execute("ALTER TABLE dnft_passes ADD COLUMN public_id TEXT")

        if "public_sig" not in cols:
            c.execute("ALTER TABLE dnft_passes ADD COLUMN public_sig TEXT")


                # --- anchoring (Phase D wiring) ---
        if "anchor_tx_hash" not in cols:
            c.execute("ALTER TABLE dnft_passes ADD COLUMN anchor_tx_hash TEXT")

        if "anchor_root_hash" not in cols:
            c.execute("ALTER TABLE dnft_passes ADD COLUMN anchor_root_hash TEXT")

        if "anchored_at" not in cols:
            c.execute("ALTER TABLE dnft_passes ADD COLUMN anchored_at TEXT")

        if "updated_at" not in cols:
            c.execute("ALTER TABLE dnft_passes ADD COLUMN updated_at TEXT")


                # --- permissioned domains (Tier B) ---
        if "permissioned_domain_id" not in cols:
            c.execute("ALTER TABLE dnft_passes ADD COLUMN permissioned_domain_id TEXT")

        if "permissioned_domain_purpose" not in cols:
            c.execute("ALTER TABLE dnft_passes ADD COLUMN permissioned_domain_purpose TEXT")

        if "credential_issuer" not in cols:
            c.execute("ALTER TABLE dnft_passes ADD COLUMN credential_issuer TEXT")

        if "credential_type" not in cols:
            c.execute("ALTER TABLE dnft_passes ADD COLUMN credential_type TEXT")

        # JSON string (future-proof for KYC/AML profile blobs, jurisdiction, etc)
        if "compliance_profile_json" not in cols:
            c.execute("ALTER TABLE dnft_passes ADD COLUMN compliance_profile_json TEXT")


                # ---------------------------------------
        # Authority: role grants (per vertical)
        # ---------------------------------------
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS dnft_role_grants (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                vertical_id TEXT NOT NULL,          -- e.g. "aviation"
                actor_address TEXT NOT NULL,        -- wallet/address identifier
                role TEXT NOT NULL,                 -- e.g. "MRO"
                scope_json TEXT,                    -- optional scoping payload
                granted_by TEXT NOT NULL,           -- issuer/creator address
                granted_at TEXT NOT NULL,
                revoked_at TEXT,
                revoked_by TEXT
            )
            """
        )

        # Helpful indexes (safe to run every time)
        c.execute(
            "CREATE INDEX IF NOT EXISTS idx_dnft_role_grants_actor ON dnft_role_grants(actor_address, vertical_id)"
        )
        c.execute(
            "CREATE INDEX IF NOT EXISTS idx_dnft_role_grants_active ON dnft_role_grants(vertical_id, role, revoked_at)"
        )






def init_db():
    """
    Creates tables if missing and applies migrations if DB already exists.
    Safe to run on every startup.
    """
    with _conn() as c:
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS dnft_passes (
    id TEXT PRIMARY KEY,
    creator_address TEXT NOT NULL,
    nft_id TEXT,
    metadata_uri TEXT NOT NULL,
    metadata_json TEXT,

    -- lifecycle / policy
    state TEXT DEFAULT 'live',
    state_reason TEXT,
    policy_json TEXT,

    permissioned_domain_id TEXT,
    permissioned_domain_purpose TEXT,
    credential_issuer TEXT,
    credential_type TEXT,
    compliance_profile_json TEXT,


    revoked_at TEXT,
    expires_at TEXT,
    usage_count INTEGER DEFAULT 0,
    usage_limit INTEGER,
    created_at TEXT NOT NULL,
    xumm_uuid TEXT,
    mint_tx_hash TEXT,

    -- anchoring (Phase D wiring)
    anchor_tx_hash TEXT,
    anchor_root_hash TEXT,
    anchored_at TEXT
)


            """
        )

        c.execute(
            """
            CREATE TABLE IF NOT EXISTS dnft_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pass_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                actor_address TEXT,
                meta_json TEXT,
                created_at TEXT NOT NULL
            )
            """
        )

        
        
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS dnft_xumm_actions (
                xumm_uuid TEXT PRIMARY KEY,
                pass_id TEXT NOT NULL,
                action_type TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )

        c.execute(
    """
    CREATE TABLE IF NOT EXISTS dnft_actions (
        uuid TEXT PRIMARY KEY,
        pass_id TEXT NOT NULL,
        action_type TEXT NOT NULL,
        created_at TEXT NOT NULL
    )
    """
)
        
        # -------------------------------
# Latest external verification facts (supply-chain / sensors / recall feeds)
# -------------------------------
        c.execute("""
CREATE TABLE IF NOT EXISTS dnft_external_facts (
  pass_id TEXT PRIMARY KEY,
  facts_json TEXT NOT NULL,
  updated_at TEXT
)
""")


# ✅ RIGHT HERE:
        _ensure_column("dnft_actions", "meta_json", "TEXT")
        _ensure_column("dnft_actions", "created_at", "TEXT")



                # ---------------------------------------
        # Trust: immutable proof chain (append-only)
        # ---------------------------------------
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS dnft_state_proofs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pass_id TEXT NOT NULL,

                identity_hash TEXT NOT NULL,

                state TEXT NOT NULL,
                prev_state TEXT,

                actor_address TEXT,
                reason TEXT,
                created_at TEXT NOT NULL,

                prev_hash TEXT,
                proof_hash TEXT NOT NULL,

                anchor_xumm_uuid TEXT,
                anchor_tx_hash TEXT,
                anchor_ledger_index INTEGER
            )
            """
        )

        # Migration: add identity_hash to dnft_passes if missing
        cols = [r[1] for r in c.execute("PRAGMA table_info(dnft_passes)").fetchall()]
        if "identity_hash" not in cols:
            c.execute("ALTER TABLE dnft_passes ADD COLUMN identity_hash TEXT")




    _ensure_columns()








from datetime import datetime, timezone

def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

def _table_has_column(c, table: str, col: str) -> bool:
    rows = c.execute(f"PRAGMA table_info({table})").fetchall()
    return any(r[1] == col for r in rows)  # r[1] is column name

def ensure_dnft_schema():
    """
    Launch hardening:
    - Adds missing anchor columns safely (no breaking deploys)
    - Adds useful indexes for idempotency + speed
    """
    with _conn() as c:
        # -------------------------
        # dnft_passes: anchor fields
        # -------------------------
        # (These let the UI flip to Anchored instantly)
        if _table_has_column(c, "dnft_passes", "anchor_tx_hash") is False:
            c.execute("ALTER TABLE dnft_passes ADD COLUMN anchor_tx_hash TEXT")
        if _table_has_column(c, "dnft_passes", "anchor_root_hash") is False:
            c.execute("ALTER TABLE dnft_passes ADD COLUMN anchor_root_hash TEXT")
        if _table_has_column(c, "dnft_passes", "anchored_at") is False:
            c.execute("ALTER TABLE dnft_passes ADD COLUMN anchored_at TEXT")

        # -------------------------
        # dnft_state_proofs: ledger index
        # -------------------------
        if _table_has_column(c, "dnft_state_proofs", "anchor_ledger_index") is False:
            c.execute("ALTER TABLE dnft_state_proofs ADD COLUMN anchor_ledger_index INTEGER")

        # -------------------------
        # Idempotency / speed indexes
        # -------------------------
        # dnft_actions: prevent duplicate webhook/action rows by uuid
        # (If you already have these, sqlite will ignore errors only if you wrap try/except)
        try:
            c.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_dnft_actions_uuid ON dnft_actions(uuid)")
        except Exception:
            pass


        # dnft_passes: anchors
        try:
            c.execute("CREATE INDEX IF NOT EXISTS idx_dnft_passes_anchor_tx ON dnft_passes(anchor_tx_hash)")
        except Exception:
            pass

        # dnft_state_proofs: latest proof lookups
        try:
            c.execute("CREATE INDEX IF NOT EXISTS idx_dnft_state_proofs_pass_ts ON dnft_state_proofs(pass_id, created_at)")
        except Exception:
            pass

        c.commit()












from typing import Optional, List, Dict, Any
import json

def grant_role(vertical_id: str, actor_address: str, role: str, granted_by: str, scope: Optional[dict] = None) -> dict:
    now = _utc_now_iso()
    scope_json = json.dumps(scope, ensure_ascii=False) if isinstance(scope, dict) else None

    with _conn() as c:
        # If an identical active grant exists, do nothing (idempotent)
        row = c.execute(
            """
            SELECT id FROM dnft_role_grants
            WHERE vertical_id=? AND actor_address=? AND role=? AND revoked_at IS NULL
            LIMIT 1
            """,
            (vertical_id, actor_address, role),
        ).fetchone()
        if row:
            return {"ok": True, "status": "already_granted", "grant_id": row[0]}

        c.execute(
            """
            INSERT INTO dnft_role_grants
            (vertical_id, actor_address, role, scope_json, granted_by, granted_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (vertical_id, actor_address, role, scope_json, granted_by, now),
        )
        grant_id = c.execute("SELECT last_insert_rowid()").fetchone()[0]

    return {"ok": True, "status": "granted", "grant_id": grant_id}


def revoke_role(vertical_id: str, actor_address: str, role: str, revoked_by: str) -> dict:
    now = _utc_now_iso()
    with _conn() as c:
        res = c.execute(
            """
            UPDATE dnft_role_grants
            SET revoked_at=?, revoked_by=?
            WHERE vertical_id=? AND actor_address=? AND role=? AND revoked_at IS NULL
            """,
            (now, revoked_by, vertical_id, actor_address, role),
        )
    return {"ok": True, "status": "revoked", "rows": getattr(res, "rowcount", None)}


def list_roles_for_actor(vertical_id: str, actor_address: str) -> List[Dict[str, Any]]:
    with _conn() as c:
        rows = c.execute(
            """
            SELECT vertical_id, actor_address, role, scope_json, granted_by, granted_at, revoked_at, revoked_by
            FROM dnft_role_grants
            WHERE vertical_id=? AND actor_address=?
            ORDER BY granted_at DESC
            """,
            (vertical_id, actor_address),
        ).fetchall()

    out = []
    for r in rows:
        d = dict(r)
        sj = d.get("scope_json")
        if isinstance(sj, str) and sj.strip():
            try:
                d["scope"] = json.loads(sj)
            except Exception:
                d["scope"] = None
        else:
            d["scope"] = None
        d.pop("scope_json", None)
        out.append(d)
    return out


def has_active_role(vertical_id: str, actor_address: str, role: str) -> bool:
    with _conn() as c:
        row = c.execute(
            """
            SELECT 1
            FROM dnft_role_grants
            WHERE vertical_id=? AND actor_address=? AND role=? AND revoked_at IS NULL
            LIMIT 1
            """,
            (vertical_id, actor_address, role),
        ).fetchone()
    return bool(row)




def _latest_proof_hash(pass_id: str) -> Optional[str]:
    with _conn() as c:
        row = c.execute(
            "SELECT proof_hash FROM dnft_state_proofs WHERE pass_id=? ORDER BY id DESC LIMIT 1",
            (pass_id,),
        ).fetchone()
    return row[0] if row else None


def append_state_proof(
    pass_id: str,
    identity_hash: str,
    state: str,
    prev_state: Optional[str],
    actor_address: Optional[str],
    reason: Optional[str],
    created_at: str,
) -> str:
    prev_hash = _latest_proof_hash(pass_id)

    # Hash-chain payload
    payload = {
        "v": 1,
        "pass_id": pass_id,
        "identity_hash": identity_hash,
        "state": state,
        "prev_state": prev_state,
        "actor": actor_address,
        "reason": reason,
        "created_at": created_at,
        "prev_hash": prev_hash,
    }

    proof_hash = _sha256_hex(json.dumps(payload, sort_keys=True, separators=(",", ":")))

    with _conn() as c:
        c.execute(
            """
            INSERT INTO dnft_state_proofs
            (pass_id, identity_hash, state, prev_state, actor_address, reason, created_at, prev_hash, proof_hash)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                pass_id,
                identity_hash,
                state,
                prev_state,
                actor_address,
                reason,
                created_at,
                prev_hash,
                proof_hash,
            ),
        )

    return proof_hash



def store_state_proof(pass_id: str, rec: dict, verdict: dict, external_facts: dict | None = None) -> str:
    """
    Canonical "state proof" writer.
    Creates a deterministic proof JSON, hashes it, and appends to dnft_state_proofs.
    Returns proof_hash (sha256 hex).
    """
    external_facts = external_facts or {}

    # snapshot metadata (DB copy if present)
    meta = (
        (rec.get("metadata_json") if isinstance(rec.get("metadata_json"), dict) else None)
        or (rec.get("pass_metadata") if isinstance(rec.get("pass_metadata"), dict) else None)
        or (rec.get("metadata") if isinstance(rec.get("metadata"), dict) else None)
        or {}
    )

    proof = {
        "type": "xertify_state_proof",
        "version": 1,
        "ts": _utcnow_z(),
        "pass_id": pass_id,

        # ledger identity (optional but useful)
        "nft_id": rec.get("nft_id") or "",
        "mint_tx_hash": rec.get("mint_tx_hash") or rec.get("tx_hash") or "",
        "issuer_address": rec.get("issuer_address") or rec.get("issuer") or "",
        "creator_address": rec.get("creator_address") or rec.get("creator") or "",
        "owner_address": rec.get("owner_address") or rec.get("owner") or "",

        # state + expiry
        "state": (rec.get("state") or rec.get("status") or ""),
        "valid_until": (rec.get("expires_at") or rec.get("valid_until") or ""),

        # truth inputs
        "external_facts": external_facts,
        "external_facts_sha256": _sha256_hex(_stable_dumps(external_facts)),

        # verdict output
        "verdict": verdict,

        # metadata snapshot hash (don’t include full metadata unless you want it)
        "metadata_sha256": _sha256_hex(_stable_dumps(meta)),
    }

    proof_hash = _sha256_hex(_stable_dumps(proof))
    append_state_proof(pass_id, proof_hash, proof)
    return proof_hash



def get_latest_state_proof(pass_id: str) -> Optional[Dict[str, Any]]:
    with _conn() as c:
        row = c.execute(
            """
            SELECT *
            FROM dnft_state_proofs
            WHERE pass_id=?
            ORDER BY id DESC
            LIMIT 1
            """,
            (pass_id,),
        ).fetchone()
    return dict(row) if row else None


def set_proof_anchor(pass_id: str, proof_hash: str, xumm_uuid: str):
    with _conn() as c:
        c.execute(
            """
            UPDATE dnft_state_proofs
            SET anchor_xumm_uuid=?
            WHERE pass_id=? AND proof_hash=?
            """,
            (xumm_uuid, pass_id, proof_hash),
        )


def set_proof_anchor_result(pass_id: str, xumm_uuid: str, tx_hash: str, ledger_index: Optional[int] = None):
    with _conn() as c:
        c.execute(
            """
            UPDATE dnft_state_proofs
            SET anchor_tx_hash=?, anchor_ledger_index=?
            WHERE pass_id=? AND anchor_xumm_uuid=?
            """,
            (tx_hash, ledger_index, pass_id, xumm_uuid),
        )


def set_external_facts(pass_id: str, facts: dict, actor_address: Optional[str] = None) -> None:
    if not pass_id:
        return
    if not isinstance(facts, dict):
        facts = {"_raw": str(facts)}

    now = datetime.now(timezone.utc).isoformat()
    raw = json.dumps(facts, ensure_ascii=False)

    with _conn() as c:
        c.execute(
            """
            INSERT INTO dnft_external_facts (pass_id, facts_json, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(pass_id) DO UPDATE SET
              facts_json=excluded.facts_json,
              updated_at=excluded.updated_at
            """,
            (pass_id, raw, now),
        )

    rec = get_pass(pass_id) or {}
    if not rec:
        return

    identity_hash = rec.get("identity_hash") or compute_identity_hash(
        rec["id"], rec["creator_address"], rec["created_at"]
    )
    st = (rec.get("state") or "live").strip().lower()

    facts_sha = hashlib.sha256(json.dumps(facts, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")).hexdigest()
    log_event(pass_id, "external_facts_updated", actor_address, {"external_facts_sha256": facts_sha})

    # proof tick
    append_state_proof(
        pass_id=pass_id,
        identity_hash=identity_hash,
        state=st,
        prev_state=st,
        actor_address=actor_address,
        reason="external_facts_updated",
        created_at=now,
    )



def get_external_facts(pass_id: str) -> dict:
    if not pass_id:
        return {}

    with _conn() as c:
        row = c.execute(
            "SELECT facts_json FROM dnft_external_facts WHERE pass_id=?",
            (pass_id,),
        ).fetchone()

    if not row:
        return {}

    try:
        return json.loads(row[0]) or {}
    except Exception:
        return {}


def create_pass(
    creator_address: str,
    metadata_uri: str,
    expires_at: Optional[str] = None,
    usage_limit: Optional[int] = None,
    metadata_json: Optional[Dict[str, Any]] = None,
) -> str:
    pid = uuid.uuid4().hex[:16]
    now = datetime.now(timezone.utc).isoformat()
    identity_hash = compute_identity_hash(pid, creator_address, now)

    # --- Hard guarantee: every pass is born with properties.vertical ---
    allowed_verticals = {"generic", "aviation", "pharma"}

    if isinstance(metadata_json, dict):
        metadata_json.setdefault("properties", {})
        props = metadata_json.get("properties") or {}
        if not isinstance(props, dict):
            props = {}
            metadata_json["properties"] = props

        v = (props.get("vertical") or props.get("vertical_id") or "").strip().lower()
        if v not in allowed_verticals:
            v = "generic"

        props["vertical"] = v
        # optional cleanup so we don't carry 2 names forever
        if "vertical_id" in props:
            try:
                del props["vertical_id"]
            except Exception:
                pass

    meta_json_str = json.dumps(metadata_json, ensure_ascii=False) if isinstance(metadata_json, dict) else None

    with _conn() as c:
        c.execute(
            """
            INSERT INTO dnft_passes
            (id, creator_address, metadata_uri, metadata_json, expires_at, usage_limit, created_at, policy_json, state, state_reason, identity_hash)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                pid,
                creator_address,
                metadata_uri,
                meta_json_str,
                expires_at,
                usage_limit,
                now,
                _json_dumps(DEFAULT_POLICY_V1),
                "live",
                None,
                identity_hash,
            ),
        )

    log_event(pid, "created", creator_address, {})

    # Initial proof (genesis) — this must happen
    append_state_proof(
        pass_id=pid,
        identity_hash=identity_hash,
        state="live",
        prev_state=None,
        actor_address=creator_address,
        reason="created",
        created_at=now,
    )

    return pid








def list_passes_created_by(address: str, limit: int = 200) -> List[Dict[str, Any]]:
    with _conn() as c:
        rows = c.execute(
            """
            SELECT id, creator_address, nft_id, state, revoked_at, expires_at, usage_count, usage_limit, created_at, metadata_uri, metadata_json
            FROM dnft_passes
            WHERE creator_address = ?
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (address, limit),
        ).fetchall()

    out = []
    for r in rows:
        d = dict(r)
        mj = d.get("metadata_json")
        if isinstance(mj, str) and mj.strip():
            try:
                d["metadata_json"] = json.loads(mj)
            except Exception:
                pass
        out.append(d)
    return out


def list_passes_related_to_address(address: str, limit: int = 200) -> List[Dict[str, Any]]:
    """
    Finds passes where metadata_json references the wallet (custody holder_address OR destination).
    This is v1 heuristic search until you promote holder_address into a dedicated DB column.
    """
    needle = address.strip()
    like1 = f'%"{needle}"%'
    like2 = f"%{needle}%"

    with _conn() as c:
        rows = c.execute(
            """
            SELECT id, creator_address, nft_id, state, revoked_at, expires_at, usage_count, usage_limit, created_at, metadata_uri, metadata_json
            FROM dnft_passes
            WHERE metadata_json LIKE ? OR metadata_json LIKE ?
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (like1, like2, limit),
        ).fetchall()

    out = []
    for r in rows:
        d = dict(r)
        mj = d.get("metadata_json")
        if isinstance(mj, str) and mj.strip():
            try:
                d["metadata_json"] = json.loads(mj)
            except Exception:
                pass
        out.append(d)
    return out


def list_recent_events_for_actor(address: str, limit: int = 200) -> List[Dict[str, Any]]:
    with _conn() as c:
        rows = c.execute(
            """
            SELECT id, pass_id, event_type, actor_address, meta_json, created_at
            FROM dnft_events
            WHERE actor_address = ?
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (address, limit),
        ).fetchall()

    out = []
    for r in rows:
        d = dict(r)
        mj = d.get("meta_json")
        if isinstance(mj, str) and mj.strip():
            try:
                d["meta_json"] = json.loads(mj)
            except Exception:
                pass
        out.append(d)
    return out




def set_public_share(pass_id: str, public_id: str, public_sig: str):
    with _conn() as c:
        c.execute(
            "UPDATE dnft_passes SET public_id=?, public_sig=? WHERE id=?",
            (public_id, public_sig, pass_id),
        )

def get_pass_by_public_id(public_id: str) -> Optional[Dict[str, Any]]:
    with _conn() as c:
        row = c.execute(
            "SELECT * FROM dnft_passes WHERE public_id=?",
            (public_id,),
        ).fetchone()
        if not row:
            return None

    rec = dict(row)
    mj = rec.get("metadata_json")
    if isinstance(mj, str) and mj.strip():
        try:
            rec["metadata_json"] = json.loads(mj)
        except Exception:
            pass
    return rec



def set_metadata_json(pass_id: str, metadata: Dict[str, Any], actor_address: Optional[str] = None):
    with _conn() as c:
        c.execute(
            "UPDATE dnft_passes SET metadata_json=? WHERE id=?",
            (json.dumps(metadata, ensure_ascii=False), pass_id),
        )

    rec = get_pass(pass_id) or {}
    if not rec:
        return

    identity_hash = rec.get("identity_hash") or compute_identity_hash(
        rec["id"], rec["creator_address"], rec["created_at"]
    )
    st = (rec.get("state") or "live").strip().lower()
    now = datetime.now(timezone.utc).isoformat()

    log_event(pass_id, "metadata_updated", actor_address, {"metadata_sha256": hashlib.sha256(json.dumps(metadata, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")).hexdigest()})

    # proof tick
    append_state_proof(
        pass_id=pass_id,
        identity_hash=identity_hash,
        state=st,
        prev_state=st,
        actor_address=actor_address,
        reason="metadata_updated",
        created_at=now,
    )



def store_xumm_uuid(pass_id: str, xumm_uuid: str):
    with _conn() as c:
        c.execute("UPDATE dnft_passes SET xumm_uuid=? WHERE id=?", (xumm_uuid, pass_id))


def store_mint_tx_hash(pass_id: str, tx_hash: str):
    with _conn() as c:
        c.execute("UPDATE dnft_passes SET mint_tx_hash=? WHERE id=?", (tx_hash, pass_id))


def store_anchor_result(
    pass_id: str,
    anchor_tx_hash: str,
    anchor_root_hash: str = None,
    anchored_at: str = None,
):
    """
    Hardened:
    - If already anchored, do not overwrite (unless same tx)
    - Fill anchored_at automatically (UTC)
    """
    if not pass_id or not anchor_tx_hash:
        return

    anchored_at = anchored_at or _utc_now_iso()

    with _conn() as c:
        row = c.execute(
            "SELECT anchor_tx_hash, anchor_root_hash, anchored_at FROM dnft_passes WHERE id=?",
            (pass_id,),
        ).fetchone()

        if row:
            existing_tx = row[0]
            # already anchored to a different tx -> leave it alone
            if existing_tx and existing_tx != anchor_tx_hash:
                return

        c.execute(
            """
            UPDATE dnft_passes
            SET anchor_tx_hash=?,
                anchor_root_hash=COALESCE(?, anchor_root_hash),
                anchored_at=COALESCE(?, anchored_at)
            WHERE id=?
            """,
            (anchor_tx_hash, anchor_root_hash, anchored_at, pass_id),
        )
        c.commit()




def get_pass(pass_id: str) -> Optional[Dict[str, Any]]:
    with _conn() as c:
        row = c.execute("SELECT * FROM dnft_passes WHERE id=?", (pass_id,)).fetchone()
        if not row:
            return None

    rec = dict(row)

    # Parse metadata_json if stored as string
    mj = rec.get("metadata_json")
    if isinstance(mj, str) and mj.strip():
        try:
            rec["metadata_json"] = json.loads(mj)
        except Exception:
            pass

    cp = rec.get("compliance_profile_json")
    if isinstance(cp, str) and cp.strip():
        try:
            rec["compliance_profile"] = json.loads(cp)
        except Exception:
            rec["compliance_profile"] = None
    else:
        rec["compliance_profile"] = None


    # ✅ Attach latest proof-chain info (anchor request + tx result)
    # ✅ Attach latest proof-chain info (DO NOT overwrite dnft_passes anchor fields)
    try:
        proof = get_latest_state_proof(pass_id)
        if proof:
            rec["proof_hash"] = proof.get("proof_hash") or ""

            # keep proof-chain fields separate so UI/debug can still show them
            rec["proof_anchor_xumm_uuid"] = proof.get("anchor_xumm_uuid") or ""
            rec["proof_anchor_tx_hash"] = proof.get("anchor_tx_hash") or ""
            rec["proof_anchor_ledger_index"] = proof.get("anchor_ledger_index")
    except Exception:
        pass


    return rec

# -----------------------------
# Authority / Roles (v1)
# -----------------------------

_PACKS_DIR = (Path(__file__).resolve().parent / "verticals" / "packs").resolve()

def _load_authority_model(vertical_id: str) -> dict:
    fp = _PACKS_DIR / vertical_id / "authority.model.json"
    if not fp.exists():
        raise FileNotFoundError(f"Missing authority.model.json for vertical: {vertical_id}")
    return json.loads(fp.read_text())

def _normalize_event_type(event_type: str, authority_model: dict) -> str:
    aliases = authority_model.get("event_aliases", {}) or {}
    return aliases.get(event_type, event_type)

def _required_roles_for_event(event_type: str, authority_model: dict) -> List[str]:
    entry = (authority_model.get("event_authority", {}) or {}).get(event_type)
    if not entry:
        return []

    if isinstance(entry, list):
        return [str(r) for r in entry]

    if isinstance(entry, dict):
        roles = entry.get("allowed_roles") or []
        if isinstance(roles, list):
            return [str(r) for r in roles]

    return []


def get_authority_grants(pass_id: str) -> Dict[str, List[str]]:
    """
    Stored inside external_facts under:
      {
        "_authority": {
          "grants": {
             "<rAddress>": ["OPERATOR", "MAINTENANCE"]
          }
        }
      }
    """
    facts = get_external_facts(pass_id) or {}
    auth = facts.get("_authority", {}) or {}
    grants = auth.get("grants", {}) or {}
    # normalize
    out: Dict[str, List[str]] = {}
    for addr, roles in grants.items():
        if isinstance(roles, list):
            out[addr] = [str(r) for r in roles]
    return out

def grant_authority_roles(
    pass_id: str,
    subject_address: str,
    roles: List[str],
    granted_by: str,
    note: Optional[str] = None,
) -> dict:
    facts = get_external_facts(pass_id) or {}
    auth = facts.get("_authority", {}) or {}
    grants = auth.get("grants", {}) or {}
    history = auth.get("history", []) or []

    grants[subject_address] = sorted(list(set([str(r) for r in roles])))

    history.append({
        "subject_address": subject_address,
        "roles": grants[subject_address],
        "granted_by": granted_by,
        "note": note or "",
    })

    facts["_authority"] = {
        "grants": grants,
        "history": history[-200:]  # cap
    }

    set_external_facts(pass_id, facts)
    return facts["_authority"]















def resolve_actor_roles(pass_rec: dict, pass_id: str, vertical_id: str, actor_address: str) -> List[str]:
    """
    v1 rules:
    - creator_address is always ISSUER
    - otherwise roles come from:
        A) external_facts._authority.grants (pass-scoped)
        B) dnft_role_grants (vertical-scoped)  <-- what /studio/actions/authority/grant uses
    """
    creator = pass_rec.get("creator_address")
    if creator and actor_address == creator:
        return ["ISSUER"]

    out = set()

    # A) pass-scoped grants
    grants = get_authority_grants(pass_id)
    for r in (grants.get(actor_address) or []):
        out.add(str(r))

    # B) vertical-scoped db grants
        # B) vertical-scoped db grants
    try:
        rows = list_roles_for_actor(vertical_id, actor_address)
        for row in rows or []:
            role = row.get("role") if isinstance(row, dict) else row["role"]
            revoked_at = row.get("revoked_at") if isinstance(row, dict) else row["revoked_at"]
            if not revoked_at and role:
                out.add(str(role))
    except Exception:
        # Don't crash authority if roles table isn't available for some reason
        pass


    return sorted(out)


















from fastapi import HTTPException
import json

def enforce_authority(rec: dict, viewer_address: str | None, action: str) -> None:
    """
    Single-gate authority enforcement (Phase 3.1 scope-aware).

    A viewer is authorized if they have at least one role grant that:
      - matches the pass vertical_id
      - is not revoked
      - provides one of the required roles for this action/event (as per authority.model.json)
      - scope matches (if scope exists): pass_id/domain_id/template_key/actions
    """
    if not viewer_address:
        raise HTTPException(status_code=401, detail="Not logged in")
    


    pass_id = (rec.get("pass_id") or rec.get("id") or rec.get("_id") or "").strip()

    # ✅ vertical_id: prefer DB column, fallback to metadata_json.properties.vertical
    vertical_id = (rec.get("vertical_id") or "").strip()
    if not vertical_id:
        meta = rec.get("metadata_json") or {}
        if isinstance(meta, dict):
            props = meta.get("properties") or {}
            if isinstance(props, dict):
                vertical_id = (props.get("vertical") or props.get("vertical_id") or "").strip()

    vertical_id = (vertical_id or "").strip().lower()
    if not vertical_id:
        raise HTTPException(status_code=403, detail="Missing vertical_id on pass")


    # Optional creator override (keep ONLY if you already support it elsewhere)
    creator = (rec.get("creator_address") or rec.get("issuer_address") or "").strip()
    if creator and creator == viewer_address:
        return

    # Load authority model for this vertical and determine required roles
    model = _load_authority_model(vertical_id)
    event_type = _normalize_event_type(action, model)
    required_roles = _required_roles_for_event(event_type, model)

    # If model does not define authority for this event, deny by default (single-gate)
    if not required_roles:
        raise HTTPException(status_code=403, detail=f"No authority rule for action: {event_type}")

    # ---- helpers ----
    def _get_meta_props(r: dict) -> dict:
        meta = r.get("metadata_json") or {}
        if not isinstance(meta, dict):
            return {}
        props = meta.get("properties") or {}
        return props if isinstance(props, dict) else {}

    def _scope_matches(scope: dict | None) -> bool:
        """
        Scope matching logic (recommended):
          - if scope.pass_id: must match this pass_id
          - if scope.domain_id: must match meta.properties.domain_id
          - if scope.template_key: must match meta.properties.template_key (or template_id if you use that)
          - if scope.actions: must include this event_type
        """
        if not scope or not isinstance(scope, dict):
            return True

        props = _get_meta_props(rec)
        meta_domain_id = (props.get("domain_id") or "").strip()
        meta_template_key = (props.get("template_key") or props.get("template_id") or "").strip()

        s_pass = (scope.get("pass_id") or "").strip()
        if s_pass and s_pass != pass_id:
            return False

        s_domain = (scope.get("domain_id") or "").strip()
        if s_domain and s_domain != meta_domain_id:
            return False

        s_tpl = (scope.get("template_key") or scope.get("template_id") or "").strip()
        if s_tpl and s_tpl != meta_template_key:
            return False

        s_actions = scope.get("actions")
        if s_actions:
            if isinstance(s_actions, str):
                s_actions = [s_actions]
            if isinstance(s_actions, list):
                norm = set([str(a).strip() for a in s_actions if a is not None])
                if event_type not in norm and action not in norm:
                    return False

        return True

    # ---- gather grants (two sources) ----
    # A) pass-scoped grants (external_facts._authority.grants) => scope implicitly pass_id
    pass_scoped = get_authority_grants(pass_id) or {}
    pass_roles = pass_scoped.get(viewer_address) or []
    pass_roles = [str(r).strip() for r in pass_roles if r]

    # B) vertical-scoped DB grants => includes scope_json
    db_rows = []
    try:
        db_rows = list_roles_for_actor(vertical_id, viewer_address) or []
    except Exception:
        db_rows = []

    # Normalize db grant rows to dicts with parsed scope
    db_grants: list[dict] = []
    for row in db_rows:
        if isinstance(row, dict):
            g = dict(row)
        else:
            # If your sqlite rows are tuples, adapt here (but your list_roles_for_actor looks dict-based)
            continue

        # Skip revoked
        if g.get("revoked_at") or g.get("revoked") is True:
            continue

        scope = g.get("scope")
        if scope is None:
            scope = g.get("scope_json")

        if isinstance(scope, str):
            try:
                scope = json.loads(scope)
            except Exception:
                scope = None

        g["_scope_obj"] = scope if isinstance(scope, dict) else None
        db_grants.append(g)

    # ---- check authorization ----
    required_set = set([str(r).strip() for r in required_roles if r])

    # 1) pass-scoped roles: scope is always this pass_id
    for r in pass_roles:
        if r in required_set:
            return

    # 2) db grants: role must match AND scope must match
    for g in db_grants:
        role_name = (g.get("role") or "").strip()
        if not role_name:
            continue
        if role_name not in required_set:
            continue
        if not _scope_matches(g.get("_scope_obj")):
            continue
        return

    # If we reached here => deny
    raise HTTPException(
        status_code=403,
        detail=f"Not authorized for {event_type}. Required roles: {sorted(list(required_set))}",
    )


def _scope_matches(*, scope: dict | None, rec: dict, action: str | None) -> bool:
    """
    Scope matching rules (Phase 3.1):
    - If scope is missing/empty -> matches everything
    - pass_id: must match rec["_id"] or rec["pass_id"] (string compare)
    - domain_id: must match rec.metadata_json.properties.domain_id
    - template_key: must match rec.metadata_json.properties.template_key
    - actions: if provided, action must be included (exact match, case-insensitive)
    """
    if not scope or not isinstance(scope, dict):
        return True

    # Normalize
    action_norm = (action or "").strip().lower() or None

    pass_id_scope = (scope.get("pass_id") or "").strip() or None
    domain_id_scope = (scope.get("domain_id") or "").strip() or None
    template_key_scope = (scope.get("template_key") or "").strip() or None

    actions_scope = scope.get("actions")
    actions_norm = None
    if isinstance(actions_scope, (list, tuple)):
        actions_norm = {str(a).strip().lower() for a in actions_scope if str(a).strip()}

    # Pass id match
    if pass_id_scope:
        rec_pid = str(rec.get("_id") or rec.get("pass_id") or "").strip()
        if not rec_pid or rec_pid != pass_id_scope:
            return False

    # Read canonical metadata_json props
    meta = rec.get("metadata_json") or {}
    if not isinstance(meta, dict):
        meta = {}
    props = meta.get("properties") or {}
    if not isinstance(props, dict):
        props = {}

    # Domain match
    if domain_id_scope:
        rec_domain = (props.get("domain_id") or "").strip()
        if not rec_domain or rec_domain != domain_id_scope:
            return False

    # Template match
    if template_key_scope:
        rec_tpl = (props.get("template_key") or props.get("template_id") or "").strip()
        if not rec_tpl or rec_tpl != template_key_scope:
            return False

    # Actions match
    if actions_norm is not None:
        if not action_norm or action_norm not in actions_norm:
            return False

    return True


def register_xumm_action(xumm_uuid: str, pass_id: str, action_type: str):
    """
    Backwards-compatible wrapper.
    We standardize on dnft_actions + store_action_uuid().
    """
    store_action_uuid(xumm_uuid, pass_id, action_type)


def last_action_ts(pass_id: str, action_type: str) -> str:
    with _conn() as c:
        try:
            row = c.execute(
                """
                SELECT created_at FROM dnft_actions
                WHERE pass_id=? AND action_type=?
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (pass_id, action_type),
            ).fetchone()
            return row[0] if row else ""
        except Exception:
            return ""


def attach_nft_id(pass_id: str, nft_id: str):
    with _conn() as c:
        c.execute("UPDATE dnft_passes SET nft_id=? WHERE id=?", (nft_id, pass_id))
    log_event(pass_id, "mint_confirmed", None, {"nft_id": nft_id})


def revoke_pass(pass_id: str, actor_address: Optional[str] = None, reason: str = "revoked"):
    now = datetime.now(timezone.utc).isoformat()
    with _conn() as c:
        c.execute("UPDATE dnft_passes SET revoked_at=? WHERE id=?", (now, pass_id))
    log_event(pass_id, "revoked", actor_address, {"reason": reason})


def reset_pass(pass_id: str, actor_address: Optional[str] = None):
    with _conn() as c:
        c.execute(
            "UPDATE dnft_passes SET usage_count=0, revoked_at=NULL WHERE id=?",
            (pass_id,),
        )
    log_event(pass_id, "reset", actor_address, {})


def reassign_pass(
    pass_id: str,
    new_metadata: Dict[str, Any],
    actor_address: str,
):
    """
    Reset + prepare a pass for a new owner assignment.
    """
    now = datetime.now(timezone.utc).isoformat()

    with _conn() as c:
        c.execute(
            """
            UPDATE dnft_passes
            SET
              revoked_at = NULL,
              expires_at = NULL,
              usage_count = 0,
              metadata_json = ?,
              state = 'live',
              state_reason = NULL
            WHERE id=?
            """,
            (json.dumps(new_metadata, ensure_ascii=False), pass_id),
        )

    log_event(
        pass_id,
        "reassigned",
        actor_address,
        {"note": "Pass reassigned to new owner"},
    )




def extend_expiry(pass_id: str, new_expiry: str, actor_address: Optional[str] = None):
    with _conn() as c:
        c.execute("UPDATE dnft_passes SET expires_at=? WHERE id=?", (new_expiry, pass_id))

    rec = get_pass(pass_id) or {}
    if not rec:
        return

    st = (rec.get("state") or "live").strip().lower()
    identity_hash = rec.get("identity_hash") or compute_identity_hash(
        rec["id"], rec["creator_address"], rec["created_at"]
    )
    now = datetime.now(timezone.utc).isoformat()

    log_event(pass_id, "expiry_extended", actor_address, {"expires_at": new_expiry})

    # proof tick
    append_state_proof(
        pass_id=pass_id,
        identity_hash=identity_hash,
        state=st,
        prev_state=st,
        actor_address=actor_address,
        reason="expiry_extended",
        created_at=now,
    )


def increment_usage(pass_id: str, actor_address: Optional[str] = None):
    with _conn() as c:
        c.execute(
            "UPDATE dnft_passes SET usage_count = COALESCE(usage_count, 0) + 1 WHERE id=?",
            (pass_id,),
        )
    log_event(pass_id, "used", actor_address, {})


def _parse_iso_utc(s: str) -> Optional[datetime]:
    if not s:
        return None
    try:
        if s.endswith("Z"):
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        else:
            dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None

def _pass_vertical_id(rec: Dict[str, Any]) -> str:
    """
    Vertical is a skin chosen per pass, stored in metadata_json.properties.
    Falls back to 'generic'.
    """
    meta = rec.get("metadata_json") or rec.get("metadata") or {}
    if isinstance(meta, dict):
        props = meta.get("properties") or {}
        if isinstance(props, dict):
            v = (props.get("vertical") or props.get("vertical_id") or "").strip().lower()
            if v:
                return v
    return "generic"




from typing import Dict, Any
from datetime import datetime, timezone

def compute_verdict(
    rec: Dict[str, Any],
    *,
    current_owner: str | None = None,
    external_facts: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    """
    Canonical verification verdict

    Order of evaluation:
      0. Missing / not found
      1. Revoked
      2. Expired
      3. Custody mismatch (self-held)
      4. Supply-chain / Pharma compliance gate (canonical external_facts)
      5. Conditions failed
      6. Usage exhausted
      7. Aviation maintenance_overdue (vertical skin)
      8. Valid
    """

    now = datetime.now(timezone.utc)
    external_facts = external_facts or {}

    # -------------------------------------------------
    # 0. Missing record
    # -------------------------------------------------
    if not rec:
        return {
            "status": "not_found",
            "ok": False,
            "reason": "Pass not found",
            "vertical": "generic",
            "policy_sha256": "",
            "now": now.isoformat(),
        }

    # Vertical policy pack (skins, not engines)
    try:
        from backend.app.services.verticals import get_policy_sha256
        vertical_id = _pass_vertical_id(rec)
        policy_sha256 = get_policy_sha256(vertical_id)
    except Exception:
        vertical_id = "generic"
        policy_sha256 = ""

    # Helper to ensure every return includes vertical + policy hash
    def _out(d: Dict[str, Any]) -> Dict[str, Any]:
        d.setdefault("vertical", vertical_id)
        d.setdefault("policy_sha256", policy_sha256)
        d.setdefault("now", now.isoformat())
        return d

    # Helpers
    def _as_float(v):
        try:
            if v is None:
                return None
            if isinstance(v, (int, float)):
                return float(v)
            s = str(v).strip()
            if not s:
                return None
            return float(s)
        except Exception:
            return None

    def _attr_value(trait: str):
        """
        Read from:
          - rec['metadata_json']['attributes'][...]
          - rec['metadata_json']['properties'] (fallback)
          - rec top-level keys (fallback)
        """
        if trait in rec and rec.get(trait) not in (None, ""):
            return rec.get(trait)

        meta = rec.get("metadata_json") or rec.get("metadata") or {}
        if isinstance(meta, dict):
            attrs = meta.get("attributes") or []
            if isinstance(attrs, list):
                for a in attrs:
                    if not isinstance(a, dict):
                        continue
                    if (a.get("trait_type") or "").strip().lower() == trait.lower():
                        return a.get("value")
            props = meta.get("properties") or {}
            if isinstance(props, dict) and trait in props:
                return props.get(trait)
        return None

    def _get_props():
        meta = rec.get("metadata_json") or rec.get("metadata") or {}
        if isinstance(meta, dict):
            props = meta.get("properties") or {}
            if isinstance(props, dict):
                return props
        return {}

    # Normalize state
    state = (rec.get("state") or rec.get("status") or "").strip().lower()

    # -------------------------------------------------
    # 1. Revoked (HARD RULE: state overrides everything)
    # -------------------------------------------------
    if state == "revoked":
        return _out({
            "status": "revoked",
            "ok": False,
            "reason": "This pass was revoked",
            "revoked_at": rec.get("revoked_at") or None,
        })

    if rec.get("revoked_at"):
        return _out({
            "status": "revoked",
            "ok": False,
            "reason": "This pass was revoked",
            "revoked_at": rec.get("revoked_at"),
        })

    # -------------------------------------------------
    # 2. Expired
    # -------------------------------------------------
    expires_at = rec.get("expires_at") or ""
    exp_dt = _parse_iso_utc(expires_at)

    if exp_dt and exp_dt <= now:
        return _out({
            "status": "expired",
            "ok": False,
            "reason": "This pass has expired",
            "expires_at": expires_at,
        })

    # -------------------------------------------------
    # 3. Custody enforcement (self-held passes)
    # -------------------------------------------------
    custody = rec.get("custody") or {}
    if isinstance(custody, dict) and custody.get("mode") == "self":
        expected = custody.get("holder_address")
        if expected and current_owner and expected != current_owner:
            return _out({
                "status": "custody_mismatch",
                "ok": False,
                "reason": "Pass is not held by the expected wallet",
                "expected_owner": expected,
                "actual_owner": current_owner,
            })

    # -------------------------------------------------
    # 4. Supply Chain / Pharma compliance gate
    #    ✅ Canonical truth comes from external_facts
    # -------------------------------------------------
    props = _get_props()
    def _as_str(x) -> str:
        return x.strip() if isinstance(x, str) else ""

        # Prefer canonical keys first
    tmpl = _as_str(props.get("template_key")) or _as_str(props.get("template_id"))

        # Back-compat: some older passes stored template as a string
    if not tmpl:
        tmpl = _as_str(props.get("template")) or _as_str(rec.get("template"))

    tmpl = tmpl.lower()

            

    # Prefer external_facts for policy (because your policy route writes there)
    cold_chain_required = bool(external_facts.get("cold_chain_required", False))

    # Fallback to metadata props if you still store it there
    if not cold_chain_required:
        sc_props = props.get("supply_chain") if isinstance(props.get("supply_chain"), dict) else {}
        cold_chain_required = bool(sc_props.get("cold_chain_required", False))

    # Heuristic: pharma if template matches OR cold chain required OR pharma-ish facts exist
    is_pharma = (
        tmpl == "pharma_batch_passport"
        or cold_chain_required
        or ("temp_c" in external_facts)
        or ("recall_status" in external_facts)
        or ("recalled" in external_facts)
    )

    if is_pharma:
        recalled_flag = external_facts.get("recalled")
        recall_status = (external_facts.get("recall_status") or "").strip().lower()

        # --- Recall gate ---
        if recalled_flag is True or recall_status == "recalled":
            return _out({
                "status": "recalled",
                "ok": False,
                "reason": "Batch is recalled",
                "facts": {
                    "recalled": recalled_flag,
                    "recall_status": recall_status or None,
                    "recall_id": external_facts.get("recall_id"),
                    "recall_reason": external_facts.get("recall_reason"),
                },
            })

        # --- Cold-chain explicit fail ---
        if external_facts.get("cold_chain_ok") is False:
            return _out({
                "status": "cold_chain_failed",
                "ok": False,
                "reason": "Cold-chain compliance failed",
                "facts": {
                    "cold_chain_ok": False,
                    "custodian": external_facts.get("custodian"),
                },
            })

        # --- Temperature window gate ---
        temp_c = _as_float(external_facts.get("temp_c"))
        if temp_c is not None:
            # Prefer limits from external_facts
            tmin = _as_float(external_facts.get("temp_min"))
            tmax = _as_float(external_facts.get("temp_max"))

            # Fallback to metadata attrs if needed
            if tmin is None:
                tmin = _as_float(_attr_value("temp_min"))
            if tmax is None:
                tmax = _as_float(_attr_value("temp_max"))

            if tmin is not None and temp_c < tmin:
                return _out({
                    "status": "temp_breach",
                    "ok": False,
                    "reason": f"Temperature below allowed minimum ({temp_c} < {tmin})",
                    "facts": {
                        "temp_c": temp_c,
                        "temp_min": tmin,
                        "temp_max": tmax,
                        "cold_chain_required": cold_chain_required,
                    },
                })

            if tmax is not None and temp_c > tmax:
                return _out({
                    "status": "temp_breach",
                    "ok": False,
                    "reason": f"Temperature above allowed maximum ({temp_c} > {tmax})",
                    "facts": {
                        "temp_c": temp_c,
                        "temp_min": tmin,
                        "temp_max": tmax,
                        "cold_chain_required": cold_chain_required,
                    },
                })

    # -------------------------------------------------
    # 5. Conditions evaluation
    # -------------------------------------------------
    conditions = rec.get("conditions") or []
    for cond in conditions:
        if not isinstance(cond, dict):
            continue
        key = cond.get("key")
        op = cond.get("op", "equals")
        val = cond.get("value")

        if not key:
            continue

        actual = external_facts.get(key)

        failed = False
        if op == "equals" and actual != val:
            failed = True
        elif op == "not_equals" and actual == val:
            failed = True
        elif op == "contains" and (actual is None or val not in str(actual)):
            failed = True
        elif op == "gt" and not (actual is not None and actual > val):
            failed = True
        elif op == "lt" and not (actual is not None and actual < val):
            failed = True

        if failed:
            return _out({
                "status": "conditions_failed",
                "ok": False,
                "reason": f"Condition failed: {key} {op} {val}",
                "condition": cond,
                "actual": actual,
            })

    # -------------------------------------------------
    # 6. Usage limits
    # -------------------------------------------------
    usage_limit = rec.get("usage_limit")
    usage_count = rec.get("usage_count") or 0

    if isinstance(usage_limit, int) and usage_limit > 0:
        if usage_count >= usage_limit:
            return _out({
                "status": "exhausted",
                "ok": False,
                "reason": "Usage limit reached",
                "usage_limit": usage_limit,
                "usage_count": usage_count,
            })

    remaining = (
        max(usage_limit - usage_count, 0)
        if isinstance(usage_limit, int) and usage_limit > 0
        else None
    )

    # -------------------------------------------------
    # 7. Aviation policy: inspection overdue (skin rule)
    # -------------------------------------------------
    if vertical_id == "aviation":
        av = {}
        if isinstance(props, dict):
            av = props.get("aviation") or {}
        if not isinstance(av, dict):
            av = {}

        last_inspected_at = (av.get("last_inspected_at") or external_facts.get("last_inspected_at") or "").strip()
        interval_days = external_facts.get("inspection_interval_days") or av.get("inspection_interval_days") or 365

        last_dt = _parse_iso_utc(last_inspected_at) if last_inspected_at else None
        try:
            interval_days = float(interval_days)
        except Exception:
            interval_days = 365.0

        if last_dt:
            age_days = (now - last_dt).total_seconds() / 86400.0
            if age_days > interval_days:
                return _out({
                    "status": "maintenance_overdue",
                    "ok": False,
                    "reason": "Inspection window expired",
                    "details": {
                        "last_inspected_at": last_inspected_at,
                        "inspection_interval_days": interval_days,
                        "age_days": age_days,
                    },
                })

    # -------------------------------------------------
    # 8. VALID
    # -------------------------------------------------
    return _out({
        "status": "valid",
        "ok": True,
        "reason": None,
        "expires_at": expires_at or None,
        "usage_limit": usage_limit,
        "usage_count": usage_count,
        "usage_remaining": remaining,
    })



def log_event(pass_id: str, event_type: str, actor_address: Optional[str], meta: Dict[str, Any]):
    event_type = (event_type or "").strip() or "unknown"
    with _conn() as c:
        c.execute(
            """
            INSERT INTO dnft_events
            (pass_id, event_type, actor_address, meta_json, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                pass_id,
                event_type,
                actor_address,
                json.dumps(meta or {}, ensure_ascii=False),
                datetime.now(timezone.utc).isoformat(),
            ),
        )


def list_events(pass_id: str) -> List[Dict[str, Any]]:
    with _conn() as c:
        rows = c.execute(
            """
            SELECT id, event_type, actor_address, meta_json, created_at
            FROM dnft_events
            WHERE pass_id=?
            ORDER BY id DESC
            """,
            (pass_id,),
        ).fetchall()

    out: List[Dict[str, Any]] = []
    for r in rows:
        try:
            meta = json.loads(r["meta_json"] or "{}")
        except Exception:
            meta = {}

        # UI-friendly “detail” line (human readable)
        parts = []
        if meta.get("action_type"):
            parts.append(f"action={meta.get('action_type')}")
        if meta.get("xumm_uuid"):
            parts.append(f"xumm={str(meta.get('xumm_uuid'))[:8]}…")
        if meta.get("txid"):
            parts.append(f"tx={str(meta.get('txid'))[:8]}…")
        if meta.get("nft_id"):
            parts.append(f"nft={str(meta.get('nft_id'))[:8]}…")
        if meta.get("uri"):
            parts.append("uri=updated")
        if meta.get("verdict"):
            parts.append(f"verdict={meta.get('verdict')}")

        detail = " • ".join(parts)

        out.append(
            {
                # canonical/raw
                "id": r["id"],
                "event_type": r["event_type"],
                "actor_address": r["actor_address"],
                "meta": meta,
                "created_at": r["created_at"],

                # UI-friendly (matches your audit HTML expectations)
                "event": r["event_type"],
                "detail": detail,
                "ts": r["created_at"],
            }
        )
    return out

def get_latest_event_by_type(pass_id: str, event_type: str):
    with _conn() as c:
        row = c.execute(
            """
            SELECT id, pass_id, event_type, actor_address, meta_json, created_at
            FROM dnft_events
            WHERE pass_id=? AND event_type=?
            ORDER BY id DESC
            LIMIT 1
            """,
            (pass_id, event_type),
        ).fetchone()

    if not row:
        return None

    meta_raw = row[4]
    try:
        meta = json.loads(meta_raw) if isinstance(meta_raw, str) and meta_raw else (meta_raw or {})
        if not isinstance(meta, dict):
            meta = {}
    except Exception:
        meta = {"_raw": meta_raw}

    return {
        "id": row[0],
        "pass_id": row[1],
        "event_type": row[2],
        "actor_address": row[3],
        "meta": meta,
        "created_at": row[5],
    }



def get_event(pass_id: str, event_id: int) -> Optional[Dict[str, Any]]:
    with _conn() as c:
        row = c.execute(
            """
            SELECT id, pass_id, event_type, actor_address, meta_json, created_at
            FROM dnft_events
            WHERE pass_id=? AND id=?
            """,
            (pass_id, event_id),
        ).fetchone()

    if not row:
        return None

    try:
        meta = json.loads(row["meta_json"] or "{}")
    except Exception:
        meta = {}

    return {
        "id": row["id"],
        "pass_id": row["pass_id"],
        "event_type": row["event_type"],
        "actor_address": row["actor_address"],
        "meta": meta,
        "created_at": row["created_at"],
    }



def store_action_uuid(uuid: str, pass_id: str, action_type: str, meta: dict | None = None):
    now = datetime.now(timezone.utc).isoformat()
    meta_json = json.dumps(meta or {}, ensure_ascii=False)

    with _conn() as c:
        c.execute(
            """
            INSERT OR REPLACE INTO dnft_actions (uuid, pass_id, action_type, meta_json, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (uuid, pass_id, (action_type or "").strip().lower(), meta_json, now),
        )


def get_action_by_uuid(uuid: str) -> dict | None:
    with _conn() as c:
        row = c.execute(
            """
            SELECT
                uuid, pass_id, action_type, meta_json, created_at,
                signed, txid, signed_at, updated_at
            FROM dnft_actions
            WHERE uuid=?
            """,
            (uuid,),
        ).fetchone()

    if not row:
        return None

    try:
        meta = json.loads(row[3] or "{}")
        if not isinstance(meta, dict):
            meta = {}
    except Exception:
        meta = {}

    return {
        "uuid": row[0],
        "pass_id": row[1],
        "action_type": row[2],
        "meta": meta,
        "created_at": row[4],
        "signed": bool(row[5]) if row[5] is not None else bool(meta.get("signed")),
        "txid": row[6] or meta.get("txid"),
        "signed_at": row[7] or meta.get("signed_at"),
        "updated_at": row[8],
    }



def mark_action_signed(uuid: str, txid: str | None = None):
    if not uuid:
        return

    conn = _conn()
    try:
        cur = conn.cursor()

        # pull existing meta_json so we don't destroy it
        cur.execute("SELECT meta_json FROM dnft_actions WHERE uuid = ?", (uuid,))
        row = cur.fetchone()

        meta = {}
        if row and row[0]:
            try:
                meta = json.loads(row[0]) if isinstance(row[0], str) else (row[0] or {})
                if not isinstance(meta, dict):
                    meta = {}
            except Exception:
                meta = {}

        meta["signed"] = True
        meta["signed_at"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        if txid:
            meta["txid"] = txid

        cur.execute(
            """
            UPDATE dnft_actions
               SET signed = 1,
                   txid = COALESCE(?, txid),
                   signed_at = COALESCE(signed_at, ?),
                   meta_json = ?
             WHERE uuid = ?
            """,
            (
                txid,
                meta["signed_at"],
                json.dumps(meta, ensure_ascii=False),
                uuid,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def set_action_meta(uuid: str, patch: Dict[str, Any]) -> None:
    """
    Merge `patch` into dnft_actions.meta_json for this uuid (shallow merge).
    Used by webhook to mark action as applied, failed, etc.
    """
    if not uuid:
        return

    now = datetime.now(timezone.utc).isoformat()

    conn = _conn()
    try:
        cur = conn.cursor()

        cur.execute("SELECT meta_json FROM dnft_actions WHERE uuid = ?", (uuid,))
        row = cur.fetchone()

        meta = {}
        if row and row[0]:
            try:
                meta = json.loads(row[0]) if isinstance(row[0], str) else (row[0] or {})
                if not isinstance(meta, dict):
                    meta = {}
            except Exception:
                meta = {}

        if isinstance(patch, dict):
            meta.update(patch)

        cur.execute(
            """
            UPDATE dnft_actions
               SET meta_json = ?,
                   updated_at = ?
             WHERE uuid = ?
            """,
            (json.dumps(meta, ensure_ascii=False), now, uuid),
        )
        conn.commit()
    finally:
        conn.close()


import hashlib
from typing import List

def _sha256_hex(data) -> str:
    """
    Return SHA-256 hex digest for either bytes or str.
    """
    if data is None:
        data = ""
    if isinstance(data, str):
        data = data.encode("utf-8")
    elif not isinstance(data, (bytes, bytearray)):
        data = str(data).encode("utf-8")

    return hashlib.sha256(data).hexdigest()


def _merkle_root_hex(leaves: List[str]) -> str:
    """
    Deterministic Merkle root.
    leaves are hex strings (already hashed leaf IDs).
    If odd, duplicate last.
    """
    if not leaves:
        return _sha256_hex(b"")  # deterministic empty root

    # normalize to bytes
    level = [bytes.fromhex(x) for x in leaves]

    while len(level) > 1:
        nxt = []
        for i in range(0, len(level), 2):
            a = level[i]
            b = level[i + 1] if i + 1 < len(level) else level[i]
            nxt.append(hashlib.sha256(a + b).digest())
        level = nxt

    return level[0].hex()

def list_proof_hashes(pass_id: str) -> List[str]:
    """
    Return proof_hash list in chronological order.
    (You already store proof_hash in dnft_state_proofs)
    """
    with _conn() as c:
        rows = c.execute(
            """
            SELECT proof_hash
            FROM dnft_state_proofs
            WHERE pass_id=?
            ORDER BY id ASC
            """,
            (pass_id,),
        ).fetchall()

    out = []
    for r in rows:
        h = r["proof_hash"] if isinstance(r, dict) else r[0]
        if h:
            out.append(str(h))
    return out

def compute_anchor_root(pass_id: str):
    """
    Returns (root_hash, count, tip)
      - root_hash: merkle root of all proof_hash leaves (hex string)
      - count: number of proofs in the tree
      - tip: last proof_hash (useful for debugging/UI)
    """
    leaves = list_proof_hashes(pass_id) or []
    if not leaves:
        return ("", 0, "")

    root_hash = _merkle_root_hex(leaves)
    tip = leaves[-1]
    return (root_hash, len(leaves), tip)



# Ensure schema is hardened at import time
try:
    ensure_dnft_schema()
except Exception:
    pass



def get_pass_by_uuid(xumm_uuid: str) -> Optional[Dict[str, Any]]:
    """
    Lookup a pass by its Xumm payload UUID.
    Used by the webhook to finalize mint after signing.
    """
    if not xumm_uuid:
        return None

    with _conn() as c:
        row = c.execute(
            "SELECT * FROM dnft_passes WHERE xumm_uuid=?",
            (xumm_uuid,),
        ).fetchone()

    if not row:
        return None

    rec = dict(row)

    # Parse metadata_json if stored as string
    mj = rec.get("metadata_json")
    if isinstance(mj, str) and mj.strip():
        try:
            rec["metadata_json"] = json.loads(mj)
        except Exception:
            pass

    return rec



def mark_action_requested(
    pass_id: str,
    action_type: str,
    actor_address: Optional[str],
    *,
    xumm_uuid: Optional[str] = None,
    target_state: Optional[str] = None,
    uri: Optional[str] = None,
    note: Optional[str] = None,
):
    """
    DB-first action marker:
    - optionally flips state immediately (so UI + rules behave instantly)
    - always logs an audit event with useful metadata

    This is "Option B": XRPL is the confirmation layer, not the state source.
    """
    action_type = (action_type or "").strip().lower() or "unknown"

    meta: Dict[str, Any] = {
        "action_type": action_type,
    }
    if xumm_uuid:
        meta["xumm_uuid"] = xumm_uuid
    if uri:
        meta["uri"] = uri
    if note:
        meta["note"] = note

    # Flip DB state now (optional)
    if target_state:
        set_state(
            pass_id,
            target_state,
            reason=f"{action_type}_requested",
            actor_address=actor_address,
        )

    # Always log
    log_event(
        pass_id=pass_id,
        event_type=f"{action_type}_requested",
        actor_address=actor_address,
        meta=meta,
    )


def mark_action_confirmed(
    pass_id: str,
    action_type: str,
    actor_address: Optional[str],
    *,
    xumm_uuid: Optional[str] = None,
    txid: Optional[str] = None,
    note: Optional[str] = None,
):
    action_type = (action_type or "").strip().lower() or "unknown"

    meta: Dict[str, Any] = {"action_type": action_type}
    if xumm_uuid:
        meta["xumm_uuid"] = xumm_uuid
    if txid:
        meta["txid"] = txid
    if note:
        meta["note"] = note

    log_event(
        pass_id=pass_id,
        event_type=f"{action_type}_confirmed",
        actor_address=actor_address,
        meta=meta,
    )


def set_permissioned_domain_fields(
    pass_id: str,
    *,
    permissioned_domain_id: str | None = None,
    permissioned_domain_purpose: str | None = None,
    credential_issuer: str | None = None,
    credential_type: str | None = None,
    compliance_profile: dict | None = None,
):
    if not pass_id:
        return

    compliance_profile_json = None
    if isinstance(compliance_profile, dict):
        compliance_profile_json = json.dumps(compliance_profile, ensure_ascii=False)

    with _conn() as c:
        c.execute(
            """
            UPDATE dnft_passes
            SET
              permissioned_domain_id = COALESCE(?, permissioned_domain_id),
              permissioned_domain_purpose = COALESCE(?, permissioned_domain_purpose),
              credential_issuer = COALESCE(?, credential_issuer),
              credential_type = COALESCE(?, credential_type),
              compliance_profile_json = COALESCE(?, compliance_profile_json),
              updated_at = COALESCE(?, updated_at)
            WHERE id=?
            """,
            (
                permissioned_domain_id,
                permissioned_domain_purpose,
                credential_issuer,
                credential_type,
                compliance_profile_json,
                _utc_now_iso(),
                pass_id,
            ),
        )
        c.commit()



def apply_signed_action(uuid: str, txid: str | None = None) -> dict:
    """
    When XUMM is signed, apply the action to our DB (state + audit).
    Idempotent: safe to call multiple times.
    """
    action = get_action_by_uuid(uuid)
    if not action:
        return {"ok": False, "error": "Unknown action uuid"}

    pass_id = action.get("pass_id")
    action_type = (action.get("action_type") or "").lower()

    # If your schema has something like applied_at / applied flag, use it.
    # If not, we can still be safely idempotent because revoke/reset/etc can be designed to be repeat-safe.
    # Optional: add a dnft_actions.applied_at column later.

    if action_type == "revoke":
        revoke_pass(pass_id, reason=f"XRPL signed (uuid={uuid})", txid=txid)
        return {"ok": True, "applied": "revoke", "pass_id": pass_id}

    if action_type == "reset":
        reset_pass(pass_id, reason=f"XRPL signed (uuid={uuid})", txid=txid)
        return {"ok": True, "applied": "reset", "pass_id": pass_id}

    if action_type == "extend":
        # If you store the requested expiry somewhere, pull it from your action metadata.
        # If not currently stored, Track C quick win is: don’t apply here; rely on proof.json refresh.
        # BUT best is to store expires_at when you create the action.
        return {"ok": True, "applied": "extend", "pass_id": pass_id, "note": "Extend signed (expiry apply depends on stored target)"}

    if action_type == "reassign":
        # Same note: best is storing reassign payload fields in action metadata.
        return {"ok": True, "applied": "reassign", "pass_id": pass_id, "note": "Reassign signed (apply depends on stored target)"}

    return {"ok": True, "applied": "noop", "pass_id": pass_id, "note": f"Unhandled action_type={action_type}"}
