from __future__ import annotations
from typing import Any, Dict, List, Tuple


def _get_fact(facts: Dict[str, Any], key: str) -> Any:
    cur: Any = facts
    for part in str(key).split("."):
        if isinstance(cur, dict) and part in cur:
            cur = cur[part]
        else:
            return None
    return cur


def _to_number(x: Any) -> Any:
    if isinstance(x, (int, float)):
        return x
    if isinstance(x, str):
        s = x.strip()
        try:
            if "." in s:
                return float(s)
            return int(s)
        except Exception:
            return x
    return x


def evaluate_conditions(conditions: List[dict], facts: Dict[str, Any]) -> Tuple[bool, List[dict]]:
    """
    conditions example:
      [{"key":"meta.properties.level","op":"eq","value":"VIP"}]

    Supported ops (normalized):
      eq/equals, neq/not_equals,
      in,
      contains,
      gte, lte, gt, lt,
      truthy, falsy
    """
    failed: List[dict] = []
    if not conditions:
        return True, failed

    # UI-friendly aliases
    OP_ALIAS = {
        "equals": "eq",
        "not_equals": "neq",
        "not-equals": "neq",
        "gte": "gte",
        "lte": "lte",
        "gt": "gt",
        "lt": "lt",
    }

    for c in conditions:
        key = c.get("key")
        op_raw = (c.get("op") or "eq")
        op = OP_ALIAS.get(str(op_raw).lower(), str(op_raw).lower())
        expected = c.get("value")

        actual = _get_fact(facts, key)

        ok = True
        try:
            if op == "eq":
                ok = (actual == expected)
            elif op == "neq":
                ok = (actual != expected)
            elif op == "in":
                ok = actual in (expected or [])
            elif op == "contains":
                ok = (expected in actual) if isinstance(actual, (list, str)) else False
            elif op in ("gte", "lte", "gt", "lt"):
                a = _to_number(actual)
                b = _to_number(expected)
                if a is None or b is None:
                    ok = False
                else:
                    if op == "gte":
                        ok = a >= b
                    elif op == "lte":
                        ok = a <= b
                    elif op == "gt":
                        ok = a > b
                    elif op == "lt":
                        ok = a < b
            elif op == "truthy":
                ok = bool(actual) is True
            elif op == "falsy":
                ok = bool(actual) is False
            else:
                ok = False
        except Exception:
            ok = False

        if not ok:
            failed.append({"key": key, "op": op, "expected": expected, "actual": actual})

    return (len(failed) == 0), failed


def validate_pharma_batch(meta: dict):
    errors = []

    attrs = {a["trait_type"]: a.get("value") for a in meta.get("attributes", [])}

    if "expiry_date" in attrs and "mfg_date" in attrs:
        if attrs["expiry_date"] <= attrs["mfg_date"]:
            errors.append("Expiry date must be after manufacture date.")

    try:
        tmin = float(attrs.get("temp_min"))
        tmax = float(attrs.get("temp_max"))
        if tmin >= tmax:
            errors.append("Temperature min must be less than max.")
    except Exception:
        errors.append("Temperature values must be numeric.")

    try:
        qty = int(attrs.get("initial_qty"))
        if qty <= 0:
            errors.append("Initial quantity must be > 0.")
    except Exception:
        errors.append("Initial quantity must be numeric.")

    return errors


# ==========================================================
# XERTIFY Truth Engine — Verdict Contract (Day 2)
# ==========================================================

# Canonical, machine-actionable verdict statuses.
# Add to this list only by intention (it is part of your public contract).
CANONICAL_VERDICT_STATUSES = {
    "ok",
    "expired",
    "revoked",
    "authority_missing",
    "policy_violation",
    "maintenance_overdue",
    "not_found",
    "unknown",
}


def normalize_verdict(raw: Dict[str, Any] | None, evaluated_at: str) -> Dict[str, Any]:
    """
    Normalize any internal verdict object to the canonical verdict contract:
      {
        "ok": bool,
        "status": <enum>,
        "reason": str,
        "evaluated_at": ISO-8601
      }

    This is the "contract stabilizer" that prevents compute_verdict internal changes
    from breaking partners.
    """
    raw = raw or {}

    ok = bool(raw.get("ok", False))
    status = (raw.get("status") or ("ok" if ok else "unknown")).strip()
    reason = raw.get("reason") or ""

    # Map legacy/internal strings -> canonical enum
    mapping = {
        "valid": "ok",
        "invalid": "policy_violation",
        "expired_at": "expired",
        "expired_time": "expired",
        "revoked_pass": "revoked",
        "not_authorized": "authority_missing",
        "unauthorized": "authority_missing",
        "maintenance_due": "maintenance_overdue",
    }
    status = mapping.get(status, status)

    # Enforce canonical enum
    if status not in CANONICAL_VERDICT_STATUSES:
        status = "unknown"

    # Enforce ok consistency with status (status is primary, ok derives)
    if status == "ok":
        ok = True
    elif status in {
        "expired",
        "revoked",
        "authority_missing",
        "policy_violation",
        "maintenance_overdue",
        "not_found",
    }:
        ok = False

    out = {
        "ok": ok,
        "status": status,
        "reason": reason,
        "evaluated_at": evaluated_at,
    }

    # Preserve extra internal details without letting them break the contract
    # (these are non-contractual and safe to ignore by integrators)
    extra = {k: v for k, v in raw.items() if k not in {"ok", "status", "reason", "ts", "evaluated_at"}}
    if extra:
        out["details"] = extra

    return out
