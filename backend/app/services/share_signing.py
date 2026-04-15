import os
import hmac
import hashlib

# NOTE:
# We deliberately read the secret at runtime so changes to env are respected
# without needing a server restart in some deployment setups.

def _get_secret() -> str:
    return os.getenv("XERTIFY_SHARE_SECRET", "") or ""

def require_share_secret():
    if not _get_secret():
        raise RuntimeError("XERTIFY_SHARE_SECRET is not set")

def make_public_id(pass_id: str) -> str:
    """
    Stable, non-guessy-ish public id derived from internal pass_id.
    Keep it deterministic so we can always re-derive it.
    """
    pid = (pass_id or "").strip()
    # Your pass_id is already random hex; prefix avoids collisions with other id types later
    return f"x_{pid}"

# Back-compat aliases (studio.py uses generate_public_id)
def generate_public_id(pass_id: str) -> str:
    return make_public_id(pass_id)

def sign_public_id(public_id: str) -> str:
    require_share_secret()
    msg = (public_id or "").encode("utf-8")
    key = _get_secret().encode("utf-8")
    return hmac.new(key, msg, hashlib.sha256).hexdigest()

def verify_public_id_sig(public_id: str, sig: str) -> bool:
    try:
        expected = sign_public_id(public_id)
        return hmac.compare_digest(expected, str(sig or ""))
    except Exception:
        return False
