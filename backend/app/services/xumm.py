from __future__ import annotations

from typing import Dict, Optional, Any
import os
import re



from backend.app.services import xumm

from xumm import XummSdk

from backend.app.core.settings import (
    XUMM_API_KEY,
    XUMM_API_SECRET,
    APP_BASE_URL,
)

# ----------------------------
# Errors
# ----------------------------
class XummConfigError(Exception):
    pass

class XummAuthError(Exception):
    pass

# ----------------------------
# Helpers / validation
# ----------------------------
_uuid_re = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")

def _clean(v: Optional[str]) -> str:
    return (v or "").strip().strip('"').strip("'")

def _valid_uuid(v: str) -> bool:
    return bool(_uuid_re.match(v))

_sdk: Optional[XummSdk] = None

def _read_creds() -> tuple[str, str]:
    # Support both our settings and commonly used env names
    key = _clean(XUMM_API_KEY or os.getenv("XUMM_APIKEY") or os.getenv("XUMM_API_KEY"))
    sec = _clean(XUMM_API_SECRET or os.getenv("XUMM_APISECRET") or os.getenv("XUMM_API_SECRET"))
    if not key or not sec:
        raise XummConfigError("Missing XUMM_API_KEY / XUMM_API_SECRET in .env (no quotes).")
    if not _valid_uuid(key) or not _valid_uuid(sec):
        raise XummConfigError(
            "XUMM credentials must be GUIDs like 8-4-4-4-12 hex (xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx)."
        )
    return key, sec

def get_sdk() -> XummSdk:
    """Singleton XummSdk with validation and friendly errors."""
    global _sdk
    key, sec = _read_creds()
    if _sdk is None:
        try:
            _sdk = XummSdk(key, sec)
            pong = _sdk.ping()
            if getattr(pong.application, "disabled", 0):
                raise XummAuthError("Your XUMM app is disabled/paused in the developer console.")
        except Exception as e:
            _sdk = None
            msg = str(e)
            if "Invalid API secret" in msg:
                raise XummAuthError(
                    "Invalid API secret for this API key. Rotate secret, copy BOTH fields from the same app, update .env and restart."
                )
            raise XummAuthError(f"XUMM auth failed: {e}")
    return _sdk

def get_webhook_url() -> str:
    """
    Where Xumm should POST resolved payload events.
    Prefer explicit env var; fall back to APP_BASE_URL.
    """
    env = (os.getenv("XUMM_WEBHOOK_URL") or "").strip()
    if env:
        return env.rstrip("/")

    # fallback (only works if APP_BASE_URL is publicly reachable!)
    return f"{APP_BASE_URL.rstrip('/')}/studio/actions/xrpl/xumm/webhook"

# ----------------------------
# Public utilities used by routes
# ----------------------------
def xumm_ping() -> Dict[str, Any]:
    sdk = get_sdk()
    pong = sdk.ping()
    return {
        "name": pong.application.name,
        "disabled": pong.application.disabled == 1,
        "uuid": pong.application.uuidv4,
    }

def create_signin_payload(nonce: str) -> Dict[str, Any]:
    """
    Create a SignIn payload for login/auth.
    IMPORTANT: This payload must use the AUTH webhook,
    not the studio / dNFT webhook.
    """
    sdk = get_sdk()

    created = sdk.payload.create({
        "txjson": {
            "TransactionType": "SignIn"
        },

        # ✅ CRITICAL FIX: route SignIn to AUTH webhook only
        "webhookurl": f"{APP_BASE_URL}/auth/xrpl/xumm/webhook",

        "custom_meta": {
            "instruction": "Sign to authenticate with PROJXHUB",
            "blob": {
                "nonce": nonce,
                "type": "auth_signin"  # optional but VERY helpful for debugging
            },
            "return_url": {
                "app": f"{APP_BASE_URL}/auth/return",
                "web": f"{APP_BASE_URL}/auth/return",
            },
        },
    })

    uuid = getattr(created, "uuid", None)
    nxt = getattr(created, "next", None)
    refs = getattr(created, "refs", None)

    if not uuid:
        raise XummAuthError("XUMM created payload without a UUID (unexpected).")

    return {
        "uuid": uuid,
        "deeplink": getattr(nxt, "always", None) if nxt else None,
        "qr_png": getattr(refs, "qr_png", None) if refs else None,
        "ws": getattr(refs, "websocket_status", None) if refs else None,
    }














def create_payment_payload(account: str, destination: str, drops: int, memo_text: str = "DCA") -> Dict[str, Any]:
    """
    Create a Payment payload (self-payment allowed) and return QR/deeplink refs.
    - account: source XRPL address
    - destination: destination XRPL address (may equal account for vaulting)
    - drops: string/int drops amount (1 XRP = 1,000,000 drops)
    """
    sdk = get_sdk()
    tx = {
        "TransactionType": "Payment",
        "Account": account,
        "Destination": destination,
        "Amount": str(drops),
        "Memos": [{
            "Memo": {
                "MemoType": "444341",  # "DCA" hex
                "MemoData": memo_text.encode().hex()
            }
        }]
    }
    created = sdk.payload.create({"txjson": tx})
    uuid = getattr(created, "uuid", None)
    if not uuid:
        raise XummAuthError("XUMM created payment payload without a UUID (unexpected).")
    return {
        "uuid": uuid,
        "qr_png": getattr(created.refs, "qr_png", None),
        "websocket": getattr(created.refs, "websocket_status", None),
        "next_url": getattr(created.next, "always", None),
    }

def get_payload_status(uuid: str) -> Dict[str, Any]:
    """
    Unifies status for both SignIn and Transaction payloads.

    Returns:
      {
        opened: bool,
        resolved: bool,
        signed: bool,
        cancelled: bool,
        expired: bool,
        account: Optional[str],
        txid: Optional[str],
        hex: Optional[str],
        tx_hash: Optional[str],   # backwards-compat alias for hex/txid
        success: bool             # <-- IMPORTANT: auth.py depends on this
      }
    """
    sdk = get_sdk()
    p = sdk.payload.get(uuid)

    # helpers to safely read both dict-like + object-like payloads
    def _get(obj, key, default=None):
        if obj is None:
            return default
        if isinstance(obj, dict):
            return obj.get(key, default)
        return getattr(obj, key, default)

    meta = _get(p, "meta", None)
    response = _get(p, "response", None)

    opened = bool(_get(meta, "opened", False))
    resolved = bool(_get(meta, "resolved", False))
    signed = bool(_get(meta, "signed", False))
    cancelled = bool(_get(meta, "cancelled", False))
    expired = bool(_get(meta, "expired", False))

    # Some SDK versions place signed/account in response instead of meta
    signed = bool(signed or _get(response, "signed", False))

    account = _get(response, "account", None)

    # txid / hex can appear in different places depending on payload type + SDK version
    txid = _get(response, "txid", None) or _get(response, "hash", None)
    hex_blob = _get(response, "hex", None) or _get(response, "txblob", None)

    # If Xumm returns a dict response with nested "result"/"payloadResponse"
    # (happens in some environments), try those too.
    if isinstance(p, dict):
        pr = p.get("payloadResponse") or {}
        if not account:
            account = (p.get("response") or {}).get("account") or pr.get("account")
        if not txid:
            txid = pr.get("txid") or (p.get("response") or {}).get("txid")
        if not signed:
            signed = bool(pr.get("signed")) or bool((p.get("response") or {}).get("signed"))

    # SUCCESS rule:
    # - For SignIn: signed + account
    # - For Tx payloads: signed + (account or txid)
    success = bool(signed and (account or txid))

    return {
        "opened": opened,
        "resolved": resolved,
        "signed": signed,
        "cancelled": cancelled,
        "expired": expired,
        "account": account,
        "txid": txid,
        "hex": hex_blob,
        "tx_hash": txid or hex_blob,   # keep older callers happy
        "success": success,            # <-- this fixes login
    }



class XRPLActionError(Exception):
    pass


def _ensure(val, msg):
    if not val:
        raise XRPLActionError(msg)


# -----------------------------
# Wallet / Payments
# -----------------------------
def create_simple_xrp_payment_payload(
    account: str,
    destination: str,
    drops: int,
    memo_text: str = "PROJXHUB Pay",
) -> Dict[str, Any]:
    _ensure(account, "Missing source account")
    _ensure(drops and int(drops) > 0, "Amount must be > 0")
    dest = destination or account  # allow self-pay as “vault”
    tx = {
        "TransactionType": "Payment",
        "Account": account,
        "Destination": dest,
        "Amount": str(int(drops)),
        "Memos": [
            {
                "Memo": {
                    "MemoType": "50524F4A58485542",  # "PROJXHUB" hex (label)
                    "MemoData": memo_text.encode().hex(),
                }
            }
        ],
    }
    sdk = xumm.get_sdk()
    created = sdk.payload.create({"txjson": tx})
    uuid = getattr(created, "uuid", None)
    if not uuid:
        raise XRPLActionError("XUMM returned no UUID for Payment.")
    return {
        "uuid": uuid,
        "qr_png": getattr(created.refs, "qr_png", None),
        "deeplink": getattr(created.next, "always", None),
        "websocket": getattr(created.refs, "websocket_status", None),
    }


# -----------------------------
# Tokens (IOUs)
# -----------------------------
def create_trustline_payload(
    account: str,
    currency: str,
    issuer: str,
    limit: str = "100000000000",
) -> Dict[str, Any]:
    _ensure(account, "Missing source account")
    _ensure(currency, "Currency required")
    _ensure(issuer, "Issuer account required")

    tx = {
        "TransactionType": "TrustSet",
        "Account": account,
        "LimitAmount": {
            "currency": currency.upper(),
            "issuer": issuer,
            "value": str(limit),
        },
    }
    sdk = xumm.get_sdk()
    created = sdk.payload.create({"txjson": tx})
    uuid = getattr(created, "uuid", None)
    if not uuid:
        raise XRPLActionError("XUMM returned no UUID for TrustSet.")
    return {
        "uuid": uuid,
        "qr_png": getattr(created.refs, "qr_png", None),
        "deeplink": getattr(created.next, "always", None),
        "websocket": getattr(created.refs, "websocket_status", None),
    }


def create_iou_payment_payload(
    account: str,
    destination: str,
    currency: str,
    issuer: str,
    value: str,
    memo_text: str = "PROJXHUB IOU Pay",
) -> Dict[str, Any]:
    _ensure(account, "Missing source account")
    _ensure(destination, "Destination required")
    _ensure(currency and issuer and value, "currency, issuer, value required")

    tx = {
        "TransactionType": "Payment",
        "Account": account,
        "Destination": destination,
        "Amount": {
            "currency": currency.upper(),
            "issuer": issuer,
            "value": str(value),
        },
        "Memos": [
            {
                "Memo": {
                    "MemoType": "50524F4A58485542",
                    "MemoData": memo_text.encode().hex(),
                }
            }
        ],
    }
    sdk = xumm.get_sdk()
    created = sdk.payload.create({"txjson": tx})
    uuid = getattr(created, "uuid", None)
    if not uuid:
        raise XRPLActionError("XUMM returned no UUID for IOU Payment.")
    return {
        "uuid": uuid,
        "qr_png": getattr(created.refs, "qr_png", None),
        "deeplink": getattr(created.next, "always", None),
        "websocket": getattr(created.refs, "websocket_status", None),
    }


# -----------------------------
# NFTs
# -----------------------------
def _to_hex(s: str) -> str:
    try:
        int(s, 16)
        return s
    except Exception:
        return s.encode("utf-8").hex()


def create_nft_mint_payload(
    account: str,
    uri: str,
    flags: Optional[int] = None,
    transfer_fee: Optional[int] = None,
    webhook_url: Optional[str] = None,
    custom_meta: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Creates a XUMM payload for NFTokenMint.

    IMPORTANT:
    - XUMM webhooks won't fire unless you include a webhook URL.
    - Pass webhook_url to register the callback.
    """
    _ensure(account, "Missing source account")
    _ensure(uri, "URI required")

    tx: Dict[str, Any] = {
        "TransactionType": "NFTokenMint",
        "Account": account,
        "URI": _to_hex(uri),
        "NFTokenTaxon": 0,
    }
    if flags is not None:
        tx["Flags"] = flags
    if transfer_fee is not None:
        tx["TransferFee"] = int(transfer_fee)

    sdk = xumm.get_sdk()

    # Base payload body
    payload_body: Dict[str, Any] = {"txjson": tx}

    # ✅ Webhook registration (the missing piece)
    if webhook_url:
        cm = dict(custom_meta or {})
        cm["webhook_url"] = webhook_url
        payload_body["custom_meta"] = cm

    created = sdk.payload.create({
    "txjson": tx,
    "webhook": {
        "url": xumm.get_webhook_url(),   # we’ll add this helper next
        "data": {
            "kind": "dnft_mint"
        }
    }
})
  
    uuid = getattr(created, "uuid", None)
    if not uuid:
        raise XRPLActionError("XUMM returned no UUID for NFTokenMint.")

    return {
        "uuid": uuid,
        "qr_png": getattr(created.refs, "qr_png", None),
        "deeplink": getattr(created.next, "always", None),
        "websocket": getattr(created.refs, "websocket_status", None),
    }


def create_nft_burn_payload(account: str, nft_id: str) -> Dict[str, Any]:
    _ensure(account, "Missing source account")
    _ensure(nft_id, "NFTokenID required")
    tx = {
        "TransactionType": "NFTokenBurn",
        "Account": account,
        "NFTokenID": nft_id,
    }
    sdk = xumm.get_sdk()
    created = sdk.payload.create({"txjson": tx})
    uuid = getattr(created, "uuid", None)
    if not uuid:
        raise XRPLActionError("XUMM returned no UUID for NFTokenBurn.")
    return {
        "uuid": uuid,
        "qr_png": getattr(created.refs, "qr_png", None),
        "deeplink": getattr(created.next, "always", None),
        "websocket": getattr(created.refs, "websocket_status", None),
    }


def create_nft_offer_payload(
    account: str,
    nft_id: str,
    amount_drops: int,
    destination: Optional[str] = None,
    sell: bool = True,
) -> Dict[str, Any]:
    """
    Generic NFTokenCreateOffer helper.
    - amount_drops: integer in drops (0 is allowed)
    - destination: optional destination (for directed offers / send)
    - sell: True => tfSellNFToken, False => buy offer
    """
    _ensure(account, "Missing source account")
    _ensure(nft_id, "NFTokenID required")
    _ensure(amount_drops is not None, "Amount (drops) required")

    tx: Dict[str, Any] = {
        "TransactionType": "NFTokenCreateOffer",
        "Account": account,
        "NFTokenID": nft_id,
        "Amount": str(int(amount_drops)),
    }

    # Flag for sell offers
    if sell:
        # tfSellNFToken = 1
        tx["Flags"] = 1

    if destination:
        tx["Destination"] = destination

    sdk = xumm.get_sdk()
    created = sdk.payload.create({"txjson": tx})
    uuid = getattr(created, "uuid", None)
    if not uuid:
        raise XRPLActionError("XUMM returned no UUID for NFTokenCreateOffer.")
    return {
        "uuid": uuid,
        "qr_png": getattr(created.refs, "qr_png", None),
        "deeplink": getattr(created.next, "always", None),
        "websocket": getattr(created.refs, "websocket_status", None),
    }


def create_nft_send_payload(
    account: str,
    nft_id: str,
    destination: str,
) -> Dict[str, Any]:
    """
    'Send' an NFT by creating a zero-amount sell offer
    directed at a Destination. The recipient then accepts.
    """
    _ensure(destination, "Destination required for NFT send")

    # zero-amount sell offer with destination
    return create_nft_offer_payload(
        account=account,
        nft_id=nft_id,
        amount_drops=0,
        destination=destination,
        sell=True,
    )


def create_nft_cancel_offer_payload(
    account: str,
    offer_id: str,
) -> Dict[str, Any]:
    _ensure(account, "Missing source account")
    _ensure(offer_id, "Offer ID required")

    tx = {
        "TransactionType": "NFTokenCancelOffer",
        "Account": account,
        "NFTokenOffers": [offer_id],
    }

    sdk = xumm.get_sdk()
    created = sdk.payload.create({"txjson": tx})
    uuid = getattr(created, "uuid", None)
    if not uuid:
        raise XRPLActionError("XUMM returned no UUID for NFTokenCancelOffer.")
    return {
        "uuid": uuid,
        "qr_png": getattr(created.refs, "qr_png", None),
        "deeplink": getattr(created.next, "always", None),
        "websocket": getattr(created.refs, "websocket_status", None),
    }


def create_nft_accept_offer_payload(
    account: str,
    offer_id: str,
) -> Dict[str, Any]:
    _ensure(account, "Missing source account")
    _ensure(offer_id, "Offer ID required")

    tx = {
        "TransactionType": "NFTokenAcceptOffer",
        "Account": account,
        "NFTokenOffer": offer_id,
    }

    sdk = xumm.get_sdk()
    created = sdk.payload.create({"txjson": tx})
    uuid = getattr(created, "uuid", None)
    if not uuid:
        raise XRPLActionError("XUMM returned no UUID for NFTokenAcceptOffer.")
    return {
        "uuid": uuid,
        "qr_png": getattr(created.refs, "qr_png", None),
        "deeplink": getattr(created.next, "always", None),
        "websocket": getattr(created.refs, "websocket_status", None),
    }


# -----------------------------
# Escrow
# -----------------------------
def create_escrow_create_payload(
    account: str,
    destination: str,
    amount_drops: int,
    cancel_after_unix: Optional[int] = None,
) -> Dict[str, Any]:
    _ensure(account, "Missing source account")
    _ensure(destination, "Destination required")
    _ensure(amount_drops and int(amount_drops) > 0, "Amount must be > 0")

    tx = {
        "TransactionType": "EscrowCreate",
        "Account": account,
        "Destination": destination,
        "Amount": str(int(amount_drops)),
    }
    if cancel_after_unix:
        # XRPL uses Ripple Epoch (seconds since 2000-01-01). Many UIs pass Unix epoch.
        # If > 946684800, assume Unix and convert.
        if cancel_after_unix > 946684800:
            tx["CancelAfter"] = cancel_after_unix - 946684800
        else:
            tx["CancelAfter"] = cancel_after_unix

    sdk = xumm.get_sdk()
    created = sdk.payload.create({"txjson": tx})
    uuid = getattr(created, "uuid", None)
    if not uuid:
        raise XRPLActionError("XUMM returned no UUID for EscrowCreate.")
    return {
        "uuid": uuid,
        "qr_png": getattr(created.refs, "qr_png", None),
        "deeplink": getattr(created.next, "always", None),
        "websocket": getattr(created.refs, "websocket_status", None),
    }


# -----------------------------
# Account / Identity
# -----------------------------
def create_accountset_flag_payload(account: str, set_flag: int) -> Dict[str, Any]:
    _ensure(account, "Missing source account")
    _ensure(set_flag is not None, "set_flag required")
    tx = {
        "TransactionType": "AccountSet",
        "Account": account,
        "SetFlag": int(set_flag),
    }
    sdk = xumm.get_sdk()
    created = sdk.payload.create({"txjson": tx})
    uuid = getattr(created, "uuid", None)
    if not uuid:
        raise XRPLActionError("XUMM returned no UUID for AccountSet.")
    return {
        "uuid": uuid,
        "qr_png": getattr(created.refs, "qr_png", None),
        "deeplink": getattr(created.next, "always", None),
        "websocket": getattr(created.refs, "websocket_status", None),
    }


def create_nft_set_uri_payload(
    account: str,
    nft_id: str,
    new_uri: str,
    memo: str = "XERTIFY Set URI",
) -> Dict[str, Any]:
    _ensure(account, "Missing source account")
    _ensure(nft_id, "NFTokenID required")
    _ensure(new_uri, "new_uri required")

    tx = {
        "TransactionType": "NFTokenSetURI",
        "Account": account,
        "NFTokenID": nft_id,
        "URI": new_uri.encode("utf-8").hex(),
        "Memos": [
            {"Memo": {"MemoData": memo.encode("utf-8").hex()}}
        ],
    }

    sdk = xumm.get_sdk()
    created = sdk.payload.create({"txjson": tx})

    uuid = getattr(created, "uuid", None)
    if not uuid:
        raise XRPLActionError("XUMM returned no UUID for NFTokenSetURI.")

    return {
        "uuid": uuid,
        "qr_png": getattr(created.refs, "qr_png", None),
        "deeplink": getattr(created.next, "always", None),
        "websocket": getattr(created.refs, "websocket_status", None),
    }


