
from __future__ import annotations
from typing import Dict, Any, Optional

from backend.app.core.settings import APP_BASE_URL
from backend.app.services import xumm



class XRPLActionError(Exception):
    pass


def _ensure(val, msg):
    if not val:
        raise XRPLActionError(msg)


from typing import Dict, Any, Optional

def create_simple_xrp_payment_payload(
    account: str,
    destination: str,
    drops: int,
    memo_text: str = "PROJXHUB Pay",
    memo: Optional[str] = None,                 # ✅ allow old/new callers
    webhook_url: Optional[str] = None,          # ✅ allow action webhooks
) -> Dict[str, Any]:
    _ensure(account, "Missing source account")
    _ensure(drops and int(drops) > 0, "Amount must be > 0")

    if memo and memo.strip():
        memo_text = memo.strip()

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

    payload_req: Dict[str, Any] = {"txjson": tx}
    payload_req = _attach_webhook(payload_req, webhook_url, None)

    created = sdk.payload.create(payload_req)

    # UUID (support dict OR object)
    if isinstance(created, dict):
        uuid = created.get("uuid") or created.get("uuidv4")
    else:
        uuid = getattr(created, "uuid", None) or getattr(created, "uuidv4", None)

    if not uuid:
        raise XRPLActionError("XUMM returned no UUID for Payment.")

    # --- robust refs/next regardless of dict/object response ---
    if isinstance(created, dict):
        refs = created.get("refs") or {}
        nxt = created.get("next") or {}
        return {
            "uuid": uuid,
            "qr_png": refs.get("qr_png"),
            "deeplink": nxt.get("always"),
            "websocket": refs.get("websocket_status"),
        }

    return {
        "uuid": uuid,
        "qr_png": getattr(getattr(created, "refs", None), "qr_png", None),
        "deeplink": getattr(getattr(created, "next", None), "always", None),
        "websocket": getattr(getattr(created, "refs", None), "websocket_status", None),
    }




def create_trustline_payload(
    account: str,
    currency: str,
    issuer: str,
    limit: str = "100000000000",
    webhook_url: Optional[str] = None,
    custom_meta: Optional[Dict[str, Any]] = None,
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

    payload_body: Dict[str, Any] = {"txjson": tx}
    payload_body = _attach_webhook(payload_body, webhook_url, custom_meta)

    created = sdk.payload.create(payload_body)

    # UUID (support dict OR object)
    if isinstance(created, dict):
        uuid = created.get("uuid") or created.get("uuidv4")
    else:
        uuid = getattr(created, "uuid", None) or getattr(created, "uuidv4", None)

    if not uuid:
        raise XRPLActionError("XUMM returned no UUID for TrustSet.")

    # --- robust refs/next regardless of dict/object response ---
    if isinstance(created, dict):
        refs = created.get("refs") or {}
        nxt = created.get("next") or {}
        return {
            "uuid": uuid,
            "qr_png": refs.get("qr_png"),
            "deeplink": nxt.get("always"),
            "websocket": refs.get("websocket_status"),
        }

    return {
        "uuid": uuid,
        "qr_png": getattr(getattr(created, "refs", None), "qr_png", None),
        "deeplink": getattr(getattr(created, "next", None), "always", None),
        "websocket": getattr(getattr(created, "refs", None), "websocket_status", None),
    }



def create_iou_payment_payload(
    account: str,
    destination: str,
    currency: str,
    issuer: str,
    value: str,
    memo_text: str = "PROJXHUB IOU Pay",
    webhook_url: Optional[str] = None,
    custom_meta: Optional[Dict[str, Any]] = None,
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
                    "MemoData": memo_text.encode("utf-8").hex(),
                }
            }
        ],
    }

    sdk = xumm.get_sdk()

    payload_body: Dict[str, Any] = {"txjson": tx}
    payload_body = _attach_webhook(payload_body, webhook_url, custom_meta)

    created = sdk.payload.create(payload_body)

    # UUID (support dict OR object)
    if isinstance(created, dict):
        uuid = created.get("uuid") or created.get("uuidv4")
    else:
        uuid = getattr(created, "uuid", None) or getattr(created, "uuidv4", None)

    if not uuid:
        raise XRPLActionError("XUMM returned no UUID for IOU Payment.")

    # --- robust refs/next regardless of dict/object response ---
    if isinstance(created, dict):
        refs = created.get("refs") or {}
        nxt = created.get("next") or {}
        return {
            "uuid": uuid,
            "qr_png": refs.get("qr_png"),
            "deeplink": nxt.get("always"),
            "websocket": refs.get("websocket_status"),
        }

    return {
        "uuid": uuid,
        "qr_png": getattr(getattr(created, "refs", None), "qr_png", None),
        "deeplink": getattr(getattr(created, "next", None), "always", None),
        "websocket": getattr(getattr(created, "refs", None), "websocket_status", None),
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
    - Webhooks only fire if the payload includes a webhook URL.
    - If webhook_url is not passed, we fall back to xumm.get_webhook_url()
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
        tx["Flags"] = int(flags)
    if transfer_fee is not None:
        tx["TransferFee"] = int(transfer_fee)

    payload_body: Dict[str, Any] = {"txjson": tx}
    payload_body = _attach_webhook(payload_body, webhook_url, custom_meta)

    sdk = xumm.get_sdk()
    created = sdk.payload.create(payload_body)

    # UUID (support dict OR object)
    if isinstance(created, dict):
        uuid = created.get("uuid") or created.get("uuidv4")
    else:
        uuid = getattr(created, "uuid", None) or getattr(created, "uuidv4", None)

    if not uuid:
        raise XRPLActionError("XUMM returned no UUID for NFTokenMint.")

    # --- robust refs/next regardless of dict/object response ---
    if isinstance(created, dict):
        refs = created.get("refs") or {}
        nxt = created.get("next") or {}
        return {
            "uuid": uuid,
            "qr_png": refs.get("qr_png"),
            "deeplink": nxt.get("always"),
            "websocket": refs.get("websocket_status"),
        }

    return {
        "uuid": uuid,
        "qr_png": getattr(getattr(created, "refs", None), "qr_png", None),
        "deeplink": getattr(getattr(created, "next", None), "always", None),
        "websocket": getattr(getattr(created, "refs", None), "websocket_status", None),
    }




def create_nft_modify_payload(
    account: str,
    nft_id: str,
    new_uri: str,
    memo: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Creates a XUMM payload for NFTokenModify (dNFT update).
    This is the ONLY valid way to update a dNFT URI.
    """
    _ensure(account, "Missing account")
    _ensure(nft_id, "Missing NFTokenID")
    _ensure(new_uri, "Missing new URI")

    tx: Dict[str, Any] = {
        "TransactionType": "NFTokenModify",
        "Account": account,
        "NFTokenID": nft_id,
        "URI": _to_hex(new_uri),
    }

    if memo:
        tx["Memos"] = [
            {
                "Memo": {
                    "MemoType": _to_hex("XERTIFY"),
                    "MemoData": _to_hex(memo),
                }
            }
        ]

    payload_body: Dict[str, Any] = {"txjson": tx}
    payload_body = _attach_webhook(payload_body, None, None)

    sdk = xumm.get_sdk()
    created = sdk.payload.create(payload_body)

    # UUID (support dict OR object)
    if isinstance(created, dict):
        uuid = created.get("uuid") or created.get("uuidv4")
    else:
        uuid = getattr(created, "uuid", None) or getattr(created, "uuidv4", None)

    if not uuid:
        raise XRPLActionError("XUMM returned no UUID for NFTokenModify")

    # --- robust refs/next regardless of dict/object response ---
    if isinstance(created, dict):
        refs = created.get("refs") or {}
        nxt = created.get("next") or {}
        return {
            "uuid": uuid,
            "qr_png": refs.get("qr_png"),
            "deeplink": nxt.get("always"),
            "websocket": refs.get("websocket_status"),
        }

    return {
        "uuid": uuid,
        "qr_png": getattr(getattr(created, "refs", None), "qr_png", None),
        "deeplink": getattr(getattr(created, "next", None), "always", None),
        "websocket": getattr(getattr(created, "refs", None), "websocket_status", None),
    }








def create_nft_offer_payload(
    account: str,
    nft_id: str,
    amount_drops: int,
    destination: Optional[str] = None,
    sell: bool = True,
    webhook_url: Optional[str] = None,
    custom_meta: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    _ensure(account, "Missing source account")
    _ensure(nft_id, "NFTokenID required")
    _ensure(amount_drops is not None, "Amount (drops) required")

    tx: Dict[str, Any] = {
        "TransactionType": "NFTokenCreateOffer",
        "Account": account,
        "NFTokenID": nft_id,
        "Amount": str(int(amount_drops)),
    }

    # tfSellNFToken = 1
    if sell:
        tx["Flags"] = 1

    if destination:
        tx["Destination"] = destination

    payload_body: Dict[str, Any] = {"txjson": tx}
    payload_body = _attach_webhook(payload_body, webhook_url, custom_meta)

    sdk = xumm.get_sdk()
    created = sdk.payload.create(payload_body)

    uuid = None
    if isinstance(created, dict):
        uuid = created.get("uuid") or created.get("uuidv4")
    else:
        uuid = getattr(created, "uuid", None) or getattr(created, "uuidv4", None)

    if not uuid:
        raise XRPLActionError("XUMM returned no UUID for NFTokenCreateOffer.")

    # robust refs/next
    if isinstance(created, dict):
        refs = created.get("refs") or {}
        nxt = created.get("next") or {}
        return {
            "uuid": uuid,
            "qr_png": refs.get("qr_png"),
            "deeplink": nxt.get("always"),
            "websocket": refs.get("websocket_status"),
        }

    return {
        "uuid": uuid,
        "qr_png": getattr(getattr(created, "refs", None), "qr_png", None),
        "deeplink": getattr(getattr(created, "next", None), "always", None),
        "websocket": getattr(getattr(created, "refs", None), "websocket_status", None),
    }


def create_nft_burn_payload(
    account: str,
    nft_id: str,
    webhook_url: Optional[str] = None,
    custom_meta: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    _ensure(account, "Missing source account")
    _ensure(nft_id, "NFTokenID required")

    tx = {
        "TransactionType": "NFTokenBurn",
        "Account": account,
        "NFTokenID": nft_id,
    }

    sdk = xumm.get_sdk()

    payload_body: Dict[str, Any] = {"txjson": tx}
    payload_body = _attach_webhook(payload_body, webhook_url, custom_meta)

    created = sdk.payload.create(payload_body)

    # UUID (support dict OR object)
    if isinstance(created, dict):
        uuid = created.get("uuid") or created.get("uuidv4")
    else:
        uuid = getattr(created, "uuid", None) or getattr(created, "uuidv4", None)

    if not uuid:
        raise XRPLActionError("XUMM returned no UUID for NFTokenBurn.")

    # --- robust refs/next regardless of dict/object response ---
    if isinstance(created, dict):
        refs = created.get("refs") or {}
        nxt = created.get("next") or {}
        return {
            "uuid": uuid,
            "qr_png": refs.get("qr_png"),
            "deeplink": nxt.get("always"),
            "websocket": refs.get("websocket_status"),
        }

    return {
        "uuid": uuid,
        "qr_png": getattr(getattr(created, "refs", None), "qr_png", None),
        "deeplink": getattr(getattr(created, "next", None), "always", None),
        "websocket": getattr(getattr(created, "refs", None), "websocket_status", None),
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

def _wallet_from_seed(seed: str):
    """
    xrpl-py Wallet constructor changed across versions.
    Prefer Wallet.from_seed(seed) when available.
    """
    from xrpl.wallet import Wallet

    seed = (seed or "").strip().strip('"').strip("'")
    if not seed:
        raise ValueError("Missing seed")

    # Newer xrpl-py
    try:
        return Wallet.from_seed(seed)
    except Exception:
        pass

    # Older xrpl-py (some versions accept seed=)
    try:
        return Wallet(seed=seed)
    except Exception:
        pass

    # Very old / edge
    return Wallet(seed)

def create_nft_accept_offer_payload(
    account: str,
    offer_id: str,
    webhook_url: Optional[str] = None,
    custom_meta: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    _ensure(account, "Missing source account")
    _ensure(offer_id, "Offer ID required")

    tx = {
        "TransactionType": "NFTokenAcceptOffer",
        "Account": account,
        "NFTokenOffer": offer_id,
    }

    sdk = xumm.get_sdk()

    payload_body: Dict[str, Any] = {"txjson": tx}
    payload_body = _attach_webhook(payload_body, webhook_url, custom_meta)

    created = sdk.payload.create(payload_body)

    # UUID (support dict OR object)
    if isinstance(created, dict):
        uuid = created.get("uuid") or created.get("uuidv4")
    else:
        uuid = getattr(created, "uuid", None) or getattr(created, "uuidv4", None)

    if not uuid:
        raise XRPLActionError("XUMM returned no UUID for NFTokenAcceptOffer.")

    # --- robust refs/next regardless of dict/object response ---
    if isinstance(created, dict):
        refs = created.get("refs") or {}
        nxt = created.get("next") or {}
        return {
            "uuid": uuid,
            "qr_png": refs.get("qr_png"),
            "deeplink": nxt.get("always"),
            "websocket": refs.get("websocket_status"),
        }

    return {
        "uuid": uuid,
        "qr_png": getattr(getattr(created, "refs", None), "qr_png", None),
        "deeplink": getattr(getattr(created, "next", None), "always", None),
        "websocket": getattr(getattr(created, "refs", None), "websocket_status", None),
    }

def create_nft_cancel_offer_payload(
    account: str,
    offer_id: str,
    webhook_url: Optional[str] = None,
    custom_meta: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    _ensure(account, "Missing source account")
    _ensure(offer_id, "Offer ID required")

    tx = {
        "TransactionType": "NFTokenCancelOffer",
        "Account": account,
        "NFTokenOffers": [offer_id],
    }

    sdk = xumm.get_sdk()

    payload_body: Dict[str, Any] = {"txjson": tx}
    payload_body = _attach_webhook(payload_body, webhook_url, custom_meta)

    created = sdk.payload.create(payload_body)

    # UUID (support dict OR object)
    if isinstance(created, dict):
        uuid = created.get("uuid") or created.get("uuidv4")
    else:
        uuid = getattr(created, "uuid", None) or getattr(created, "uuidv4", None)

    if not uuid:
        raise XRPLActionError("XUMM returned no UUID for NFTokenCancelOffer.")

    # --- robust refs/next regardless of dict/object response ---
    if isinstance(created, dict):
        refs = created.get("refs") or {}
        nxt = created.get("next") or {}
        return {
            "uuid": uuid,
            "qr_png": refs.get("qr_png"),
            "deeplink": nxt.get("always"),
            "websocket": refs.get("websocket_status"),
        }

    return {
        "uuid": uuid,
        "qr_png": getattr(getattr(created, "refs", None), "qr_png", None),
        "deeplink": getattr(getattr(created, "next", None), "always", None),
        "websocket": getattr(getattr(created, "refs", None), "websocket_status", None),
    }

def create_escrow_create_payload(
    account: str,
    destination: str,
    amount_drops: int,
    cancel_after_unix: Optional[int] = None,
    webhook_url: Optional[str] = None,
    custom_meta: Optional[Dict[str, Any]] = None,
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
        # XRPL uses Ripple Epoch; your original conversion is fine
        if cancel_after_unix > 946684800:
            tx["CancelAfter"] = cancel_after_unix - 946684800
        else:
            tx["CancelAfter"] = cancel_after_unix

    sdk = xumm.get_sdk()

    payload_body: Dict[str, Any] = {"txjson": tx}
    payload_body = _attach_webhook(payload_body, webhook_url, custom_meta)

    created = sdk.payload.create(payload_body)

    # UUID (support dict OR object)
    if isinstance(created, dict):
        uuid = created.get("uuid") or created.get("uuidv4")
    else:
        uuid = getattr(created, "uuid", None) or getattr(created, "uuidv4", None)

    if not uuid:
        raise XRPLActionError("XUMM returned no UUID for EscrowCreate.")

    # --- robust refs/next regardless of dict/object response ---
    if isinstance(created, dict):
        refs = created.get("refs") or {}
        nxt = created.get("next") or {}
        return {
            "uuid": uuid,
            "qr_png": refs.get("qr_png"),
            "deeplink": nxt.get("always"),
            "websocket": refs.get("websocket_status"),
        }

    return {
        "uuid": uuid,
        "qr_png": getattr(getattr(created, "refs", None), "qr_png", None),
        "deeplink": getattr(getattr(created, "next", None), "always", None),
        "websocket": getattr(getattr(created, "refs", None), "websocket_status", None),
    }


def create_accountset_flag_payload(
    account: str,
    set_flag: int,
    webhook_url: Optional[str] = None,
    custom_meta: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    _ensure(account, "Missing source account")
    _ensure(set_flag is not None, "set_flag required")

    tx = {
        "TransactionType": "AccountSet",
        "Account": account,
        "SetFlag": int(set_flag),
    }

    sdk = xumm.get_sdk()

    payload_body: Dict[str, Any] = {"txjson": tx}
    payload_body = _attach_webhook(payload_body, webhook_url, custom_meta)

    created = sdk.payload.create(payload_body)

    # UUID (support dict OR object)
    if isinstance(created, dict):
        uuid = created.get("uuid") or created.get("uuidv4")
    else:
        uuid = getattr(created, "uuid", None) or getattr(created, "uuidv4", None)

    if not uuid:
        raise XRPLActionError("XUMM returned no UUID for AccountSet.")

    # --- robust refs/next regardless of dict/object response ---
    if isinstance(created, dict):
        refs = created.get("refs") or {}
        nxt = created.get("next") or {}
        return {
            "uuid": uuid,
            "qr_png": refs.get("qr_png"),
            "deeplink": nxt.get("always"),
            "websocket": refs.get("websocket_status"),
        }

    return {
        "uuid": uuid,
        "qr_png": getattr(getattr(created, "refs", None), "qr_png", None),
        "deeplink": getattr(getattr(created, "next", None), "always", None),
        "websocket": getattr(getattr(created, "refs", None), "websocket_status", None),
    }
def create_nft_set_uri_payload(
    account: str,
    nft_id: str,
    new_uri: str,
    memo: str = "XERTIFY Set URI",
    webhook_url: Optional[str] = None,
    custom_meta: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    _ensure(account, "Missing source account")
    _ensure(nft_id, "NFTokenID required")
    _ensure(new_uri, "new_uri required")

    tx = {
        "TransactionType": "NFTokenSetURI",
        "Account": account,
        "NFTokenID": nft_id,
        "URI": new_uri.encode("utf-8").hex(),
        "Memos": [{"Memo": {"MemoData": memo.encode("utf-8").hex()}}],
    }

    sdk = xumm.get_sdk()

    payload_body: Dict[str, Any] = {"txjson": tx}
    payload_body = _attach_webhook(payload_body, webhook_url, custom_meta)

    created = sdk.payload.create(payload_body)

    # UUID (support dict OR object)
    if isinstance(created, dict):
        uuid = created.get("uuid") or created.get("uuidv4")
    else:
        uuid = getattr(created, "uuid", None) or getattr(created, "uuidv4", None)

    if not uuid:
        raise XRPLActionError("XUMM returned no UUID for NFTokenSetURI.")

    # --- robust refs/next regardless of dict/object response ---
    if isinstance(created, dict):
        refs = created.get("refs") or {}
        nxt = created.get("next") or {}
        return {
            "uuid": uuid,
            "qr_png": refs.get("qr_png"),
            "deeplink": nxt.get("always"),
            "websocket": refs.get("websocket_status"),
        }

    return {
        "uuid": uuid,
        "qr_png": getattr(getattr(created, "refs", None), "qr_png", None),
        "deeplink": getattr(getattr(created, "next", None), "always", None),
        "websocket": getattr(getattr(created, "refs", None), "websocket_status", None),
    }


# --- Backwards-compatible alias (so older imports don't break) ---
def create_nft_modify_uri_payload(
    account: str,
    nft_id: str,
    new_uri: str,
    memo: str = "XERTIFY Update URI",
) -> Dict[str, Any]:
    # We standardise on NFTokenSetURI, but keep this name so imports don't explode.
    return create_nft_set_uri_payload(
        account=account,
        nft_id=nft_id,
        new_uri=new_uri,
        memo=memo,
    )



from xrpl.clients import JsonRpcClient
from xrpl.models.requests import Tx
from backend.app.core.settings import XRPL_RPC_URL

def get_nftoken_id_from_tx(tx_hash: str) -> str | None:
    """
    Resolve the NFTokenID minted in an NFTokenMint transaction
    by inspecting meta.AffectedNodes.
    """
    client = JsonRpcClient(XRPL_RPC_URL)
    resp = client.request(Tx(transaction=tx_hash))

    result = resp.result
    meta = result.get("meta")
    if not meta:
        return None

    for node in meta.get("AffectedNodes", []):
        created = node.get("CreatedNode")
        if not created:
            continue

        if created.get("LedgerEntryType") == "NFTokenPage":
            new_fields = created.get("NewFields", {})
            tokens = new_fields.get("NFTokens", [])
            if tokens:
                return tokens[0].get("NFToken", {}).get("NFTokenID")

    return None
def create_anchor_proof_payload(
    account: str,
    proof_memo: str,
    destination: Optional[str] = None,
    webhook_url: Optional[str] = None,
    custom_meta: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Creates a 1-drop payment with a memo containing a proof hash/root.
    """
    _ensure(account, "Missing source account")

    dest = (destination or "").strip() or account

    tx = {
        "TransactionType": "Payment",
        "Account": account,
        "Destination": dest,
        "Amount": "1",
        "Memos": [
            {
                "Memo": {
                    "MemoType": "58455254494659",  # "XERTIFY"
                    "MemoData": (proof_memo or "").encode("utf-8").hex(),
                }
            }
        ],
    }

    sdk = xumm.get_sdk()

    payload_body: Dict[str, Any] = {"txjson": tx}
    payload_body = _attach_webhook(payload_body, webhook_url, custom_meta)

    created = sdk.payload.create(payload_body)

    # UUID (support dict OR object)
    if isinstance(created, dict):
        uuid = created.get("uuid") or created.get("uuidv4")
    else:
        uuid = getattr(created, "uuid", None) or getattr(created, "uuidv4", None)

    if not uuid:
        raise XRPLActionError("XUMM returned no UUID for anchor payload")

    # Keep your original behavior of returning the raw payload too
    return {"uuid": uuid, "payload": created}


import os
from typing import Optional, Dict, Any

from xrpl.asyncio.clients import AsyncWebsocketClient
from xrpl.asyncio.transaction import (
    autofill,
    sign,
    submit_and_wait,
)
from xrpl.models.transactions import Payment, Memo
from xrpl.utils import str_to_hex
from xrpl.wallet import Wallet


def _env(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()


async def submit_anchor_root_tx(
    *,
    rpc_url: str,
    seed: str,
    destination: str,
    memo_type: str,
    memo_data: str,
    amount_drops: str = "1",
) -> Dict[str, Any]:
    """
    Server-signed XRPL Payment (anchor tx).

    This implementation is tolerant across xrpl-py versions:
      - Uses AsyncJsonRpcClient (works with rippled JSON-RPC endpoints)
      - Prefers submit_and_wait(tx, client, wallet) when available
      - Falls back to autofill+sign+submit if submit_and_wait signature differs
    """
    from xrpl.asyncio.clients import AsyncJsonRpcClient
    from xrpl.models.transactions import Payment
    from xrpl.models.transactions import Memo as XrplMemo

    client = AsyncJsonRpcClient(rpc_url)
    wallet = _wallet_from_seed(seed)

    try:
        tx = Payment(
            account=wallet.classic_address,
            amount=str(int(amount_drops)),
            destination=destination,
            memos=[
                XrplMemo(
                    memo_type=memo_type.encode("utf-8").hex(),
                    memo_data=memo_data.encode("utf-8").hex(),
                )
            ],
        )

        # --- Preferred path (many versions support this) ---
        try:
            from xrpl.asyncio.transaction import submit_and_wait

            resp = await submit_and_wait(tx, client, wallet)
            out = resp.result if hasattr(resp, "result") else (resp or {})
        except TypeError:
            # --- Fallback path for versions where submit_and_wait signature differs ---
            from xrpl.asyncio.transaction import autofill, sign, submit

            filled = await autofill(tx, client)
            signed = sign(filled, wallet)
            resp = await submit(signed, client)
            out = resp.result if hasattr(resp, "result") else (resp or {})

        if not isinstance(out, dict):
            out = {"raw": out}

        engine_result = out.get("engine_result") or (out.get("result") or {}).get("engine_result")
        engine_result_message = out.get("engine_result_message") or (out.get("result") or {}).get("engine_result_message")
        validated = out.get("validated")
        if validated is None and isinstance(out.get("result"), dict):
            validated = out["result"].get("validated")

        # tx hash can live in different places depending on xrpl-py/rippled response
        txid = ""
        try:
            txid = (
                (out.get("tx_json") or {}).get("hash")
                or (out.get("result") or {}).get("tx_json", {}).get("hash")
                or out.get("hash")
                or ""
            )
        except Exception:
            txid = ""

        ok = (engine_result == "tesSUCCESS") or (validated is True)

        return {
            "ok": bool(ok),
            "txid": txid,
            "engine_result": engine_result,
            "engine_result_message": engine_result_message,
            "validated": validated,
            "result": out,
        }

    finally:
        try:
            await client.close()
        except Exception:
            pass


def _final_webhook_url(webhook_url: Optional[str] = None) -> str:
    u = (webhook_url or "").strip()
    return u or (xumm.get_webhook_url() or "").strip()


def _attach_webhook(
    payload_req: Dict[str, Any],
    webhook_url: Optional[str] = None,
    custom_meta: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Attach webhook in BOTH formats for maximum compatibility:
      - webhookurl (string)
      - webhook: { url, data }
    """
    final = _final_webhook_url(webhook_url)
    if not final:
        return payload_req

    payload_req["webhookurl"] = final
    payload_req["webhook"] = {"url": final, "data": (custom_meta or {})}
    return payload_req
