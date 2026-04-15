# backend/app/services/xrpl_account.py

from typing import Dict, Any

from xrpl.clients import JsonRpcClient
from xrpl.models.requests import AccountInfo

JSON_RPC_URL = "https://s1.ripple.com:51234"  # public mainnet server
client = JsonRpcClient(JSON_RPC_URL)


def account_overview(address: str) -> Dict[str, Any]:
    """
    Synchronous XRPL account overview.
    NO asyncio.run, NO AsyncJsonRpcClient.
    """
    # Basic account_info request
    req = AccountInfo(
        account=address,
        ledger_index="validated",
        strict=True,
    )

    resp = client.request(req)
    result = resp.result

    account_data = result.get("account_data", {}) or {}

    # Balance is in drops; convert to XRP
    balance_drops = int(account_data.get("Balance", "0"))
    balance_xrp = balance_drops / 1_000_000

    # For now, keep reserve simple / placeholder
    # (You can refine later using server_state or account_objects)
    reserve_xrp = 0.0

    funded = bool(account_data)

    return {
        "account": address,
        "funded": funded,
        "xrp": {
            "total": balance_xrp,
            "available": balance_xrp,  # until you compute reserve properly
            "reserve": reserve_xrp,
        },
    }
