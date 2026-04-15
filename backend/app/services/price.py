from __future__ import annotations

import time
import json
from typing import Dict, List
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError

# super-light in-memory cache
_cache: Dict[str, Dict] = {}
_TTL_SEC = 60  # 1 minute cache (tune as you like)

def _get(url: str) -> dict:
    req = Request(url, headers={"User-Agent": "projxhub/1.0"})
    with urlopen(req, timeout=10) as resp:
        return json.loads(resp.read().decode("utf-8"))

def _cache_get(key: str):
    item = _cache.get(key)
    if not item:
        return None
    if (time.time() - item["t"]) > _TTL_SEC:
        return None
    return item["v"]

def _cache_set(key: str, val):
    _cache[key] = {"v": val, "t": time.time()}

def get_xrp_prices(vs: List[str]) -> Dict[str, float]:
    """
    Returns a dict like {"usd": 0.52, "gbp": 0.43} for the requested list.
    Uses CoinGecko simple price API.
    """
    # normalize vs list
    vs_norm = [v.lower() for v in vs if v]
    key = "coingecko:xrp:" + ",".join(sorted(vs_norm))
    cached = _cache_get(key)
    if cached:
        return cached

    # Build URL
    vs_param = ",".join(vs_norm) or "usd"
    url = f"https://api.coingecko.com/api/v3/simple/price?ids=ripple&vs_currencies={vs_param}"

    try:
        data = _get(url)
        ripple = data.get("ripple", {})
        out = {}
        for k in vs_norm:
            val = ripple.get(k)
            if isinstance(val, (int, float)):
                out[k] = float(val)
        _cache_set(key, out)
        return out
    except (HTTPError, URLError, TimeoutError):
        # return empty (caller can handle lack of price)
        return {}
