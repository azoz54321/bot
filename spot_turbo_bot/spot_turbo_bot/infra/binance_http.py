from __future__ import annotations
import aiohttp
import hashlib
import hmac
import time
from decimal import Decimal
from typing import Any, Dict, Optional
from urllib.parse import urlencode

from ..core.config import (
    BINANCE_API_KEY,
    BINANCE_API_SECRET,
    REST_BASE,
    HTTP_TIMEOUT_SEC,
    SIGN_DEBUG,
    DEBUG_VERBOSE,
    require_env,
)
from ..core.logger import dbg


def _normalize_credential(value: str | None) -> str | None:
    if value is None:
        return None
    value = value.strip()
    return value or None


class BinanceHTTP:
    def __init__(self, api_key: str | None = None, api_secret: str | None = None):
        self._api_key = _normalize_credential(api_key if api_key is not None else BINANCE_API_KEY)
        self._api_secret = _normalize_credential(api_secret if api_secret is not None else BINANCE_API_SECRET)

    def _ensure_api_key(self) -> str:
        key = _normalize_credential(self._api_key)
        if not key:
            key = require_env("BINANCE_API_KEY")
        self._api_key = key
        return key

    def _ensure_api_secret(self) -> str:
        secret = _normalize_credential(self._api_secret)
        if not secret:
            secret = require_env("BINANCE_API_SECRET")
        self._api_secret = secret
        return secret

    async def _request(self, method: str, path: str, params: Optional[Dict[str, Any]] = None,
                       sign: bool = False) -> Any:
        base = f"{REST_BASE}{path}"
        headers: Dict[str, str] = {}
        secret_bytes: Optional[bytes] = None
        if sign:
            key = self._ensure_api_key()
            secret = self._ensure_api_secret()
            secret_bytes = secret.encode()
            headers["X-MBX-APIKEY"] = key
        elif self._api_key:
            headers["X-MBX-APIKEY"] = self._api_key
        params = dict(params or {})

        timeout = aiohttp.ClientTimeout(total=HTTP_TIMEOUT_SEC)
        async with aiohttp.ClientSession(timeout=timeout) as sess:
            if sign:
                # Ensure required params and stable encoding
                params.setdefault("timestamp", int(time.time() * 1000))
                params.setdefault("recvWindow", 5000)
                # URL-encode exactly once and sign that exact string
                # Sort keys for stability
                query_str = urlencode(sorted(((k, str(v)) for k, v in params.items())), doseq=True)
                if secret_bytes is None:
                    # Should never happen; defensive guard
                    secret = self._ensure_api_secret()
                    secret_bytes = secret.encode()
                signature = hmac.new(secret_bytes, query_str.encode(), hashlib.sha256).hexdigest()
                final_query = f"{query_str}&signature={signature}"
                if SIGN_DEBUG:
                    dbg(f"HTTP {method} {path} string_to_sign='{query_str}'")
                    dbg(f"HTTP {method} {path} final_query_sent='{final_query}'")
                url = f"{base}?{final_query}"
                if method == "GET":
                    async with sess.get(url, headers=headers) as resp:
                        return await _raise_or_json(resp)
                elif method == "POST":
                    async with sess.post(url, headers=headers) as resp:
                        return await _raise_or_json(resp)
                elif method == "PUT":
                    async with sess.put(url, headers=headers) as resp:
                        return await _raise_or_json(resp)
                elif method == "DELETE":
                    async with sess.delete(url, headers=headers) as resp:
                        return await _raise_or_json(resp)
                else:
                    raise ValueError(f"Unsupported method {method}")
            else:
                # Unsigned public endpoint
                url = base
                if params:
                    url = f"{base}?{urlencode(sorted(((k, str(v)) for k, v in params.items())), doseq=True)}"
                if method == "GET":
                    async with sess.get(url, headers=headers) as resp:
                        return await _raise_or_json(resp)
                elif method == "POST":
                    async with sess.post(url, headers=headers) as resp:
                        return await _raise_or_json(resp)
                elif method == "PUT":
                    async with sess.put(url, headers=headers) as resp:
                        return await _raise_or_json(resp)
                elif method == "DELETE":
                    async with sess.delete(url, headers=headers) as resp:
                        return await _raise_or_json(resp)
                else:
                    raise ValueError(f"Unsupported method {method}")

    # Public endpoints (unsigned)
    async def exchange_info(self) -> Any:
        return await self._request("GET", "/api/v3/exchangeInfo")

    async def klines(self, params: Dict[str, Any]) -> Any:
        return await self._request("GET", "/api/v3/klines", params=params)

    async def server_time(self) -> Any:
        return await self._request("GET", "/api/v3/time")

    async def ticker_price(self, symbol: str) -> float:
        """GET /api/v3/ticker/price?symbol=SYMBOL -> float price"""
        data = await self._request("GET", "/api/v3/ticker/price", params={"symbol": symbol.upper()})
        try:
            return float(data.get("price"))
        except Exception:
            return 0.0

    # Account endpoints (signed)
    async def account(self) -> dict:
        """
        GET /api/v3/account (signed)
        Returns the raw dict from Binance. Optionally prints a USDT summary
        when DEBUG_VERBOSE is enabled.
        """
        data = await self._request("GET", "/api/v3/account", sign=True)
        if DEBUG_VERBOSE:
            try:
                balances = data.get("balances", []) if isinstance(data, dict) else []
                usdt = next((b for b in balances if (b.get("asset") or "").upper() == "USDT"), None)
                if usdt:
                    free = Decimal(str(usdt.get("free", "0") or "0"))
                    locked = Decimal(str(usdt.get("locked", "0") or "0"))
                    total = free + locked
                    dbg(f"[ACC] USDT free={free} locked={locked} total={total}")
                else:
                    dbg("[ACC] USDT balance not found")
            except Exception as e:
                dbg(f"[ACC] error parsing USDT balance: {e}")
        return data

    # Orders
    async def order(self, params: dict) -> dict:
        """POST /api/v3/order (signed)"""
        return await self._request("POST", "/api/v3/order", params=params, sign=True)

    async def order_test(self, params: Dict[str, Any]) -> Any:
        return await self._request("POST", "/api/v3/order/test", params=params, sign=True)

    async def get_order(self, params: Dict[str, Any]) -> Any:
        """GET /api/v3/order (signed)
        Typical usage: { 'symbol': 'BTCUSDT', 'orderId': 123 } or
        { 'symbol': 'BTCUSDT', 'origClientOrderId': 'abc-123' }
        """
        return await self._request("GET", "/api/v3/order", params=params, sign=True)

    # Exits are market-only

    async def cancel_order(self, params: Dict[str, Any]) -> Any:
        return await self._request("DELETE", "/api/v3/order", params=params, sign=True)

    # Exits are market-only

    # User data stream (Spot)
    async def user_stream_listen_key(self) -> str:
        self._ensure_api_key()
        data = await self._request("POST", "/api/v3/userDataStream")
        return data["listenKey"]

    async def user_stream_keepalive(self, listen_key: str) -> None:
        self._ensure_api_key()
        await self._request("PUT", "/api/v3/userDataStream", params={"listenKey": listen_key})

    # Exits are market-only


async def _raise_or_json(resp: aiohttp.ClientResponse) -> Any:
    if resp.status >= 400:
        try:
            data = await resp.json()
        except Exception:
            text = await resp.text()
            raise RuntimeError(f"HTTP {resp.status}: {text}")
        code = data.get("code", resp.status)
        msg = data.get("msg", "")
        # External issue log for rate limits and filter violations
        try:
            c = int(code)
        except Exception:
            c = resp.status
        # no external issue logging
        raise BinanceHTTPError(code=code, msg=msg, status=resp.status)
    # Successful
    ctype = resp.headers.get("Content-Type", "")
    if "application/json" in ctype:
        return await resp.json()
    return await resp.text()


class BinanceHTTPError(Exception):
    def __init__(self, *, code: int | str, msg: str, status: int | None = None):
        super().__init__(f"{code}: {msg}")
        self.code = code
        self.msg = msg
        self.status = status
