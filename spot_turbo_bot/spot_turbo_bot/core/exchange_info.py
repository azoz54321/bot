from __future__ import annotations
import json
import os
from typing import Any, Dict, Optional

from .config import EXCHANGE_INFO_PATH


class ExchangeInfo:
    def __init__(self, data: Dict[str, Any]):
        self._symbols = {}
        self._price_decimals: Dict[str, int] = {}
        self._qty_decimals: Dict[str, int] = {}
        for s in data.get("symbols", []):
            sym = s["symbol"].upper()
            self._symbols[sym] = s
            # Precompute decimals from raw filter strings for robust formatting
            fs = {f["filterType"]: f for f in s.get("filters", [])}
            tick_str = str(fs.get("PRICE_FILTER", {}).get("tickSize", "0.00000001"))
            step_str = str(fs.get("LOT_SIZE", {}).get("stepSize", "0.00000001"))
            self._price_decimals[sym] = _decimals_from_step_like(tick_str)
            self._qty_decimals[sym] = _decimals_from_step_like(step_str)

    @classmethod
    def load_cached(cls) -> "ExchangeInfo":
        if not os.path.exists(EXCHANGE_INFO_PATH):
            raise FileNotFoundError(
                f"exchangeInfo not found at {EXCHANGE_INFO_PATH}. Populate it first."
            )
        with open(EXCHANGE_INFO_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        return cls(data)

    @classmethod
    async def aload(cls, http) -> "ExchangeInfo":
        # Try fetching from REST first, cache file, else fallback to cached file
        try:
            data = await http.exchange_info()
            os.makedirs(os.path.dirname(EXCHANGE_INFO_PATH), exist_ok=True)
            with open(EXCHANGE_INFO_PATH, "w", encoding="utf-8") as f:
                json.dump(data, f)
            return cls(data)
        except Exception:
            return cls.load_cached()

    def get_symbol(self, symbol: str) -> Dict[str, Any]:
        s = self._symbols.get(symbol.upper())
        if not s:
            raise KeyError(f"Symbol {symbol} not in exchangeInfo cache")
        return s

    def is_trading(self, symbol: str) -> bool:
        try:
            return (self.get_symbol(symbol).get("status") == "TRADING")
        except Exception:
            return False

    def usdt_trading_symbols(self) -> list[str]:
        return [
            s for s, meta in self._symbols.items()
            if meta.get("quoteAsset") == "USDT" and meta.get("status") == "TRADING"
        ]

    def oco_allowed(self, symbol: str) -> bool:
        return bool(self.get_symbol(symbol).get("ocoAllowed", False))

    def filters(self, symbol: str) -> Dict[str, Dict[str, Any]]:
        fs: Dict[str, Dict[str, Any]] = {}
        for f in self.get_symbol(symbol).get("filters", []):
            fs[f["filterType"]] = f
        return fs

    def tick_size(self, symbol: str) -> float:
        return float(self.filters(symbol).get("PRICE_FILTER", {}).get("tickSize", "0.00000001"))

    def step_size(self, symbol: str) -> float:
        return float(self.filters(symbol).get("LOT_SIZE", {}).get("stepSize", "0.00000001"))

    def min_qty(self, symbol: str) -> float:
        return float(self.filters(symbol).get("LOT_SIZE", {}).get("minQty", "0"))

    def min_notional(self, symbol: str) -> float:
        return float(self.filters(symbol).get("MIN_NOTIONAL", {}).get("minNotional", "0"))

    def price_decimals(self, symbol: str) -> int:
        return int(self._price_decimals.get(symbol.upper(), 8))

    def qty_decimals(self, symbol: str) -> int:
        return int(self._qty_decimals.get(symbol.upper(), 8))

    def fmt_price(self, symbol: str, price: float) -> str:
        d = self.price_decimals(symbol)
        return f"{price:.{d}f}"

    def fmt_qty(self, symbol: str, qty: float) -> str:
        d = self.qty_decimals(symbol)
        return f"{qty:.{d}f}"


def round_down(value: float, step: float) -> float:
    if step <= 0:
        return value
    return (int(value / step)) * step


def round_to_tick(price: float, tick_size: float) -> float:
    return round_down(price, tick_size)


def round_to_step(qty: float, step_size: float) -> float:
    return round_down(qty, step_size)


def _decimals_from_step_like(step: str) -> int:
    # step like "1", "0.1", "0.00001000". Count non-zero decimals.
    if "." not in step:
        return 0
    frac = step.split(".", 1)[1].rstrip("0")
    return max(0, len(frac))
