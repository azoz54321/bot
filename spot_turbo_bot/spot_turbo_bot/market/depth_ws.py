from __future__ import annotations

import asyncio
import math
import time
from typing import Dict, Iterable, List, Tuple

import aiohttp

try:
    import orjson as _json  # type: ignore

    def jloads(data: bytes | str) -> dict:
        if isinstance(data, bytes):
            return _json.loads(data)
        return _json.loads(data.encode("utf-8"))

except Exception:  # pragma: no cover
    import json as _json  # type: ignore

    def jloads(data: bytes | str) -> dict:
        if isinstance(data, bytes):
            data = data.decode("utf-8")
        return _json.loads(data)


from ..core.config import (
    DEBUG_WS_LOGS,
    DEPTH_BASELINE_N,
    DEPTH_SHARD_SIZE,
    DEPTH_WS_CADENCE_MS,
    DEPTH_WS_RECONNECT_SEC,
    IMPACT_MAX_STALENESS_MS,
)
from ..core.logger import log_line


WS_BASE = "wss://stream.binance.com:9443"
NOWMS = lambda: int(time.time() * 1000)
_DEBUG_WS_LOGS = bool(DEBUG_WS_LOGS)
_DEPTH_BASELINE_N = int(DEPTH_BASELINE_N)
_DEPTH_SHARD_SIZE = int(DEPTH_SHARD_SIZE)
_DEPTH_WS_CADENCE_MS = int(DEPTH_WS_CADENCE_MS)
_DEPTH_WS_RECONNECT_SEC = float(DEPTH_WS_RECONNECT_SEC)
_IMPACT_MAX_STALENESS_MS = int(IMPACT_MAX_STALENESS_MS)


class Book:
    __slots__ = (
        "symbol",
        "tick",
        "step",
        "best_ask",
        "best_bid",
        "asks_px",
        "asks_q",
        "prefix_pq",
        "ts_book",
        "ts_depth",
        "baseline_N",
        "_last_book_arrival",
        "_last_depth_arrival",
    )

    def __init__(self, symbol: str, tick: float, step: float, baseline_N: int = 20) -> None:
        self.symbol = symbol
        self.tick = tick
        self.step = step
        self.best_ask = 0.0
        self.best_bid = 0.0
        self.asks_px: List[float] = []
        self.asks_q: List[float] = []
        self.prefix_pq: List[float] = []
        self.ts_book = 0
        self.ts_depth = 0
        self.baseline_N = baseline_N
        self._last_book_arrival = 0
        self._last_depth_arrival = 0

    def _floor_step(self, qty: float) -> float:
        return math.floor(qty / self.step) * self.step if self.step > 0 else qty

    def rebuild_prefix(self) -> None:
        acc = 0.0
        out: List[float] = []
        for price, qty in zip(self.asks_px, self.asks_q):
            qty_f = self._floor_step(qty)
            acc += price * qty_f
            out.append(acc)
        self.prefix_pq = out


class DepthWS:
    def __init__(self, symbols_meta: Dict[str, dict], baseline_N: int = _DEPTH_BASELINE_N, shard_size: int = _DEPTH_SHARD_SIZE, cadence_ms: int = _DEPTH_WS_CADENCE_MS) -> None:
        self.meta = symbols_meta
        self.books = {
            symbol: Book(symbol, meta["tick"], meta["step"], baseline_N)
            for symbol, meta in symbols_meta.items()
        }
        self.shard_size = shard_size
        self.N = baseline_N
        self.cadence_ms = cadence_ms
        self._tasks: List[asyncio.Task] = []
        self.metrics = {"book_ok": 0, "book_total": 0, "depth_ok": 0, "depth_total": 0}
        self._stale_ms = _IMPACT_MAX_STALENESS_MS
        self._book_last_ms = 0
        self._depth_last_ms = 0

    def get(self, symbol: str) -> Book | None:
        return self.books.get(symbol)

    async def _ws_bookticker(self) -> None:
        url = f"{WS_BASE}/ws/!bookTicker"
        while True:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(url, heartbeat=20) as ws:
                        if _DEBUG_WS_LOGS:
                            log_line("[DEPTH-WS] connected bookTicker")
                        async for msg in ws:
                            if msg.type != aiohttp.WSMsgType.TEXT:
                                continue
                            payload = msg.data
                            decoded = jloads(payload if isinstance(payload, bytes) else payload.encode("utf-8"))
                            data = decoded.get("data", decoded)
                            symbol = str(data.get("s") or "").upper()
                            ask = data.get("a") or data.get("A")
                            bid = data.get("b") or data.get("B")
                            if not symbol or symbol not in self.books or not ask or not bid:
                                continue
                            now = NOWMS()
                            if self._book_last_ms > 0:
                                dt = now - self._book_last_ms
                                if dt <= self._stale_ms:
                                    self.metrics["book_ok"] += 1
                            self.metrics["book_total"] += 1
                            self._book_last_ms = now
                            book = self.books[symbol]
                            if book._last_book_arrival == 0 and _DEBUG_WS_LOGS:
                                log_line(f"[DEPTH-WS] first book tick {symbol}")
                            book._last_book_arrival = now
                            book.best_ask = float(ask)
                            book.best_bid = float(bid)
                            book.ts_book = now
            except Exception as exc:
                if _DEBUG_WS_LOGS:
                    log_line(f"[DEPTH-WS] bookTicker error {exc}; reconnecting in {_DEPTH_WS_RECONNECT_SEC:.1f}s")
                await asyncio.sleep(_DEPTH_WS_RECONNECT_SEC)

    async def _ws_depth_shard(self, symbols: Iterable[str], depth_levels: int) -> None:
        stream = "/".join(f"{sym.lower()}@depth{depth_levels}@{self.cadence_ms}ms" for sym in symbols)
        url = f"{WS_BASE}/stream?streams={stream}"
        shard_symbols = list(symbols)
        while True:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(url, heartbeat=20) as ws:
                        if _DEBUG_WS_LOGS:
                            log_line(f"[DEPTH-WS] connected depth{depth_levels} for {len(shard_symbols)} syms")
                        async for msg in ws:
                            if msg.type != aiohttp.WSMsgType.TEXT:
                                continue
                            payload = msg.data
                            decoded = jloads(payload if isinstance(payload, bytes) else payload.encode("utf-8"))
                            data = decoded.get("data", decoded)
                            stream_name = decoded.get("stream") or ""
                            symbol = data.get("s") or (stream_name.split("@")[0].upper() if stream_name else None)
                            if not symbol or symbol not in self.books:
                                continue
                            asks = data.get("a") or data.get("asks") or []
                            bids = data.get("b") or data.get("bids") or []
                            now = NOWMS()
                            if self._depth_last_ms > 0:
                                dt = now - self._depth_last_ms
                                if dt <= self._stale_ms:
                                    self.metrics["depth_ok"] += 1
                            self.metrics["depth_total"] += 1
                            self._depth_last_ms = now
                            ask_pairs: List[Tuple[float, float]] = []
                            top_bid: float | None = None
                            for price, qty in asks:
                                try:
                                    fp = float(price)
                                    fq = float(qty)
                                except Exception:
                                    continue
                                if fp > 0 and fq > 0:
                                    ask_pairs.append((fp, fq))
                            ask_pairs.sort(key=lambda item: item[0])
                            book = self.books[symbol]
                            if book._last_depth_arrival == 0 and _DEBUG_WS_LOGS:
                                log_line(f"[DEPTH-WS] first depth tick {symbol}")
                            book._last_depth_arrival = now
                            if bids:
                                try:
                                    # bids arrive as [price, qty]; assume sorted descending
                                    top_bid = float(bids[0][0])
                                except Exception:
                                    top_bid = None
                            if ask_pairs:
                                book.asks_px = [price for price, _ in ask_pairs][:depth_levels]
                                book.asks_q = [qty for _, qty in ask_pairs][:depth_levels]
                                book.rebuild_prefix()
                                if book.asks_px:
                                    book.best_ask = book.asks_px[0]
                                    book.ts_book = now
                            if top_bid and top_bid > 0.0:
                                book.best_bid = top_bid
                                book.ts_book = now
                            book.ts_depth = now
            except Exception as exc:
                if _DEBUG_WS_LOGS:
                    log_line(f"[DEPTH-WS] depth shard error {exc}; reconnecting in {_DEPTH_WS_RECONNECT_SEC:.1f}s")
                await asyncio.sleep(_DEPTH_WS_RECONNECT_SEC)

    async def start(self) -> None:
        symbols = list(self.books.keys())
        for idx in range(0, len(symbols), self.shard_size):
            shard = symbols[idx : idx + self.shard_size]
            self._tasks.append(asyncio.create_task(self._ws_depth_shard(shard, self.N)))
        self._tasks.append(asyncio.create_task(self._ws_bookticker()))

    def stats_snapshot(self) -> Dict[str, float]:
        metrics = self.metrics

        def ratio(ok: int, total: int) -> float:
            return (ok / total) if total > 0 else 0.0

        return {
            "book_ratio": ratio(metrics.get("book_ok", 0), metrics.get("book_total", 0)),
            "depth_ratio": ratio(metrics.get("depth_ok", 0), metrics.get("depth_total", 0)),
            "book_ok": float(metrics.get("book_ok", 0)),
            "book_total": float(metrics.get("book_total", 0)),
            "depth_ok": float(metrics.get("depth_ok", 0)),
            "depth_total": float(metrics.get("depth_total", 0)),
        }

    def reset_metrics(self) -> None:
        self.metrics = {"book_ok": 0, "book_total": 0, "depth_ok": 0, "depth_total": 0}
        self._book_last_ms = 0
        self._depth_last_ms = 0

    async def stop(self) -> None:
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
