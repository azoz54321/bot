from __future__ import annotations

import argparse
import asyncio
import time
from dataclasses import dataclass
from typing import Dict, Iterable, List, Sequence, Tuple

import aiohttp
import os

try:
    import orjson as _json  # type: ignore

    def _jloads(data: bytes | str) -> dict:
        if isinstance(data, bytes):
            return _json.loads(data)
        return _json.loads(data.encode("utf-8"))

    def _jdumps(obj: dict) -> bytes:
        return _json.dumps(obj)

except Exception:  # pragma: no cover
    import json as _json  # type: ignore

    def _jloads(data: bytes | str) -> dict:
        if isinstance(data, bytes):
            return _json.loads(data.decode("utf-8"))
        return _json.loads(data)

    def _jdumps(obj: dict) -> bytes:
        return _json.dumps(obj).encode("utf-8")

from nats.aio.client import Client as NATS  # type: ignore

from ..core.config import (
    DEBUG_VERBOSE,
    ONE_SEC_HEARTBEAT_SUBJECT,
    ONE_SEC_HEARTBEAT_WAIT_MS,
)
from ..core.logger import dbg, log_line
from ..core.exchange_info import ExchangeInfo
from ..infra.binance_http import BinanceHTTP
from ..infra.nats_adapter import get_nats_connect_config


_DEBUG_VERBOSE = bool(DEBUG_VERBOSE)
_SUBJECT_PREFIX = "ONE_SEC.usdt."
_SOURCE_LABEL = "one_sec_aggregator"
_BOOK_TICKER_CHUNK = 75
_FLUSH_INTERVAL_SEC = 0.2
_FLUSH_SENTINEL = object()
_STOP_SENTINEL = object()


@dataclass(slots=True)
class SymbolState:
    bucket: int
    open: float
    high: float
    low: float
    close: float
    updates: int
    bid: float
    ask: float
    last_event_ms: int


@dataclass(slots=True)
class SymbolHistory:
    bucket: int
    close: float
    bid: float
    ask: float
    last_event_ms: int


def _chunked(sequence: Sequence[str], size: int) -> Iterable[Tuple[str, ...]]:
    for idx in range(0, len(sequence), size):
        yield tuple(sequence[idx : idx + size])


class OneSecAggregator:
    def __init__(self, symbols: Sequence[str], queue_size: int = 200_000) -> None:
        ordered = tuple(sorted({s.upper() for s in symbols}))
        if not ordered:
            raise ValueError("No symbols provided for aggregation")
        self._symbols = ordered
        self._subjects = {sym: f"{_SUBJECT_PREFIX}{sym}" for sym in self._symbols}
        self._queue: asyncio.Queue[Tuple[object, float, float, int]] = asyncio.Queue(maxsize=queue_size)
        self._states: Dict[str, SymbolState] = {}
        self._history: Dict[str, SymbolHistory] = {}
        self._stop_evt = asyncio.Event()
        self._processor_task: asyncio.Task | None = None
        self._flush_task: asyncio.Task | None = None
        self._stream_tasks: List[asyncio.Task] = []
        self._session: aiohttp.ClientSession | None = None
        self._nc: NATS | None = None
        self._heartbeat_task: asyncio.Task | None = None
        self._heartbeat_subject = ONE_SEC_HEARTBEAT_SUBJECT.strip()
        wait_ms = max(500, int(ONE_SEC_HEARTBEAT_WAIT_MS))
        self._heartbeat_interval = max(0.5, wait_ms / 2000.0)

    async def run(self) -> None:
        config = get_nats_connect_config()
        self._nc = NATS()
        await self._nc.connect(servers=list(config.servers), **config.kwargs)
        self._processor_task = asyncio.create_task(self._process_events())
        self._flush_task = asyncio.create_task(self._flusher())
        if self._heartbeat_subject:
            self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

        session_timeout = aiohttp.ClientTimeout(total=None, sock_read=30)
        self._session = aiohttp.ClientSession(timeout=session_timeout)
        try:
            for chunk in _chunked(self._symbols, _BOOK_TICKER_CHUNK):
                self._stream_tasks.append(asyncio.create_task(self._stream_chunk(chunk)))
            if _DEBUG_VERBOSE:
                dbg(f"[AGG] subscribed to {len(self._symbols)} symbols across {len(self._stream_tasks)} shards")
            await self._stop_evt.wait()
        except asyncio.CancelledError:
            raise
        finally:
            self._stop_evt.set()
            for task in self._stream_tasks:
                task.cancel()
            if self._stream_tasks:
                await asyncio.gather(*self._stream_tasks, return_exceptions=True)
            if self._flush_task is not None:
                self._flush_task.cancel()
                try:
                    await self._flush_task
                except asyncio.CancelledError:
                    pass
            if self._processor_task is not None:
                await self._queue.put((_STOP_SENTINEL, 0.0, 0.0, 0))
                await self._processor_task
            await self._flush_remaining()
            if self._heartbeat_task is not None:
                self._heartbeat_task.cancel()
                try:
                    await self._heartbeat_task
                except asyncio.CancelledError:
                    pass
                except Exception:
                    pass
                finally:
                    self._heartbeat_task = None
            if self._nc is not None:
                try:
                    await self._nc.flush()
                finally:
                    await self._nc.close()
            if self._session is not None:
                await self._session.close()

    def request_stop(self) -> None:
        self._stop_evt.set()

    async def _stream_chunk(self, symbols: Tuple[str, ...]) -> None:
        if self._session is None:
            return
        streams = "/".join(f"{sym.lower()}@bookTicker" for sym in symbols)
        url = f"wss://stream.binance.com:9443/stream?streams={streams}"
        session = self._session
        while not self._stop_evt.is_set():
            try:
                async with session.ws_connect(url, heartbeat=20) as ws:
                    async for msg in ws:
                        if self._stop_evt.is_set():
                            break
                        if msg.type != aiohttp.WSMsgType.TEXT:
                            continue
                        payload = _jloads(msg.data)
                        data = payload.get("data", payload)
                        symbol = (data.get("s") or "").upper()
                        if not symbol:
                            continue
                        try:
                            bid = float(data.get("b") or data.get("B") or 0.0)
                            ask = float(data.get("a") or data.get("A") or 0.0)
                        except Exception:
                            continue
                        evt_ms_raw = data.get("T") or data.get("E") or 0
                        try:
                            evt_ms = int(evt_ms_raw or 0)
                        except Exception:
                            evt_ms = 0
                        if evt_ms <= 0:
                            evt_ms = int(time.time() * 1000)
                        await self._queue.put((symbol, bid, ask, evt_ms))
            except asyncio.CancelledError:
                break
            except Exception as exc:
                if _DEBUG_VERBOSE:
                    dbg(f"[AGG] ws error for shard {len(symbols)} syms: {exc}")
                await asyncio.sleep(1.0)

    async def _process_events(self) -> None:
        while True:
            symbol, bid, ask, evt_ms = await self._queue.get()
            if symbol is _STOP_SENTINEL:
                break
            if symbol is _FLUSH_SENTINEL:
                await self._flush_due(evt_ms)
                continue
            await self._apply_tick(symbol, bid, ask, evt_ms)

    async def _apply_tick(self, symbol: str, bid: float, ask: float, event_ms: int) -> None:
        bucket = event_ms // 1000
        state = self._states.get(symbol)
        if state is None:
            await self._emit_gaps_if_needed(symbol, bucket)
            self._states[symbol] = self._make_state(bucket, bid, ask, event_ms)
            return
        if bucket == state.bucket:
            mid = (bid + ask) * 0.5
            state.close = mid
            state.high = max(state.high, mid)
            state.low = min(state.low, mid)
            state.updates += 1
            state.bid = bid
            state.ask = ask
            state.last_event_ms = event_ms
            return
        if bucket < state.bucket:
            # out-of-order tick; ignore
            return
        await self._emit_state(symbol, state, stale=False)
        self._states.pop(symbol, None)
        await self._emit_gaps_if_needed(symbol, bucket)
        self._states[symbol] = self._make_state(bucket, bid, ask, event_ms)

    async def _emit_gaps_if_needed(self, symbol: str, target_bucket: int) -> None:
        history = self._history.get(symbol)
        if not history:
            return
        gap_start = history.bucket + 1
        if target_bucket <= gap_start:
            return
        close = history.close
        bid = history.bid
        ask = history.ask
        last_ms = history.last_event_ms
        for bucket in range(gap_start, target_bucket):
            await self._emit_gap(symbol, bucket, close, bid, ask, last_ms)
            history = self._history.get(symbol)  # updated by _emit_gap
            if history:
                close = history.close
                bid = history.bid
                ask = history.ask
                last_ms = history.last_event_ms

    async def _emit_state(self, symbol: str, state: SymbolState, stale: bool) -> None:
        if self._nc is None:
            return
        bucket = state.bucket
        bid = state.bid
        ask = state.ask
        mid = (bid + ask) * 0.5 if bid > 0 and ask > 0 else state.close
        spread_bps = ((ask - bid) / mid * 10_000.0) if (bid > 0 and ask > 0 and mid > 0) else 0.0
        payload = {
            "symbol": symbol,
            "epoch_s": bucket,
            "sec_start": bucket * 1_000_000,
            "sec_end": (bucket + 1) * 1_000_000,
            "open": state.open,
            "high": state.high,
            "low": state.low,
            "close": state.close,
            "updates": state.updates,
            "updates_count": state.updates,
            "spread_bps": spread_bps,
            "stale": stale,
            "source": _SOURCE_LABEL,
        }
        subject = self._subjects[symbol]
        await self._nc.publish(subject, _jdumps(payload))
        self._history[symbol] = SymbolHistory(
            bucket=bucket,
            close=state.close,
            bid=bid,
            ask=ask,
            last_event_ms=state.last_event_ms,
        )

    async def _emit_gap(self, symbol: str, bucket: int, close: float, bid: float, ask: float, last_ms: int) -> None:
        if self._nc is None:
            return
        mid = (bid + ask) * 0.5 if bid > 0 and ask > 0 else close
        spread_bps = ((ask - bid) / mid * 10_000.0) if (bid > 0 and ask > 0 and mid > 0) else 0.0
        payload = {
            "symbol": symbol,
            "epoch_s": bucket,
            "sec_start": bucket * 1_000_000,
            "sec_end": (bucket + 1) * 1_000_000,
            "open": close,
            "high": close,
            "low": close,
            "close": close,
            "updates": 0,
            "updates_count": 0,
            "spread_bps": spread_bps,
            "stale": True,
            "source": _SOURCE_LABEL,
        }
        await self._nc.publish(self._subjects[symbol], _jdumps(payload))
        self._history[symbol] = SymbolHistory(
            bucket=bucket,
            close=close,
            bid=bid,
            ask=ask,
            last_event_ms=last_ms,
        )

    async def _flush_due(self, now_ms: int) -> None:
        current_bucket = now_ms // 1000
        pending = list(self._states.items())
        for symbol, state in pending:
            if state.bucket < current_bucket:
                await self._emit_state(symbol, state, stale=False)
                self._states.pop(symbol, None)

    async def _flush_remaining(self) -> None:
        pending = list(self._states.items())
        for symbol, state in pending:
            await self._emit_state(symbol, state, stale=False)
            self._states.pop(symbol, None)

    async def _flusher(self) -> None:
        try:
            while not self._stop_evt.is_set():
                await asyncio.sleep(_FLUSH_INTERVAL_SEC)
                now_ms = int(time.time() * 1000)
                await self._queue.put((_FLUSH_SENTINEL, 0.0, 0.0, now_ms))
        except asyncio.CancelledError:
            pass
        finally:
            now_ms = int(time.time() * 1000)
            await self._queue.put((_FLUSH_SENTINEL, 0.0, 0.0, now_ms))

    @staticmethod
    def _make_state(bucket: int, bid: float, ask: float, event_ms: int) -> SymbolState:
        mid = (bid + ask) * 0.5
        return SymbolState(
            bucket=bucket,
            open=mid,
            high=mid,
            low=mid,
            close=mid,
            updates=1,
            bid=bid,
            ask=ask,
            last_event_ms=event_ms,
        )

    async def _heartbeat_loop(self) -> None:
        try:
            await self._publish_heartbeat()
        except Exception:
            pass
        while not self._stop_evt.is_set():
            try:
                await asyncio.sleep(self._heartbeat_interval)
                await self._publish_heartbeat()
            except asyncio.CancelledError:
                break
            except Exception:
                continue

    async def _publish_heartbeat(self) -> None:
        if not self._heartbeat_subject or self._nc is None:
            return
        if not getattr(self._nc, "is_connected", False):
            return
        payload = {
            "ts_ms": int(time.time() * 1000),
            "pid": os.getpid(),
            "symbols": len(self._symbols),
        }
        try:
            await self._nc.publish(self._heartbeat_subject, _jdumps(payload))
        except Exception:
            pass


async def _resolve_symbols(all_usdt: bool, explicit: Sequence[str]) -> List[str]:
    if explicit:
        return [sym.strip().upper() for sym in explicit if sym.strip()]
    if not all_usdt:
        raise ValueError("No symbols provided; specify --all-usdt or --symbols")
    http = BinanceHTTP()
    exinfo = await ExchangeInfo.aload(http)
    symbols = [s for s in exinfo.usdt_trading_symbols() if exinfo.is_trading(s)]
    symbols.sort()
    return symbols


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Local 1-second aggregator for Binance bookTicker")
    parser.add_argument(
        "--all-usdt",
        action="store_true",
        default=False,
        help="Subscribe to all USDT symbols (default if --symbols not provided)",
    )
    parser.add_argument(
        "--symbols",
        type=str,
        default="",
        help="Comma-separated list of symbols (e.g. WLDUSDT,BNBUSDT). Overrides --all-usdt.",
    )
    return parser.parse_args()


async def _async_main() -> None:
    args = _parse_args()
    explicit_symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    all_usdt = args.all_usdt or (not explicit_symbols)
    symbols = await _resolve_symbols(all_usdt, explicit_symbols)
    aggregator = OneSecAggregator(symbols)
    connect_cfg = get_nats_connect_config()
    if connect_cfg.user_jwt_cb is not None:
        auth_mode = "jwt"
    elif connect_cfg.signature_cb is not None:
        auth_mode = "nkey"
    elif ("user" in connect_cfg.kwargs) and ("password" in connect_cfg.kwargs):
        auth_mode = "userpass"
    else:
        auth_mode = "none"
    log_line(
        f"[AGG-BOOT] streaming {len(symbols)} symbols -> {_SUBJECT_PREFIX}* auth={auth_mode}"
    )
    try:
        await aggregator.run()
    finally:
        aggregator.request_stop()


def main() -> None:
    try:
        asyncio.run(_async_main())
    except KeyboardInterrupt:
        if _DEBUG_VERBOSE:
            dbg("[AGG] interrupted via KeyboardInterrupt")


if __name__ == "__main__":
    main()
