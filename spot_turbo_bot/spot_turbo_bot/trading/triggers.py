from __future__ import annotations
import asyncio
try:
    import ujson as json
except Exception:  # pragma: no cover
    import json
from dataclasses import dataclass
from typing import Dict, Optional, List, Iterable

import time
import websockets

from ..core.config import (
    TRIGGER_PCT,
    TRIGGER_SYMBOLS,
    TRIGGER_SYMBOLS_LIST,
    WS_MARKET_BASE,
    WS_BASE,
    FAST_ALL_USDT,
    WS_DEBOUNCE_MS,
    WS_TICK_FILTER,
    WS_COMBINED_CHUNK,
    WARMUP_SEC,
    ENABLE_SERVER_TIME_SYNC,
    SERVER_TIME_SYNC_SEC,
    OPEN_SOURCE,
    OPEN_FALLBACK_MS,
    KLINE_SCOPE,
)
from .trading_engine import TradingEngine, BuyRequest
from ..core.exchange_info import ExchangeInfo, round_to_tick
from ..runners.diag import touch_activity


@dataclass
class PriceSnapshot:
    open_1m: Optional[float] = None
    minute_target_px: Optional[float] = None
    last_ask: Optional[float] = None
    minute_ep: Optional[int] = None
    # Synthetic 1s candle tracking
    sec_ep: Optional[int] = None
    sec_last_mid: Optional[float] = None
    # First-touch logic within the minute
    first_touch_sec_ep: Optional[int] = None
    ignore_until_next_minute: bool = False
    # Hybrid/kline open tracking
    open_source_used: Optional[str] = None  # 'kline' | 'book'
    open_book_first: Optional[float] = None
    open_kline_last: Optional[float] = None
    fallback_deadline_mono: Optional[float] = None


# Static blacklist for prohibited USDT pairs (leave empty by default)
# Example: {"BTCUSDT", "ETHUSDT"}
HARAM_USDT: set[str] = set({
    # ط§ظ„ظ‚ط§ط¦ظ…ط© ط§ظ„ط£طµظ„ظٹط©
    "BBUSDT", "WBETHUSDT", "WNXMUSDT", "RENUSDT", "XVSUSDT", "SHIBUSDT", "KMNOUSDT",
    "REZUSDT", "OMNIUSDT", "CAKEUSDT", "BZRXUSDT", "BETAUSDT", "FRONTUSDT", "RAMPUSDT",
    "PERPUSDT", "KAVAUSDT", "ALPACAUSDT", "RUNEUSDT", "DFUSDT", "OMUSDT", "MLNUSDT",
    "YFIUSDT", "GNOUSDT", "MIRUSDT", "BADGERUSDT", "TROYUSDT", "ZROUSDT", "AKROUSDT",
    "BURGERUSDT", "SUNUSDT", "MBOXUSDT", "FARMUSDT", "WBTCUSDT", "TUTUSDT", "NILUSDT",
    "REDUSDT", "LAYERUSDT", "TSTUSDT", "BERAUSDT", "BIOUSDT", "CGPTUSDT", "PENGUUSDT",
    "VANAUSDT", "ACXUSDT", "ORCAUSDT", "THEUSDT", "BMTUSDT", "CETUSUSDT", "VIRTUALUSDT",
    "KERNELUSDT", "INITUSDT", "SIGNUSDT", "SYRUPUSDT", "AIXBTUSDT", "USD1USDT",
    "SAHARAUSDT", "PUMPUSDT",

    # ط§ظ„ط£ط²ظˆط§ط¬ ط§ظ„ظƒط¨ظٹط±ط© ط§ظ„ظ„ظٹ ط§طھظپظ‚ظ†ط§ ظ†ط¶ظٹظپظ‡ط§ ظ„ط£ظ†ظ‡ط§ ط³ظٹظˆظ„طھظ‡ط§ ط¹ط§ظ„ظٹط© ط¬ط¯ظ‹ط§
    "XRPUSDT", "ADAUSDT", "SOLUSDT", "DOGEUSDT",
    "LTCUSDT", "LINKUSDT", "MATICUSDT", "TRXUSDT", "UNIUSDT", "DOTUSDT", "AVAXUSDT",
    "USDCUSDT", "BUSDUSDT"
}
)
    

class TriggerRunner:
    def __init__(self, engine: TradingEngine, exinfo: ExchangeInfo, symbols: Optional[List[str]] = None,
                 trigger_pct: float = TRIGGER_PCT, allowed_usdt: Optional[Iterable[str]] = None,
                 audit=None):
        self.engine = engine
        self.exinfo = exinfo
        self.audit = audit
        self.audit = audit
        # Symbols: if includes special token 'ALL_USDT', subscribe to entire USDT trading list
        default_syms = TRIGGER_SYMBOLS_LIST or ([TRIGGER_SYMBOLS.upper()] if TRIGGER_SYMBOLS else [])
        syms = [s.upper() for s in (symbols or default_syms)]
        self.use_all_usdt = any(s in {"ALL_USDT", "*"} for s in syms)
        self.symbols = [] if self.use_all_usdt else syms
        self.trigger_pct = trigger_pct
        self._snap: Dict[str, PriceSnapshot] = {}
        for s in (self.symbols or []):
            self._snap[s] = PriceSnapshot()
        self._pending: set[str] = set()  # avoid duplicate enqueues
        self._stop_evt = asyncio.Event()
        self._book_ws_task: Optional[asyncio.Task] = None
        self._book_ws_tasks: list[asyncio.Task] = []
        self._worker_task: Optional[asyncio.Task] = None
        self._queue: asyncio.Queue[tuple[str, float, float, str]] = asyncio.Queue()
        # Server clock sync
        self._clock_task: Optional[asyncio.Task] = None
        self._server_offset_s: float = 0.0
        # Fast ALL_USDT options
        self.allowed_usdt: list[str] = sorted({s.upper() for s in (allowed_usdt or [])})
        self._warmup_sec: float = WARMUP_SEC
        self._debounce_ms: int = WS_DEBOUNCE_MS
        self._last_handled_ts: Dict[str, float] = {}
        self._last_mid: Dict[str, float] = {}
        self.ws_base: str = WS_MARKET_BASE
        # Kline streams
        self._kline_ws_task: Optional[asyncio.Task] = None
        self._kline_ws_tasks: list[asyncio.Task] = []
        self._kline_symbol_tasks: dict[str, asyncio.Task] = {}
        self._kline_watchlist: list[str] = []
        # Metrics (silent)
        self._open_source_counts: Dict[str, int] = {"kline": 0, "book": 0}
        self._drift_samples: list[float] = []  # |open_kline - open_book|
        self._kline_delay_ms: list[int] = []
        # Market refresh task for ALL_USDT symbol set
        self._market_refresh_task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        self._stop_evt.clear()
        if self.use_all_usdt:
            await self._start_all_usdt_combined()
        else:
            self._book_ws_task = asyncio.create_task(self._run_bookticker_ws())
        self._worker_task = asyncio.create_task(self._worker())
        self._clock_task = None
        if ENABLE_SERVER_TIME_SYNC and SERVER_TIME_SYNC_SEC > 0:
            self._clock_task = asyncio.create_task(self._sync_server_clock())
        # KLINE subscriptions disabled
        # Periodic refresh of USDT list under ALL_USDT mode
        if self.use_all_usdt:
            self._market_refresh_task = asyncio.create_task(self._periodic_market_refresh())

    async def stop(self) -> None:
        self._stop_evt.set()
        for t in (self._book_ws_task, self._worker_task, self._clock_task):
            if t:
                t.cancel()
        for t in self._book_ws_tasks:
            t.cancel()
        if self._kline_ws_task:
            self._kline_ws_task.cancel()
        for t in self._kline_ws_tasks:
            t.cancel()
        for t in list(self._kline_symbol_tasks.values()):
            t.cancel()
        if self._market_refresh_task:
            self._market_refresh_task.cancel()

    def _server_minute_epoch(self) -> int:
        t = time.time() + self._server_offset_s
        t_int = int(t)
        return t_int - (t_int % 60)

    def _server_second_epoch(self) -> int:
        t = time.time() + self._server_offset_s
        return int(t)

    async def _sync_server_clock(self) -> None:
        # Optional server time sync using REST /time outside WS loops
        interval = max(1, SERVER_TIME_SYNC_SEC)
        from ..infra.binance_http import BinanceHTTP
        http = BinanceHTTP()
        try:
            while not self._stop_evt.is_set():
                try:
                    local_send = time.time()
                    data = await http.server_time()
                    local_recv = time.time()
                    server_ms = float(data.get("serverTime", 0.0))
                    if server_ms:
                        # Approximate offset using midpoint of RTT
                        midpoint = (local_send + local_recv) / 2.0
                        self._server_offset_s = (server_ms / 1000.0) - midpoint
                except Exception:
                    pass
                await asyncio.sleep(interval)
        except asyncio.CancelledError:
            return

    async def _run_bookticker_ws(self) -> None:
        # Combined stream for specified symbols only
        if not self.symbols:
            return
        streams = "/".join(f"{s.lower()}@bookTicker" for s in self.symbols)
        url = f"{WS_MARKET_BASE}?streams={streams}"
        while not self._stop_evt.is_set():
            try:
                start_ts = time.monotonic()
                async with websockets.connect(
                    url,
                    ping_interval=20,
                    ping_timeout=20,
                    max_queue=1,
                    compression=None,
                ) as ws:
                    async for raw in ws:
                        try:
                            msg = json.loads(raw)
                        except Exception:
                            continue
                        touch_activity("md_ws")
                        data = msg.get("data") or msg
                        s = (data.get("s") or data.get("symbol") or "").upper()
                        if not s:
                            continue
                        try:
                            ask = float(data.get("a"))
                            bid = float(data.get("b"))
                        except Exception:
                            continue
                        try:
                            evt_ms = int(data.get("E") or 0)
                        except Exception:
                            evt_ms = None
                        self._handle_md_tick(s, bid, ask, start_ts, evt_ms)
            except Exception:
                await asyncio.sleep(1.0)

    def _build_combined_urls(self, symbols: list[str], chunk: int) -> list[str]:
        parts = [sym.lower() + "@bookTicker" for sym in symbols]
        urls: list[str] = []
        for i in range(0, len(parts), chunk):
            seg = "/".join(parts[i:i+chunk])
            urls.append(f"{self.ws_base}?streams={seg}")
        return urls

    def _build_combined_kline_urls(self, symbols: list[str], chunk: int) -> list[str]:
        parts = [sym.lower() + "@kline_1m" for sym in symbols]
        urls: list[str] = []
        for i in range(0, len(parts), chunk):
            seg = "/".join(parts[i:i+chunk])
            urls.append(f"{self.ws_base}?streams={seg}")
        return urls

    async def _start_kline_streams(self) -> None:
        # watchlist: use provided symbols; all_usdt: use allowed_usdt chunked
        if KLINE_SCOPE == "watchlist":
            # Dynamic per-symbol tasks created on demand upon seen_cross
            return
        else:
            chunk = max(50, min(WS_COMBINED_CHUNK, 250))
            symbols = self.allowed_usdt
            urls = self._build_combined_kline_urls(symbols, chunk)
            for url in urls:
                self._kline_ws_tasks.append(asyncio.create_task(self._run_kline_ws(url)))

    async def _run_kline_ws(self, url: str) -> None:
        while not self._stop_evt.is_set():
            try:
                async with websockets.connect(
                    url,
                    ping_interval=20,
                    ping_timeout=20,
                    max_queue=1,
                    compression=None,
                ) as ws:
                    async for raw in ws:
                        try:
                            msg = json.loads(raw)
                        except Exception:
                            continue
                        touch_activity("kline_ws")
                        data = msg.get("data") or msg
                        # Expect combined stream wrapper with { stream, data }
                        s = (data.get("s") or data.get("symbol") or "").upper()
                        k = data.get("k") or {}
                        if not s or not k:
                            # Some servers send e="kline" at top-level
                            if (data.get("e") or "").lower() == "kline":
                                s = (data.get("s") or "").upper()
                                k = data.get("k") or {}
                            else:
                                continue
                        try:
                            o = float(k.get("o")) # type: ignore
                            t_ms = int(k.get("t") or 0)
                        except Exception:
                            continue
                        self._handle_kline_open(s, o, t_ms) # type: ignore
            except Exception:
                await asyncio.sleep(1.0)

    # Dynamic per-symbol kline WS for watchlist scope
    async def _run_kline_symbol_ws(self, url: str, s: str) -> None:
        while not self._stop_evt.is_set():
            try:
                async with websockets.connect(
                    url,
                    ping_interval=20,
                    ping_timeout=20,
                    max_queue=1,
                    compression=None,
                ) as ws:
                    async for raw in ws:
                        try:
                            data = json.loads(raw)
                        except Exception:
                            continue
                        touch_activity("kline_ws")
                        if (data.get("e") or "").lower() != "kline":
                            continue
                        k = data.get("k") or {}
                        try:
                            o = float(k.get("o")) # type: ignore
                            t_ms = int(k.get("t") or 0)
                        except Exception:
                            continue
                        self._handle_kline_open(s, o, t_ms) # type: ignore
            except Exception:
                await asyncio.sleep(1.0)

    async def _ensure_kline_for_symbol(self, symbol: str) -> None:
        if KLINE_SCOPE != "watchlist":
            return
        s = symbol.upper()
        if s in self._kline_symbol_tasks:
            # Duplicate subscribe prevented (silent)
            # refresh LRU order
            try:
                self._kline_watchlist.remove(s)
            except ValueError:
                pass
            self._kline_watchlist.append(s)
            return
        # LRU cap 100
        if len(self._kline_watchlist) >= 100:
            old = self._kline_watchlist.pop(0)
            t = self._kline_symbol_tasks.pop(old, None)
            if t:
                t.cancel()
            # silent
        self._kline_watchlist.append(s)
        stream = f"{s.lower()}@kline_1m"
        url = f"{WS_BASE}/{stream}"
        task = asyncio.create_task(self._run_kline_symbol_ws(url, s))
        self._kline_symbol_tasks[s] = task

    def _drop_kline_symbol(self, symbol: str) -> None:
        s = symbol.upper()
        t = self._kline_symbol_tasks.pop(s, None)
        if t:
            t.cancel()
        try:
            self._kline_watchlist.remove(s)
        except ValueError:
            pass

    async def _periodic_market_refresh(self) -> None:
        try:
            while not self._stop_evt.is_set():
                await asyncio.sleep(12 * 60)
                try:
                    new_list = sorted({x.upper() for x in self.exinfo.usdt_trading_symbols() if x.upper() not in HARAM_USDT})
                except Exception:
                    continue
                if sorted(self.allowed_usdt) != new_list:
                    # restart bookTicker streams
                    for t in self._book_ws_tasks:
                        t.cancel()
                    self._book_ws_tasks.clear()
                    self.allowed_usdt = new_list
                    await self._start_all_usdt_combined()
                    # restart kline all_usdt streams
                    if OPEN_SOURCE in {"hybrid", "kline"} and KLINE_SCOPE == "all_usdt":
                        for t in self._kline_ws_tasks:
                            t.cancel()
                        self._kline_ws_tasks.clear()
                        await self._start_kline_streams()
        except asyncio.CancelledError:
            return

    # -------- Fast ALL_USDT: combined streams by chunks --------
    async def _start_all_usdt_combined(self) -> None:
        chunk = max(50, min(WS_COMBINED_CHUNK, 250))
        symbols = self.allowed_usdt
        urls = self._build_combined_urls(symbols, chunk)
        for url in urls:
            self._book_ws_tasks.append(asyncio.create_task(self._run_ws(url)))

    async def _run_ws(self, url: str) -> None:
        while not self._stop_evt.is_set():
            try:
                start_ts = time.monotonic()
                async with websockets.connect(
                    url,
                    ping_interval=20,
                    ping_timeout=20,
                    max_queue=1,
                    compression=None,
                ) as ws:
                    async for raw in ws:
                        try:
                            msg = json.loads(raw)
                        except Exception:
                            continue
                        touch_activity("md_ws")
                        data = msg.get("data") or msg
                        s = (data.get("s") or data.get("symbol") or "").upper()
                        if not s:
                            continue
                        try:
                            ask = float(data.get("a")); bid = float(data.get("b"))
                        except Exception:
                            continue
                        try:
                            evt_ms = int(data.get("E") or 0)
                        except Exception:
                            evt_ms = None
                        self._handle_md_tick(s, bid, ask, start_ts, evt_ms)
            except Exception:
                await asyncio.sleep(1.0)

    def _handle_md_tick(self, s: str, bid: float, ask: float, start_ts: float, evt_ms: Optional[int] = None) -> None:
        # Ignore non-TRADING pairs if info available
        try:
            if not self.exinfo.is_trading(s):
                return
        except Exception:
            pass
        # Static blacklist for USDT pairs (user-editable)
        if s in HARAM_USDT:
            return
        mid = (bid + ask) / 2.0
        snap = self._snap.setdefault(s, PriceSnapshot())
        # warm-up ignore triggers (monotonic clock)
        if (time.monotonic() - start_ts) < self._warmup_sec:
            minute_ep = self._minute_epoch_from_evt(evt_ms) # type: ignore
            if snap.minute_ep != minute_ep:
                # Reset per-minute state
                self._reset_minute_state(snap, minute_ep) # type: ignore
                # For hybrid/kline, set fallback window; for book, set open immediately
                try:
                    tick = self.exinfo.tick_size(s)
                except Exception:
                    tick = 0.0
                if OPEN_SOURCE == "book":
                    snap.open_1m = round_to_tick(ask, tick) if tick else ask
                    snap.open_source_used = "book"
                    snap.minute_target_px = round_to_tick(snap.open_1m * (1.0 + self.trigger_pct), tick)
                    self._open_source_counts["book"] = self._open_source_counts.get("book", 0) + 1
                else:
                    snap.fallback_deadline_mono = time.monotonic() + max(0, OPEN_FALLBACK_MS) / 1000.0
                    # Capture first mid for potential fallback
                    if snap.open_book_first is None:
                        snap.open_book_first = round_to_tick(mid, tick) if tick else mid
            self._last_mid[s] = mid
            # init 1s state
            sec_ep = self._second_epoch_from_evt(evt_ms) # type: ignore
            snap.sec_ep = sec_ep
            snap.sec_last_mid = mid
            return
        # minute open update
        minute_ep = self._minute_epoch_from_evt(evt_ms) # type: ignore
        if snap.minute_ep != minute_ep:
            # Reset per-minute state
            self._reset_minute_state(snap, minute_ep) # type: ignore
            # In watchlist kline scope, drop symbol at minute rollover
            if KLINE_SCOPE == "watchlist":
                self._drop_kline_symbol(s)
            try:
                tick = self.exinfo.tick_size(s)
            except Exception:
                tick = 0.0
            if OPEN_SOURCE == "book":
                snap.open_1m = round_to_tick(ask, tick) if tick else ask
                snap.open_source_used = "book"
                snap.minute_target_px = round_to_tick(snap.open_1m * (1.0 + self.trigger_pct), tick)
                self._open_source_counts["book"] = self._open_source_counts.get("book", 0) + 1
            else:
                snap.fallback_deadline_mono = time.monotonic() + max(0, OPEN_FALLBACK_MS) / 1000.0
                # Capture first mid for potential fallback
                if snap.open_book_first is None:
                    snap.open_book_first = round_to_tick(mid, tick) if tick else mid
        snap.last_ask = ask
        # tick filter (do not block second boundary updates)
        if WS_TICK_FILTER:
            last_mid = self._last_mid.get(s)
            if last_mid is not None:
                try:
                    tick = self.exinfo.tick_size(s)
                    if abs(mid - last_mid) < tick:
                        pass
                except Exception:
                    pass
        self._last_mid[s] = mid
        # Hybrid fallback: if open still unset after deadline and we have first book mid, use it
        if OPEN_SOURCE == "hybrid" and snap.open_1m is None:
            if snap.fallback_deadline_mono is not None and time.monotonic() >= snap.fallback_deadline_mono:
                if snap.open_book_first is None:
                    # Capture first mid now if still missing
                    try:
                        tick = self.exinfo.tick_size(s)
                    except Exception:
                        tick = 0.0
                    snap.open_book_first = round_to_tick(mid, tick) if tick else mid
                if snap.open_book_first is not None:
                    snap.open_1m = snap.open_book_first
                    snap.open_source_used = "book"
                    try:
                        tick = self.exinfo.tick_size(s)
                    except Exception:
                        tick = 0.0
                    snap.minute_target_px = round_to_tick(snap.open_1m * (1.0 + self.trigger_pct), tick)
                    self._open_source_counts["book"] = self._open_source_counts.get("book", 0) + 1
        # Second aggregation and close-evaluation
        cur_sec = self._second_epoch_from_evt(evt_ms)
        if snap.sec_ep is None:
            snap.sec_ep = cur_sec
            snap.sec_last_mid = mid
        elif cur_sec != snap.sec_ep:
            prev_close = snap.sec_last_mid if snap.sec_last_mid is not None else mid
            prev_sec = snap.sec_ep
            # Move to new second
            snap.sec_ep = cur_sec
            snap.sec_last_mid = mid
            # Evaluate every 1s close within the same minute: if close >= target → buy
            try:
                tick = self.exinfo.tick_size(s)
            except Exception:
                return
            target = snap.minute_target_px
            if (target is not None) and (prev_close >= target):
                # Debounce enqueue
                now_mono = time.monotonic()
                if (now_mono - self._last_handled_ts.get(s, 0.0)) * 1000.0 >= self._debounce_ms:
                    if s not in self._pending:
                        self._pending.add(s)
                        try:
                            ctx_id = f"CTX-{s}-{snap.minute_ep or 0}"
                            # audit: trigger fired
                            try:
                                if getattr(self, 'audit', None) is not None:
                                    base_at_signal = ask
                                    mode = "sec_close"
                                    asyncio.create_task(self.audit.trigger( # type: ignore
                                        symbol=s,
                                        signal_type=mode,
                                        trigger_pct=self.trigger_pct,
                                        signal_base=base_at_signal,
                                        signal_id=ctx_id,
                                    ))
                            except Exception:
                                pass
                            self._queue.put_nowait((s, ask, snap.open_1m or mid, ctx_id))
                            # Add to kline watchlist on cross if enabled
                            if KLINE_SCOPE == "watchlist":
                                try:
                                    asyncio.create_task(self._ensure_kline_for_symbol(s))
                                except Exception:
                                    pass
                        except Exception:
                            self._pending.discard(s)
                    self._last_handled_ts[s] = now_mono
        else:
            # Still within the same second: update last mid for close
            snap.sec_last_mid = mid
        # No arming/ignore: keep monitoring every second within the minute
        
    def _handle_kline_open(self, s: str, open_px: float, start_ms: int) -> None:
        # Map kline start to minute epoch
        minute_ep = int((start_ms // 1000))
        minute_ep = minute_ep - (minute_ep % 60)
        snap = self._snap.setdefault(s, PriceSnapshot())
        # If we detect a new minute via kline ahead of book, reset state
        if snap.minute_ep != minute_ep:
            self._reset_minute_state(snap, minute_ep)
            if OPEN_SOURCE in {"hybrid", "kline"}:
                snap.fallback_deadline_mono = time.monotonic() + max(0, OPEN_FALLBACK_MS) / 1000.0
            else:
                snap.fallback_deadline_mono = None
        # Round to tick
        try:
            tick = self.exinfo.tick_size(s)
        except Exception:
            tick = 0.0
        ok = round_to_tick(open_px, tick) if tick else open_px
        snap.open_kline_last = ok
        # Optional: record kline delay vs minute start (server time basis)
        try:
            now_server_ms = int((time.time() + self._server_offset_s) * 1000)
            delay = max(0, now_server_ms - int(start_ms))
            self._kline_delay_ms.append(int(delay))
        except Exception:
            pass
        # Selection based on OPEN_SOURCE / fallback window
        if OPEN_SOURCE == "kline":
            if snap.open_1m is None:
                snap.open_1m = ok
                snap.open_source_used = "kline"
                try:
                    tick = self.exinfo.tick_size(s)
                except Exception:
                    tick = 0.0
                snap.minute_target_px = round_to_tick(snap.open_1m * (1.0 + self.trigger_pct), tick)
                self._open_source_counts["kline"] = self._open_source_counts.get("kline", 0) + 1
        elif OPEN_SOURCE == "hybrid":
            if snap.open_1m is None:
                # If within fallback window, prefer kline
                within = True
                if snap.fallback_deadline_mono is not None:
                    within = (time.monotonic() < snap.fallback_deadline_mono)
                if within:
                    snap.open_1m = ok
                    snap.open_source_used = "kline"
                    try:
                        tick = self.exinfo.tick_size(s)
                    except Exception:
                        tick = 0.0
                    snap.minute_target_px = round_to_tick(snap.open_1m * (1.0 + self.trigger_pct), tick)
                    self._open_source_counts["kline"] = self._open_source_counts.get("kline", 0) + 1
                else:
                    # If we already chose book, record drift metric only
                    pass
            else:
                # Already selected, compute drift if we used book
                if snap.open_source_used == "book" and snap.open_book_first is not None:
                    try:
                        drift = abs((snap.open_kline_last or ok) - snap.open_book_first)
                        self._drift_samples.append(drift)
                        # silent metrics only (no external logging)
                    except Exception:
                        pass

    # --- Helpers for time bucketing and resets ---
    def _minute_epoch_from_evt(self, evt_ms: Optional[int]) -> int:
        if evt_ms is not None and evt_ms > 0:
            s = int(evt_ms // 1000)
            return s - (s % 60)
        return self._server_minute_epoch()

    def _second_epoch_from_evt(self, evt_ms: Optional[int]) -> int:
        if evt_ms is not None and evt_ms > 0:
            return int(evt_ms // 1000)
        return self._server_second_epoch()

    def _reset_minute_state(self, snap: PriceSnapshot, minute_ep: int) -> None:
        snap.minute_ep = minute_ep
        snap.first_touch_sec_ep = None
        snap.ignore_until_next_minute = False
        snap.open_1m = None
        snap.minute_target_px = None
        snap.open_source_used = None
        snap.open_book_first = None
        snap.open_kline_last = None
    async def _worker(self) -> None:
        try:
            while not self._stop_evt.is_set():
                s, price_for_entry, open_1m, ctx_id = await self._queue.get()
                try:
                    # Use bestAsk as entry candidate
                    entry = price_for_entry
                    # Compute targets with tick rounding
                    tick = self.exinfo.tick_size(s)
                    target_p5 = round_to_tick((open_1m) * (1.0 + self.trigger_pct), tick)
                    tp_p10 = round_to_tick(entry * 1.10, tick)
                    sl_m5 = round_to_tick(entry * 0.95, tick)
                    # Build buy request (spend from chosen slot; engine clamps to slot balance)
                    req = BuyRequest(
                        symbol=s,
                        quote_to_spend=1e12,  # spend full slot balance
                        entry_price=entry,
                        open_1m=open_1m,
                        target_p5=target_p5,
                        tp_p10=tp_p10,
                        sl_m5=sl_m5,
                        context_id=ctx_id,
                    )
                    await self.engine.handle_buy_signal(req)
                except Exception:
                    pass
                finally:
                    self._pending.discard(s)
                    # Drop from kline watchlist after decision
                    try:
                        self._drop_kline_symbol(s)
                    except Exception:
                        pass
        except asyncio.CancelledError:
            return
