from __future__ import annotations

import asyncio
import time
from typing import Optional, Iterable

from ..core.exchange_info import ExchangeInfo, round_to_tick
from ..trading.trading_engine import TradingEngine, BuyRequest
from ..infra.nats_adapter import NATSOneSecAdapter, OneSecondEvent
from ..core.config import (
    DATA_BUS_URL,
    DATA_TOPIC_ONE_SEC,
    DATA_CLIENT_GROUP,
    NATS_URLS,
    TRIGGER_PCT_1S,
    TRIGGER_MIN_UPDATES_1S,
    SPREAD_BPS_CUTOFF,
    TRIGGER_COOLDOWN_S,
    EXIT_V2_TAKE_LIMIT_PCT,
    EXIT_V2_STOP_DROP_PCT,
    EXIT_V2_PULLBACK_UP_PCT,
    ONE_SEC_HEARTBEAT_WAIT_MS,
)
from ..core.logger import dbg, log_line

_DATA_BUS_URL = DATA_BUS_URL
_NATS_URLS = NATS_URLS
_DATA_TOPIC_ONE_SEC = DATA_TOPIC_ONE_SEC
_DATA_CLIENT_GROUP = DATA_CLIENT_GROUP
_TRIGGER_PCT_1S = float(TRIGGER_PCT_1S)
_TRIGGER_MIN_UPDATES_1S = int(TRIGGER_MIN_UPDATES_1S)
_SPREAD_BPS_CUTOFF = float(SPREAD_BPS_CUTOFF)
_TRIGGER_COOLDOWN_S = float(TRIGGER_COOLDOWN_S)
_EXIT_V2_TAKE_LIMIT_PCT = float(EXIT_V2_TAKE_LIMIT_PCT)
_EXIT_V2_STOP_DROP_PCT = float(EXIT_V2_STOP_DROP_PCT)
_EXIT_V2_PULLBACK_UP_PCT = float(EXIT_V2_PULLBACK_UP_PCT)
_TRIGGER_MULT = 1.0 + _TRIGGER_PCT_1S
_TP_MULT = 1.0 + _EXIT_V2_TAKE_LIMIT_PCT
_SL_MULT = 1.0 + _EXIT_V2_STOP_DROP_PCT
_PULLBACK_MULT = 1.0 + _EXIT_V2_PULLBACK_UP_PCT
_BUS_DOWN_THRESHOLD_MS = max(1000, int(ONE_SEC_HEARTBEAT_WAIT_MS))
class OneSecRunner:
    def __init__(self, engine: TradingEngine, exinfo: ExchangeInfo):
        self.engine = engine
        self.exinfo = exinfo
        self._q: asyncio.Queue[OneSecondEvent] = asyncio.Queue(maxsize=10000)
        self._adapter = NATSOneSecAdapter(
            servers=_NATS_URLS,
            bus_url=_DATA_BUS_URL,
            subject=_DATA_TOPIC_ONE_SEC,
            queue_group=_DATA_CLIENT_GROUP,
            out_queue=self._q,
        )
        self._stop_evt = asyncio.Event()
        self._worker_task: Optional[asyncio.Task] = None
        self._health_task: Optional[asyncio.Task] = None
        # Cooldown per symbol
        self._last_fire_ts: dict[str, float] = {}
        # Metrics
        self._evt_count = 0
        self._last_lag_ms = 0.0
        self._last_event_ts = time.time()
        self._bus_down_logged = False
        self._bus_down_threshold_ms = _BUS_DOWN_THRESHOLD_MS

    async def start(self) -> None:
        await self._adapter.start()
        self._worker_task = asyncio.create_task(self._worker())
        self._health_task = asyncio.create_task(self._health_logger())

    async def stop(self) -> None:
        self._stop_evt.set()
        try:
            await self._adapter.stop()
        except Exception:
            pass
        if self._worker_task is not None:
            try:
                self._worker_task.cancel()
            except Exception:
                pass
        if self._health_task is not None:
            try:
                self._health_task.cancel()
            except Exception:
                pass

    async def _worker(self) -> None:
        try:
            while not self._stop_evt.is_set():
                evt = await self._q.get()
                self._evt_count += 1
                self._last_event_ts = time.time()
                if self._bus_down_logged:
                    log_line("[TRIG] ONE_SEC bus up (messages resumed)")
                    self._bus_down_logged = False
                # Track lag (microseconds -> ms)
                try:
                    now_us = int(time.time() * 1_000_000)
                    if evt.sec_end:
                        self._last_lag_ms = max(0.0, (now_us - int(evt.sec_end)) / 1000.0)
                except Exception:
                    pass
                # Filter basic quality
                if int(evt.updates) < _TRIGGER_MIN_UPDATES_1S:
                    continue
                if float(evt.spread_bps) > _SPREAD_BPS_CUTOFF:
                    continue
                # Compute 1s pct move
                try:
                    if evt.open <= 0:
                        continue
                    pct_1s = (float(evt.close) / float(evt.open)) - 1.0
                except Exception:
                    continue
                if pct_1s < _TRIGGER_PCT_1S:
                    continue
                # Cooldown check
                s = evt.symbol.upper()
                now_ts = time.time()
                last = float(self._last_fire_ts.get(s, 0.0) or 0.0)
                if last and (now_ts - last) < _TRIGGER_COOLDOWN_S:
                    continue
                self._last_fire_ts[s] = now_ts
                # Build BuyRequest and send to engine
                try:
                    tick = float(self.exinfo.tick_size(s) or 0.0)
                except Exception:
                    tick = 0.0
                entry = float(evt.close)
                # For compatibility with legacy logs: open_1m is 1s open now
                open_1m = float(evt.open)
                target_p = round_to_tick(entry * _PULLBACK_MULT, tick) if tick else (entry * _PULLBACK_MULT)
                tp_p10 = round_to_tick(entry * _TP_MULT, tick) if tick else (entry * _TP_MULT)
                sl_m5 = round_to_tick(entry * _SL_MULT, tick) if tick else (entry * _SL_MULT)
                ctx_id = f"1S-{s}-{int(evt.sec_start//1_000_000) if evt.sec_start else 0}"
                req = BuyRequest(
                    symbol=s,
                    quote_to_spend=1e12,  # engine clamps to slot balance
                    entry_price=entry,
                    open_1m=open_1m,
                    target_p5=target_p,
                    tp_p10=tp_p10,
                    sl_m5=sl_m5,
                    context_id=ctx_id,
                )
                try:
                    await self.engine.handle_buy_signal(req)
                except Exception:
                    # swallow to keep loop hot
                    pass
        except asyncio.CancelledError:
            return

    async def _health_logger(self) -> None:
        try:
            while not self._stop_evt.is_set():
                await asyncio.sleep(5.0)
                # Aggregate simple rate over window
                cnt = self._evt_count
                self._evt_count = 0
                rate = cnt / 5.0
                idle_ms = int(max(0.0, (time.time() - self._last_event_ts) * 1000.0))
                if idle_ms >= self._bus_down_threshold_ms and not self._bus_down_logged:
                    log_line(f"[TRIG] ONE_SEC bus down (no messages ≥ {idle_ms}ms) -> no triggers/no buys")
                    self._bus_down_logged = True
                try:
                    qsz = self._q.qsize()
                except Exception:
                    qsz = 0
                # Minimal periodic line
                dbg(f"[DATA] 1s_rate={rate:.1f}/s lag_ms~{self._last_lag_ms:.0f} q={qsz}")
        except asyncio.CancelledError:
            return
