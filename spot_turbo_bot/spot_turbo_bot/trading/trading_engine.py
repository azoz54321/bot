from __future__ import annotations
import math
import random
import asyncio
import statistics
from dataclasses import dataclass
import time
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, NamedTuple, Optional
from decimal import Decimal, ROUND_DOWN

from ..infra.binance_http import BinanceHTTP, BinanceHTTPError
from ..core.exchange_info import ExchangeInfo, round_to_step, round_to_tick
from ..core.logger import log_buy, log_sell, dbg, log_line
from ..market.impact_estimator import impact_1pct_lb, impact_1pct_estimate
from ..market import context as market_ctx
from ..core.state import GlobalState, Position, SlotState, save_state
from .trading_rules import TradingRules
from ..core.utils_time import now_ksa
from ..core.config import (
    ENTRY_IMPACT_SLIP_PCT,
    IMPACT_LOG_ENABLED,
    IMPACT_MAX_STALENESS_MS,
    IMPACT_RATIO_CAP,
    IMPACT_UTIL_LAST,
    EXIT_V2_TAKE_LIMIT_PCT,
    EXIT_V2_STOP_DROP_PCT,
    EXIT_V2_PULLBACK_UP_PCT,
    EXIT_V2_ENTRY_REGION_TOL_PCT,
    EXIT_V2_PRICE_SOURCE,
    EXIT_V2_DEBOUNCE_MS,
    EXIT_V2_MAX_STALENESS_MS,
    EXIT_V2_CANCEL_TIMEOUT_SEC,
    EXIT_V2_MARKET_RETRY,
    SELL_FEE_SAFETY,
    PLACE_TP_ON,
    ENTRY_QUOTE_BUFFER_BPS,
    ENTRY_MAX_SLIP_PCT,
    TP_CANCEL_REPLACE,
    TP_RETRY_MAX_ATTEMPTS,
    TP_RETRY_SCHEDULE_MS,
    TP_RETRY_JITTER_MS,
    TP_PLACE_DEADLINE_MS,
    TP_PLACE_DELAY_MS,
    TP_RETRY_ON_ERRORS,
    UNWIND_ON_TP_FAILURE,
    UNWIND_RETRY_MAX,
    UNWIND_RETRY_BACKOFF_MS,
    TP_MODIFY_DEBOUNCE_MS,
    TP_QUEUE_BOOST_DELAY_MS,
    TP_QUEUE_BOOST_MIN_QTY,
    TP_QUEUE_BOOST_MAX_TICKS,
    EXIT_CANCEL_ACK_TIMEOUT_MS,
    EXIT_DUST_QTY_THRESHOLD,
    EXIT_DUST_NOTIONAL_THRESHOLD,
    USERSTREAM_ENABLE,
    USERSTREAM_STALE_MS,
    USERSTREAM_FALLBACK_QUERY_MS,
    PRICE_AMEND_MIN_MS,
    MAKER_FLOOR_OFFSET_TICKS,
    POST_ONLY_BUMP_MAX_ATTEMPTS,
    BIN_WIDTH_TICKS,
    WALL_MULT,
    WALL_WIDTH_BINS,
    RANGE_MARGIN_BPS,
    RANGE_CAP_TICKS,
    TARGET_FLOOR_BPS,
    TARGET_FLOOR_ENFORCE,
    FEE_BUY_RATE,
    FEE_SELL_RATE,
)
from .exit_v2 import ExitV2Controller, ExitV2Params


KSA_TZ = timezone(timedelta(hours=3))
_ENTRY_IMPACT_SLIP_PCT = float(ENTRY_IMPACT_SLIP_PCT)
_IMPACT_LOG_ENABLED = bool(IMPACT_LOG_ENABLED)
_IMPACT_MAX_STALENESS_MS = int(IMPACT_MAX_STALENESS_MS)
_IMPACT_RATIO_CAP = float(IMPACT_RATIO_CAP)
_IMPACT_UTIL_LAST = float(IMPACT_UTIL_LAST)
_EXIT_V2_TAKE_LIMIT_PCT = float(EXIT_V2_TAKE_LIMIT_PCT)
_EXIT_V2_STOP_DROP_PCT = float(EXIT_V2_STOP_DROP_PCT)
_EXIT_V2_PULLBACK_UP_PCT = float(EXIT_V2_PULLBACK_UP_PCT)
_EXIT_V2_ENTRY_REGION_TOL_PCT = float(EXIT_V2_ENTRY_REGION_TOL_PCT)
_EXIT_V2_PRICE_SOURCE = EXIT_V2_PRICE_SOURCE.strip().lower()
_EXIT_V2_DEBOUNCE_MS = int(EXIT_V2_DEBOUNCE_MS)
_EXIT_V2_MAX_STALENESS_MS = int(EXIT_V2_MAX_STALENESS_MS)
_EXIT_V2_CANCEL_TIMEOUT_SEC = float(EXIT_V2_CANCEL_TIMEOUT_SEC)
_EXIT_V2_MARKET_RETRY = max(1, int(EXIT_V2_MARKET_RETRY))
_SELL_FEE_SAFETY = float(SELL_FEE_SAFETY)
_PLACE_TP_ON = PLACE_TP_ON.strip().lower()
_TP_CANCEL_REPLACE = bool(TP_CANCEL_REPLACE)
_TP_RETRY_MAX_ATTEMPTS = max(1, int(TP_RETRY_MAX_ATTEMPTS))
_TP_RETRY_SCHEDULE_MS = tuple(float(ms) for ms in TP_RETRY_SCHEDULE_MS) or (80.0, 120.0, 180.0, 250.0, 350.0, 500.0, 750.0, 1000.0)
_TP_RETRY_JITTER_MS = float(TP_RETRY_JITTER_MS)
_TP_PLACE_DEADLINE_MS = float(TP_PLACE_DEADLINE_MS)
_TP_PLACE_DELAY_MS = float(TP_PLACE_DELAY_MS)
_TP_RETRY_ON_ERRORS = set(TP_RETRY_ON_ERRORS)
_UNWIND_ON_TP_FAILURE = bool(UNWIND_ON_TP_FAILURE)
_UNWIND_RETRY_MAX = max(1, int(UNWIND_RETRY_MAX))
_UNWIND_RETRY_BACKOFF_MS = float(UNWIND_RETRY_BACKOFF_MS)
_TP_MODIFY_DEBOUNCE_MS = max(0, int(TP_MODIFY_DEBOUNCE_MS))
_PRICE_AMEND_MIN_MS = max(0, int(PRICE_AMEND_MIN_MS))
_MAKER_FLOOR_OFFSET_TICKS = max(0, int(MAKER_FLOOR_OFFSET_TICKS))
_POST_ONLY_BUMP_MAX_ATTEMPTS = max(1, int(POST_ONLY_BUMP_MAX_ATTEMPTS))
_TP_QUEUE_BOOST_DELAY_MS = max(0, int(TP_QUEUE_BOOST_DELAY_MS))
_TP_QUEUE_BOOST_MIN_QTY = float(TP_QUEUE_BOOST_MIN_QTY)
_TP_QUEUE_BOOST_MAX_TICKS = max(0, int(TP_QUEUE_BOOST_MAX_TICKS))
_EXIT_CANCEL_ACK_TIMEOUT_MS = max(0, int(EXIT_CANCEL_ACK_TIMEOUT_MS))
_EXIT_DUST_QTY_THRESHOLD = max(0.0, float(EXIT_DUST_QTY_THRESHOLD))
_EXIT_DUST_NOTIONAL_THRESHOLD = max(0.0, float(EXIT_DUST_NOTIONAL_THRESHOLD))
_ENTRY_QUOTE_BUFFER_BPS = max(0, int(ENTRY_QUOTE_BUFFER_BPS))
_ENTRY_QUOTE_BUFFER_PCT = _ENTRY_QUOTE_BUFFER_BPS / 10_000.0
_ENTRY_MAX_SLIP = max(0.0, float(ENTRY_MAX_SLIP_PCT))
_BIN_WIDTH_TICKS = max(1, int(BIN_WIDTH_TICKS))
_WALL_MULT = max(0.0, float(WALL_MULT))
_WALL_WIDTH_BINS = max(1, int(WALL_WIDTH_BINS))
_RANGE_MARGIN_BPS = max(0, int(RANGE_MARGIN_BPS))
_RANGE_CAP_TICKS = max(1, int(RANGE_CAP_TICKS))
_TARGET_FLOOR_BPS = max(0, int(TARGET_FLOOR_BPS))
_TARGET_FLOOR_ENFORCE = bool(TARGET_FLOOR_ENFORCE)
_FEE_BUY_RATE = float(FEE_BUY_RATE)
_FEE_SELL_RATE = float(FEE_SELL_RATE)


def _ceil_to_tick(value: float, tick: float) -> float:
    if tick <= 0:
        return value
    return math.ceil(value / tick) * tick

def _floor_to_step(value: float, step: float) -> float:
    if step <= 0:
        return value
    return math.floor(value / step) * step

def _now_ms() -> int:
    return int(time.time()*1000)

def compute_safe_quote(symbol: str) -> dict:
    dw = getattr(market_ctx, 'depth_ws', None)
    b = dw.get(symbol) if dw else None
    if not b:
        return {"ok":False,"reason":"no_book"}
    ts = _now_ms()
    # Consider stale only if both depth and book are stale (or both missing)
    if ((b.ts_depth == 0 and b.ts_book == 0) or
        ((ts - b.ts_depth > _IMPACT_MAX_STALENESS_MS) and (ts - b.ts_book > _IMPACT_MAX_STALENESS_MS))):
        return {"ok":False,"reason":"stale_book"}
    lb = impact_1pct_lb(b, slip=_ENTRY_IMPACT_SLIP_PCT, util_last=_IMPACT_UTIL_LAST)
    mc = getattr(market_ctx, 'micro', None)
    if mc:
        try:
            mc.set_quote(symbol, lb["quote_lb"])  # type: ignore[attr-defined]
        except Exception:
            pass
    if lb.get("covered"):
        rm = mc.get_ctx(symbol)["rolling_min_quote"] if mc else None  # type: ignore[attr-defined]
        safe = min(lb["quote_lb"], rm) if rm is not None else lb["quote_lb"]
        return {"ok":True,"safe_quote":safe,"mode":"LB","ceil":lb.get("ceil"),"ask0":lb.get("ask0",0.0)}
    ctx = mc.get_ctx(symbol) if mc else {"rolling_min_quote":None, "cancel_add":0.0}  # type: ignore[attr-defined]
    est = impact_1pct_estimate(b, lb, ctx, ratio_cap=_IMPACT_RATIO_CAP, util_last=_IMPACT_UTIL_LAST)
    if mc:
        try:
            mc.set_quote(symbol, est["quote_est_fused"])  # type: ignore[attr-defined]
        except Exception:
            pass
    rm = ctx.get("rolling_min_quote")
    safe = min(est["quote_est_fused"], rm) if rm is not None else est["quote_est_fused"]
    return {"ok":True,"safe_quote":safe,"mode":"EST",
            "ceil":lb.get("ceil"),"ask0":lb.get("ask0",0.0),"r2_pl":est.get("r2_pl"),"fit_ex":est.get("fit_ex")}


@dataclass
class BuyRequest:
    symbol: str
    quote_to_spend: float
    entry_price: float
    open_1m: Optional[float]
    target_p5: float
    tp_p10: float
    sl_m5: float
    context_id: Optional[str] = None


@dataclass
class _TPPlan:
    symbol: str
    slot: str
    order_id: Optional[str]
    client_order_id: Optional[str]
    placed_ms: int
    fallback_task: Optional[asyncio.Task] = None
    consumed: bool = False
    source: Optional[str] = None
    first_fill_ms: int = 0
    rest_logged: bool = False


class _TPAttemptResult(NamedTuple):
    success: bool
    attempted: bool
    code: Optional[str]
    msg: Optional[str]


@dataclass
class SellExecution:
    symbol: str
    order_type: str
    order_id: Optional[str]
    client_order_id: Optional[str]
    qty: float
    avg_price: float
    quote_gross: float
    quote_net: float
    fee_quote: float
    commission_asset: str
    event_time_ms: int
    reason_hint: Optional[str] = None


class TradingEngine:
    def __init__(self, http: BinanceHTTP, exinfo: ExchangeInfo, state: GlobalState, audit=None):
        self.http = http
        self.exinfo = exinfo
        self.state = state
        self.rules = TradingRules(exinfo, state)
        self.audit = audit
        self._buy_lock = asyncio.Lock()
        # Execution tracking (BUY fills) via user stream
        self._buy_fill_events: dict[str, asyncio.Event] = {}
        self._buy_fills: dict[str, dict] = {}
        # BNB auto-fee topup background state
        self._bnb_topup_task: Optional[asyncio.Task] = None
        self._bnb_topup_pending: Dict[str, Any] = {}
        # Exit V2 state
        self._exit_controllers: Dict[str, ExitV2Controller] = {}
        self._exit_limit_by_order_id: Dict[str, str] = {}
        self._exit_client_tags: Dict[str, str] = {}
        self._exit_close_reason: Dict[str, str] = {}
        self._exit_price_task: Optional[asyncio.Task] = None
        self._tp_retry_tasks: Dict[str, asyncio.Task] = {}
        self._tp_retry_signals: Dict[str, asyncio.Event] = {}
        self._cancel_ack_events: Dict[str, asyncio.Event] = {}
        self._cancel_ack_targets: Dict[str, str] = {}
        self._exit_task_lock = asyncio.Lock()
        self._exit_poll_interval = 0.05  # seconds
        self._user_stream_ready = not USERSTREAM_ENABLE
        self._user_stream_healthy = not USERSTREAM_ENABLE
        self._user_stream_last_event_ms = 0
        self._user_stream_msg_count = 0
        self._tp_plans: Dict[str, _TPPlan] = {}
        self._tp_plan_by_order: Dict[str, _TPPlan] = {}
        self._tp_ready_source: Dict[str, Optional[str]] = {}
        self._tp_first_fill_ts: Dict[str, int] = {}
        self._tp_delay_tasks: Dict[str, asyncio.Task] = {}
        self._tp_place_locks: Dict[str, asyncio.Lock] = {}
        self._slot_cooldown_until: Dict[str, float] = {}
        self._last_spot_free: float = 0.0
        self._entry_skip_last_reason: Dict[str, str] = {}
        self._open_positions = 0
        self._global_backoff_until = 0.0

    def _make_position_id(self, symbol: str) -> str:
        return f"{symbol.upper()}-{int(time.time() * 1000)}"

    def _ensure_position_identity(self, pos: Position) -> str:
        if pos.position_id:
            return pos.position_id
        pos.position_id = self._make_position_id(pos.symbol)
        return pos.position_id

    def has_user_stream_sensitive_activity(self) -> bool:
        if not USERSTREAM_ENABLE:
            return False
        if any(not plan.consumed for plan in self._tp_plans.values()):
            return True
        for controller in self._exit_controllers.values():
            state = controller.state
            if controller.has_active_limit():
                return True
            if state.waiting_cancel_ack or state.tp_pending:
                return True
        return False

    def userstream_pending_activity(self) -> bool:
        return self.has_user_stream_sensitive_activity()

    def _total_free_usdt(self) -> float:
        return max(0.0, self.state.slot_A.balance_quote + self.state.slot_B.balance_quote)

    def _entry_skip(self, symbol: str, reason: str, free_usdt: Optional[float] = None) -> None:
        s = symbol.upper()
        if free_usdt is None:
            free_usdt = self._total_free_usdt()
        last = self._entry_skip_last_reason.get(s)
        if last == reason:
            return
        log_line(f"[ENTRY-SKIP] symbol={s} reason={reason} free_usdt={free_usdt:.2f}")
        self._entry_skip_last_reason[s] = reason

    def _clear_entry_skip(self, symbol: str) -> None:
        self._entry_skip_last_reason.pop(symbol.upper(), None)

    def _select_buy_slot(self) -> Optional[SlotState]:
        now = time.time()
        ordered = (self.state.slot_A, self.state.slot_B)
        fallback: Optional[SlotState] = None
        fallback_until = float("inf")
        for slot in ordered:
            if not slot.is_free:
                continue
            cooldown_until = self._slot_cooldown_until.get(slot.name, 0.0)
            if now >= cooldown_until:
                if cooldown_until:
                    self._slot_cooldown_until.pop(slot.name, None)
                return slot
            if cooldown_until < fallback_until:
                fallback = slot
                fallback_until = cooldown_until
        return None

    def _apply_slot_cooldown(self, slot_name: str, seconds: float = 1.0) -> None:
        deadline = time.time() + max(0.0, seconds)
        current = self._slot_cooldown_until.get(slot_name, 0.0)
        if deadline > current:
            self._slot_cooldown_until[slot_name] = deadline
        self._global_backoff_until = max(self._global_backoff_until, deadline)

    # ------------------------------------------------------------------
    # Exit V2 helpers
    # ------------------------------------------------------------------
    def _ensure_exit_price_task(self) -> None:
        if self._exit_price_task is None or self._exit_price_task.done():
            self._exit_price_task = asyncio.create_task(self._exit_price_runner())

    async def _exit_price_runner(self) -> None:
        try:
            sleep = asyncio.sleep
            poll = self._exit_poll_interval
            while True:
                controllers = list(self._exit_controllers.items())
                if not controllers:
                    await sleep(poll)
                    continue
                now_ms = _now_ms()
                for symbol, controller in controllers:
                    if controller.is_closed or controller.qty_open <= 0.0:
                        self._stop_tp_retry(symbol)
                        self._exit_controllers.pop(symbol, None)
                        continue
                    if controller.is_closing:
                        continue
                    price, stale = self._exit_price_snapshot(symbol, now_ms)
                    controller.state.last_price = price
                    controller.state.last_price_ms = now_ms
                    if price <= 0.0:
                        continue
                    reason = controller.observe_price(price, now_ms, stale)
                    if reason:
                        # fire-and-forget executor
                        asyncio.create_task(self._execute_exit(symbol, controller, reason))
                await sleep(poll)
        except asyncio.CancelledError:
            return

    def _exit_price_snapshot(self, symbol: str, now_ms: int) -> tuple[float, bool]:
        dw = getattr(market_ctx, "depth_ws", None)
        if dw is None:
            return 0.0, True
        book = dw.get(symbol)
        if book is None:
            return 0.0, True
        bid = getattr(book, "best_bid", 0.0)
        ask = getattr(book, "best_ask", 0.0)
        if _EXIT_V2_PRICE_SOURCE == "bid":
            price = bid
        else:
            price = ((bid + ask) * 0.5) if (bid > 0.0 and ask > 0.0) else 0.0
        ts_book = getattr(book, "ts_book", 0)
        stale = True
        if ts_book:
            stale = (now_ms - ts_book) > _EXIT_V2_MAX_STALENESS_MS
        return price, stale

    def _make_exit_client_id(self, symbol: str, tag: str) -> str:
        nonce = int(time.time() * 1000) % 1_000_000_000
        base = f"{tag}-{symbol[:8]}-{nonce}"
        return base.upper()[:36]

    def _build_exit_params(self) -> ExitV2Params:
        return ExitV2Params(
            take_limit_pct=_EXIT_V2_TAKE_LIMIT_PCT,
            stop_drop_pct=_EXIT_V2_STOP_DROP_PCT,
            pullback_up_pct=_EXIT_V2_PULLBACK_UP_PCT,
            entry_region_tol_pct=_EXIT_V2_ENTRY_REGION_TOL_PCT,
            debounce_ms=_EXIT_V2_DEBOUNCE_MS,
        )

    async def _execute_exit(self, symbol: str, controller: ExitV2Controller, reason: str) -> None:
        s = symbol.upper()
        async with self._exit_task_lock:
            if controller.is_closed or controller.qty_open <= 0.0:
                return
            self._exit_close_reason[s] = reason
            controller.state.pending_reason = reason
            limit_order_id = controller.limit_order_id
            if limit_order_id:
                cancelled = await self._cancel_exit_limit(s, controller, limit_order_id, wait_for_ack=True)
                if not cancelled:
                    controller.mark_close_failed()
                    return
            await self._market_sell_remaining(s, controller, reason)

    async def _cancel_exit_limit(
        self,
        symbol: str,
        controller: ExitV2Controller,
        order_id: str,
        wait_for_ack: bool = False,
    ) -> bool:
        try:
            params: Dict[str, Any] = {"symbol": symbol, "orderId": int(order_id)}
        except Exception:
            params = {"symbol": symbol, "origClientOrderId": order_id}
        if wait_for_ack:
            self._register_cancel_ack(symbol, str(order_id))
            controller.state.waiting_cancel_ack = True
        try:
            await asyncio.wait_for(self.http.cancel_order(params), timeout=_EXIT_V2_CANCEL_TIMEOUT_SEC)
        except BinanceHTTPError as e:
            code = getattr(e, "code", None)
            if code in (-2011, "UNKNOWN_ORDER", -2013):
                if wait_for_ack:
                    self._resolve_cancel_ack(symbol, order_id)
                    controller.state.waiting_cancel_ack = False
                controller.clear_limit()
                self._exit_limit_by_order_id.pop(order_id, None)
                controller.state.last_tp_warn = None
                return True
            warn_key = (str(code), str(getattr(e, "msg", "")))
            if controller.state.last_tp_warn != warn_key:
                controller.state.last_tp_warn = warn_key
                log_line(f"[WARN][EXITV2] tp_cancel_failed symbol={symbol} code={code} msg={getattr(e, 'msg', '')}")
            controller.state.waiting_cancel_ack = False
            return False
        except Exception as exc:
            warn_key = ("CANCEL", repr(exc))
            if controller.state.last_tp_warn != warn_key:
                controller.state.last_tp_warn = warn_key
                log_line(f"[WARN][EXITV2] tp_cancel_failed symbol={symbol} msg={exc}")
            controller.state.waiting_cancel_ack = False
            return False
        if wait_for_ack:
            acked = await self._wait_for_cancel_ack(symbol)
            controller.state.waiting_cancel_ack = False
            if not acked:
                warn_key = ("CANCEL_ACK_TIMEOUT", str(order_id))
                if controller.state.last_tp_warn != warn_key:
                    controller.state.last_tp_warn = warn_key
                    log_line(f"[WARN][EXITV2] tp_cancel_ack_timeout symbol={symbol} order={order_id}")
                return False
        if controller.limit_order_id == order_id:
            controller.clear_limit()
        else:
            controller.clear_limit()
        self._exit_limit_by_order_id.pop(order_id, None)
        controller.state.last_tp_warn = None
        controller.state.waiting_cancel_ack = False
        return True

    async def _market_sell_remaining(self, symbol: str, controller: ExitV2Controller, reason: str) -> None:
        ref_price = controller.state.last_price if controller.state.last_price > 0.0 else controller.state.entry_price
        if self._sweep_exit_dust(symbol, controller, ref_price=ref_price):
            return
        remaining = controller.qty_open
        if remaining <= 0.0:
            controller.mark_closed(reason)
            self._stop_tp_retry(symbol)
            self._tp_ready_source.pop(symbol.upper(), None)
            return
        attempts = _EXIT_V2_MARKET_RETRY
        step = float(self.exinfo.step_size(symbol))
        min_qty = float(self.exinfo.min_qty(symbol) or 0.0)
        try:
            min_notional = float(self.exinfo.min_notional(symbol) or 0.0)
        except Exception:
            min_notional = 0.0
        for attempt in range(attempts):
            qty = controller.qty_open
            if qty <= 0.0:
                break
            qty = round_to_step(qty, step)
            if qty <= 0.0:
                controller.mark_closed(reason)
                self._stop_tp_retry(symbol)
                self._tp_ready_source.pop(symbol.upper(), None)
                break
            ref_price = controller.state.last_price if controller.state.last_price > 0 else controller.state.entry_price
            if min_qty > 0.0 and qty < min_qty:
                qty = min_qty
            if min_notional > 0.0 and ref_price > 0.0 and qty * ref_price < min_notional:
                qty = max(qty, min_notional / max(ref_price, 1e-12))
                qty = round_to_step(qty, step)
            if _EXIT_DUST_QTY_THRESHOLD > 0.0 and qty < _EXIT_DUST_QTY_THRESHOLD:
                controller.mark_closed(reason)
                self._stop_tp_retry(symbol)
                break
            if _EXIT_DUST_NOTIONAL_THRESHOLD > 0.0 and ref_price > 0.0 and (qty * ref_price) < _EXIT_DUST_NOTIONAL_THRESHOLD:
                controller.mark_closed(reason)
                self._stop_tp_retry(symbol)
                break
            if qty <= 0.0:
                controller.mark_closed(reason)
                self._stop_tp_retry(symbol)
                self._tp_ready_source.pop(symbol.upper(), None)
                break
            client_id = self._make_exit_client_id(symbol, "EXITMK")
            self._exit_client_tags[client_id] = reason
            params = {
                "symbol": symbol,
                "side": "SELL",
                "type": "MARKET",
                "quantity": self.exinfo.fmt_qty(symbol, qty),
                "newClientOrderId": client_id,
            }
            try:
                resp = await self.http.order(params)
            except BinanceHTTPError:
                if attempt == attempts - 1:
                    controller.mark_close_failed()
                continue
            filled_qty = float(resp.get("executedQty", 0.0) or 0.0)
            controller.on_market_fill(filled_qty)
            if controller.qty_open <= 0.0:
                controller.mark_closed(reason)
                self._stop_tp_retry(symbol)
                self._tp_ready_source.pop(symbol.upper(), None)
                break
        else:
            controller.mark_close_failed()

    def _on_position_closed(
        self,
        symbol: str,
        slot: SlotState,
        controller: Optional[ExitV2Controller],
        *,
        reason_key: Optional[str] = None,
    ) -> None:
        symbol_u = symbol.upper()
        ctrl = controller or self._exit_controllers.get(symbol_u)
        if ctrl:
            state = ctrl.state
            state.limit_order_id = None
            state.limit_active_qty = 0.0
            state.tp_pending = False
            state.waiting_cancel_ack = False
            state.tp_attempts = 0
            state.tp_post_only_bump = 0
            state.tp_last_submit_ms = 0
            state.min_notional_blocked = False
            try:
                ctrl.clear_limit()
            except Exception:
                pass
            try:
                ctrl.mark_closed(reason_key)
            except Exception:
                pass
        self._stop_tp_retry(symbol_u)
        self._exit_controllers.pop(symbol_u, None)
        self._exit_close_reason.pop(symbol_u, None)
        for oid, sym in list(self._exit_limit_by_order_id.items()):
            if sym == symbol_u:
                self._exit_limit_by_order_id.pop(oid, None)
        for cid, tag in list(self._exit_client_tags.items()):
            if tag and symbol_u in cid:
                self._exit_client_tags.pop(cid, None)
        for order_id, plan in list(self._tp_plan_by_order.items()):
            if plan.symbol == symbol_u:
                self._tp_plan_by_order.pop(order_id, None)
        for key, plan in list(self._tp_plans.items()):
            if plan.symbol == symbol_u:
                task = plan.fallback_task
                if task and not task.done():
                    task.cancel()
                self._tp_plans.pop(key, None)
        delay_task = self._tp_delay_tasks.pop(symbol_u, None)
        if delay_task and not delay_task.done():
            delay_task.cancel()
        self._tp_ready_source.pop(symbol_u, None)
        self._tp_first_fill_ts.pop(symbol_u, None)
        self._tp_place_locks.pop(symbol_u, None)
        self._clear_entry_skip(symbol_u)
        self._slot_cooldown_until.pop(slot.name, None)
        self._global_backoff_until = 0.0
        self._open_positions = max(0, self._open_positions - 1)
        if not self._exit_controllers and self._exit_price_task is not None:
            if not self._exit_price_task.done():
                self._exit_price_task.cancel()
            self._exit_price_task = None
        if ctrl and remaining_qty > 0.0:
            try:
                min_qty = float(self.exinfo.min_qty(symbol_u))
            except Exception:
                min_qty = 0.0
            if min_qty > 0.0 and remaining_qty < min_qty:
                log_line(
                    f"[EXIT][DUST] leftover symbol={symbol_u} qty={remaining_qty:.8f} < minQty={min_qty:.8f}"
                )
        try:
            min_notional = float(self.exinfo.min_notional(symbol_u))
        except Exception:
            min_notional = 0.0
        if min_notional > 0.0 and slot.balance_quote < min_notional:
            log_line(
                f"[EXIT][DUST] quote symbol={symbol_u} free_quote={slot.balance_quote:.2f} < minNotional={min_notional:.2f}"
            )
        if (
            self.state.midday_rebalance_on_both_free
            and self.state.slot_A.is_free
            and self.state.slot_B.is_free
        ):
            total = self.state.slot_A.balance_quote + self.state.slot_B.balance_quote
            half = total / 2.0
            self.state.slot_A.balance_quote = half
            self.state.slot_B.balance_quote = half
            dbg("midday rebalance 50/50 applied (both slots free)")
        free_usdt = self._total_free_usdt()
        log_line(
            f"[SLOT] released symbol={symbol_u} free_usdt={free_usdt:.2f} "
            f"slots: A={self.state.slot_A.balance_quote:.2f} B={self.state.slot_B.balance_quote:.2f}"
        )


    async def _register_exit_controller(self, symbol: str, entry_price: float, qty: float) -> ExitV2Controller:
        s = symbol.upper()
        params = self._build_exit_params()
        controller = ExitV2Controller(s, entry_price, qty, params)
        self._exit_controllers[s] = controller
        self._ensure_exit_price_task()
        return controller

    async def _ensure_take_profit(self, symbol: str, controller: ExitV2Controller) -> None:
        state = controller.state
        if controller.is_closed or controller.qty_open <= 0.0:
            state.tp_pending = False
            self._stop_tp_retry(symbol)
            return
        if state.tp_deadline_ts <= 0.0:
            state.tp_deadline_ts = time.time() + (_TP_PLACE_DEADLINE_MS / 1000.0)
            state.tp_attempts = 0
            state.warn_once_flags.discard("tp_place_failed")
            state.warn_once_flags.discard("unwind_failed")
        state.tp_pending = True
        self._start_tp_retry_task(symbol)
        self._signal_tp_retry(symbol)

    def _start_tp_retry_task(self, symbol: str) -> None:
        task = self._tp_retry_tasks.get(symbol)
        if task and not task.done():
            return
        self._tp_retry_signals.pop(symbol, None)
        self._tp_retry_tasks[symbol] = asyncio.create_task(self._tp_retry_runner(symbol))

    def _signal_tp_retry(self, symbol: str) -> None:
        event = self._tp_retry_signals.get(symbol)
        if event:
            event.set()

    def _stop_tp_retry(self, symbol: str) -> None:
        task = self._tp_retry_tasks.pop(symbol, None)
        if task and not task.done():
            task.cancel()
        event = self._tp_retry_signals.pop(symbol, None)
        if event:
            event.set()

    def _schedule_tp_delay(self, symbol: str, controller: ExitV2Controller) -> None:
        s = symbol.upper()
        task = self._tp_delay_tasks.pop(s, None)
        if task and not task.done(): task.cancel()
        self._tp_delay_tasks[s] = asyncio.create_task(self._tp_delay_worker(s, controller))

    async def _tp_delay_worker(self, symbol: str, controller: ExitV2Controller) -> None:
        try:
            await asyncio.sleep(_TP_PLACE_DELAY_MS / 1000.0); state = controller.state
            if controller.has_active_limit() or state.tp_pending or controller.is_closed or controller.qty_open <= 0.0: return
            px, _ = self._exit_price_snapshot(symbol, _now_ms()); take_px, stop_px = state.take_limit_px, state.stop_px
            if px > 0.0 and ((take_px > 0.0 and px >= take_px) or (stop_px > 0.0 and px <= stop_px)):
                asyncio.create_task(self._execute_exit(symbol, controller, "tp_delay_hit")); return
            price, qty, tick, _, _, info = self._compute_tp_plan(symbol, controller)
            if price <= 0.0 or qty <= 0.0: return
            if tick > 0.0: best_bid = float(info.get("best_bid", 0.0) or 0.0); price = max(price, round_to_tick(info.get("base_price", price), tick), round_to_tick(best_bid + tick, tick) if best_bid > 0.0 else 0.0)
            info["base_price"] = price; state.tp_pending = True; ok, _, _ = await self._submit_take_profit_order(symbol, controller, price, qty, info)
            if ok: return
            await self._ensure_take_profit(symbol, controller)
        finally:
            self._tp_delay_tasks.pop(symbol, None)

    def on_user_stream_event(self, event: str, info: Optional[dict]) -> None:
        info = info or {}
        event = (event or "").lower()
        now_ms = info.get("ts")
        if not isinstance(now_ms, int):
            now_ms = _now_ms()
        if event == "connected":
            self._user_stream_ready = True
            self._user_stream_healthy = True
            self._user_stream_last_event_ms = now_ms
        elif event == "message":
            self._user_stream_last_event_ms = now_ms
            self._user_stream_msg_count += 1
            self._user_stream_healthy = True
        elif event in {"stale", "keepalive_failed", "listen_key_expired"}:
            self._user_stream_healthy = False
        elif event == "reconnecting":
            self._user_stream_healthy = False
            self._user_stream_ready = False
        elif event == "listen_key":
            self._user_stream_ready = False
        elif event == "keepalive_ok":
            pass
        elif event == "first_message":
            self._user_stream_last_event_ms = now_ms
            self._user_stream_msg_count += 1
            self._user_stream_healthy = True

    def _plan_key(self, client_order_id: Optional[str], order_id: Optional[str]) -> Optional[str]:
        if client_order_id:
            return client_order_id
        if order_id:
            return order_id
        return None

    def _register_tp_plan(
        self,
        *,
        symbol: str,
        slot: str,
        client_order_id: Optional[str],
        order_id: Optional[str],
        placed_ms: int,
    ) -> None:
        key = self._plan_key(client_order_id, order_id)
        symbol_u = symbol.upper()
        if key is None:
            key = f"{symbol_u}:{placed_ms}"
        plan = _TPPlan(
            symbol=symbol_u,
            slot=slot,
            order_id=order_id,
            client_order_id=client_order_id,
            placed_ms=placed_ms,
        )
        # Cancel and replace any existing plan with same key
        old = self._tp_plans.pop(key, None)
        if old and old.fallback_task and not old.fallback_task.done():
            old.fallback_task.cancel()
        if plan.order_id:
            self._tp_plan_by_order[plan.order_id] = plan
        self._tp_plans[key] = plan
        self._tp_ready_source[symbol_u] = None
        plan.fallback_task = asyncio.create_task(self._bridge_first_fill(plan))
        self._tp_first_fill_ts.pop(symbol_u, None)

    async def _bridge_first_fill(self, plan: _TPPlan) -> None:
        if USERSTREAM_FALLBACK_QUERY_MS <= 0:
            return
        try:
            await asyncio.sleep(USERSTREAM_FALLBACK_QUERY_MS / 1000.0)
            if plan.consumed:
                return
            params: Dict[str, Any] = {"symbol": plan.symbol}
            if plan.order_id:
                try:
                    params["orderId"] = int(plan.order_id)
                except Exception:
                    params["origClientOrderId"] = plan.client_order_id or plan.order_id
            elif plan.client_order_id:
                params["origClientOrderId"] = plan.client_order_id
            else:
                return
            try:
                resp = await self.http.get_order(params)
            except BinanceHTTPError as e:
                log_line(
                    f"[ORDER] fill bridge failed symbol={plan.symbol} code={e.code} msg={e.msg}"
                )
                return
            except Exception as exc:
                log_line(f"[ORDER] fill bridge exception symbol={plan.symbol} err={exc}")
                return
            executed_base = float(resp.get("executedQty", 0.0) or 0.0)
            cumm_quote = float(resp.get("cummulativeQuoteQty", 0.0) or 0.0)
            status = (resp.get("status") or "").upper()
            if executed_base <= 0.0 or cumm_quote <= 0.0:
                return
            avg_price = cumm_quote / executed_base if executed_base > 0.0 else 0.0
            if avg_price <= 0.0:
                avg_price = float(resp.get("avgPrice", 0.0) or 0.0)
            if avg_price <= 0.0:
                return
            await self._handle_buy_execution(
                symbol=plan.symbol,
                client_id=plan.client_order_id or "",
                order_id=plan.order_id or "",
                executed_base=executed_base,
                cumm_quote=cumm_quote,
                avg_price=avg_price,
                final=(status == "FILLED"),
                source="REST",
            )
        except asyncio.CancelledError:
            return

    def _resolve_tp_plan(self, client_order_id: str, order_id: str, symbol: str) -> Optional[_TPPlan]:
        key = self._plan_key(client_order_id, order_id)
        if key:
            plan = self._tp_plans.get(key)
            if plan:
                return plan
        if order_id:
            plan = self._tp_plan_by_order.get(order_id)
            if plan:
                return plan
        symbol_u = symbol.upper()
        for plan in list(self._tp_plans.values()):
            if plan.symbol == symbol_u:
                return plan
        return None

    def _consume_tp_plan(self, plan: _TPPlan, source: str) -> None:
        if plan.consumed:
            return
        plan.consumed = True
        src_label = (source or "UNKNOWN").upper()
        plan.source = src_label
        if plan.first_fill_ms <= 0:
            plan.first_fill_ms = _now_ms()
        key = self._plan_key(plan.client_order_id, plan.order_id)
        if key:
            self._tp_plans.pop(key, None)
        if plan.order_id:
            self._tp_plan_by_order.pop(plan.order_id, None)
        if plan.fallback_task and not plan.fallback_task.done():
            plan.fallback_task.cancel()
        current_src = self._tp_ready_source.get(plan.symbol)
        if not current_src:
            self._tp_ready_source[plan.symbol] = src_label
        self._tp_first_fill_ts.setdefault(plan.symbol, plan.first_fill_ms)

    def _tp_ready(self, symbol: str) -> Optional[str]:
        return self._tp_ready_source.get(symbol.upper())

    def _clear_tp_ready(self, symbol: str) -> None:
        s = symbol.upper()
        self._tp_ready_source.pop(s, None)
        self._tp_first_fill_ts.pop(s, None)

    def _normalize_limit(
        self,
        price: float,
        qty: float,
        *,
        tick: float,
        step: float,
        min_price: float = 0.0,
    ) -> tuple[float, float]:
        if qty < 0.0:
            qty = 0.0
        if min_price < 0.0:
            min_price = 0.0
        min_tick_price = 0.0
        if min_price > 0.0 and tick > 0.0:
            min_tick_price = _ceil_to_tick(min_price, tick)
        elif min_price > 0.0:
            min_tick_price = min_price
        if tick > 0.0:
            price = round_to_tick(price, tick)
            if price <= 0.0 and min_tick_price > 0.0:
                price = min_tick_price
            elif min_tick_price > 0.0 and price < min_tick_price:
                price = min_tick_price
            elif price <= 0.0:
                price = _ceil_to_tick(tick, tick)
        else:
            price = max(price, min_price)
        if step > 0.0:
            qty = _floor_to_step(qty, step)
        if qty < 0.0:
            qty = 0.0
        return price, qty

    def _tp_wall_candidate(self, symbol: str, tick: float, best_ask: float, tp_target: float) -> Optional[dict[str, float]]:
        if tick <= 0.0 or best_ask <= 0.0:
            return None
        dw = getattr(market_ctx, "depth_ws", None)
        if dw is None:
            return None
        book = dw.get(symbol.upper())
        if book is None:
            return None
        now_ms = _now_ms()
        ts_depth = getattr(book, "ts_depth", 0)
        ts_book = getattr(book, "ts_book", 0)
        if ts_depth and (now_ms - ts_depth) > _IMPACT_MAX_STALENESS_MS:
            return None
        if ts_book and (now_ms - ts_book) > _IMPACT_MAX_STALENESS_MS:
            return None
        asks_px = getattr(book, "asks_px", [])
        asks_q = getattr(book, "asks_q", [])
        if not asks_px or not asks_q:
            return None
        bin_width = tick * _BIN_WIDTH_TICKS if _BIN_WIDTH_TICKS > 0 else tick
        if bin_width <= 0.0:
            bin_width = tick
        range_cap_price = best_ask + tick * _RANGE_CAP_TICKS
        margin_price = tp_target * (1.0 + (_RANGE_MARGIN_BPS / 10_000.0))
        max_price = min(range_cap_price, margin_price)
        if max_price <= best_ask:
            max_price = best_ask + bin_width
        bin_values: Dict[int, float] = {}
        for price, qty in zip(asks_px, asks_q):
            if price < best_ask:
                continue
            if price > max_price:
                break
            ticks_from_best = (price - best_ask) / tick if tick > 0.0 else 0.0
            if ticks_from_best < 0.0:
                continue
            bin_idx = int(ticks_from_best // _BIN_WIDTH_TICKS) if _BIN_WIDTH_TICKS > 0 else int(ticks_from_best)
            if bin_idx < 0:
                continue
            notional = price * qty
            bin_values[bin_idx] = bin_values.get(bin_idx, 0.0) + notional
        if not bin_values:
            return None
        volumes = sorted(bin_values.items())
        values_only = [value for _, value in volumes]
        baseline = statistics.median(values_only)
        if baseline <= 0.0:
            return None
        threshold = baseline * _WALL_MULT
        run_count = 0
        prev_idx: Optional[int] = None
        for idx, value in volumes:
            contiguous = prev_idx is None or (idx - prev_idx) <= 1
            if value >= threshold and contiguous:
                run_count += 1
                if run_count >= _WALL_WIDTH_BINS:
                    wall_idx = idx - (run_count - 1)
                    wall_price = best_ask + wall_idx * bin_width
                    return {
                        "price": wall_price,
                        "baseline": baseline,
                        "bin_count": run_count,
                    }
            else:
                run_count = 1 if value >= threshold else 0
            prev_idx = idx
        return None

    def _tp_maker_floor(self, symbol: str, tick: float) -> tuple[float, float, float]:
        dw = getattr(market_ctx, "depth_ws", None)
        if not dw:
            return 0.0, 0.0, 0.0
        book = dw.get(symbol)
        if not book:
            return 0.0, 0.0, 0.0
        ts_book = getattr(book, "ts_book", 0)
        if ts_book and (_now_ms() - ts_book) > _EXIT_V2_MAX_STALENESS_MS:
            return 0.0, float(getattr(book, "best_bid", 0.0) or 0.0), float(getattr(book, "best_ask", 0.0) or 0.0)
        best_bid = float(getattr(book, "best_bid", 0.0) or 0.0)
        best_ask = float(getattr(book, "best_ask", 0.0) or 0.0)
        floor = 0.0
        if best_bid > 0.0:
            if tick > 0.0:
                floor = best_bid + tick * max(0, _MAKER_FLOOR_OFFSET_TICKS)
            else:
                floor = best_bid
        return floor, best_bid, best_ask

    def _tp_update_queue_boost(self, controller: ExitV2Controller, now_ms: int) -> None:
        if _TP_QUEUE_BOOST_MAX_TICKS <= 0:
            controller.state.tp_queue_boost_ticks = 0
            return
        state = controller.state
        if controller.qty_open < _TP_QUEUE_BOOST_MIN_QTY:
            state.tp_queue_boost_ticks = 0
            return
        if state.tp_first_submit_ms <= 0:
            return
        if now_ms - state.tp_first_submit_ms < _TP_QUEUE_BOOST_DELAY_MS:
            return
        if state.tp_queue_boost_ticks >= _TP_QUEUE_BOOST_MAX_TICKS:
            return
        last = state.tp_last_boost_ms or state.tp_first_submit_ms
        if (now_ms - last) < max(_TP_QUEUE_BOOST_DELAY_MS, 1):
            return
        state.tp_queue_boost_ticks += 1
        state.tp_last_boost_ms = now_ms
        waited_ms = now_ms - state.tp_first_submit_ms if state.tp_first_submit_ms > 0 else 0
        log_line(
            f"[EXIT] boost -1 tick symbol={controller.state.symbol} queue_qty={controller.qty_open:.8f} waited_ms={waited_ms}"
        )

    def _sweep_exit_dust(
        self,
        symbol: str,
        controller: ExitV2Controller,
        ref_price: Optional[float] = None,
    ) -> bool:
        if controller.limit_order_id:
            return False
        qty_open = controller.qty_open
        if qty_open <= 0.0:
            return True
        if _EXIT_DUST_QTY_THRESHOLD > 0.0 and qty_open < _EXIT_DUST_QTY_THRESHOLD:
            controller.state.qty_open = 0.0
            controller.mark_closed("dust")
            self._stop_tp_retry(symbol)
            self._exit_close_reason[symbol.upper()] = "dust"
            self._tp_ready_source.pop(symbol.upper(), None)
            if "dust" not in controller.state.warn_once_flags:
                controller.state.warn_once_flags.add("dust")
                log_line(f"[EXIT][DUST] close symbol={symbol} qty={qty_open:.8f}")
            return True
        if ref_price is None or ref_price <= 0.0:
            ref_price = controller.state.last_price if controller.state.last_price > 0.0 else controller.state.entry_price
        if _EXIT_DUST_NOTIONAL_THRESHOLD > 0.0 and ref_price > 0.0:
            notional = qty_open * ref_price
            if notional < _EXIT_DUST_NOTIONAL_THRESHOLD:
                controller.state.qty_open = 0.0
                controller.mark_closed("dust")
                self._stop_tp_retry(symbol)
                self._exit_close_reason[symbol.upper()] = "dust"
                self._tp_ready_source.pop(symbol.upper(), None)
                if "dust" not in controller.state.warn_once_flags:
                    controller.state.warn_once_flags.add("dust")
                    log_line(f"[EXIT][DUST] close symbol={symbol} notional={notional:.4f}")
                return True
        return False

    def _register_cancel_ack(self, symbol: str, order_id: str) -> asyncio.Event:
        evt = self._cancel_ack_events.get(symbol)
        if evt is None:
            evt = asyncio.Event()
            self._cancel_ack_events[symbol] = evt
        else:
            evt.clear()
        self._cancel_ack_targets[symbol] = order_id
        return evt

    def _resolve_cancel_ack(self, symbol: str, order_id: Optional[str]) -> None:
        target = self._cancel_ack_targets.get(symbol)
        if target and order_id and str(target) != str(order_id):
            return
        evt = self._cancel_ack_events.get(symbol)
        if evt:
            try:
                evt.set()
            except Exception:
                pass
        self._cancel_ack_targets.pop(symbol, None)

    async def _wait_for_cancel_ack(self, symbol: str) -> bool:
        if _EXIT_CANCEL_ACK_TIMEOUT_MS <= 0:
            self._cancel_ack_targets.pop(symbol, None)
            return True
        evt = self._cancel_ack_events.get(symbol)
        if evt is None:
            self._cancel_ack_targets.pop(symbol, None)
            return True
        try:
            await asyncio.wait_for(evt.wait(), timeout=_EXIT_CANCEL_ACK_TIMEOUT_MS / 1000.0)
            return True
        except asyncio.TimeoutError:
            return False
        finally:
            evt.clear()
            self._cancel_ack_targets.pop(symbol, None)

    def _compute_tp_plan(self, symbol: str, controller: ExitV2Controller) -> tuple[float, float, float, float, float, dict[str, float]]:
        try:
            tick = float(self.exinfo.tick_size(symbol))
        except Exception:
            tick = 0.0
        try:
            step = float(self.exinfo.step_size(symbol))
        except Exception:
            step = 0.00000001
        if step <= 0.0:
            step = 0.00000001
        try:
            min_notional = float(self.exinfo.min_notional(symbol) or 0.0)
        except Exception:
            min_notional = 0.0

        ref_price = controller.state.last_price if controller.state.last_price > 0.0 else controller.state.entry_price
        if self._sweep_exit_dust(symbol, controller, ref_price=ref_price):
            return 0.0, 0.0, tick, step, min_notional, {}

        now_ms = _now_ms()
        state = controller.state
        if state.tp_anchor_price <= 0.0 and state.entry_price > 0.0:
            state.tp_anchor_price = state.entry_price
        anchor_price = state.tp_anchor_price if state.tp_anchor_price > 0.0 else state.entry_price
        target_bps = int(round(_EXIT_V2_TAKE_LIMIT_PCT * 10_000))
        tp_target = anchor_price * (1.0 + _EXIT_V2_TAKE_LIMIT_PCT)
        if tick > 0.0:
            tp_target = _ceil_to_tick(tp_target, tick)
        maker_floor, best_bid, best_ask = self._tp_maker_floor(symbol, tick)
        post_only_pad = (state.tp_post_only_bump * tick) if (tick > 0.0 and state.tp_post_only_bump > 0) else 0.0

        tick_gap = 0.0
        bps_gap = 0.0
        if best_ask > 0.0 and tp_target > 0.0:
            diff = tp_target - best_ask
            if tick > 0.0:
                tick_gap = diff / tick
            if tp_target > 0.0:
                bps_gap = (diff / tp_target) * 10_000

        candidate_price = tp_target
        wall_price = 0.0
        if best_ask > 0.0 and tick > 0.0:
            wall_info = self._tp_wall_candidate(symbol, tick, best_ask, tp_target)
            if wall_info:
                wall_price = float(wall_info.get("price", 0.0) or 0.0)
                wall_candidate = max(0.0, wall_price - tick)
                if wall_candidate > 0.0 and (candidate_price <= 0.0 or wall_candidate < candidate_price):
                    candidate_price = wall_candidate
                    log_key = f"wall_{wall_candidate:.8f}"
                    if log_key not in state.warn_once_flags:
                        baseline = float(wall_info.get("baseline", 0.0) or 0.0)
                        log_line(
                            f"[WALL] chosen wall={wall_price:.8f} bins>={_WALL_WIDTH_BINS} mult>={_WALL_MULT} baseline={baseline:.2f}"
                        )
                        state.warn_once_flags.add(log_key)
        tp_floor_price = 0.0
        if _TARGET_FLOOR_ENFORCE and target_bps > _TARGET_FLOOR_BPS:
            floor_bps = target_bps - _TARGET_FLOOR_BPS
            tp_floor_price = anchor_price * (1.0 + floor_bps / 10_000.0)
            if tick > 0.0:
                tp_floor_price = _ceil_to_tick(tp_floor_price, tick)
            if tp_floor_price > 0.0 and (candidate_price <= 0.0 or candidate_price < tp_floor_price):
                candidate_price = tp_floor_price
                floor_key = f"tp_floor_{tp_floor_price:.8f}"
                if floor_key not in state.warn_once_flags:
                    log_line(f"[TPFLOOR] applied floor={tp_floor_price:.8f} from target={tp_target:.8f}")
                    state.warn_once_flags.add(floor_key)

        floor_price = maker_floor if maker_floor > 0.0 else 0.0
        if floor_price > 0.0 and (candidate_price <= 0.0 or candidate_price < floor_price):
            candidate_price = floor_price

        base_no_pad = candidate_price if candidate_price > 0.0 else tp_target
        base_no_pad = max(base_no_pad, floor_price)
        maker_price_base = base_no_pad + post_only_pad

        self._tp_update_queue_boost(controller, now_ms)
        queue_ticks = min(state.tp_queue_boost_ticks, _TP_QUEUE_BOOST_MAX_TICKS)
        maker_price = maker_price_base
        price_floor = max(floor_price + post_only_pad, 0.0)
        if queue_ticks > 0 and tick > 0.0:
            drop = queue_ticks * tick
            candidate = maker_price - drop
            maker_price = max(price_floor, candidate) if price_floor > 0.0 else candidate

        min_guard = price_floor
        safe_raw = state.qty_open * max(0.0, 1.0 - _SELL_FEE_SAFETY)
        price, qty = self._normalize_limit(
            maker_price,
            safe_raw,
            tick=tick,
            step=step,
            min_price=min_guard,
        )
        state.take_limit_px = price
        info = {
            "anchor_price": anchor_price,
            "tp_target": tp_target,
            "maker_floor": floor_price,
            "queue_ticks": float(state.tp_queue_boost_ticks),
            "best_bid": best_bid,
            "best_ask": best_ask,
            "candidate_price": base_no_pad,
            "base_price": base_no_pad,
            "tp_floor": tp_floor_price,
            "wall_price": wall_price,
            "maker_price_base": maker_price_base,
            "tick_gap": tick_gap,
            "bps_gap": bps_gap,
        }
        return price, qty, tick, step, min_notional, info

    def _tp_log_failure(
        self,
        controller: ExitV2Controller,
        symbol: str,
        code: Optional[str],
        msg: Optional[str],
        price: float,
        qty: float,
    ) -> None:
        warn_key = (code or "?", msg or "")
        if controller.state.last_tp_warn == warn_key:
            return
        controller.state.last_tp_warn = warn_key
        log_line(
            f"[WARN][EXITV2] tp_place_failed symbol={symbol} code={code or '?'} msg={msg or ''} "
            f"price={price:.8f} qty={qty:.8f}"
        )

    async def _submit_take_profit_order(
        self,
        symbol: str,
        controller: ExitV2Controller,
        price: float,
        qty: float,
        plan_info: dict[str, float],
    ) -> tuple[bool, Optional[str], Optional[str]]:
        client_id = self._make_exit_client_id(symbol, "EXITTP")
        self._exit_client_tags[client_id] = "take_profit"
        state = controller.state
        trigger_src = str(plan_info.get("trigger_source") or self._tp_ready(symbol) or "UNKNOWN").upper()
        log_line(
            f"[EXIT] placing TP maker symbol={symbol} (SRC={trigger_src}) price={price:.8f} qty={qty:.8f}"
        )
        params = {
            "symbol": symbol,
            "side": "SELL",
            "type": "LIMIT_MAKER",
            "quantity": self.exinfo.fmt_qty(symbol, qty),
            "price": self.exinfo.fmt_price(symbol, price),
            "newClientOrderId": client_id,
        }
        submit_ms = _now_ms()
        try:
            resp = await self.http.order(params)
        except BinanceHTTPError as e:
            code = str(getattr(e, "code", "?"))
            msg = str(getattr(e, "msg", ""))
            self._tp_log_failure(controller, symbol, code, msg, price, qty)
            self._exit_client_tags.pop(client_id, None)
            controller.state.tp_pending = True
            state.tp_last_submit_ms = submit_ms
            if code not in _TP_RETRY_ON_ERRORS:
                controller.state.warn_once_flags.add("tp_place_failed")
            return False, code, msg
        except Exception as exc:
            code = "EXC"
            msg = repr(exc)
            self._tp_log_failure(controller, symbol, code, msg, price, qty)
            self._exit_client_tags.pop(client_id, None)
            controller.state.tp_pending = True
            controller.state.warn_once_flags.add("tp_place_failed")
            state.tp_last_submit_ms = submit_ms
            return False, code, msg

        controller.state.warn_once_flags.discard("tp_place_failed")
        controller.state.last_tp_warn = None
        controller.state.tp_pending = False
        controller.state.tp_attempts = 0
        if state.tp_first_submit_ms <= 0:
            state.tp_first_submit_ms = submit_ms
        state.tp_last_submit_ms = submit_ms
        state.tp_queue_boost_ticks = min(state.tp_queue_boost_ticks, _TP_QUEUE_BOOST_MAX_TICKS)
        state.warn_once_flags.discard("dust")
        order_id = str(resp.get("orderId", ""))
        controller.bind_limit(order_id, qty, price)
        if order_id:
            self._exit_limit_by_order_id[order_id] = symbol
        log_line(f"[EXIT] TP bound symbol={symbol} orderId={order_id or '?'} price={price:.8f}")
        metric_start = self._tp_first_fill_ts.pop(symbol.upper(), None)
        if metric_start is not None:
            delta_ms = max(0, submit_ms - metric_start)
            log_line(f"[METRIC] buy→tp Δms={delta_ms}")
        return True, None, None

    async def _attempt_tp_once(self, symbol: str, controller: ExitV2Controller) -> _TPAttemptResult:
        symbol_u = symbol.upper()
        lock = self._tp_place_locks.setdefault(symbol_u, asyncio.Lock())
        if lock.locked():
            log_line(f"[EXIT] place_locked symbol={symbol_u} skip parallel attempt")
            return _TPAttemptResult(False, False, None, None)
        async with lock:
            return await self._attempt_tp_once_locked(symbol, controller)

    async def _attempt_tp_once_locked(self, symbol: str, controller: ExitV2Controller) -> _TPAttemptResult:
        price, qty, tick, step, min_notional, plan_info = self._compute_tp_plan(symbol, controller)
        if price <= 0.0 or qty <= 0.0:
            controller.state.tp_pending = True
            log_line(f"[EXIT] skip TP maker symbol={symbol} reason=insufficient_qty price={price:.8f} qty={qty:.8f}")
            return _TPAttemptResult(False, False, None, None)
        if min_notional > 0.0 and (price * qty) < min_notional:
            controller.state.min_notional_blocked = True
            controller.state.tp_pending = True
            log_line(
                f"[EXIT] skip TP maker symbol={symbol} reason=min_notional_block price={price:.8f} qty={qty:.8f} min_notional={min_notional:.8f}"
            )
            return _TPAttemptResult(False, False, None, None)
        controller.state.min_notional_blocked = False
        now_ms = _now_ms()
        state = controller.state
        existing_id = controller.limit_order_id
        if existing_id:
            if state.waiting_cancel_ack:
                return _TPAttemptResult(False, False, None, None)
            price_diff = abs(controller.state.limit_price - price)
            qty_diff = abs(controller.state.limit_active_qty - qty)
            if price_diff <= max(tick, 1e-12) and qty_diff <= max(step, 1e-12):
                controller.state.tp_pending = False
                controller.state.last_tp_warn = None
                return _TPAttemptResult(True, False, None, None)
            if not _TP_CANCEL_REPLACE:
                controller.state.tp_pending = False
                controller.state.last_tp_warn = None
                return _TPAttemptResult(True, False, None, None)
            if _TP_MODIFY_DEBOUNCE_MS > 0 and state.tp_last_submit_ms > 0:
                if (now_ms - state.tp_last_submit_ms) < _TP_MODIFY_DEBOUNCE_MS:
                    controller.state.tp_pending = True
                    return _TPAttemptResult(False, False, None, None)
            controller.state.tp_pending = True
            cancelled = await self._cancel_exit_limit(symbol, controller, existing_id)
            if not cancelled:
                return _TPAttemptResult(False, False, None, None)

        controller.state.tp_pending = True
        trigger_src = self._tp_ready(symbol) or plan_info.get("trigger_source") or "UNKNOWN"
        plan_info["trigger_source"] = trigger_src
        max_attempts = max(1, _POST_ONLY_BUMP_MAX_ATTEMPTS)
        attempt = 1
        current_price = price
        total_bump_ticks = int(state.tp_post_only_bump) if (tick > 0.0 and state.tp_post_only_bump > 0) else 0
        last_code: Optional[str] = None
        last_msg: Optional[str] = None

        while attempt <= max_attempts:
            plan_info["attempt"] = float(attempt)
            success, code, msg = await self._submit_take_profit_order(symbol, controller, current_price, qty, plan_info)
            if success:
                if tick > 0.0:
                    bump_ticks = int(round(max(0.0, (current_price - price)) / tick))
                    state.tp_post_only_bump = max(0, bump_ticks)
                else:
                    state.tp_post_only_bump = 0
                return _TPAttemptResult(True, True, None, None)

            last_code = code
            last_msg = msg
            if tick > 0.0:
                total_bump_ticks = int(round(max(0.0, (current_price - price)) / tick))
            else:
                total_bump_ticks = 0

            if code != "-2010":
                state.tp_post_only_bump = total_bump_ticks
                return _TPAttemptResult(False, True, code, msg)
            if attempt >= max_attempts:
                state.tp_post_only_bump = total_bump_ticks
                return _TPAttemptResult(False, True, code, msg)

            log_line(
                f"[EXIT] post-only rejected -> bump +1 tick (attempt {attempt}/{_POST_ONLY_BUMP_MAX_ATTEMPTS})"
            )
            fresh_floor, fresh_best_bid, _ = self._tp_maker_floor(symbol, tick)
            floor_price = max(fresh_floor, 0.0)
            min_floor_offset = floor_price
            if tick > 0.0 and fresh_best_bid > 0.0:
                min_floor_offset = max(min_floor_offset, fresh_best_bid + tick * max(0, _MAKER_FLOOR_OFFSET_TICKS))
            candidate = current_price + (tick if tick > 0.0 else 0.0)
            candidate = max(candidate, min_floor_offset)
            if tick > 0.0:
                candidate = _ceil_to_tick(candidate, tick)
            current_price = candidate
            plan_info["best_bid"] = fresh_best_bid

            if _PRICE_AMEND_MIN_MS > 0 and state.tp_last_submit_ms > 0:
                elapsed = _now_ms() - state.tp_last_submit_ms
                wait_ms = max(0, _PRICE_AMEND_MIN_MS - elapsed)
                if wait_ms > 0:
                    await asyncio.sleep(wait_ms / 1000.0)

            attempt += 1

        state.tp_post_only_bump = total_bump_ticks
        return _TPAttemptResult(False, True, last_code, last_msg)

    async def _tp_handle_deadline(self, symbol: str, controller: ExitV2Controller) -> None:
        state = controller.state
        log_line(f"[WARN][EXITV2] tp_not_secured symbol={symbol} deadline attempts={state.tp_attempts}")
        state.tp_pending = False
        if _UNWIND_ON_TP_FAILURE:
            await self._attempt_tp_unwind(symbol, controller)

    async def _attempt_tp_unwind(self, symbol: str, controller: ExitV2Controller) -> None:
        state = controller.state
        price, safe_qty, _, _, min_notional, _ = self._compute_tp_plan(symbol, controller)
        if safe_qty <= 0.0:
            return
        price_ref = state.last_price if state.last_price > 0.0 else state.entry_price
        if price_ref <= 0.0:
            price_ref = price
        if min_notional > 0.0 and price_ref > 0.0 and (price_ref * safe_qty) < min_notional:
            return

        qty_fmt = self.exinfo.fmt_qty(symbol, safe_qty)
        attempts = max(1, _UNWIND_RETRY_MAX)
        backoff = max(0.0, _UNWIND_RETRY_BACKOFF_MS) / 1000.0
        for attempt in range(attempts):
            client_id = self._make_exit_client_id(symbol, "EXITUW")
            self._exit_client_tags[client_id] = "take_profit_unwind"
            params = {
                "symbol": symbol,
                "side": "SELL",
                "type": "MARKET",
                "quantity": qty_fmt,
                "newClientOrderId": client_id,
            }
            try:
                resp = await self.http.order(params)
            except BinanceHTTPError as e:
                code = str(getattr(e, "code", "?"))
                msg = str(getattr(e, "msg", ""))
                self._exit_client_tags.pop(client_id, None)
                if "unwind_failed" not in state.warn_once_flags:
                    state.warn_once_flags.add("unwind_failed")
                    log_line(f"[ERROR][EXITV2] unwind_failed symbol={symbol} code={code} msg={msg} qty={safe_qty:.8f}")
                if attempt < attempts - 1 and backoff > 0.0:
                    await asyncio.sleep(backoff)
                continue
            except Exception as exc:
                code = "EXC"
                msg = repr(exc)
                self._exit_client_tags.pop(client_id, None)
                if "unwind_failed" not in state.warn_once_flags:
                    state.warn_once_flags.add("unwind_failed")
                    log_line(f"[ERROR][EXITV2] unwind_failed symbol={symbol} code={code} msg={msg} qty={safe_qty:.8f}")
                if attempt < attempts - 1 and backoff > 0.0:
                    await asyncio.sleep(backoff)
                continue

            self._exit_client_tags.pop(client_id, None)
            state.warn_once_flags.discard("unwind_failed")
            filled_qty = float(resp.get("executedQty", 0.0) or 0.0)
            controller.on_market_fill(filled_qty)
            controller.mark_closed("tp_unwind")
            controller.clear_limit()
            log_line(f"[INFO][EXITV2] unwind_success symbol={symbol} qty={safe_qty:.8f} attempts={attempt + 1}")
            self._stop_tp_retry(symbol)
            return

        state.tp_pending = False

    async def _tp_retry_runner(self, symbol: str) -> None:
        event = self._tp_retry_signals.get(symbol)
        if event is None:
            event = asyncio.Event()
            self._tp_retry_signals[symbol] = event
        else:
            event.clear()
        try:
            while True:
                controller = self._exit_controllers.get(symbol)
                if not controller:
                    break
                state = controller.state
                if not controller.is_active or controller.qty_open <= 0.0:
                    state.tp_pending = False
                    break
                if not state.tp_pending and controller.limit_order_id:
                    break
                now = time.time()
                if state.tp_deadline_ts > 0.0 and now >= state.tp_deadline_ts:
                    await self._tp_handle_deadline(symbol, controller)
                    break

                attempt_result = await self._attempt_tp_once(symbol, controller)
                if attempt_result.success:
                    break

                now = time.time()
                if state.tp_deadline_ts > 0.0 and now >= state.tp_deadline_ts:
                    await self._tp_handle_deadline(symbol, controller)
                    break

                if attempt_result.attempted:
                    state.tp_attempts += 1

                now = time.time()
                if (state.tp_deadline_ts > 0.0 and now >= state.tp_deadline_ts) or state.tp_attempts >= _TP_RETRY_MAX_ATTEMPTS:
                    await self._tp_handle_deadline(symbol, controller)
                    break

                idx = state.tp_attempts - 1 if attempt_result.attempted and state.tp_attempts > 0 else state.tp_attempts
                idx = max(0, min(idx, len(_TP_RETRY_SCHEDULE_MS) - 1))
                delay_ms = _TP_RETRY_SCHEDULE_MS[idx]
                if _TP_RETRY_JITTER_MS > 0.0:
                    delay_ms += random.uniform(-_TP_RETRY_JITTER_MS, _TP_RETRY_JITTER_MS)
                wait_seconds = max(delay_ms / 1000.0, 0.0)

                event = self._tp_retry_signals.get(symbol)
                if event and event.is_set():
                    event.clear()
                    continue
                if wait_seconds <= 0.0:
                    await asyncio.sleep(0)
                    continue
                if event:
                    try:
                        await asyncio.wait_for(event.wait(), timeout=wait_seconds)
                    except asyncio.TimeoutError:
                        pass
                    finally:
                        event.clear()
                else:
                    await asyncio.sleep(wait_seconds)
        except asyncio.CancelledError:
            pass
        finally:
            self._tp_retry_tasks.pop(symbol, None)
            event = self._tp_retry_signals.get(symbol)
            if event:
                event.clear()

    async def _adopt_existing_tp(self, symbol: str, controller: ExitV2Controller) -> bool:
        s = symbol.upper()
        try:
            orders = await self.http.open_orders({"symbol": s})
        except BinanceHTTPError as e:
            dbg(f"adopt open_orders failed: {e.code} {e.msg}")
            return False
        except Exception:
            return False
        chosen_order = None
        for order in orders or []:
            side = (order.get("side") or "").upper()
            if side != "SELL":
                continue
            order_type = (order.get("type") or "").upper()
            if order_type not in {"LIMIT", "LIMIT_MAKER", "TAKE_PROFIT_LIMIT"}:
                continue
            tif = (order.get("timeInForce") or "").upper()
            if tif and tif != "GTC":
                continue
            status = (order.get("status") or "").upper()
            if status not in {"NEW", "PARTIALLY_FILLED"}:
                continue
            try:
                price = float(order.get("price", 0.0) or 0.0)
                orig_qty = float(order.get("origQty", 0.0) or 0.0)
                executed_qty = float(order.get("executedQty", 0.0) or 0.0)
            except Exception:
                continue
            if price <= 0.0 or orig_qty <= 0.0:
                continue
            remaining_qty = max(0.0, orig_qty - executed_qty)
            if remaining_qty <= 0.0:
                continue
            chosen_order = (order, price, remaining_qty)
            break
        if not chosen_order:
            return False
        order, price, remaining_qty = chosen_order
        order_id = str(order.get("orderId", "") or "")
        client_id = str(order.get("clientOrderId", "") or "")
        ref_id = order_id or client_id
        controller.bind_limit(ref_id, remaining_qty, price)
        controller.state.tp_pending = False
        self._tp_ready_source[s] = "ADOPT"
        controller.state.tp_attempts = 0
        controller.state.tp_post_only_bump = 0
        controller.state.tp_last_submit_ms = 0
        controller.state.last_tp_warn = None
        controller.state.min_notional_blocked = False
        controller.state.limit_active_qty = remaining_qty
        controller.state.take_limit_px = price
        controller.state.qty_open = remaining_qty
        slot_state = self._slot_for_symbol(s)
        if slot_state and slot_state.position:
            slot_state.position.qty_base = remaining_qty
            slot_state.position.tp_price = price
            slot_state.position.client_order_id = ref_id or slot_state.position.client_order_id
            save_state(self.state)
            self._exit_limit_by_order_id[ref_id] = s
        if client_id:
            self._exit_client_tags[client_id] = "take_profit"
    async def bootstrap_exit_v2(self) -> None:
        for slot in (self.state.slot_A, self.state.slot_B):
            pos = slot.position
            if not pos or pos.qty_base <= 0.0:
                continue
            symbol = pos.symbol.upper()
            entry = pos.entry_price
            if entry <= 0.0:
                continue
            qty = pos.qty_base
            try:
                controller = self._exit_controllers.get(symbol)
                if controller is None:
                    controller = await self._register_exit_controller(symbol, entry, qty)
                controller.state.entry_price = entry
                controller.state.qty_open = qty
                if controller.state.tp_anchor_price <= 0.0 and entry > 0.0:
                    controller.state.tp_anchor_price = entry
                controller.state.entry_region_px = entry * (1.0 + _EXIT_V2_ENTRY_REGION_TOL_PCT)
                controller.state.take_limit_px = entry * (1.0 + _EXIT_V2_TAKE_LIMIT_PCT)
                controller.state.stop_px = entry * (1.0 + _EXIT_V2_STOP_DROP_PCT)
                adopted = await self._adopt_existing_tp(symbol, controller)
                if not adopted:
                    await self._ensure_take_profit(symbol, controller)
            except BinanceHTTPError:
                dbg(f"exit limit placement failed during bootstrap for {symbol}")
    async def _handle_buy_execution(
        self,
        *,
        symbol: str,
        client_id: str,
        order_id: str,
        executed_base: float,
        cumm_quote: float,
        avg_price: float,
        final: bool,
        source: str = "USERSTREAM",
    ) -> None:
        symbol_u = symbol.upper()
        source_label = (source or "USERSTREAM").upper()
        plan = self._resolve_tp_plan(client_id, order_id, symbol_u)
        ready_src = self._tp_ready(symbol_u)
        if executed_base > 0.0:
            prev_ready = ready_src
            now_ms = _now_ms()
            delta_ms = None
            if plan and not plan.consumed:
                delta_ms = max(0, now_ms - plan.placed_ms)
                self._consume_tp_plan(plan, source_label)
                ready_src = self._tp_ready(symbol_u)
            if not ready_src:
                self._tp_ready_source[symbol_u] = source_label
                self._tp_first_fill_ts.setdefault(symbol_u, now_ms)
                ready_src = source_label
            if not prev_ready:
                order_ref = (
                    (plan.order_id if plan else None)
                    or (order_id if order_id else None)
                    or (plan.client_order_id if plan else None)
                    or (client_id if client_id else None)
                    or symbol_u
                )
                delta_text = f"{delta_ms}" if delta_ms is not None else "?"
                log_line(
                    f"[ORDER] first_fill via {source_label} delta_ms={delta_text} orderId={order_ref} qty={executed_base:.8f}"
                )
        slot = self._slot_for_symbol(symbol_u)
        if not slot or not slot.position:
            return
        pos = slot.position
        if executed_base > 0.0:
            pos.qty_base = executed_base
        if cumm_quote > 0.0:
            pos.entry_quote_spent = cumm_quote
        if avg_price > 0.0:
            pos.entry_price = avg_price
            pos.tp_price = avg_price * (1.0 + _EXIT_V2_TAKE_LIMIT_PCT)
            pos.sl_stop = avg_price * (1.0 + _EXIT_V2_STOP_DROP_PCT)
        if order_id and not pos.buy_order_id:
            pos.buy_order_id = order_id
        try:
            save_state(self.state)
        except Exception:
            pass
        try:
            opened_at = datetime.fromtimestamp(pos.opened_at_ts, tz=KSA_TZ)
        except Exception:
            opened_at = now_ksa()
        log_buy(
            position_id=self._ensure_position_identity(pos),
            symbol=pos.symbol,
            slot=pos.slot,
            entry=pos.entry_price,
            qty=pos.qty_base,
            quote_in=pos.entry_quote_spent,
            opened_at=opened_at,
        )
        await self._update_take_profit(symbol_u, pos, final, source_label)

    async def _update_take_profit(self, symbol: str, position: Position, final: bool, source: str) -> None:
        s = symbol.upper()
        controller = self._exit_controllers.get(s)
        if controller is None:
            controller = await self._register_exit_controller(s, position.entry_price, position.qty_base)
        controller.state.entry_price = position.entry_price
        controller.state.qty_open = position.qty_base
        if controller.state.tp_anchor_price <= 0.0 and position.entry_price > 0.0:
            controller.state.tp_anchor_price = position.entry_price
        controller.state.entry_region_px = position.entry_price * (1.0 + _EXIT_V2_ENTRY_REGION_TOL_PCT)
        controller.state.take_limit_px = position.entry_price * (1.0 + _EXIT_V2_TAKE_LIMIT_PCT)
        controller.state.stop_px = position.entry_price * (1.0 + _EXIT_V2_STOP_DROP_PCT)
        controller.state.hit_up5 = False
        self._schedule_tp_delay(s, controller)
        if not final and _PLACE_TP_ON != "first_fill":
            return
        trigger_source = self._tp_ready(s) or (source or "UNKNOWN").upper()
        self._tp_ready_source[s] = trigger_source
        if controller.has_active_limit() or controller.state.tp_pending:
            return
        await self._ensure_take_profit(s, controller)

    # Called by start.py user stream handler for every executionReport
    def on_exec_report(self, msg: dict) -> None:
        try:
            if (msg.get("e") or "") != "executionReport":
                return
            side = (msg.get("S") or msg.get("side") or "").upper()
            symbol = (msg.get("s") or msg.get("symbol") or "").upper()
            order_id = str(msg.get("i") or msg.get("orderId") or "")
            client_id = (msg.get("c") or msg.get("clientOrderId") or msg.get("C") or "") or ""
            status = (msg.get("X") or msg.get("orderStatus") or "").upper()
            executed_base = float((msg.get("z") or msg.get("executedQty") or 0.0) or 0.0)
            cumm_quote = float((msg.get("Z") or msg.get("cummulativeQuoteQty") or 0.0) or 0.0)
            last_qty = float((msg.get("l") or msg.get("lastExecutedQty") or 0.0) or 0.0)

            if side == "BUY":
                avg_px = (cumm_quote / executed_base) if executed_base > 0 else 0.0
                self._buy_fills[client_id] = {
                    "status": status,
                    "executed_qty": executed_base,
                    "cumm_quote": cumm_quote,
                    "avg_price": avg_px,
                    "ts": time.time(),
                }
                if status in {"PARTIALLY_FILLED", "FILLED"}:
                    asyncio.create_task(
                        self._handle_buy_execution(
                            symbol=symbol,
                            client_id=client_id,
                            order_id=order_id,
                            executed_base=executed_base,
                            cumm_quote=cumm_quote,
                            avg_price=avg_px,
                            final=(status == "FILLED"),
                            source="USERSTREAM",
                        )
                    )
                if status == "FILLED":
                    evt = self._buy_fill_events.get(client_id)
                    if evt and not evt.is_set():
                        try:
                            evt.set()
                        except Exception:
                            pass
            elif side == "SELL":
                self._process_exit_execution(
                    symbol=symbol,
                    order_id=order_id,
                    client_id=client_id,
                    status=status,
                    executed_base=executed_base,
                    cumm_quote=cumm_quote,
                    last_qty=last_qty,
                    order_type=(msg.get("o") or msg.get("orderType") or "").upper(),
                )
        except Exception:
            return

    def _process_exit_execution(
        self,
        *,
        symbol: str,
        order_id: str,
        client_id: str,
        status: str,
        executed_base: float,
        cumm_quote: float,
        last_qty: float,
        order_type: str,
    ) -> None:
        s = symbol.upper()
        controller = self._exit_controllers.get(s)
        if not controller:
            return
        if order_type == "LIMIT":
            if last_qty > 0.0:
                controller.on_limit_fill(last_qty)
            if status == "CANCELED":
                self._resolve_cancel_ack(s, order_id)
                controller.clear_limit()
                if order_id:
                    self._exit_limit_by_order_id.pop(order_id, None)
                if controller.is_active and controller.qty_open > 0.0:
                    controller.state.tp_pending = True
                    self._start_tp_retry_task(s)
                    self._signal_tp_retry(s)
                controller.state.waiting_cancel_ack = False
                controller.state.tp_first_submit_ms = 0
                controller.state.tp_last_submit_ms = 0
                controller.state.tp_last_boost_ms = 0
                controller.state.tp_queue_boost_ticks = 0
                controller.state.tp_post_only_bump = 0
            if status == "FILLED":
                controller.mark_closed("take_profit")
                controller.clear_limit()
                controller.state.last_tp_warn = None
                self._exit_close_reason[s] = "take_profit"
                if order_id:
                    self._exit_limit_by_order_id.pop(order_id, None)
                self._stop_tp_retry(s)
                controller.state.tp_first_submit_ms = 0
                controller.state.tp_last_submit_ms = 0
                controller.state.tp_last_boost_ms = 0
                controller.state.tp_queue_boost_ticks = 0
                controller.state.tp_post_only_bump = 0
                controller.state.tp_anchor_price = 0.0
                self._tp_ready_source.pop(s, None)
            if status in {"EXPIRED", "REJECTED"}:
                self._resolve_cancel_ack(s, order_id)
                controller.state.waiting_cancel_ack = False
            if client_id:
                self._exit_client_tags.pop(client_id, None)
        else:
            reason_hint = self._exit_client_tags.pop(client_id, None) if client_id else None
            if last_qty > 0.0:
                controller.on_market_fill(last_qty)
            if status == "FILLED":
                reason = reason_hint or self._exit_close_reason.get(s) or controller.pending_reason or "market_exit"
                controller.mark_closed(reason)
                controller.clear_limit()
                controller.state.last_tp_warn = None
                self._exit_close_reason[s] = reason
                self._stop_tp_retry(s)
                controller.state.tp_first_submit_ms = 0
                controller.state.tp_last_submit_ms = 0
                controller.state.tp_last_boost_ms = 0
                controller.state.tp_queue_boost_ticks = 0
                controller.state.tp_post_only_bump = 0
                controller.state.tp_anchor_price = 0.0
                self._tp_ready_source.pop(s, None)

    async def _wait_for_buy_filled(self, client_order_id: Optional[str], timeout_ms: int) -> tuple[bool, int]:
        if not client_order_id or timeout_ms <= 0:
            return False, 0
        start = time.perf_counter()
        # Quick check if already seen as filled
        info = self._buy_fills.get(client_order_id)
        if info and (info.get("status") == "FILLED"):
            elapsed_ms = int((time.perf_counter() - start) * 1000)
            return True, elapsed_ms
        # Wait on event
        evt = self._buy_fill_events.get(client_order_id)
        if not evt:
            evt = asyncio.Event()
            self._buy_fill_events[client_order_id] = evt
        try:
            await asyncio.wait_for(evt.wait(), timeout=timeout_ms / 1000.0)
            elapsed_ms = int((time.perf_counter() - start) * 1000)
            return True, elapsed_ms
        except asyncio.TimeoutError:
            elapsed_ms = int((time.perf_counter() - start) * 1000)
            return False, elapsed_ms

    async def refresh_capital_day_start(self, account: Optional[dict] = None) -> None:
        try:
            acc = account if account is not None else await self.http.account()
        except Exception:
            return
        usdt = 0.0
        for b in acc.get("balances", []):
            if b.get("asset") == "USDT":
                usdt = float(b.get("free", 0.0)) + float(b.get("locked", 0.0))
                break
        self.state.capital_day_start_quote = usdt
        self.state.pnl_today_quote = 0.0
        self.state.freeze_buy = False
        save_state(self.state)

    async def split_free_usdt(self, account: Optional[dict] = None) -> tuple[Decimal, Decimal, Decimal]:
        try:
            acc = account if account is not None else await self.http.account()
        except Exception:
            return Decimal("0"), Decimal("0"), Decimal("0")

        balances = acc.get("balances", []) if isinstance(acc, dict) else []
        free_dec = Decimal("0")
        for b in balances:
            if (b.get("asset") or "").upper() == "USDT":
                free_dec = Decimal(str(b.get("free", "0") or "0"))
                break

        quant = Decimal("0.00000001")
        free_dec = free_dec.quantize(quant, rounding=ROUND_DOWN)
        half_dec = (free_dec / Decimal("2")).quantize(quant, rounding=ROUND_DOWN)
        other_dec = (free_dec - half_dec).quantize(quant, rounding=ROUND_DOWN)

        self._last_spot_free = float(free_dec)
        self.state.slot_A.balance_quote = float(half_dec)
        self.state.slot_B.balance_quote = float(other_dec)
        save_state(self.state)
        return free_dec, half_dec, other_dec

    def _slot_for_symbol(self, symbol: str) -> Optional[SlotState]:
        s = symbol.upper()
        for slot in (self.state.slot_A, self.state.slot_B):
            if slot.position and slot.position.symbol.upper() == s:
                return slot
        return None

    async def handle_buy_signal(self, req: BuyRequest) -> Optional[Position]:
        return await self._handle_buy_signal_rewired(req)
    async def _handle_buy_signal_rewired(self, req: BuyRequest) -> Optional[Position]:
        s = req.symbol.upper()
        async with self._buy_lock:
            # Enforce daily ban per symbol
            free_usdt = self._total_free_usdt()
            if self.rules.is_symbol_banned(s):
                self._entry_skip(s, "openpos_limit", free_usdt)
                return None
            # Enforce freeze after -10%
            self.rules.check_and_update_freeze()
            if self.rules.should_freeze_buys():
                self._entry_skip(s, "rate_pause", free_usdt)
                return None

            now_ts = time.time()
            if now_ts < self._global_backoff_until:
                self._entry_skip(s, "global_backoff", free_usdt)
                return None

            slot = self._select_buy_slot()
            if not slot:
                self._entry_skip(s, "slot_locked", free_usdt)
                return None

            # Spend from slot balance with configured buffer
            slot_bal = max(0.0, slot.balance_quote)
            if slot_bal <= 0.0:
                self._entry_skip(s, "thin_balance", free_usdt)
                return None

            r = compute_safe_quote(s)
            if not r.get("ok"):
                if _IMPACT_LOG_ENABLED:
                    log_line(f"[IMPACT-STALE] {s} reason={r.get('reason')}")
                self._entry_skip(s, "book_stale", free_usdt)
                return None

            total_free = max(0.0, self.state.slot_A.balance_quote + self.state.slot_B.balance_quote)
            safe_depth_quote = float(r.get("safe_quote", 0.0) or 0.0)
            if safe_depth_quote <= 0.0:
                if _IMPACT_LOG_ENABLED:
                    log_line(f"[IMPACT-SKIP] {s} safe_quote=0.0 mode={r.get('mode')}")
                self._entry_skip(s, "thin_balance", free_usdt)
                return None
            safe_cap = min(slot_bal, total_free, safe_depth_quote)
            buffer_pct = _ENTRY_QUOTE_BUFFER_PCT
            safe_quote = max(0.0, safe_cap * (1.0 - buffer_pct))
            safe_quote = min(safe_quote, req.quote_to_spend)
            if safe_quote <= 0.0:
                self._entry_skip(s, "thin_balance", free_usdt)
                return None

            best_ask = float(r.get("ask0") or 0.0)
            if best_ask <= 0.0:
                base_px = req.entry_price if (hasattr(req, "entry_price") and (req.entry_price or 0.0) > 0) else 0.0
                if base_px > 0.0:
                    best_ask = base_px
                else:
                    try:
                        best_ask = float(await self.http.ticker_price(s))
                    except Exception:
                        best_ask = 0.0
            if best_ask <= 0.0:
                self._entry_skip(s, "book_stale", free_usdt)
                return None

            worst_price = best_ask * (1.0 + _ENTRY_MAX_SLIP)
            if worst_price <= 0.0:
                worst_price = best_ask

            # Info line
            if _IMPACT_LOG_ENABLED:
                try:
                    ask0 = best_ask
                    ceil = r.get("ceil")
                    log_line(
                        f"[IMPACT] {s} ask={ask0:.8f} ceil={ceil if ceil is not None else 0.0:.8f} "
                        f"safe={float(safe_cap):.2f} spend={safe_quote:.2f} mode={r.get('mode')}"
                        + (f" r2_pl={r.get('r2_pl')} fit_ex={r.get('fit_ex')}" if r.get('mode') == 'EST' else "")
                    )
                except Exception:
                    pass

            try:
                step_sz = float(self.exinfo.step_size(s))
            except Exception:
                step_sz = 0.00000001
            try:
                min_qty = float(self.exinfo.min_qty(s))
            except Exception:
                min_qty = 0.0
            try:
                min_notional = float(self.exinfo.min_notional(s))
            except Exception:
                min_notional = 0.0
            qty_est = safe_quote / worst_price if worst_price > 0.0 else 0.0
            qty = round_to_step(qty_est, step_sz)
            while qty > 0.0 and (qty * worst_price) > safe_quote and step_sz > 0.0:
                qty = round_to_step(qty - step_sz, step_sz)
            if qty <= 0.0:
                self._entry_skip(s, "thin_balance", free_usdt)
                return None

            notional_now = qty * best_ask
            if min_qty > 0.0 and qty < min_qty:
                self._entry_skip(s, "thin_balance", free_usdt)
                return None
            if min_notional > 0.0 and notional_now < min_notional:
                self._entry_skip(s, "thin_balance", free_usdt)
                return None

            params = {
                "symbol": s,
                "side": "BUY",
                "type": "MARKET",
            }
            placed_ms = _now_ms()
            attempt_qty = qty
            attempts = 0
            reduce_once = False
            order = None
            while attempts < 2:
                attempts += 1
                order_qty = self.exinfo.fmt_qty(s, attempt_qty)
                params["quantity"] = order_qty
                dbg(
                    f"[BUY] MARKET BASE {s} qty={attempt_qty:.8f} worst_px={worst_price:.8f} safe_quote={safe_quote:.2f} attempt={attempts}"
                )
                log_line(
                    f"[ORDER] buy sent symbol={s} slot={slot.name} qty={attempt_qty:.8f} safe_quote≈{safe_quote:.2f} worst_px≈{worst_price:.8f} attempt={attempts}"
                )
                try:
                    order = await self.http.order(params)
                    qty = attempt_qty
                    break
                except BinanceHTTPError as e:
                    code = getattr(e, "code", None)
                    if str(code) == "-2010" or code == -2010:
                        if not reduce_once:
                            reduce_once = True
                            log_line("[BUY-RETRY] -2010 -> reduce qty once")
                            attempt_qty = round_to_step(attempt_qty * 0.998, step_sz)
                            while attempt_qty > 0.0 and (attempt_qty * worst_price) > safe_quote and step_sz > 0.0:
                                attempt_qty = round_to_step(attempt_qty - step_sz, step_sz)
                            if attempt_qty <= 0.0:
                                self._apply_slot_cooldown(slot.name)
                                self._entry_skip(s, "rate_pause", self._total_free_usdt())
                                return None
                            continue
                    log_line(f"[ORDER] buy failed symbol={s} code={code} msg={getattr(e,'msg','')}")
                    self._apply_slot_cooldown(slot.name)
                    self._entry_skip(s, "rate_pause", self._total_free_usdt())
                    return None
                except Exception as exc:
                    log_line(f"[ORDER] buy failed symbol={s} err={exc}")
                    self._apply_slot_cooldown(slot.name)
                    self._entry_skip(s, "rate_pause", self._total_free_usdt())
                    return None
            if order is None:
                self._apply_slot_cooldown(slot.name)
                self._entry_skip(s, "rate_pause", self._total_free_usdt())
                return None

            order_id = str(order.get("orderId", "") or "")
            client_id_ack = str(order.get("clientOrderId", "") or "")
            status_ack = (order.get("status") or "").upper()
            log_line(
                f"[ORDER] buy ACK symbol={s} orderId={order_id or '?'} clientId={client_id_ack or '?'} status={status_ack or '?'}"
            )

            cumm_quote = float(order.get("cummulativeQuoteQty", 0.0) or 0.0)
            executed_qty = float(order.get("executedQty", 0.0) or 0.0)
            avg_price = (cumm_quote / executed_qty) if executed_qty > 0 else req.entry_price

            take_limit_px = avg_price * (1.0 + _EXIT_V2_TAKE_LIMIT_PCT)
            stop_px = avg_price * (1.0 + _EXIT_V2_STOP_DROP_PCT)
            pullback_px = avg_price * (1.0 + _EXIT_V2_PULLBACK_UP_PCT)
            req.tp_p10 = take_limit_px
            req.sl_m5 = stop_px
            req.target_p5 = pullback_px
            opened_at_dt = now_ksa()
            pos = Position(
                symbol=s,
                entry_price=avg_price,
                qty_base=executed_qty,
                entry_quote_spent=cumm_quote,
                opened_at_ts=int(opened_at_dt.timestamp()),
                slot=slot.name,
                open_1m=req.open_1m,
                tp_price=take_limit_px,
                sl_stop=stop_px,
                client_order_id=client_id_ack or order.get("clientOrderId"),
                position_id=self._make_position_id(s),
                buy_order_id=order_id or None,
                fees_buy_quote=cumm_quote * _FEE_BUY_RATE,
            )
            slot.position = pos
            self._clear_entry_skip(s)
            self._open_positions += 1
            self._register_tp_plan(
                symbol=s,
                slot=slot.name,
                client_order_id=client_id_ack or None,
                order_id=order_id or None,
                placed_ms=placed_ms,
            )
            slot.balance_quote = max(0.0, slot.balance_quote - cumm_quote)
            self._last_spot_free = max(0.0, self._last_spot_free - cumm_quote)
            self.rules.ban_symbol_until_reset(s)
            save_state(self.state)

            log_buy(
                position_id=self._ensure_position_identity(pos),
                symbol=s,
                slot=slot.name,
                entry=avg_price,
                qty=executed_qty,
                quote_in=cumm_quote,
                opened_at=opened_at_dt,
            )

            dbg(f"BUY {s} executed qty={executed_qty:.8f} cumm_quote={cumm_quote:.2f} avg_price={avg_price:.8f} slot={slot.name}")
            return pos

    # Called by user data stream handlers upon SELL fill
    def on_sell_closed(self, summary: SellExecution) -> None:
        symbol = summary.symbol.upper()
        slot = self._slot_for_symbol(symbol)
        if not slot or not slot.position:
            return

        pos = slot.position
        position_id = self._ensure_position_identity(pos)
        qty_closed = summary.qty if summary.qty > 0 else pos.qty_base
        if qty_closed <= 0.0:
            qty_closed = pos.qty_base
        exit_price = summary.avg_price if summary.avg_price > 0 else (
            (summary.quote_gross / qty_closed) if qty_closed > 0 else summary.avg_price
        )
        quote_gross = summary.quote_gross if summary.quote_gross > 0 else exit_price * qty_closed
        fee_sell = summary.fee_quote if summary.fee_quote > 0 else quote_gross * _FEE_SELL_RATE
        quote_net = summary.quote_net if summary.quote_net > 0 else max(0.0, quote_gross - fee_sell)

        entry_quote = pos.entry_quote_spent
        fees_buy = pos.fees_buy_quote if pos.fees_buy_quote > 0 else entry_quote * _FEE_BUY_RATE
        pos.fees_buy_quote = fees_buy
        net_in = entry_quote + fees_buy
        net_out = max(0.0, quote_net)

        event_ms = summary.event_time_ms if summary.event_time_ms > 0 else int(time.time() * 1000)
        try:
            closed_at = datetime.fromtimestamp(event_ms / 1000.0, tz=timezone.utc).astimezone(KSA_TZ)
        except Exception:
            closed_at = now_ksa()
        try:
            opened_at = datetime.fromtimestamp(pos.opened_at_ts, tz=KSA_TZ)
        except Exception:
            opened_at = now_ksa()
        held_secs = max(0, int(closed_at.timestamp() - pos.opened_at_ts))

        pnl_abs = net_out - net_in
        pnl_pct = (pnl_abs / net_in * 100.0) if net_in > 0 else 0.0

        reason_key = (
            self._exit_close_reason.get(symbol, "")
            or (summary.reason_hint or "")
        ).lower()
        is_tp = summary.order_type.upper().startswith("TAKE_PROFIT") or reason_key == "take_profit"
        if is_tp:
            sale_kind = "tp"
            method = "TP_LIMIT_MAKER"
            reason_label = None
            reason_key = "take_profit"
        else:
            reason_map = {
                "stop": ("STOP", "MARKET_STOP"),
                "pullback": ("PULLBACK", "MARKET_PULLBACK"),
                "market_exit": ("UNWIND", "MARKET_UNWIND"),
                "unwind": ("UNWIND", "MARKET_UNWIND"),
                "dust": ("UNWIND", "MARKET_UNWIND"),
            }
            reason_label, method = reason_map.get(reason_key, ("UNWIND", "MARKET_UNWIND"))
            sale_kind = "mkt"

        slot.balance_quote += net_out
        self._last_spot_free += net_out
        slot.position = None
        self.state.pnl_today_quote += pnl_abs

        log_sell(
            position_id=position_id,
            symbol=symbol,
            slot=slot.name,
            exit_price=exit_price,
            qty=qty_closed,
            quote_out=quote_gross,
            pnl_pct=pnl_pct,
            held_secs=held_secs,
            closed_at=closed_at,
            sale_kind=sale_kind,
            reason=reason_label,
        )

        try:
            reporter = getattr(market_ctx, "report", None)
            if reporter is not None:
                reporter.trade(
                    symbol=symbol,
                    side="SELL",
                    method=method,
                    qty_base=float(qty_closed),
                    entry_px=float(pos.entry_price),
                    exit_px=float(exit_price),
                    quote_in=float(entry_quote),
                    quote_out=float(quote_gross),
                    fees_buy=float(fees_buy),
                    fees_sell=float(fee_sell),
                    pnl_abs=float(pnl_abs),
                    pnl_pct=float(pnl_pct),
                    held_seconds=int(held_secs),
                    t_open=opened_at,
                    t_close=closed_at,
                    buy_order_id=pos.buy_order_id or pos.client_order_id or "",
                    sell_order_id=summary.order_id or summary.client_order_id or "",
                )
        except Exception:
            pass

        controller = self._exit_controllers.get(symbol)
        self._on_position_closed(symbol, slot, controller, reason_key=reason_key)

        self.rules.check_and_update_freeze()
        save_state(self.state)

        # Exit V1 timeout plumbing removed; Exit V2 handles lifecycle internally.

    async def auto_topup_bnb_for_fees(self) -> None:
        """Ensure BNB holdings â‰ˆ1% of total USD equity with hysteresis.
        Runs once after daily reset (scheduler calls this), then schedules limited re-attempts
        if conditions (USDT free increase or cooldown) are met. No logging/printing.
        """
        # Avoid duplicate runners
        if self._bnb_topup_task and not self._bnb_topup_task.done():
            return

        async def _runner():
            try:
                # Initial delay 25s after reset to avoid contention
                await asyncio.sleep(25)
                # Attempt once immediately after delay
                success = await self._attempt_bnb_topup()
                if success:
                    return
                # If pending deficit recorded, schedule sparse retries
                # Cooldown baseline: 45 minutes, or earlier if USDT free increases by >= $10
                last_usdt = float(self._bnb_topup_pending.get("last_usdt_free", 0.0))
                last_attempt_ts = float(self._bnb_topup_pending.get("last_attempt_ts", 0.0))
                # Run for up to ~20 hours or until success/new day
                while True:
                    await asyncio.sleep(300)  # 5 minutes granularity, no busy loop
                    # Terminate if new day (03:00 reset expected to start a fresh runner)
                    # Also stop if there's no pending deficit
                    pending_def = float(self._bnb_topup_pending.get("pending_deficit_usd", 0.0))
                    if pending_def <= 0:
                        return
                    # Check time-based cooldown
                    now_ts = time.time()
                    time_ok = (now_ts - last_attempt_ts) >= (45 * 60)
                    # Check USDT free increase >= $10
                    try:
                        acc = await self.http.account()
                        usdt_bal = next((b for b in acc.get("balances", []) if (b.get("asset") or "").upper() == "USDT"), None)
                        usdt_free = float(usdt_bal.get("free", 0.0) or 0.0) if usdt_bal else 0.0
                    except Exception:
                        usdt_free = 0.0
                    inc_ok = (usdt_free - last_usdt) >= 10.0
                    if time_ok or inc_ok:
                        success = await self._attempt_bnb_topup()
                        if success:
                            return
                        last_attempt_ts = float(self._bnb_topup_pending.get("last_attempt_ts", now_ts))
                        last_usdt = float(self._bnb_topup_pending.get("last_usdt_free", usdt_free))
            except asyncio.CancelledError:
                return

        self._bnb_topup_task = asyncio.create_task(_runner())

    async def _attempt_bnb_topup(self) -> bool:
        """Single evaluation + (optional) market BUY for BNBUSDT. Returns True if a BUY was placed."""
        # Fetch account balances
        try:
            acc = await self.http.account()
        except Exception:
            return False
        balances = acc.get("balances", []) if isinstance(acc, dict) else []
        # Build total USD valuation
        total_usd = 0.0
        assets = []
        for b in balances:
            try:
                asset = (b.get("asset") or "").upper()
                free = float(b.get("free", 0.0) or 0.0)
                locked = float(b.get("locked", 0.0) or 0.0)
            except Exception:
                continue
            amt = free + locked
            if amt <= 0:
                continue
            assets.append((asset, amt))
        # Price cache to reduce REST calls
        price_cache: Dict[str, float] = {}

        def _price_usd(sym_asset: str) -> float:
            if sym_asset == "USDT":
                return 1.0
            p = price_cache.get(sym_asset)
            if p is not None:
                return p
            pair = f"{sym_asset}USDT"
            # Ensure symbol exists
            try:
                self.exinfo.get_symbol(pair)
            except Exception:
                price_cache[sym_asset] = 0.0
                return 0.0
            return 0.0  # placeholder; actual fetch outside to allow await

        # First pass: determine which assets need price fetch
        to_fetch: list[str] = []
        for asset, _amt in assets:
            if asset == "USDT":
                total_usd += _amt
            else:
                if price_cache.get(asset) is None:
                    price_cache[asset] = None  # mark
                    # Only if symbol exists
                    try:
                        self.exinfo.get_symbol(f"{asset}USDT")
                        to_fetch.append(asset)
                    except Exception:
                        price_cache[asset] = 0.0
        # Fetch prices sequentially for needed assets
        for a in to_fetch:
            try:
                price = await self.http.ticker_price(f"{a}USDT")
            except Exception:
                price = 0.0
            price_cache[a] = float(price or 0.0)
        # Second pass: sum using cache
        for asset, amt in assets:
            if asset == "USDT":
                continue  # already added
            px = float(price_cache.get(asset, 0.0) or 0.0)
            total_usd += amt * px

        # BNB valuation
        try:
            bnb_price = float(await self.http.ticker_price("BNBUSDT"))
        except Exception:
            bnb_price = 0.0
        bnb_units = 0.0
        for a, amt in assets:
            if a == "BNB":
                bnb_units = amt
                break
        bnb_usd = bnb_units * bnb_price if bnb_price > 0 else 0.0

        # Hysteresis thresholds
        target_pct_high = 0.0102  # 1.02%
        target_pct_low = 0.0098   # 0.98%
        if total_usd <= 0.0:
            return False
        # Only act if below low band
        if bnb_usd >= (total_usd * target_pct_low):
            self._bnb_topup_pending.clear()
            return False

        target_usd = total_usd * target_pct_high
        deficit_usd = max(0.0, target_usd - bnb_usd)
        # Minimum deficit $2
        if deficit_usd < 2.0:
            self._bnb_topup_pending = {
                "pending_deficit_usd": deficit_usd,
                "last_attempt_ts": time.time(),
            }
            return False

        # USDT free and reserve check
        try:
            usdt_bal = next((b for b in balances if (b.get("asset") or "").upper() == "USDT"), None)
            usdt_free = float(usdt_bal.get("free", 0.0) or 0.0) if usdt_bal else 0.0
        except Exception:
            usdt_free = 0.0
        if usdt_free < (deficit_usd + 20.0):
            self._bnb_topup_pending = {
                "pending_deficit_usd": deficit_usd,
                "last_attempt_ts": time.time(),
                "last_usdt_free": usdt_free,
            }
            return False

        # Quantity calculation respecting step/minNotional
        symbol = "BNBUSDT"
        try:
            step = float(self.exinfo.step_size(symbol))
        except Exception:
            step = 0.0001
        try:
            min_notional = float(self.exinfo.min_notional(symbol))
            if min_notional <= 0:
                # Fallback to NOTIONAL if present
                f = self.exinfo.filters(symbol).get("NOTIONAL", {})
                min_notional = float(f.get("minNotional", 0.0) or 0.0)
        except Exception:
            min_notional = 0.0

        qty_raw = deficit_usd / (bnb_price or 1.0)
        qty = round_to_step(qty_raw, step)
        if qty <= 0:
            self._bnb_topup_pending = {
                "pending_deficit_usd": deficit_usd,
                "last_attempt_ts": time.time(),
                "last_usdt_free": usdt_free,
            }
            return False
        notional = qty * (bnb_price or 0.0)
        if min_notional > 0 and notional < min_notional:
            self._bnb_topup_pending = {
                "pending_deficit_usd": deficit_usd,
                "last_attempt_ts": time.time(),
                "last_usdt_free": usdt_free,
            }
            return False

        # Place MARKET BUY for BNBUSDT with base quantity
        params = {
            "symbol": symbol,
            "side": "BUY",
            "type": "MARKET",
            "quantity": self.exinfo.fmt_qty(symbol, qty),
        }
        try:
            await self.http.order(params)
            # Clear pending on success
            self._bnb_topup_pending.clear()
            return True
        except BinanceHTTPError:
            # Keep pending; will let watchdog retry
            self._bnb_topup_pending = {
                "pending_deficit_usd": deficit_usd,
                "last_attempt_ts": time.time(),
                "last_usdt_free": usdt_free,
            }
            return False
