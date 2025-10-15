from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional, Set


_EPS = 1e-9


@dataclass(slots=True)
class ExitV2Params:
    take_limit_pct: float
    stop_drop_pct: float
    pullback_up_pct: float
    entry_region_tol_pct: float
    debounce_ms: int


@dataclass(slots=True)
class ExitV2State:
    symbol: str
    entry_price: float
    qty_open: float
    entry_region_px: float
    take_limit_px: float
    stop_px: float
    pullback_arm_px: float
    status: str = "active"  # active | closing | closed
    limit_order_id: Optional[str] = None
    hit_up5: bool = False
    debounce_target: Optional[str] = None
    debounce_start_ms: Optional[int] = None
    pending_reason: Optional[str] = None
    last_price: float = 0.0
    last_price_ms: int = 0
    limit_active_qty: float = 0.0
    limit_price: float = 0.0
    min_notional_blocked: bool = False
    last_tp_warn: Optional[tuple[str, str]] = None
    tp_attempts: int = 0
    tp_deadline_ts: float = 0.0
    tp_pending: bool = False
    warn_once_flags: Set[str] = field(default_factory=set)
    tp_anchor_price: float = 0.0
    tp_first_submit_ms: int = 0
    tp_last_submit_ms: int = 0
    tp_last_boost_ms: int = 0
    tp_queue_boost_ticks: int = 0
    tp_post_only_bump: int = 0
    waiting_cancel_ack: bool = False


class ExitV2Controller:
    """Pure state machine for Exit V2 behaviour."""

    def __init__(self, symbol: str, entry_price: float, filled_qty: float, params: ExitV2Params) -> None:
        take_limit_px = entry_price * (1.0 + params.take_limit_pct)
        stop_px = entry_price * (1.0 + params.stop_drop_pct)
        pullback_px = entry_price * (1.0 + params.pullback_up_pct)
        entry_region_px = entry_price * (1.0 + params.entry_region_tol_pct)
        self.params = params
        self.state = ExitV2State(
            symbol=symbol.upper(),
            entry_price=entry_price,
            qty_open=max(filled_qty, 0.0),
            entry_region_px=entry_region_px,
            take_limit_px=take_limit_px,
            stop_px=stop_px,
            pullback_arm_px=pullback_px,
            tp_anchor_price=0.0,
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _reset_debounce(self) -> None:
        self.state.debounce_target = None
        self.state.debounce_start_ms = None

    def _should_trigger(self, target: str, now_ms: int) -> Optional[str]:
        if self.state.status != "active":
            return None
        if self.state.qty_open <= _EPS:
            return None
        if self.state.debounce_target != target:
            self.state.debounce_target = target
            self.state.debounce_start_ms = now_ms
            return None
        if self.state.debounce_start_ms is None:
            self.state.debounce_start_ms = now_ms
            return None
        if (now_ms - self.state.debounce_start_ms) < self.params.debounce_ms:
            return None
        self.state.status = "closing"
        self.state.pending_reason = target
        self._reset_debounce()
        return target

    # ------------------------------------------------------------------
    # External event hooks
    # ------------------------------------------------------------------
    def bind_limit(self, order_id: str, qty: float, price: float) -> None:
        self.state.limit_order_id = order_id
        self.state.limit_active_qty = max(qty, 0.0)
        self.state.limit_price = price

    def clear_limit(self) -> None:
        self.state.limit_order_id = None
        self.state.limit_active_qty = 0.0
        self.state.limit_price = 0.0
        self.state.min_notional_blocked = False

    def on_limit_fill(self, qty_filled: float) -> None:
        if qty_filled <= 0:
            return
        self.state.qty_open = max(0.0, self.state.qty_open - qty_filled)
        self.state.limit_active_qty = max(0.0, self.state.limit_active_qty - qty_filled)
        if self.state.qty_open <= _EPS:
            self.state.qty_open = 0.0
            self.state.status = "closed"
            self.state.pending_reason = "take_profit"
            self._reset_debounce()

    def on_market_fill(self, qty_filled: float) -> None:
        if qty_filled <= 0:
            return
        self.state.qty_open = max(0.0, self.state.qty_open - qty_filled)
        if self.state.qty_open <= _EPS:
            self.state.qty_open = 0.0
            self.state.status = "closed"
            self._reset_debounce()

    def mark_close_failed(self) -> None:
        if self.state.status == "closing" and self.state.qty_open > _EPS:
            self.state.status = "active"
            self.state.pending_reason = None
            self._reset_debounce()

    def mark_closed(self, reason: Optional[str] = None) -> None:
        self.state.status = "closed"
        if reason:
            self.state.pending_reason = reason
        self._reset_debounce()

    # ------------------------------------------------------------------
    # Price observation
    # ------------------------------------------------------------------
    def observe_price(self, price: float, now_ms: int, stale: bool) -> Optional[str]:
        self.state.last_price = price
        self.state.last_price_ms = now_ms
        if stale:
            self._reset_debounce()
            return None

        trigger: Optional[str] = None
        if price >= self.state.pullback_arm_px:
            self.state.hit_up5 = True

        if price <= self.state.stop_px:
            trigger = self._should_trigger("stop", now_ms)
        elif self.state.hit_up5 and price <= self.state.entry_region_px:
            trigger = self._should_trigger("pullback", now_ms)
        else:
            self._reset_debounce()

        return trigger

    # ------------------------------------------------------------------
    # Accessors
    # ------------------------------------------------------------------
    @property
    def is_active(self) -> bool:
        return self.state.status == "active"

    @property
    def is_closing(self) -> bool:
        return self.state.status == "closing"

    @property
    def is_closed(self) -> bool:
        return self.state.status == "closed"

    @property
    def pending_reason(self) -> Optional[str]:
        return self.state.pending_reason

    @property
    def qty_open(self) -> float:
        return self.state.qty_open

    @property
    def limit_order_id(self) -> Optional[str]:
        return self.state.limit_order_id

    def has_active_limit(self) -> bool:
        return bool(self.state.limit_order_id)

    @property
    def symbol(self) -> str:
        return self.state.symbol

    def reset_pending_reason(self) -> None:
        self.state.pending_reason = None
