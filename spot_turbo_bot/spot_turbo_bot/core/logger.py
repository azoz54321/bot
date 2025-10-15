from __future__ import annotations

from datetime import datetime, timezone, timedelta
from typing import Optional

from .config import (
    DEBUG_VERBOSE,
    DEBUG_WS_LOGS,
    KSA_UTC_OFFSET,
    LOG_DEBUG,
    LOG_TP_EVENTS,
    LOG_TRADES_ONLY,
)


KSA_TZ = timezone(timedelta(seconds=KSA_UTC_OFFSET))
_LOG_TRADES_ONLY = bool(LOG_TRADES_ONLY)
_LOG_TP_EVENTS = bool(LOG_TP_EVENTS)
_LOG_DEBUG = bool(LOG_DEBUG)

_BUY_LOGGED: set[str] = set()
_SELL_LOGGED: set[str] = set()

_TRADE_PREFIXES = {"[BUY]", "[SELL-MKT]", "[SELL-TP]"}


def _to_iso(ts: datetime) -> str:
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(KSA_TZ).replace(microsecond=0).isoformat()


def _format_held(held_secs: int) -> str:
    held_secs = max(0, int(held_secs))
    hours = held_secs // 3600
    minutes = (held_secs % 3600) // 60
    seconds = held_secs % 60
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def _prefix_of(msg: str) -> str:
    end = msg.find("]")
    if end == -1:
        return ""
    return msg[: end + 1]


def _should_emit(msg: str) -> bool:
    prefix = _prefix_of(msg)
    upper_msg = msg.upper()

    if _LOG_TRADES_ONLY:
        return prefix in _TRADE_PREFIXES

    if not _LOG_DEBUG and prefix not in _TRADE_PREFIXES:
        return False

    if not _LOG_TP_EVENTS:
        if prefix in _TRADE_PREFIXES:
            return True
        if "TP" in upper_msg:
            return False

    return True


def log_line(msg: str) -> None:
    if not _should_emit(msg):
        return
    print(msg, flush=True)


def log_boot(msg: Optional[str] = None) -> None:
    if msg:
        log_line(f"[BOOT] {msg}")
    else:
        log_line("[BOOT]")


def log_buy(
    *,
    position_id: str,
    symbol: str,
    slot: str,
    entry: float,
    qty: float,
    quote_in: float,
    opened_at: datetime,
) -> bool:
    if not position_id:
        return False
    if position_id in _BUY_LOGGED:
        return False
    _BUY_LOGGED.add(position_id)
    iso_time = _to_iso(opened_at)
    log_line(
        f"[BUY] {symbol.upper()} entry={entry:.8f} qty={qty:.8f} "
        f"quote_in={quote_in:.2f} slot={slot} t={iso_time}"
    )
    return True


def log_sell(
    *,
    position_id: str,
    symbol: str,
    slot: str,
    exit_price: float,
    qty: float,
    quote_out: float,
    pnl_pct: float,
    held_secs: int,
    closed_at: datetime,
    sale_kind: str,
    reason: Optional[str] = None,
) -> bool:
    if not position_id:
        return False
    if position_id in _SELL_LOGGED:
        return False
    _SELL_LOGGED.add(position_id)

    iso_time = _to_iso(closed_at)
    held_str = _format_held(held_secs)
    pnl_str = f"{pnl_pct:+.2f}"

    if sale_kind.lower() == "tp":
        line = (
            f"[SELL-TP] {symbol.upper()} exit={exit_price:.8f} qty={qty:.8f} "
            f"quote_out={quote_out:.2f} pnl%={pnl_str} held={held_str} t={iso_time}"
        )
    else:
        reason_label = (reason or "").upper() or "UNKNOWN"
        line = (
            f"[SELL-MKT] {symbol.upper()} exit={exit_price:.8f} reason={reason_label} "
            f"qty={qty:.8f} quote_out={quote_out:.2f} pnl%={pnl_str} "
            f"held={held_str} t={iso_time}"
        )
    log_line(line)
    return True


def forget_position(position_id: str) -> None:
    _BUY_LOGGED.discard(position_id)
    _SELL_LOGGED.discard(position_id)


def log_daily_reset() -> None:
    log_line("[BOOT] daily reset @ 03:00 KSA")


def dbg(msg: str) -> None:
    if DEBUG_VERBOSE and _LOG_DEBUG:
        log_line(f"[DBG] {msg}")


def dbg_ws(symbol: str, msg: str) -> None:
    if DEBUG_WS_LOGS and _LOG_DEBUG:
        log_line(f"[DBG-WS] {symbol} {msg}")
