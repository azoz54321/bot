from __future__ import annotations

from datetime import datetime, timezone, timedelta

from .config import (
    DEBUG_VERBOSE,
    DEBUG_WS_LOGS,
    EXIT_V2_TAKE_LIMIT_PCT,
    EXIT_V2_STOP_DROP_PCT,
    EXIT_V2_PULLBACK_UP_PCT,
    KSA_UTC_OFFSET,
    LOG_DEBUG,
    LOG_TP_EVENTS,
    LOG_TRADES_ONLY,
)


KSA_TZ = timezone(timedelta(seconds=KSA_UTC_OFFSET))


def _now_ksa_str() -> str:
    return datetime.now(tz=KSA_TZ).strftime("%Y-%m-%d %H:%M:%S")


_TRADE_PREFIXES = ("[BUY]", "[SELL-TP]", "[SELL-MKT]")
_DEBUG_PREFIXES = ("[DBG", "[DATA]", "[USER]", "[AGG", "[EXIT", "[WARN]", "[IMPACT", "[ENTRY", "[TRIG]", "[BUS]", "[BOOT]")


def _should_emit(msg: str) -> bool:
    if LOG_TRADES_ONLY:
        return msg.startswith(_TRADE_PREFIXES)
    if not LOG_TP_EVENTS and msg.startswith("[EXIT"):
        return False
    if not LOG_DEBUG and msg.startswith(tuple(p for p in _DEBUG_PREFIXES if p != "[EXIT")):
        return False
    return True


def log_line(msg: str) -> None:
    """Emit a single log line via stdout respecting configured filters."""
    if not _should_emit(msg):
        return
    print(msg, flush=True)


def log_boot(msg: str | None = None) -> None:
    if msg:
        log_line(f"[BOOT] {msg}")
    else:
        log_line("[BOOT]")


def log_buy(
    *,
    symbol: str,
    slot: str,
    ksa_time: datetime,
    entry: float,
    open_1m: float | None,
    target_p5: float,
    tp_p10: float,
    sl_m5: float,
    quote_spent: float,
) -> None:
    ksa_str = ksa_time.strftime("%Y-%m-%d %H:%M:%S")
    open_1m_str = f"{open_1m:.8f}" if open_1m is not None else "-"
    try:
        pct = ((target_p5 / entry) - 1.0) if (entry and target_p5) else 0.0
    except Exception:
        pct = 0.0
    pct_label = f"+{pct * 100:.2f}%"
    tp_label = f"{EXIT_V2_TAKE_LIMIT_PCT * 100:+.2f}%"
    sl_label = f"{EXIT_V2_STOP_DROP_PCT * 100:+.2f}%"
    pullback_label = f"{EXIT_V2_PULLBACK_UP_PCT * 100:+.2f}%"
    log_line(
        f"[BUY] {symbol} slot={slot} t={ksa_str} "
        f"entry={entry:.8f} open_1m={open_1m_str} "
        f"target({pct_label})={target_p5:.8f} TP({tp_label})={tp_p10:.8f} "
        f"Stop({sl_label})={sl_m5:.8f} Pullback={pullback_label} quote_spent={quote_spent:.2f}"
    )


def log_sell(
    *,
    symbol: str,
    slot: str,
    reason: str,
    entry: float,
    exit: float,
    pnl_pct: float,
    quote_out: float,
    held_secs: int,
) -> None:
    held_h = held_secs // 3600
    held_m = (held_secs % 3600) // 60
    held_str = f"{held_h}:{held_m:02d}"
    log_line(
        f"[SELL] {symbol} slot={slot} reason={reason} "
        f"{entry:.8f} -> {exit:.8f} pnl%={pnl_pct:.2f} "
        f"quote_out={quote_out:.2f} held={held_str}"
    )


def log_daily_reset() -> None:
    log_line("[BOOT] daily reset @ 03:00 KSA")


def dbg(msg: str) -> None:
    if DEBUG_VERBOSE and LOG_DEBUG:
        log_line(f"[DBG] {msg}")


def dbg_ws(symbol: str, msg: str) -> None:
    if DEBUG_WS_LOGS and LOG_DEBUG:
        log_line(f"[DBG-WS] {symbol} {msg}")
