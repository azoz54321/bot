from __future__ import annotations

from typing import Any

from ..core.config import (
    ENABLE_SERVER_TIME_SYNC,
    SERVER_TIME_SYNC_SEC,
    LISTENKEY_RENEW_MIN,
)


def run_boot_lint(exinfo: Any) -> None:
    warnings = 0
    errors = 0

    # exchangeInfo available
    try:
        syms = exinfo.usdt_trading_symbols()
        if not syms:
            errors += 1
    except Exception:
        errors += 1

    # Time sync
    if not ENABLE_SERVER_TIME_SYNC or SERVER_TIME_SYNC_SEC <= 0:
        warnings += 1

    # listenKey renew
    if not (1 <= LISTENKEY_RENEW_MIN <= 29):
        warnings += 1

    # Silent result; use stdout or dbg in caller if needed
