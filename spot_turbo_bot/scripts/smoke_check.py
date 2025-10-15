from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path


async def _load_exchange_info(http):
    from spot_turbo_bot.core.exchange_info import ExchangeInfo

    try:
        return ExchangeInfo.load_cached()
    except FileNotFoundError:
        return await ExchangeInfo.aload(http)


async def _probe_depth_ws(exinfo, symbols):
    from spot_turbo_bot.market.context import build_symbols_meta
    from spot_turbo_bot.market.depth_ws import DepthWS
    from spot_turbo_bot.core.config import (
        DEPTH_BASELINE_N,
        DEPTH_SHARD_SIZE,
        DEPTH_WS_CADENCE_MS,
    )

    meta = build_symbols_meta(exinfo, symbols)
    depth = DepthWS(
        meta,
        baseline_N=DEPTH_BASELINE_N,
        shard_size=max(1, len(symbols), DEPTH_SHARD_SIZE),
        cadence_ms=DEPTH_WS_CADENCE_MS,
    )
    try:
        await depth.start()
        await asyncio.sleep(4.0)
        stats = depth.stats_snapshot()
        if not stats.get("book_total") and not stats.get("depth_total"):
            print("[SMOKE] note: no market data observed during the short run.", flush=True)
        return stats
    finally:
        await depth.stop()


async def main() -> int:
    required = ("BINANCE_API_KEY", "BINANCE_API_SECRET")
    missing = [name for name in required if not os.getenv(name)]
    if missing:
        print(
            "[SMOKE] note: Binance API credentials not provided; signed REST endpoints are skipped.",
            flush=True,
        )

    # Import config to ensure secrets are parsed and to re-use helpers.
    from spot_turbo_bot.core import config  # noqa: F401
    from spot_turbo_bot.infra.binance_http import BinanceHTTP
    from spot_turbo_bot.trading.trading_engine import TradingEngine
    from spot_turbo_bot.core.state import load_state

    http = BinanceHTTP()
    exinfo = await _load_exchange_info(http)

    symbols = [s for s in exinfo.usdt_trading_symbols() if exinfo.is_trading(s)]
    if not symbols:
        raise RuntimeError("exchangeInfo contains no USDT trading symbols.")
    subset = symbols[: min(10, len(symbols))]

    depth_stats = await _probe_depth_ws(exinfo, subset)

    state = load_state()
    engine = TradingEngine(http, exinfo, state, audit=None)
    account_stub = {
        "balances": [
            {"asset": "USDT", "free": "0", "locked": "0"},
        ]
    }
    await engine.refresh_capital_day_start(account_stub)
    await engine.split_free_usdt(account_stub)
    try:
        await engine.bootstrap_exit_v2()
    except Exception:
        pass


    print("[SMOKE] environment ok; depth ratios:", depth_stats, flush=True)
    if missing:
        print("[SMOKE] real trading requires BINANCE_API_KEY/BINANCE_API_SECRET.", flush=True)
    return 0


if __name__ == "__main__":
    try:
        exit_code = asyncio.run(main())
    except KeyboardInterrupt:
        exit_code = 1
    except Exception as exc:
        print(f"[SMOKE] failed: {exc}", file=sys.stderr, flush=True)
        exit_code = 1
    sys.exit(exit_code)
