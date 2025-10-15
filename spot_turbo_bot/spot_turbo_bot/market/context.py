import asyncio
from typing import Dict, Optional

from .depth_ws import DepthWS
from .micro_stats import MicroStats
from ..core.logger import log_line

try:
    from ..reports.report_writer import DailyTextReport
except Exception:  # pragma: no cover - optional dependency
    DailyTextReport = None  # type: ignore[assignment]

from ..core.config import (
    DEBUG_WS_LOGS,
    DEPTH_BASELINE_N,
    DEPTH_HEALTH_SUMMARY_SEC,
    DEPTH_SHARD_SIZE,
    DEPTH_WS_CADENCE_MS,
    MICRO_ROLLING_WINDOW_MS,
    MICRO_TICK_INTERVAL_SEC,
    STALE_SUMMARY_LOG_ENABLED,
)

_WS_DEBUG_ENABLED = bool(DEBUG_WS_LOGS)
_DEPTH_BASELINE_N = int(DEPTH_BASELINE_N)
_DEPTH_SHARD_SIZE = int(DEPTH_SHARD_SIZE)
_DEPTH_WS_CADENCE_MS = int(DEPTH_WS_CADENCE_MS)
_MICRO_ROLLING_WINDOW_MS = int(MICRO_ROLLING_WINDOW_MS)
_MICRO_TICK_INTERVAL = float(MICRO_TICK_INTERVAL_SEC)
_DEPTH_SUMMARY_INTERVAL = int(DEPTH_HEALTH_SUMMARY_SEC)
_STALE_SUMMARY_ENABLED = bool(STALE_SUMMARY_LOG_ENABLED)

depth_ws: Optional[DepthWS] = None
micro: Optional[MicroStats] = None
_micro_task: Optional[asyncio.Task] = None
_fresh_task: Optional[asyncio.Task] = None
report: Optional[DailyTextReport] = None  # type: ignore[var-annotated]


def build_symbols_meta(exinfo, symbols: list[str]) -> Dict[str, dict]:
    out: Dict[str, dict] = {}
    for s in symbols:
        try:
            out[s] = {"tick": float(exinfo.tick_size(s)), "step": float(exinfo.step_size(s))}
        except Exception:
            continue
    return out


def setup(symbols_meta: Dict[str, dict]) -> None:
    global depth_ws, micro
    depth_ws = DepthWS(
        symbols_meta,
        baseline_N=_DEPTH_BASELINE_N,
        shard_size=_DEPTH_SHARD_SIZE,
        cadence_ms=_DEPTH_WS_CADENCE_MS,
    )
    micro = MicroStats(depth_ws.books, rolling_ms=_MICRO_ROLLING_WINDOW_MS)


async def start_services() -> None:
    global _micro_task, _fresh_task
    if depth_ws is None or micro is None:
        return

    await depth_ws.start()

    if _WS_DEBUG_ENABLED:
        try:
            log_line(
                "[DEPTH-BOOT] "
                f"books={len(depth_ws.books)} "
                f"baseline_N={getattr(depth_ws, 'N', None)} "
                f"cadence_ms={getattr(depth_ws, 'cadence_ms', None)} "
                f"shardsz={getattr(depth_ws, 'shard_size', None)}"
            )
        except Exception:
            pass

    local_depth_ws = depth_ws
    local_micro = micro

    async def _tick() -> None:
        books = local_depth_ws.books
        update_for = local_micro.update_for
        sleep = asyncio.sleep
        interval = _MICRO_TICK_INTERVAL
        while True:
            for sym in books.keys():
                try:
                    update_for(sym)
                except Exception:
                    pass
            await sleep(interval)

    _micro_task = asyncio.create_task(_tick())

    if _DEPTH_SUMMARY_INTERVAL > 0:

        async def _fresh_loop() -> None:
            sleep = asyncio.sleep
            interval = _DEPTH_SUMMARY_INTERVAL
            while True:
                await sleep(interval)
                try:
                    snap = local_depth_ws.stats_snapshot()
                    if _STALE_SUMMARY_ENABLED:
                        book_ok = snap.get("book_ok", 0.0)
                        book_total = snap.get("book_total", 0.0)
                        depth_ok = snap.get("depth_ok", 0.0)
                        depth_total = snap.get("depth_total", 0.0)
                        msg = (
                            f"[STALE-SUM] book_ratio={snap.get('book_ratio', 0.0):.3f} "
                            f"({book_ok}/{book_total}) "
                            f"depth_ratio={snap.get('depth_ratio', 0.0):.3f} "
                            f"({depth_ok}/{depth_total})"
                        )
                        if book_total == 0.0:
                            msg += " | bookTicker inactive or no data observed"
                        log_line(msg)
                    local_depth_ws.reset_metrics()
                except Exception:
                    pass

        _fresh_task = asyncio.create_task(_fresh_loop())


async def stop_services() -> None:
    global _micro_task, _fresh_task
    try:
        if depth_ws is not None:
            await depth_ws.stop()
    except Exception:
        pass

    if _micro_task is not None:
        try:
            _micro_task.cancel()
        except Exception:
            pass
        _micro_task = None

    if _fresh_task is not None:
        try:
            _fresh_task.cancel()
        except Exception:
            pass
        _fresh_task = None

