from __future__ import annotations
import asyncio
import os
from typing import Optional

from decimal import Decimal

try:
    import uvloop  # type: ignore
    uvloop.install()
except Exception:
    pass
from ..infra.binance_http import BinanceHTTP, BinanceHTTPError
from ..core.exchange_info import ExchangeInfo
from ..core.logger import log_boot, dbg, log_line
from ..core.config import (
    TRIGGER_PCT_1S,
    ENTRY_IMPACT_SLIP_PCT,
    EXIT_V2_TAKE_LIMIT_PCT,
    EXIT_V2_STOP_DROP_PCT,
    EXIT_V2_PULLBACK_UP_PCT,
    EXIT_V2_ENTRY_REGION_TOL_PCT,
    EXIT_V2_PRICE_SOURCE,
    FEE_SOURCE,
    FEE_BUY_RATE,
    FEE_SELL_RATE,
    fmt_pct,
    LINT_ON_BOOT,
    ENABLE_SERVER_TIME_SYNC,
    SERVER_TIME_SYNC_SEC,
    EXCHANGE_INFO_REFRESH_SEC,
    MAIN_LOOP_HEARTBEAT_SEC,
    NATS_AUTOSTART,
    NATS_BIN_PATH,
    NATS_HOST,
    NATS_PORT,
    NATS_ARGS,
    NATS_READY_TIMEOUT_MS,
    NATS_RESTART_BACKOFF_MS_MIN,
    NATS_RESTART_BACKOFF_MS_MAX,
    NATS_SINGLETON_KEY,
    ONE_SEC_AUTOSTART,
    ONE_SEC_ALL_USDT,
    ONE_SEC_HEARTBEAT_SUBJECT,
    ONE_SEC_HEARTBEAT_WAIT_MS,
    ONE_SEC_RESTART_BACKOFF_MS_MIN,
    ONE_SEC_RESTART_BACKOFF_MS_MAX,
    ONE_SEC_SINGLETON_KEY,
)
from ..core.state import load_state
from ..trading.trading_engine import TradingEngine
from .runtime_tasks import DailyResetScheduler
from .user_stream import UserStreamManager
from .one_sec_runner import OneSecRunner
from .boot_lint import run_boot_lint
from ..market.context import build_symbols_meta, setup as market_setup, start_services as market_start, stop_services as market_stop
from ..market import context as market_ctx
from ..reports.report_writer import DailyTextReport
from .bus_supervisor import NATSSupervisor
from .aggregator_supervisor import OneSecAggregatorSupervisor


def _calc_proceeds_and_price_from_exec(exec_msg: dict) -> tuple[float, float, str]:
    # Returns (proceeds_quote_net, exit_price, reason)
    # Commission handling: if fee is in USDT, subtract from proceeds; else ignore.
    side = exec_msg.get("S") or exec_msg.get("side")
    order_type = exec_msg.get("o") or exec_msg.get("orderType")
    status = exec_msg.get("X") or exec_msg.get("orderStatus")
    cumm_quote = float(exec_msg.get("Z", 0.0) or 0.0)
    last_price = float(exec_msg.get("L", 0.0) or 0.0)
    commission = float(exec_msg.get("n", 0.0) or 0.0)
    commission_asset = (exec_msg.get("N") or "").upper()

    proceeds = cumm_quote
    if commission_asset == "USDT":
        proceeds = max(0.0, proceeds - commission)

    reason = "TP" if (order_type or "").upper().startswith("TAKE_PROFIT") else (
        "SL" if (order_type or "").upper().startswith("STOP") else (
            "MARKET" if (order_type or "").upper() == "MARKET" else status or "FILLED"
        )
    )
    return proceeds, last_price if last_price > 0 else 0.0, reason


async def main() -> None:
    # Optional one-time cleanup of old reports
    try:
        import glob, shutil

        if os.getenv("MIGRATE_CLEAN_REPORTS", "0") == "1":
            for pat in [
                "logs/trigger_hits*.jsonl",
                "logs/journal.jsonl",
                "logs/journal*.jsonl",
                "logs/journal/**/*.jsonl",
                "logs/journal",
                "reports/*.csv",
                "reports/*.md",
            ]:
                for p in glob.glob(pat, recursive=True):
                    try:
                        os.remove(p)
                    except IsADirectoryError:
                        shutil.rmtree(p, ignore_errors=True)
                    except Exception:
                        pass
            log_line("[MIGRATE] old reports removed")
    except Exception:
        dbg("migrate cleanup failed")
    # Load state & exchange info
    state = load_state()
    http = BinanceHTTP()
    exinfo = await ExchangeInfo.aload(http)
    audit = None
    # Initialize daily text report singleton
    try:
        market_ctx.report = DailyTextReport()
        # ensure trigger-once state loaded for today before triggers start
        try:
            market_ctx.report.load_today()
        except Exception:
            pass
    except Exception:
        market_ctx.report = None
    engine = TradingEngine(http, exinfo, state, audit=audit)

    # Daily reset scheduler (03:00 KSA)
    resetter = DailyResetScheduler(state, engine)
    await resetter.start()

    # User data stream: detect SELL closures (queue work outside WS loop)
    sell_q: asyncio.Queue[tuple[str, float, float, str]] = asyncio.Queue()
    def on_exec(msg: dict) -> None:
        try:
            # Always forward execution reports to engine for BUY fill tracking
            if (msg.get("e") == "executionReport"):
                try:
                    engine.on_exec_report(msg)
                except Exception:
                    pass
            if (msg.get("e") == "executionReport" and (msg.get("S") == "SELL" or msg.get("side") == "SELL") and (msg.get("X") == "FILLED" or msg.get("orderStatus") == "FILLED")):
                symbol = msg.get("s") or msg.get("symbol")
                proceeds, exit_price, reason = _calc_proceeds_and_price_from_exec(msg)
                try:
                    sell_q.put_nowait((symbol, exit_price, proceeds, reason))
                except Exception:
                    pass
        except Exception:
            # No logging in WS path
            pass

    def on_user_state(event: str, info: dict) -> None:
        try:
            engine.on_user_stream_event(event, info)
        except Exception:
            pass

    bus_supervisor: Optional[NATSSupervisor] = None
    nats_ready = True
    if NATS_AUTOSTART:
        try:
            bus_supervisor = NATSSupervisor(
                bin_path=NATS_BIN_PATH,
                host=NATS_HOST,
                port=int(NATS_PORT),
                args=NATS_ARGS,
                ready_timeout_ms=NATS_READY_TIMEOUT_MS,
                backoff_min_ms=NATS_RESTART_BACKOFF_MS_MIN,
                backoff_max_ms=NATS_RESTART_BACKOFF_MS_MAX,
                singleton_key=NATS_SINGLETON_KEY,
            )
            nats_ready = await bus_supervisor.ensure_started()
        except Exception as exc:
            dbg(f"nats auto-start failed: {exc}")
            nats_ready = False

    agg_supervisor: Optional[OneSecAggregatorSupervisor] = None
    one_sec_ready = True
    if ONE_SEC_AUTOSTART:
        try:
            supervisor = OneSecAggregatorSupervisor(
                heartbeat_subject=ONE_SEC_HEARTBEAT_SUBJECT,
                heartbeat_wait_ms=ONE_SEC_HEARTBEAT_WAIT_MS,
                singleton_key=ONE_SEC_SINGLETON_KEY,
                backoff_min_ms=ONE_SEC_RESTART_BACKOFF_MS_MIN,
                backoff_max_ms=ONE_SEC_RESTART_BACKOFF_MS_MAX,
                all_usdt=ONE_SEC_ALL_USDT,
            )
            one_sec_ready = await supervisor.ensure_started()
            agg_supervisor = supervisor
        except Exception as exc:
            dbg(f"aggregator auto-start failed: {exc}")
            one_sec_ready = False

    usm = UserStreamManager(
        http,
        on_exec,
        on_state_event=on_user_state,
        pending_activity_provider=engine.userstream_pending_activity,
    )

    if nats_ready and one_sec_ready:
        log_boot("nats 1s trigger active; depth WS on ALL_USDT")
    try:
        log_line(
            "[CONFIG] "
            f"TRIGGER_1S={fmt_pct(TRIGGER_PCT_1S)} | ENTRY_MAX_SLIP={fmt_pct(ENTRY_IMPACT_SLIP_PCT)} | "
            f"EXIT_V2 tp={fmt_pct(EXIT_V2_TAKE_LIMIT_PCT)} stop={fmt_pct(EXIT_V2_STOP_DROP_PCT)} pullback={fmt_pct(EXIT_V2_PULLBACK_UP_PCT)} tol={fmt_pct(EXIT_V2_ENTRY_REGION_TOL_PCT)} src={EXIT_V2_PRICE_SOURCE} | "
            f"FEES buy={fmt_pct(FEE_BUY_RATE)} sell={fmt_pct(FEE_SELL_RATE)}"
        )
    except Exception:
        pass

    try:
        account_data: Optional[dict] = await http.account()
    except Exception:
        account_data = None

    await engine.refresh_capital_day_start(account_data)
    spot_free, slot_a_dec, slot_b_dec = await engine.split_free_usdt(account_data)
    try:
        await engine.bootstrap_exit_v2()
    except Exception:
        pass

    def _fmt_decimal(val: Decimal) -> str:
        text = format(val, "f")
        if "." in text:
            text = text.rstrip("0").rstrip(".")
        return text or "0"

    log_line(
        f"[BOOTBAL] spot_usdt_free={_fmt_decimal(spot_free)} "
        f"slotA={_fmt_decimal(slot_a_dec)} slotB={_fmt_decimal(slot_b_dec)}"
    )

    slot_a_pos = engine.state.slot_A.position.symbol.upper() if engine.state.slot_A.position else "None"
    slot_b_pos = engine.state.slot_B.position.symbol.upper() if engine.state.slot_B.position else "None"
    log_line(f"[OPENPOS] A={slot_a_pos} B={slot_b_pos}")

    try:
        await usm.start()
    except BinanceHTTPError as e:
        dbg(f"user stream start failed: code={e.code} msg={e.msg}. Continuing without SELL tracking.")
    except Exception as e:
        dbg(f"user stream start failed: {e}. Continuing without SELL tracking.")

    # Optional lint on boot
    if LINT_ON_BOOT:
        try:
            run_boot_lint(exinfo)
        except Exception:
            dbg("lint raised exception")

    # Start market WS depth/book services (all USDT for impact/coverage)
    symbols_for_depth = list(set(exinfo.usdt_trading_symbols()))
    symbols_meta = build_symbols_meta(exinfo, symbols_for_depth)
    market_setup(symbols_meta)
    await market_start()

    # Start 1s trigger runner (NATS source)
    one_sec = OneSecRunner(engine, exinfo)
    await one_sec.start()

    # Periodic exchangeInfo refresh (filters may change; also used for USDT list updates)
    async def _exinfo_refresh_worker():
        try:
            while True:
                await asyncio.sleep(EXCHANGE_INFO_REFRESH_SEC)
                try:
                    new_ex = await ExchangeInfo.aload(http)
                    engine.exinfo = new_ex
                    # update sub-engines
                    one_sec.exinfo = new_ex
                except Exception:
                    pass
        except asyncio.CancelledError:
            return

    exinfo_task = asyncio.create_task(_exinfo_refresh_worker())

    # SELL worker to process outside WS loop
    async def _sell_worker():
        try:
            while True:
                s, px, proceeds, reason = await sell_q.get()
                try:
                    engine.on_sell_closed(symbol=s, exit_price=px, proceeds_quote=proceeds, reason=reason)
                except Exception:
                    pass
        except asyncio.CancelledError:
            return

    sell_task = asyncio.create_task(_sell_worker())

    # Keep running
    try:
        while True:
            await asyncio.sleep(MAIN_LOOP_HEARTBEAT_SEC)
    except asyncio.CancelledError:
        pass
    finally:
        await usm.stop()
        await resetter.stop()
        await one_sec.stop()
        if agg_supervisor is not None:
            try:
                await agg_supervisor.stop()
            except Exception:
                pass
        if bus_supervisor is not None:
            try:
                await bus_supervisor.stop()
            except Exception:
                pass
        try:
            await market_stop()
        except Exception:
            pass
        exinfo_task.cancel()
        sell_task.cancel()


if __name__ == "__main__":
    asyncio.run(main())
