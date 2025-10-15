from __future__ import annotations
import asyncio
from datetime import datetime

from ..core.logger import log_boot, log_daily_reset
from ..market import context as market_ctx
from ..core.state import GlobalState, save_state
from ..core.utils_time import next_reset_dt_ksa, seconds_until
from ..trading.trading_engine import TradingEngine


class DailyResetScheduler:
    def __init__(self, state: GlobalState, engine: TradingEngine):
        self.state = state
        self.engine = engine
        self._task: asyncio.Task | None = None
        self._stop = asyncio.Event()

    async def start(self) -> None:
        self._stop.clear()
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        self._stop.set()
        if self._task:
            self._task.cancel()

    async def _run(self) -> None:
        # Calculate next reset at 03:00 KSA
        next_dt = next_reset_dt_ksa()
        self.state.next_reset_epoch = int(next_dt.timestamp())
        save_state(self.state)
        while not self._stop.is_set():
            # Ensure timezone-aware datetime for countdown
            wait_s = seconds_until(datetime.fromtimestamp(self.state.next_reset_epoch))
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=wait_s)
                break
            except asyncio.TimeoutError:
                await self.perform_reset()
                next_dt = next_reset_dt_ksa()
                self.state.next_reset_epoch = int(next_dt.timestamp())
                save_state(self.state)

    async def perform_reset(self) -> None:
        # Snapshot current equity before baseline reset (quote balances only)
        try:
            equity_now = float(self.state.slot_A.balance_quote + self.state.slot_B.balance_quote)
        except Exception:
            equity_now = 0.0
        equity_start_prev = float(self.state.capital_day_start_quote or 0.0)
        # Reset daily bans & pnl & freeze; split free USDT 50/50; refresh capital baseline
        self.state.banned_until_3am.clear()
        self.state.pnl_today_quote = 0.0
        self.state.freeze_buy = False
        await self.engine.split_free_usdt()
        await self.engine.refresh_capital_day_start()
        save_state(self.state)
        log_daily_reset()
        # Report daily reset and baseline (best effort)
        try:
            if getattr(market_ctx, 'report', None) is not None:
                market_ctx.report.daily_reset(equity_start=equity_start_prev,
                                              equity_now=equity_now,
                                              baseline_set=float(self.state.capital_day_start_quote or 0.0))
                market_ctx.report.daily_baseline(equity_now=float(self.state.slot_A.balance_quote + self.state.slot_B.balance_quote),
                                                 freeze_hit=bool(self.state.freeze_buy))
        except Exception:
            pass
        # Schedule auto BNB fee top-up shortly after reset (no logging)
        try:
            await self.engine.auto_topup_bnb_for_fees()
        except Exception:
            pass
