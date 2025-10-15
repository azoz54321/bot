from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, Any, Optional

from ..core.exchange_info import ExchangeInfo, round_to_tick, round_to_step
from ..core.config import DAILY_FREEZE_PCT
from ..core.state import GlobalState, SlotState, Position


QTY_SAFETY_MARGIN = 0.001
DAILY_LOSS_FREEZE_PCT = 0.10  # deprecated; use DAILY_FREEZE_PCT from config


class TradingRules:
    def __init__(self, exinfo: ExchangeInfo, state: GlobalState):
        self.exinfo = exinfo
        self.state = state

    def choose_slot_for_buy(self) -> Optional[SlotState]:
        a, b = self.state.slot_A, self.state.slot_B
        if a.is_free:
            return a
        if b.is_free:
            return b
        return None

    def is_symbol_banned(self, symbol: str) -> bool:
        return symbol.upper() in self.state.banned_until_3am

    def ban_symbol_until_reset(self, symbol: str) -> None:
        self.state.banned_until_3am.add(symbol.upper())

    def clear_daily_bans(self) -> None:
        self.state.banned_until_3am.clear()

    def should_freeze_buys(self) -> bool:
        return self.state.freeze_buy

    def check_and_update_freeze(self) -> None:
        if self.state.capital_day_start_quote <= 0:
            self.state.freeze_buy = False
            return
        threshold = float(DAILY_FREEZE_PCT) * self.state.capital_day_start_quote
        self.state.freeze_buy = self.state.pnl_today_quote <= threshold

    # No dual-exit plan; exits are market-only
