from __future__ import annotations
import json
import os
from dataclasses import dataclass, asdict, field
from datetime import datetime
from typing import Any, Dict, Optional

from .config import STATE_PATH, RUNTIME_DIR
from .utils_time import now_ksa


MIDDAY_REBALANCE_ON_BOTH_FREE = True


@dataclass
class Position:
    symbol: str
    entry_price: float
    qty_base: float
    entry_quote_spent: float
    opened_at_ts: int  # epoch seconds (KSA or UTC agnostic)
    slot: str  # 'A' or 'B'
    open_1m: Optional[float] = None
    tp_price: Optional[float] = None
    sl_stop: Optional[float] = None
    client_order_id: Optional[str] = None
    oco_list_id: Optional[int] = None
    position_id: Optional[str] = None
    buy_order_id: Optional[str] = None
    sell_order_id: Optional[str] = None
    fees_buy_quote: float = 0.0


@dataclass
class SlotState:
    name: str
    balance_quote: float = 0.0
    position: Optional[Position] = None

    @property
    def is_free(self) -> bool:
        return self.position is None


@dataclass
class GlobalState:
    slot_A: SlotState = field(default_factory=lambda: SlotState(name="A", balance_quote=0.0))
    slot_B: SlotState = field(default_factory=lambda: SlotState(name="B", balance_quote=0.0))

    pnl_today_quote: float = 0.0
    capital_day_start_quote: float = 0.0
    freeze_buy: bool = False
    banned_until_3am: set[str] = field(default_factory=set)
    next_reset_epoch: Optional[int] = None
    midday_rebalance_on_both_free: bool = MIDDAY_REBALANCE_ON_BOTH_FREE

    def as_dict(self) -> Dict[str, Any]:
        def pos_to_dict(p: Optional[Position]) -> Optional[Dict[str, Any]]:
            return asdict(p) if p else None

        return {
            "slot_A": {
                "name": self.slot_A.name,
                "balance_quote": self.slot_A.balance_quote,
                "position": pos_to_dict(self.slot_A.position),
            },
            "slot_B": {
                "name": self.slot_B.name,
                "balance_quote": self.slot_B.balance_quote,
                "position": pos_to_dict(self.slot_B.position),
            },
            "pnl_today_quote": self.pnl_today_quote,
            "capital_day_start_quote": self.capital_day_start_quote,
            "freeze_buy": self.freeze_buy,
            "banned_until_3am": list(self.banned_until_3am),
            "next_reset_epoch": self.next_reset_epoch,
            "midday_rebalance_on_both_free": self.midday_rebalance_on_both_free,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "GlobalState":
        def pos_from_dict(p: Optional[Dict[str, Any]]) -> Optional[Position]:
            if not p:
                return None
            data = dict(p)
            if not data.get("position_id"):
                symbol = str(data.get("symbol") or "POS")
                opened = int(data.get("opened_at_ts") or 0)
                data["position_id"] = f"{symbol.upper()}-{opened}"
            if "fees_buy_quote" in data:
                try:
                    data["fees_buy_quote"] = float(data.get("fees_buy_quote", 0.0) or 0.0)
                except Exception:
                    data["fees_buy_quote"] = 0.0
            else:
                data["fees_buy_quote"] = 0.0
            return Position(**data)

        gs = cls()
        if d:
            a = d.get("slot_A", {})
            b = d.get("slot_B", {})
            gs.slot_A = SlotState(name=a.get("name", "A"), balance_quote=a.get("balance_quote", 0.0), position=pos_from_dict(a.get("position")))
            gs.slot_B = SlotState(name=b.get("name", "B"), balance_quote=b.get("balance_quote", 0.0), position=pos_from_dict(b.get("position")))
            gs.pnl_today_quote = float(d.get("pnl_today_quote", 0.0))
            gs.capital_day_start_quote = float(d.get("capital_day_start_quote", 0.0))
            gs.freeze_buy = bool(d.get("freeze_buy", False))
            gs.banned_until_3am = set(d.get("banned_until_3am", []))
            gs.next_reset_epoch = d.get("next_reset_epoch")
            gs.midday_rebalance_on_both_free = bool(d.get("midday_rebalance_on_both_free", MIDDAY_REBALANCE_ON_BOTH_FREE))
        return gs


def ensure_runtime_dir() -> None:
    os.makedirs(RUNTIME_DIR, exist_ok=True)


def load_state() -> GlobalState:
    ensure_runtime_dir()
    if not os.path.exists(STATE_PATH):
        return GlobalState()
    try:
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        return GlobalState.from_dict(data)
    except Exception:
        return GlobalState()


def save_state(state: GlobalState) -> None:
    ensure_runtime_dir()
    tmp = STATE_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state.as_dict(), f, ensure_ascii=False, indent=2)
    os.replace(tmp, STATE_PATH)
