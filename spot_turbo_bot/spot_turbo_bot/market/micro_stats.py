import time

from ..core.config import MICRO_CANCEL_ADD_MIN_DENOM, MICRO_ROLLING_WINDOW_MS

NOWMS = lambda: int(time.time() * 1000)
_MICRO_ROLLING_WINDOW_MS = int(MICRO_ROLLING_WINDOW_MS)
_CANCEL_ADD_MIN_DENOM = float(MICRO_CANCEL_ADD_MIN_DENOM)


class MicroStats:
    def __init__(self, books, rolling_ms=_MICRO_ROLLING_WINDOW_MS):
        self.books = books
        self.rolling_ms = rolling_ms
        self.quote_min = {}  # sym -> (min_value, min_ts)
        self.prev_asks = {}  # sym -> (asks_q list, ts)
        self.ctx = {}  # sym -> {"rolling_min_quote":..., "cancel_add":..., "velocity_bps":...}

    def update_for(self, sym):
        book = self.books.get(sym)
        if not book:
            return
        prev = self.prev_asks.get(sym)
        if prev:
            prev_q = sum(prev[0])
            now_q = sum(book.asks_q)
            cancel_add = (prev_q - now_q) / max(_CANCEL_ADD_MIN_DENOM, now_q)
        else:
            cancel_add = 0.0
        self.prev_asks[sym] = (book.asks_q[:], NOWMS())
        self.ctx[sym] = {
            "rolling_min_quote": self.quote_min.get(sym, (None, 0))[0],
            "cancel_add": cancel_add,
            "velocity_bps": 0.0,
        }

    def set_quote(self, sym, quote_value):
        cur = self.quote_min.get(sym)
        ts = NOWMS()
        if (not cur) or (ts - cur[1] > self.rolling_ms) or (quote_value < cur[0]):
            self.quote_min[sym] = (quote_value, ts)
        self.ctx[sym]["rolling_min_quote"] = self.quote_min[sym][0]

    def get_ctx(self, sym):
        return self.ctx.get(sym, {"rolling_min_quote": None, "cancel_add": 0.0, "velocity_bps": 0.0})
