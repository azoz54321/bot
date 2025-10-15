import os, json, datetime as dt
try:
    from zoneinfo import ZoneInfo
    KSA = ZoneInfo("Asia/Riyadh")
except Exception:  # Fallback if zoneinfo missing
    KSA = dt.timezone(dt.timedelta(hours=3))

def _ksa_now():
    try:
        return dt.datetime.now(dt.UTC).astimezone(KSA)  # type: ignore[attr-defined]
    except Exception:
        return dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc).astimezone(KSA)

def _day_key(ts):
    return ts.astimezone(KSA).strftime("%Y-%m-%d")

def _hms(delta_sec: int):
    m, s = divmod(max(0, int(delta_sec)), 60)
    h, m = divmod(m, 60)
    if h:
        return f"{h}h {m:02d}m {s:02d}s"
    return f"{m}m {s:02d}s"

class DailyTextReport:
    """Writes a single daily text file under logs/daily/YYYY-MM-DD.log.
    Guarantees trigger_once per symbol per KSA day.
    """
    def __init__(self, base_dir="logs/daily", runtime_dir="runtime"):
        self.base_dir = base_dir
        self.runtime_dir = runtime_dir
        os.makedirs(self.base_dir, exist_ok=True)
        os.makedirs(self.runtime_dir, exist_ok=True)
        self._trigger_once = None
        self._trigger_once_day = None

    def _path(self, ts):
        return os.path.join(self.base_dir, f"{_day_key(ts)}.log")

    def _trigger_state_path(self, ts):
        return os.path.join(self.runtime_dir, f"trigger_once_{_day_key(ts)}.json")

    def _load_trigger_once(self, ts):
        day = _day_key(ts)
        if self._trigger_once_day == day and self._trigger_once is not None:
            return
        p = self._trigger_state_path(ts)
        s = set()
        if os.path.exists(p):
            try:
                with open(p, "r", encoding="utf-8") as f:
                    s = set(json.load(f))
            except Exception:
                s = set()
        self._trigger_once = s
        self._trigger_once_day = day

    def _save_trigger_once(self, ts):
        p = self._trigger_state_path(ts)
        try:
            with open(p, "w", encoding="utf-8") as f:
                json.dump(sorted(self._trigger_once), f)
        except Exception:
            pass

    def _append_line(self, line: str, ts=None):
        ts = ts or _ksa_now()
        try:
            with open(self._path(ts), "a", encoding="utf-8") as f:
                f.write(line.rstrip() + "\n")
        except Exception:
            pass

    # Public helper to ensure today's trigger-once state is loaded on boot
    def load_today(self):
        try:
            self._load_trigger_once(_ksa_now())
        except Exception:
            pass

    # ---------- formats ----------
    def buy(self, symbol, slot, entry, open_1m, target_pct, target_price,
            tp_pct, tp_price, sl_pct, sl_price, quote_spent,
            allowed_quote, planned_quote, coverage_pct,
            exp_slip_pct, real_slip_pct, ts=None):
        ts = ts or _ksa_now()
        self._append_line(
f"[BUY] {symbol} slot={slot} t={ts.strftime('%Y-%m-%d %H:%M:%S')} entry={entry:.8f} open_1m={open_1m:.8f}\n"
f"target(+{target_pct:.2f}%)={target_price:.8f} TP(+{tp_pct:.2f}%)={tp_price:.8f} SL({sl_pct:+.2f}%)={sl_price:.8f} quote_spent={quote_spent:.2f}\n"
f"| allowed<={allowed_quote:.2f} planned={planned_quote:.2f} coverage={coverage_pct*100:.1f}% exp_slip~{(exp_slip_pct*100 if exp_slip_pct is not None else 0):.2f}% real_slip~{(real_slip_pct*100 if real_slip_pct is not None else 0):.2f}%",
        ts)

    def be_armed(self, symbol, mode, p_be=None, raw_pct=None, ts=None):
        ts = ts or _ksa_now()
        if mode == "on_reach":
            self._append_line(
                f"[BE-ARMED] {symbol} mode=on_reach p_be={p_be:.8f} t={ts.strftime('%Y-%m-%d %H:%M:%S')}", ts
            )
        else:
            self._append_line(
                f"[BE-ARMED] {symbol} mode=raw@{(raw_pct*100):.2f}% armed_at={p_be:.8f} t={ts.strftime('%Y-%m-%d %H:%M:%S')}", ts
            )

    def exit(self, symbol, slot, reason, exit_price, entry_price, pnl_pct, pnl_usdt, hold_seconds, ts=None):
        ts = ts or _ksa_now()
        self._append_line(
f"[EXIT-{reason}] {symbol} slot={slot} t={ts.strftime('%Y-%m-%d %H:%M:%S')}\n"
f"exit={exit_price:.8f} entry={entry_price:.8f} pnl_pct={(pnl_pct*100):+.2f}% pnl_usdt={pnl_usdt:+.2f} hold={_hms(hold_seconds)}",
        ts)

    def trigger_once(self, symbol, trigger_pct, base_price, fired_by, ts=None):
        ts = ts or _ksa_now()
        self._load_trigger_once(ts)
        if symbol in self._trigger_once:
            return
        self._trigger_once.add(symbol)
        self._save_trigger_once(ts)
        self._append_line(
            f"[TRIGGER] {symbol} t={ts.strftime('%Y-%m-%d %H:%M:%S')} base={base_price:.8f} trigger(+{trigger_pct*100:.2f}%) fired_by={fired_by}",
        ts)

    def daily_reset(self, equity_start, equity_now, baseline_set, ts=None):
        ts = ts or _ksa_now()
        self._append_line(
            f"[DAILY-RESET] t={ts.strftime('%Y-%m-%d %H:%M:%S')} equity_start={equity_start:.2f} equity_now={equity_now:.2f} baseline_set={baseline_set:.2f}",
        ts)

    def daily_baseline(self, equity_now, freeze_hit: bool, ts=None):
        ts = ts or _ksa_now()
        self._append_line(
            f"[DAILY-BASELINE] t={ts.strftime('%H:%M:%S')} equity_now={equity_now:.2f} freeze_hit={'yes' if freeze_hit else 'no'}",
        ts)

    def reject(self, symbol, reason, allowed_quote, planned_quote, coverage_pct, ts=None):
        ts = ts or _ksa_now()
        self._append_line(
            f"[REJECT] {symbol} t={ts.strftime('%Y-%m-%d %H:%M:%S')} reason={reason} allowed<={allowed_quote:.2f} planned={planned_quote:.2f} coverage={coverage_pct*100:.1f}%",
        ts)
