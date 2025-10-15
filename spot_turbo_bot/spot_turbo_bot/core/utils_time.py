from __future__ import annotations
from datetime import datetime, timedelta, timezone
from .config import KSA_UTC_OFFSET, DAILY_RESET_HOUR_KSA


KSA_TZ = timezone(timedelta(seconds=KSA_UTC_OFFSET))


def now_utc() -> datetime:
    return datetime.utcnow().replace(tzinfo=timezone.utc)


def now_ksa() -> datetime:
    return datetime.now(tz=KSA_TZ)


def next_reset_dt_ksa(base: datetime | None = None) -> datetime:
    """Return the next occurrence of 03:00 KSA from base time (KSA)."""
    base_ksa = base.astimezone(KSA_TZ) if base else now_ksa()
    target = base_ksa.replace(hour=DAILY_RESET_HOUR_KSA, minute=0, second=0, microsecond=0)
    if base_ksa >= target:
        target = target + timedelta(days=1)
    return target


def seconds_until(dt: datetime) -> int:
    # Make both aware in KSA timezone to avoid naive-aware subtraction errors
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=KSA_TZ)
    now = now_ksa()
    return max(0, int((dt - now).total_seconds()))
