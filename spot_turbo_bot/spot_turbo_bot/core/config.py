from __future__ import annotations

import os
from pathlib import Path
import shlex

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover
    load_dotenv = None


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _parse_symbols(raw: str) -> list[str]:
    if not raw:
        return []
    return [part.strip().upper() for part in raw.split(",") if part.strip()]


# helpers for env parsing and percent formatting
def env_float(name: str, default: float) -> float:
    v = os.getenv(name, "")
    try:
        return float(v) if v != "" else float(default)
    except Exception:
        return float(default)

def env_str(name: str, default: str) -> str:
    v = os.getenv(name, "")
    return v if v != "" else default

def _env_csv(name: str, default: str) -> list[str]:
    raw = os.getenv(name)
    target = raw if raw not in (None, "") else default
    return [part.strip() for part in (target or "").split(",") if part.strip()]

def _env_multiline(name: str, default: str = "") -> str:
    value = os.getenv(name)
    if value in (None, ""):
        return default
    return value.replace("\\n", "\n")

def _env_float_list(name: str, default: list[float]) -> list[float]:
    raw = os.getenv(name)
    if not raw or raw.strip() == "":
        return list(default)
    out: list[float] = []
    for part in raw.split(","):
        try:
            out.append(float(part.strip()))
        except Exception:
            continue
    return out or list(default)

def _optional_env_str(name: str) -> str | None:
    value = os.getenv(name)
    if value is None:
        return None
    value = value.strip()
    return value if value else None

def _env_args_list(name: str, default: list[str]) -> list[str]:
    raw = os.getenv(name)
    if raw in (None, ""):
        return list(default)
    try:
        parsed = shlex.split(raw)
        return [part for part in parsed if part]
    except Exception:
        return list(default)

def require_env(name: str) -> str:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        raise RuntimeError(
            f"Missing required environment variable '{name}'. "
            "Set it in your environment (see README.md Environment Variables)."
        )
    return value.strip()

def fmt_pct(x: float) -> str:
    return f"{x*100:.2f}%"


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return int(raw)
    except Exception:
        try:
            return int(float(raw))
        except Exception:
            return default


def _env_int_with_fallback(primary: str, fallback: str | None, default: int) -> int:
    candidates = (primary, fallback) if fallback else (primary,)
    for name in candidates:
        if not name:
            continue
        raw = os.getenv(name)
        if raw is None or raw.strip() == "":
            continue
        try:
            return int(raw)
        except Exception:
            try:
                return int(float(raw))
            except Exception:
                continue
    return default


# ---------------------------------------------------------------------------
# load .env for local development (never committed in git)
# ---------------------------------------------------------------------------
if load_dotenv:
    env_path = Path.cwd() / ".env"
    if env_path.exists():
        load_dotenv(dotenv_path=env_path)


# ---------------------------------------------------------------------------
# credentials / connectivity
# ---------------------------------------------------------------------------


BINANCE_API_KEY    = env_str("BINANCE_API_KEY", "MzYrHl9UayAmgCYei9dvHoc1pHbXfqkldJ3vLMtrhaxbgBlGl5VB21fAE7cRLGnA")
BINANCE_API_SECRET = env_str("BINANCE_API_SECRET", "nXugw3xOgEOic2tbehqGoVJb4z5uZG0tq3KL7jsO05Il4qUIEa69Xdsfq03IAbcq")




# Mainnet only (testnet support removed)
REST_BASE = "https://api.binance.com"
WS_BASE = "wss://stream.binance.com:9443/ws"
# Combined market streams base (appends ?streams=...)
WS_MARKET_BASE = "wss://stream.binance.com:9443/stream"

HTTP_TIMEOUT_SEC = float(os.getenv("HTTP_TIMEOUT_SEC", "10"))
# Renew listenKey proactively before 30m timeout
LISTENKEY_RENEW_MIN = int(os.getenv("LISTENKEY_RENEW_MIN", "27"))
USER_STREAM_KEEPALIVE_SEC = int(os.getenv("USERSTREAM_KEEPALIVE_SEC", str(max(1, LISTENKEY_RENEW_MIN) * 60)))
USERSTREAM_ENABLE = _env_bool("USERSTREAM_ENABLE", default=True)
USERSTREAM_VERBOSE = _env_bool("USERSTREAM_VERBOSE", default=True)
_USERSTREAM_STALE_DEFAULT = _env_int("USERSTREAM_STALE_MS", 2500)
USERSTREAM_STALE_MS_ACTIVE = _env_int("USERSTREAM_STALE_MS_ACTIVE", _USERSTREAM_STALE_DEFAULT)
USERSTREAM_RECONNECT_BACKOFF_MS_MIN = _env_int("USERSTREAM_RECONNECT_BACKOFF_MS_MIN", 300)
USERSTREAM_RECONNECT_BACKOFF_MS_MAX = _env_int("USERSTREAM_RECONNECT_BACKOFF_MS_MAX", 5000)
USERSTREAM_FALLBACK_QUERY_MS = _env_int("USERSTREAM_FALLBACK_QUERY_MS", 200)
USERSTREAM_STALE_MS_IDLE = _env_int("USERSTREAM_STALE_MS_IDLE", 6000)
USER_STREAM_KEEPALIVE_SEC = max(1, USER_STREAM_KEEPALIVE_SEC)
USERSTREAM_RECONNECT_BACKOFF_MS_MIN = max(50, USERSTREAM_RECONNECT_BACKOFF_MS_MIN)
USERSTREAM_RECONNECT_BACKOFF_MS_MAX = max(USERSTREAM_RECONNECT_BACKOFF_MS_MIN, USERSTREAM_RECONNECT_BACKOFF_MS_MAX)
USERSTREAM_STALE_MS_ACTIVE = max(500, USERSTREAM_STALE_MS_ACTIVE)
USERSTREAM_FALLBACK_QUERY_MS = max(50, USERSTREAM_FALLBACK_QUERY_MS)
USERSTREAM_STALE_MS_IDLE = max(USERSTREAM_STALE_MS_ACTIVE, USERSTREAM_STALE_MS_IDLE)
USERSTREAM_STALE_MS = USERSTREAM_STALE_MS_ACTIVE


# ---------------------------------------------------------------------------
# runtime paths
# ---------------------------------------------------------------------------
RUNTIME_DIR = os.getenv("RUNTIME_DIR", str(Path.cwd() / "runtime"))
STATE_PATH = str(Path(RUNTIME_DIR) / "state.json")
EXCHANGE_INFO_PATH = os.getenv("EXCHANGE_INFO_PATH", str(Path.cwd() / "data" / "exchangeInfo.json"))


# =========================
# NATS configuration
# =========================
# Auth mode: "none" | "nkey" | "jwt" | "userpass" | "tls"
NATS_AUTH_MODE = "none"  # شغّل NKEY مباشرة. غيّرها إلى "none" لو تبغى تشغيل بدون توثيق.

# Primary / legacy URL (kept for backward compatibility)
_DEFAULT_NATS_URL = env_str("DATA_BUS_URL", "nats://127.0.0.1:4222")

# Multiple servers support (CSV via env) with legacy fallback
_NATS_URLS = _env_csv("NATS_URLS", _DEFAULT_NATS_URL) or [_DEFAULT_NATS_URL]
NATS_URLS = tuple(_NATS_URLS)
NATS_PRIMARY_URL = NATS_URLS[0] if NATS_URLS else _DEFAULT_NATS_URL

# Connection & reconnect tuning (resolved at import time)
NATS_CONNECT_TIMEOUT_SEC = env_float("NATS_CONNECT_TIMEOUT_SEC", 1.0)
NATS_RECONNECT_MAX_TRIES = int(os.getenv("NATS_RECONNECT_MAX_TRIES", "-1"))  # -1 = no limit (client-dependent)
NATS_RECONNECT_JITTER_MS = env_float("NATS_RECONNECT_JITTER_MS", 500.0)

# --- Auth material ---
# NKEY (public key PEM + seed). "seed" يجب أن تضعه أنت لاحقًا؛ بدونه سيفشل الاتصال الموقّع (طبيعي).
NATS_NKEY_PUBLIC = _env_multiline("NATS_NKEY_PUBLIC", """-----BEGIN PUBLIC KEY-----
MCowBQYDK2VwAyEAqXk+la1qdN+CzCDgeaaHxTjdEshlPFT+2F1dHW4b0gY=
-----END PUBLIC KEY-----""")
NATS_NKEY_SEED = _env_multiline("NATS_NKEY_SEED", """-----BEGIN PRIVATE KEY-----
MC4CAQAwBQYDK2VwBCIEIBUTU8gK6QimdYUWJ2y5hLaNCLnP/vCMEgd74kqFy6ro
-----END PRIVATE KEY-----""")  # ضع الـ SEED هنا عند توفره

# JWT (اختياري إن استخدمت وضع jwt)
NATS_JWT = _env_multiline("NATS_JWT", "")

# USER/PASS (وضع بسيط بديل)
NATS_USERNAME = env_str("NATS_USERNAME", "")
NATS_PASSWORD = env_str("NATS_PASSWORD", "")

# TLS (اختياري)
NATS_TLS_REQUIRED = _env_bool("NATS_TLS_REQUIRED", default=False)
NATS_TLS_CA_PATH = env_str("NATS_TLS_CA_PATH", "")
NATS_TLS_CERT_PATH = env_str("NATS_TLS_CERT_PATH", "")
NATS_TLS_KEY_PATH = env_str("NATS_TLS_KEY_PATH", "")
NATS_TLS_INSECURE_SKIP_VERIFY = _env_bool("NATS_TLS_INSECURE_SKIP_VERIFY", default=False)


# derived / legacy aliases
DATA_BUS_URL = NATS_PRIMARY_URL


# ---------------------------------------------------------------------------
# data bus (external one-second feed)
# ---------------------------------------------------------------------------
DATA_TOPIC_ONE_SEC = env_str("DATA_TOPIC_ONE_SEC", "ONE_SEC.usdt.*")
DATA_CLIENT_GROUP = env_str("DATA_CLIENT_GROUP", "spot_turbo_bot")

# ---------------------------------------------------------------------------
# local one-second aggregator auto-start
# ---------------------------------------------------------------------------
ONE_SEC_AUTOSTART = _env_bool("ONE_SEC_AUTOSTART", default=True)
ONE_SEC_ALL_USDT = _env_bool("ONE_SEC_ALL_USDT", default=True)
ONE_SEC_HEARTBEAT_SUBJECT = env_str("ONE_SEC_HEARTBEAT_SUBJECT", "ONE_SEC.usdt.heartbeat")
ONE_SEC_HEARTBEAT_WAIT_MS = _env_int("ONE_SEC_HEARTBEAT_WAIT_MS", 3000)
ONE_SEC_RESTART_BACKOFF_MS_MIN = _env_int("ONE_SEC_RESTART_BACKOFF_MS_MIN", 1500)
ONE_SEC_RESTART_BACKOFF_MS_MAX = _env_int("ONE_SEC_RESTART_BACKOFF_MS_MAX", 5000)
ONE_SEC_SINGLETON_KEY = env_str("ONE_SEC_SINGLETON_KEY", "one_sec_aggregator_usdt")

ONE_SEC_HEARTBEAT_WAIT_MS = max(500, ONE_SEC_HEARTBEAT_WAIT_MS)
ONE_SEC_RESTART_BACKOFF_MS_MIN = max(250, ONE_SEC_RESTART_BACKOFF_MS_MIN)
ONE_SEC_RESTART_BACKOFF_MS_MAX = max(ONE_SEC_RESTART_BACKOFF_MS_MIN, ONE_SEC_RESTART_BACKOFF_MS_MAX)

# ---------------------------------------------------------------------------
# local NATS server auto-start
# ---------------------------------------------------------------------------
NATS_AUTOSTART = _env_bool("NATS_AUTOSTART", default=True)
NATS_BIN_PATH = env_str("NATS_BIN_PATH", "nats-server-v2.12.1-RC.5-windows-amd64/nats-server.exe")
NATS_HOST = env_str("NATS_HOST", "127.0.0.1")
NATS_PORT = _env_int("NATS_PORT", 4222)
NATS_ARGS = _env_args_list("NATS_ARGS", ["-p", "4222"])
NATS_READY_TIMEOUT_MS = _env_int("NATS_READY_TIMEOUT_MS", 3000)
NATS_RESTART_BACKOFF_MS_MIN = _env_int("NATS_RESTART_BACKOFF_MS_MIN", 1500)
NATS_RESTART_BACKOFF_MS_MAX = _env_int("NATS_RESTART_BACKOFF_MS_MAX", 5000)
NATS_SINGLETON_KEY = env_str("NATS_SINGLETON_KEY", "nats_server_local")

NATS_READY_TIMEOUT_MS = max(500, NATS_READY_TIMEOUT_MS)
NATS_RESTART_BACKOFF_MS_MIN = max(250, NATS_RESTART_BACKOFF_MS_MIN)
NATS_RESTART_BACKOFF_MS_MAX = max(NATS_RESTART_BACKOFF_MS_MIN, NATS_RESTART_BACKOFF_MS_MAX)

# Pricing source (for documentation/consistency)
PRICE_SOURCE = env_str("PRICE_SOURCE", "ask").strip().lower()

# ---------------------------------------------------------------------------
# 1s trigger configuration
# ---------------------------------------------------------------------------
TRIGGER_PCT_1S = env_float("TRIGGER_PCT_1S", 0.0001)
TRIGGER_PCT = TRIGGER_PCT_1S
TRIGGER_SYMBOLS = env_str("TRIGGER_SYMBOLS", "ALL_USDT").strip().upper()
TRIGGER_SYMBOLS_LIST = _parse_symbols(TRIGGER_SYMBOLS if TRIGGER_SYMBOLS != "ALL_USDT" else "")
OPEN_SOURCE = env_str("OPEN_SOURCE", "hybrid").strip().lower()
OPEN_FALLBACK_MS = int(os.getenv("OPEN_FALLBACK_MS", "150"))
KLINE_SCOPE = env_str("KLINE_SCOPE", "watchlist").strip().lower()
TRIGGER_MIN_UPDATES_1S = int(os.getenv("TRIGGER_MIN_UPDATES_1S", "3"))
SPREAD_BPS_CUTOFF = env_float("SPREAD_BPS_CUTOFF", 20.0)
TRIGGER_COOLDOWN_S = int(os.getenv("TRIGGER_COOLDOWN_S", "10"))


# ---------------------------------------------------------------------------
# fast ALL_USDT behaviour
# ---------------------------------------------------------------------------
FAST_ALL_USDT = _env_bool("FAST_ALL_USDT", default=True)
WS_COMBINED_CHUNK = int(os.getenv("WS_COMBINED_CHUNK", "75"))
WARMUP_SEC = float(os.getenv("WARMUP_SEC", "3.0"))
WS_DEBOUNCE_MS = int(os.getenv("WS_DEBOUNCE_MS", "90"))
# Accept either WS_TICK_FILTER or TICK_FILTER env toggles
WS_TICK_FILTER = _env_bool("WS_TICK_FILTER", default=True) if os.getenv("WS_TICK_FILTER") is not None else _env_bool("TICK_FILTER", default=True)




# ---------------------------------------------------------------------------
# diagnostics / debug (disabled by default)
# ---------------------------------------------------------------------------
DEBUG_VERBOSE = _env_bool("DEBUG_VERBOSE", default=False)
DEBUG_WS_LOGS = _env_bool("DEBUG_WS_LOGS", default=False)
SIGN_DEBUG = _env_bool("SIGN_DEBUG", default=False)

STALL_DIAG_ON = _env_bool("STALL_DIAG_ON", default=False)
STALL_THRESHOLD_SEC = float(os.getenv("STALL_THRESHOLD_SEC", "15"))
STALL_POLL_SEC = float(os.getenv("STALL_POLL_SEC", "1"))

LOG_TRADES_ONLY = _env_bool("LOG_TRADES_ONLY", default=True)
LOG_TP_EVENTS = _env_bool("LOG_TP_EVENTS", default=False)
LOG_DEBUG = _env_bool("LOG_DEBUG", default=False)


# ---------------------------------------------------------------------------
# optional server time sync (disabled by default)
# ---------------------------------------------------------------------------
ENABLE_SERVER_TIME_SYNC = _env_bool("ENABLE_SERVER_TIME_SYNC", default=True)
SERVER_TIME_SYNC_SEC = int(os.getenv("SERVER_TIME_SYNC_SEC", "5"))


"""
Legacy minute/kline trigger settings removed. Any previous envs such as
TRIGGER_PCT, TRIGGER_SYMBOLS, OPEN_SOURCE, KLINE_SCOPE, etc. are ignored.
"""


# ---------------------------------------------------------------------------
# regional/time constants
# ---------------------------------------------------------------------------
KSA_UTC_OFFSET = 3 * 3600
DAILY_RESET_HOUR_KSA = 3


# ---------------------------------------------------------------------------
# health / lint
# ---------------------------------------------------------------------------
LINT_ON_BOOT = _env_bool("LINT_ON_BOOT", default=True)
HEALTH_SUMMARY_SEC = _env_int("HEALTH_SUMMARY_SEC", 0)


# ---------------------------------------------------------------------------
# Entry-impact via WS depth only
# ---------------------------------------------------------------------------
ENTRY_IMPACT_SLIP_PCT = env_float("ENTRY_IMPACT_SLIP_PCT", 0.01)  # 1%

# Impact estimator defaults
IMPACT_SLIP_DEFAULT = env_float("IMPACT_SLIP_DEFAULT", 0.01)
IMPACT_UTIL_LAST = env_float("IMPACT_UTIL_LAST", 0.8)
IMPACT_PUMP_BASE = env_float("IMPACT_PUMP_BASE", 0.6)
IMPACT_PUMP_MIN = env_float("IMPACT_PUMP_MIN", 0.4)
IMPACT_PUMP_MAX = env_float("IMPACT_PUMP_MAX", 0.8)
IMPACT_PUMP_SENSITIVITY = env_float("IMPACT_PUMP_SENSITIVITY", 0.1)
IMPACT_RATIO_CAP = env_float("IMPACT_RATIO_CAP", 3.0)
IMPACT_MAX_STALENESS_MS = _env_int("IMPACT_MAX_STALENESS_MS", 120)

# ---------------------------------------------------------------------------
# market exit configuration (Exit V2)
# ---------------------------------------------------------------------------
FEE_SOURCE = env_str("FEE_SOURCE", "auto_or_manual").strip().lower()
FEE_BUY_RATE = env_float("FEE_BUY_RATE", 0.00075)
FEE_SELL_RATE = env_float("FEE_SELL_RATE", 0.00075)
EXIT_V2_TAKE_LIMIT_PCT = env_float("EXIT_V2_TAKE_LIMIT_PCT", 0.02)
EXIT_V2_STOP_DROP_PCT = env_float("EXIT_V2_STOP_DROP_PCT", -0.01)
EXIT_V2_PULLBACK_UP_PCT = env_float("EXIT_V2_PULLBACK_UP_PCT", 0.01)
EXIT_V2_ENTRY_REGION_TOL_PCT = max(FEE_BUY_RATE + FEE_SELL_RATE, 0.0005)
EXIT_V2_PRICE_SOURCE = env_str("EXIT_V2_PRICE_SOURCE", "bid").strip().lower()
EXIT_V2_DEBOUNCE_MS = _env_int("EXIT_V2_DEBOUNCE_MS", 150)
EXIT_V2_MAX_STALENESS_MS = _env_int("EXIT_V2_MAX_STALENESS_MS", IMPACT_MAX_STALENESS_MS)
EXIT_V2_CANCEL_TIMEOUT_SEC = env_float("EXIT_V2_CANCEL_TIMEOUT_SEC", 2.0)
EXIT_V2_MARKET_RETRY = _env_int("EXIT_V2_MARKET_RETRY", 2)
SELL_FEE_SAFETY = max(FEE_SELL_RATE * 2.0, 0.001)
PLACE_TP_ON = env_str("PLACE_TP_ON", "first_fill").strip().lower()
TP_CANCEL_REPLACE = _env_bool("TP_CANCEL_REPLACE", default=True)
TP_RETRY_MAX_ATTEMPTS = _env_int("TP_RETRY_MAX_ATTEMPTS", 8)
TP_RETRY_SCHEDULE_MS = _env_float_list("TP_RETRY_SCHEDULE_MS", [80, 120, 180, 250, 350, 500, 750, 1000])
TP_RETRY_JITTER_MS = _env_int("TP_RETRY_JITTER_MS", 60)
TP_PLACE_DEADLINE_MS = _env_int("TP_PLACE_DEADLINE_MS", 3000)
TP_PLACE_DELAY_MS = _env_int("TP_PLACE_DELAY_MS", 1000)
TP_RETRY_ON_ERRORS = set(_env_csv("TP_RETRY_ON_ERRORS", "-1003,-1013,-1111,-2010"))
UNWIND_ON_TP_FAILURE = _env_bool("UNWIND_ON_TP_FAILURE", default=True)
UNWIND_RETRY_MAX = _env_int("UNWIND_RETRY_MAX", 2)
UNWIND_RETRY_BACKOFF_MS = _env_int("UNWIND_RETRY_BACKOFF_MS", 120)
SELL_FEE_SAFETY = max(FEE_SELL_RATE * 2.0, 0.001)
PLACE_TP_ON = env_str("PLACE_TP_ON", "first_fill").strip().lower()
TP_CANCEL_REPLACE = _env_bool("TP_CANCEL_REPLACE", default=True)
TP_MAKER_MIN_TICKS_FROM_BID = _env_int("TP_MAKER_MIN_TICKS_FROM_BID", 1)
TP_POST_ONLY_MAX_TICKS = _env_int("TP_POST_ONLY_MAX_TICKS", 4)
TP_MODIFY_DEBOUNCE_MS = _env_int("TP_MODIFY_DEBOUNCE_MS", 120)
PRICE_AMEND_MIN_MS = _env_int("PRICE_AMEND_MIN_MS", 300)
MAKER_FLOOR_OFFSET_TICKS = _env_int("MAKER_FLOOR_OFFSET_TICKS", 1)
POST_ONLY_BUMP_MAX_ATTEMPTS = _env_int("POST_ONLY_BUMP_MAX_ATTEMPTS", 2)
FAR_TARGET_TICKS_MIN = _env_int("FAR_TARGET_TICKS_MIN", 5)
FAR_TARGET_BPS = _env_int("FAR_TARGET_BPS", 100)
TP_QUEUE_BOOST_DELAY_MS = _env_int("TP_QUEUE_BOOST_DELAY_MS", 900)
TP_QUEUE_BOOST_MIN_QTY = env_float("TP_QUEUE_BOOST_MIN_QTY", 0.0)
TP_QUEUE_BOOST_MAX_TICKS = _env_int("TP_QUEUE_BOOST_MAX_TICKS", 3)
EXIT_CANCEL_ACK_TIMEOUT_MS = _env_int("EXIT_CANCEL_ACK_TIMEOUT_MS", 400)
EXIT_DUST_QTY_THRESHOLD = env_float("EXIT_DUST_QTY_THRESHOLD", 0.0)
EXIT_DUST_NOTIONAL_THRESHOLD = env_float("EXIT_DUST_NOTIONAL_THRESHOLD", 0.0)



# Depth WS/Snapshot controls (sharding & resync)
DEPTH_BASELINE_N = _env_int("DEPTH_BASELINE_N", 20)
DEPTH_SHARD_SIZE = _env_int("DEPTH_SHARD_SIZE", 50)
DEPTH_WS_CADENCE_MS = _env_int("DEPTH_WS_CADENCE_MS", 100)
DEPTH_WS_RECONNECT_SEC = env_float("DEPTH_WS_RECONNECT_SEC", 1.0)
DEPTH_HEALTH_SUMMARY_SEC = _env_int("DEPTH_HEALTH_SUMMARY_SEC", 60)
ENTRY_PRICE_DRIFT_ENABLED = _env_bool("ENTRY_PRICE_DRIFT_ENABLED", default=False)
WS_DEPTH_LEVELS = int(os.getenv("WS_DEPTH_LEVELS", "20"))
def _parse_int_csv(raw: str) -> list[int]:
    out: list[int] = []
    for p in (raw or "").split(","):
        p = p.strip()
        if not p:
            continue
        try:
            out.append(int(float(p)))
        except Exception:
            continue
    return out
WS_DEPTH_ESCALATE_LEVELS = _parse_int_csv(os.getenv("WS_DEPTH_ESCALATE_LEVELS", "50,100")) or [50, 100]
ENTRY_EXECUTION_STYLE = (os.getenv("ENTRY_EXECUTION_STYLE", "market") or "market").strip().lower()
REST_SEED_LIMIT = int(os.getenv("REST_SEED_LIMIT", "1000"))
ENTRY_QUOTE_BUFFER_BPS = _env_int("ENTRY_QUOTE_BUFFER_BPS", 50)

# Behavior when no coverage to ceil is present
ENTRY_NO_COVERAGE_BEHAVIOR = (os.getenv("ENTRY_NO_COVERAGE_BEHAVIOR", "ioc") or "ioc").strip().lower()  # "ioc" | "skip"
ENTRY_IOC_MIN_NOTIONAL = float(os.getenv("ENTRY_IOC_MIN_NOTIONAL", "5"))
ENTRY_IOC_MIN_FILL_RATIO = float(os.getenv("ENTRY_IOC_MIN_FILL_RATIO", "0.0"))

ENTRY_MAX_SLIP_PCT = env_float("ENTRY_MAX_SLIP", 0.01)
ENTRY_MAX_SLIP_PCT = max(0.0, ENTRY_MAX_SLIP_PCT)

# ---------------------------------------------------------------------------
# take-profit wall detection / target floor
# ---------------------------------------------------------------------------
BIN_WIDTH_TICKS = _env_int("BIN_WIDTH_TICKS", 5)
WALL_MULT = env_float("WALL_MULT", 3.0)
WALL_WIDTH_BINS = _env_int("WALL_WIDTH_BINS", 2)
RANGE_MARGIN_BPS = _env_int("RANGE_MARGIN_BPS", 50)
RANGE_CAP_TICKS = _env_int("RANGE_CAP_TICKS", 200)
TARGET_FLOOR_BPS = _env_int("TARGET_FLOOR_BPS", 100)
TARGET_FLOOR_ENFORCE = _env_bool("TARGET_FLOOR_ENFORCE", default=True)

BIN_WIDTH_TICKS = max(1, BIN_WIDTH_TICKS)
WALL_WIDTH_BINS = max(1, WALL_WIDTH_BINS)
RANGE_MARGIN_BPS = max(0, RANGE_MARGIN_BPS)
RANGE_CAP_TICKS = max(1, RANGE_CAP_TICKS)
TARGET_FLOOR_BPS = max(0, TARGET_FLOOR_BPS)


# ---------------------------------------------------------------------------
# buy event logging (console only)
# ---------------------------------------------------------------------------
SIGNAL_CONTEXT_MODE = (os.getenv("SIGNAL_CONTEXT_MODE", "ref") or "ref").strip().lower()
BUY_EVENT_AFTER_FILL_ONLY = _env_bool("BUY_EVENT_AFTER_FILL_ONLY", default=True)
BUY_EVENT_SEVERITY = (os.getenv("BUY_EVENT_SEVERITY", "info") or "info").strip().lower()
BUY_LOG_INCLUDE_BALANCES = _env_bool("BUY_LOG_INCLUDE_BALANCES", default=False)

# ---------------------------------------------------------------------------
# optional logging toggles (impact/staleness)
# ---------------------------------------------------------------------------
IMPACT_LOG_ENABLED = _env_bool("IMPACT_LOG_ENABLED", default=True)
STALE_SUMMARY_LOG_ENABLED = _env_bool("STALE_SUMMARY_LOG_ENABLED", default=False)

# Break-even arming buffer to avoid instant BE exits
# daily freeze threshold (% of day-start capital), negative for loss freeze
DAILY_FREEZE_PCT = env_float("DAILY_FREEZE_PCT", -0.10)

# Optional hard kill for any legacy journal writes
DISABLE_JOURNAL = _env_bool("DISABLE_JOURNAL", default=False)


# ---------------------------------------------------------------------------
# Micro stats configuration
# ---------------------------------------------------------------------------
MICRO_ROLLING_WINDOW_MS = _env_int_with_fallback("MICRO_ROLLING_WINDOW_MS", "ROLLING_MIN_WINDOW_MS", 250)
MICRO_CANCEL_ADD_MIN_DENOM = env_float("MICRO_CANCEL_ADD_MIN_DENOM", 1e-9)
MICRO_TICK_INTERVAL_SEC = env_float("MICRO_TICK_INTERVAL_SEC", 0.1)


# ---------------------------------------------------------------------------
# Runner cadence / maintenance windows
# ---------------------------------------------------------------------------
EXCHANGE_INFO_REFRESH_SEC = _env_int("EXCHANGE_INFO_REFRESH_SEC", 6 * 3600)
MAIN_LOOP_HEARTBEAT_SEC = _env_int("MAIN_LOOP_HEARTBEAT_SEC", 3600)
