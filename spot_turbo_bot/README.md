# spot_turbo_bot

High-throughput Binance spot momentum bot tuned for ALL_USDT coverage. WebSocket handlers stay allocation-light, exits are market-only, and operational state is persisted under `runtime/` for quick restarts.

## Requirements

- Python 3.12+
- `pip install -r requirements.txt`
- Binance API key/secret with Spot trade permissions

## Project Layout

```
spot_turbo_bot/
  core/      # configuration, logging, exchange metadata, state helpers
  infra/     # HTTP/NATS adapters and other external bridges
  market/    # depth websockets, micro stats aggregation, market context
  reports/   # text report writer used by runners
  runners/   # boot/runtime orchestration entry-points
  trading/   # trading/exit engines and related rules
scripts/
  dependency_analyzer.py
  smoke_check.py          # smoke test added during cleanup
```

Runtime artefacts live in `runtime/` (state, timeout cache). Cached exchange metadata stays in `data/exchangeInfo.json` and is refreshed automatically on boot.

## Setup

1. Create a virtual environment and install dependencies:
   ```powershell
   python -m venv .venv
   .\.venv\Scripts\Activate.ps1
   pip install -r requirements.txt
   ```
2. Configure credentials and overrides through OS environment variables (optional for read-only flows):
   ```powershell
   $env:BINANCE_API_KEY = "your-key"       # required only for signed REST calls
   $env:BINANCE_API_SECRET = "your-secret" # required only for signed REST calls
   # any other overrides, e.g.:
   $env:TRIGGER_PCT_1S = "0.0001"
   ```

## Running the bot

py -3.12 -m spot_turbo_bot.runners.start

py -3.12 -m spot_turbo_bot.runners.one_sec_aggregator --all-usdt

cd nats-server-v2.12.1-RC.5-windows-amd64 .\nats-server.exe

On startup you should see `[BOOT]`, `[BOOTBAL]`, and `[OPENPOS]` lines summarising the configuration, free capital, and any warm positions. Hot-path loops no longer use raw `print`; all output flows through `spot_turbo_bot.core.logger` while preserving the existing `[BOOT] / [BUY] / [SELL] / [DAILY-RESET]` prefixes.

## Local 1s Aggregator

For environments without an external ONE_SEC feed you can bootstrap a local publisher that compresses Binance `bookTicker` updates into one-second bars and publishes them to NATS:

```powershell
py -3.12 -m spot_turbo_bot.runners.one_sec_aggregator --all-usdt
```

Pass an explicit subset with `--symbols WLDUSDT,XPLUSUSDT` to limit the stream. The aggregator honours the same NATS configuration (auth mode, TLS, reconnect strategy) defined in `core/config.py` and emits to `ONE_SEC.usdt.<SYMBOL>` for seamless consumption by `OneSecRunner`.

## Environment Variables

### Required

| Name | Description |
|------|-------------|
| `BINANCE_API_KEY` | Spot API key used for authenticated REST requests. |
| `BINANCE_API_SECRET` | Spot API secret (keep private; read only from environment variables when supplied). |

### Connectivity & Runtime

| Name | Default | Notes |
|------|---------|-------|
| `DATA_BUS_URL` | `nats://localhost:4222` | NATS endpoint for one-second micro-stat feed. |
| `DATA_TOPIC_ONE_SEC` | `ONE_SEC.usdt.*` | Subject pattern consumed by `OneSecRunner`. |
| `DATA_CLIENT_GROUP` | `spot_turbo_bot` | Queue group for NATS subscription. |
| `HTTP_TIMEOUT_SEC` | `10` | REST client timeout in seconds. |
| `LISTENKEY_RENEW_MIN` | `27` | Minutes between user-stream keep-alive calls. |
| `RUNTIME_DIR` | `runtime` | Directory used for persisted state/timeouts. |
| `EXCHANGE_INFO_PATH` | `data/exchangeInfo.json` | Local cache of exchange metadata. |

### User Stream

| Name | Default | Notes |
|------|---------|-------|
| `USERSTREAM_ENABLE` | `true` | Toggle the spot user-stream bootstrap and health probes. |
| `USERSTREAM_KEEPALIVE_SEC` | `1620` | Interval between listenKey keepalive calls (falls back to `LISTENKEY_RENEW_MIN*60`). |
| `USERSTREAM_STALE_MS` | `2500` | Consider the user-stream stale when no events arrive within this window. |
| `USERSTREAM_RECONNECT_BACKOFF_MS_MIN` | `300` | Initial reconnect delay (ms) when the stream drops. |
| `USERSTREAM_RECONNECT_BACKOFF_MS_MAX` | `5000` | Maximum reconnect backoff (ms) with exponential growth and jitter. |
| `USERSTREAM_FALLBACK_QUERY_MS` | `200` | Delay before REST order-status bridging kicks in if no fill event is received. |
| `USERSTREAM_VERBOSE` | `true` | Emit keepalive success logs in addition to failures. |

### Trigger & Market Controls

| Name | Default | Notes |
|------|---------|-------|
| `TRIGGER_PCT_1S` | `0.0001` | 1s trigger threshold (0.01% by default). |
| `TRIGGER_MIN_UPDATES_1S` | `3` | Minimum updates per second sample. |
| `SPREAD_BPS_CUTOFF` | `20.0` | Disqualify symbols with wider spread (bps). |
| `TRIGGER_COOLDOWN_S` | `10` | Cooldown between successive trigger fires per symbol. |
| `FAST_ALL_USDT` | `true` | Enable chunked combined streams for ALL_USDT mode. |
| `WS_COMBINED_CHUNK` | `75` | Symbols per combined stream request. |
| `WARMUP_SEC` | `3.0` | Ignore triggers for `WARMUP_SEC` seconds per symbol. |
| `WS_DEBOUNCE_MS` | `90` | Minimum milliseconds between per-symbol updates. |
| `WS_TICK_FILTER` | `true` | Drop depth updates smaller than tick size. |
| `ENTRY_IMPACT_SLIP_PCT` | `0.01` | Max slip used when assessing depth coverage. |

### Trading & Exit Controls

| Name | Default | Notes |
|------|---------|-------|
| `EXIT_V2_TAKE_LIMIT_PCT` | `0.02` | LIMIT_MAKER take-profit distance from entry. |
| `EXIT_V2_STOP_DROP_PCT` | `-0.01` | Hard stop trigger from entry. |
| `EXIT_V2_PULLBACK_UP_PCT` | `0.01` | Up move required to arm pullback exit. |
| `EXIT_V2_ENTRY_REGION_TOL_PCT` | `max(FEE_BUY_RATE + FEE_SELL_RATE, 0.0005)` | Margin around entry considered the pullback region. |
| `EXIT_V2_PRICE_SOURCE` | `bid` | Price source for exit checks (`bid` or `mid`). |
| `EXIT_V2_DEBOUNCE_MS` | `150` | Minimum ms condition must persist before firing. |
| `EXIT_V2_MAX_STALENESS_MS` | `120` | Ignore exit signals when book data is older than this. |
| `EXIT_V2_CANCEL_TIMEOUT_SEC` | `2.0` | Timeout when cancelling the take-profit limit. |
| `EXIT_CANCEL_ACK_TIMEOUT_MS` | `400` | Milliseconds to wait for cancel ACK before stop/pullback MARKET execution. |
| `EXIT_V2_MARKET_RETRY` | `2` | Retries when issuing market liquidation. |
| `EXIT_DUST_QTY_THRESHOLD` | `0.0` | Auto-close TP flow when base quantity falls below this value. |
| `EXIT_DUST_NOTIONAL_THRESHOLD` | `0.0` | Treat residual notional below this (quote) as dust. |
| `TP_MAKER_MIN_TICKS_FROM_BID` | `1` | Minimum ticks above best bid used when computing maker price floors. |
| `TP_POST_ONLY_MAX_TICKS` | `4` | Cap on extra ticks added after post-only rejections. |
| `TP_MODIFY_DEBOUNCE_MS` | `120` | Debounce between TP cancel/replace cycles. |
| `TP_QUEUE_BOOST_DELAY_MS` | `900` | Delay before queue boost nudges become active. |
| `TP_QUEUE_BOOST_MIN_QTY` | `0.0` | Minimum open quantity required to allow queue boost. |
| `TP_QUEUE_BOOST_MAX_TICKS` | `3` | Maximum ticks queue boost may shave from the target price. |
| `FEE_SOURCE` | `auto_or_manual` | Fee lookup strategy (auto via balances or manual fallback). |
| `FEE_BUY_RATE` | `0.00075` | Manual buy fee rate when fallback required. |
| `FEE_SELL_RATE` | `0.00075` | Manual sell fee rate when fallback required. |
| `RESUME_TIMEOUTS_ON_BOOT` | `true` | Restore timeout timers from persisted state on boot. |

Take-profit exits are submitted as post-only `LIMIT_MAKER` orders; maker floors, queue boost nudges, post-only retries, dust sweeping, and cancel ACK gating can be tuned with the environment switches above.

### Diagnostics & Logging

| Name | Default | Notes |
|------|---------|-------|
| `DEBUG_VERBOSE` | `false` | Enables `[DBG]` messages and verbose HTTP traces. |
| `DEBUG_WS_LOGS` | `false` | Per-tick `[DBG-WS]` output (keep `false` for ALL_USDT). |
| `IMPACT_LOG_ENABLED` | `true` | Toggle impact/coverage log lines. |
| `STALE_SUMMARY_LOG_ENABLED` | `false` | Enables periodic depth health summaries. |
| `STALL_DIAG_ON` | `false` | Activate stall watchdog reporting. |

All environment variables are read once on boot; update the runtime environment before launching.

Configuration defaults are defined in `spot_turbo_bot/core/config.py`; override them by exporting environment variables only when you need behaviour different from the packaged defaults.

## Smoke Test

A lightweight connectivity check is provided under `scripts/smoke_check.py`. It validates:

1. Secrets are loaded from the environment.
2. Exchange metadata is available (local cache or REST fallback).
3. `DepthWS` can connect and stream for five seconds before stopping.
4. `TradingEngine` initialises in dry-run mode without placing orders.

Run it with:

```powershell
py -3.12 -m scripts.smoke_check
```

It exits with code 0 on success and raises a descriptive exception otherwise.

## Logging & Runtime Artefacts

- Logs continue to use `[BOOT]`, `[BUY]`, `[SELL]`, and `[DAILY-RESET]` prefixes for downstream parsing.
- `runtime/state.json` stores slot balances and bans; delete it for a fresh reset.
- `runtime/` and other temp outputs are ignored by git via the updated `.gitignore`.
- Cached exchange info lives in `data/exchangeInfo.json`; it is regenerated automatically when missing.

## Operational Notes

- WebSocket handlers avoid blocking I/O and now rely on the shared logger for messaging.
- REST calls run through `BinanceHTTP`, which respects signing/debug toggles and reports account snapshots via `[ACC]` lines when `DEBUG_VERBOSE=true`.
- User-stream boot produces `[USER] listenKey created` / `[USER] stream connected` logs before triggers arm; stale detection, reconnects, and keepalives are reported under the `[USER]` prefix.
- First-fill anchoring logs `[ORDER] first_fill via USERSTREAM Δms=…` (or `via REST`) and falls back to REST bridging when no execution report arrives within `USERSTREAM_FALLBACK_QUERY_MS`.
- Import paths from the legacy flat layout continue to work via compatibility shims in `spot_turbo_bot/__init__.py`.
