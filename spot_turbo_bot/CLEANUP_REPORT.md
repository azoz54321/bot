# Cleanup Report

## Status
- branch: `chore/cleanup-structure`
- last generated: 2025-10-14

## Candidate Removals (Pending Verification)
- `spot_turbo_bot.market.depth_baseline` (`spot_turbo_bot.market.depth_baseline.py`): no internal imports; appears unused.
- `spot_turbo_bot.trading.triggers` (`spot_turbo_bot/trading/triggers.py`): not imported within project; confirm external usage before removal.
- `spot_turbo_bot.market.context.start_services` / `stop_services` (`spot_turbo_bot/market/context.py`): exported but no call sites detected.
- `spot_turbo_bot.runners.diag.StallWatchdog` (`spot_turbo_bot/runners/diag.py`): exported but no call sites detected.
- `spot_turbo_bot.core.logger.dbg_ws` (`spot_turbo_bot/core/logger.py`): exported but no call sites detected.
- `spot_turbo_bot.config._parse_symbols` (`spot_turbo_bot/core/config.py`): helper appears unused.
- `spot_turbo_bot.core.utils_time.now_utc` (`spot_turbo_bot/core/utils_time.py`): helper appears unused.

> Actions will only proceed after confirming no dynamic or external dependencies rely on the items above.

## Completed Changes
## Removed (Exit V1)
- `spot_turbo_bot/trading/exit_engine.py` (legacy TP/SL/BE implementation).
- `spot_turbo_bot/core/timeout_store.py` (timeout persistence for Exit V1).
- `spot_turbo_bot/trading/monthly_ban.py` (legacy monthly loss ban helper).
- Deprecated config keys: `EXIT_TP_NET_PCT`, `EXIT_SL_NET_PCT`, `EXIT_BE_MODE`, `EXIT_BE_RAW_PCT`, `EXIT_BE_ARM_BUFFER_PCT`, `EXIT_TIMEOUT_SEC`, `TIMEOUT_EXIT_ENABLED`, `TIMEOUT_MINUTES`, `RESUME_TIMEOUTS_ON_BOOT`, `EXIT_EVENT_LOGGING`, `EXIT_EVENT_AFTER_FILL_ONLY`, `EXIT_EVENT_SEVERITY`.

- Reorganised `spot_turbo_bot` into `core/`, `infra/`, `market/`, `trading/`, `runners/`, and `reports/` with import shims in `__init__.py`.
- Sanitised configuration: removed hard-coded Binance credentials and centralised optional env overrides in `core/config.py`.
- Replaced hot-path `print` usage with logger helpers; centralised log formatting in `core/logger.py`.
- Added `scripts/smoke_check.py` for connectivity and engine dry-run validation.
- Refreshed documentation (`README.md`) and dependency pins (`requirements.txt`).
- Removed `.env.example`; README now points to OS-level environment variable configuration.
- Added `runners/one_sec_aggregator.py` to publish local one-second aggregates to NATS using the centralised connection options.

## Config Keys Added
## Config Keys Added (Exit V2)
- `EXIT_V2_TAKE_LIMIT_PCT`, `EXIT_V2_STOP_DROP_PCT`, `EXIT_V2_PULLBACK_UP_PCT`, `EXIT_V2_ENTRY_REGION_TOL_PCT`, `EXIT_V2_PRICE_SOURCE`, `EXIT_V2_DEBOUNCE_MS`, `EXIT_V2_MAX_STALENESS_MS`, `EXIT_V2_CANCEL_TIMEOUT_SEC`, `EXIT_V2_MARKET_RETRY`, `SELL_FEE_SAFETY`, `PLACE_TP_ON`, `TP_CANCEL_REPLACE`.

- Impact estimator: `IMPACT_SLIP_DEFAULT`, `IMPACT_UTIL_LAST`, `IMPACT_PUMP_BASE`, `IMPACT_PUMP_MIN`, `IMPACT_PUMP_MAX`, `IMPACT_PUMP_SENSITIVITY`, `IMPACT_RATIO_CAP`, `IMPACT_MAX_STALENESS_MS`.
- Depth WS / market context: `DEPTH_BASELINE_N`, `DEPTH_SHARD_SIZE`, `DEPTH_WS_CADENCE_MS`, `DEPTH_WS_RECONNECT_SEC`, `DEPTH_HEALTH_SUMMARY_SEC`.
- Micro stats: `MICRO_ROLLING_WINDOW_MS`, `MICRO_CANCEL_ADD_MIN_DENOM`, `MICRO_TICK_INTERVAL_SEC`.
- Runner cadence: `EXCHANGE_INFO_REFRESH_SEC`, `MAIN_LOOP_HEARTBEAT_SEC`.
- NATS client/auth: `NATS_AUTH_MODE`, `NATS_URLS`, `NATS_CONNECT_TIMEOUT_SEC`, `NATS_RECONNECT_MAX_TRIES`, `NATS_RECONNECT_JITTER_MS`, `NATS_NKEY_PUBLIC`, `NATS_NKEY_SEED`, `NATS_JWT`, `NATS_USERNAME`, `NATS_PASSWORD`, `NATS_TLS_REQUIRED`, `NATS_TLS_CA_PATH`, `NATS_TLS_CERT_PATH`, `NATS_TLS_KEY_PATH`, `NATS_TLS_INSECURE_SKIP_VERIFY`.
- Confirmed that each new `IMPACT_*`, `MICRO_*`, and `DEPTH_*` configuration key matches the legacy literal values one-to-one. Updated modules: `spot_turbo_bot/market/depth_ws.py`, `spot_turbo_bot/market/context.py`, `spot_turbo_bot/market/micro_stats.py`, `spot_turbo_bot/runners/one_sec_runner.py`, `spot_turbo_bot/trading/trading_engine.py`.

## Deferred Items
- _None_

## Next Configuration Candidates
- Externalise hard-coded `slip`, `util_last`, and `pump_k` defaults in `market/impact_estimator.py`.
- Surface `rolling_ms` window sizing from `market/micro_stats.py` for env-based tuning.
- Review fixed cadence/baseline parameters in `market/depth_ws.py` for potential config wiring.
- Catalog any remaining magic constants in trading/runners modules after confirming runtime dependencies.
