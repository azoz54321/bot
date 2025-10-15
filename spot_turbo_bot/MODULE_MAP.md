# Module Map

## Unreferenced Modules
- `spot_turbo_bot` (spot_turbo_bot\__init__.py)
- `spot_turbo_bot.core` (spot_turbo_bot\core\__init__.py)
- `spot_turbo_bot.infra` (spot_turbo_bot\infra\__init__.py)
- `spot_turbo_bot.market.depth_baseline` (spot_turbo_bot\market\depth_baseline.py)
- `spot_turbo_bot.reports` (spot_turbo_bot\reports\__init__.py)
- `spot_turbo_bot.runners` (spot_turbo_bot\runners\__init__.py)
- `spot_turbo_bot.runners.one_sec_aggregator` (spot_turbo_bot\runners\one_sec_aggregator.py)
- `spot_turbo_bot.runners.start` (spot_turbo_bot\runners\start.py)
- `spot_turbo_bot.trading` (spot_turbo_bot\trading\__init__.py)
- `spot_turbo_bot.trading.triggers` (spot_turbo_bot\trading\triggers.py)

## Module Details
### `spot_turbo_bot`
- Path: `spot_turbo_bot\__init__.py`
- Imports: `__future__`, `importlib`, `types`, `typing`
- Imported by: _none_
- Defines: functions=`_expose_legacy`, `_expose_package`
- Possibly unused: _none_

### `spot_turbo_bot.core`
- Path: `spot_turbo_bot\core\__init__.py`
- Imports: _none_
- Imported by: _none_
- Defines: _none_
- Possibly unused: _none_

### `spot_turbo_bot.core.config`
- Path: `spot_turbo_bot\core\config.py`
- Imports: `__future__`, `dotenv`, `pathlib`
- Imported by: `spot_turbo_bot.core.exchange_info`, `spot_turbo_bot.core.logger`, `spot_turbo_bot.core.state`, `spot_turbo_bot.core.utils_time`, `spot_turbo_bot.infra.binance_http`, `spot_turbo_bot.infra.nats_adapter`, `spot_turbo_bot.market.context`, `spot_turbo_bot.market.depth_ws`, `spot_turbo_bot.market.impact_estimator`, `spot_turbo_bot.market.micro_stats`, `spot_turbo_bot.runners.boot_lint`, `spot_turbo_bot.runners.diag`, `spot_turbo_bot.runners.one_sec_aggregator`, `spot_turbo_bot.runners.one_sec_runner`, `spot_turbo_bot.runners.start`, `spot_turbo_bot.runners.user_stream`, `spot_turbo_bot.trading.trading_engine`, `spot_turbo_bot.trading.trading_rules`, `spot_turbo_bot.trading.triggers`
- Defines: functions=`_env_bool`, `_env_csv`, `_env_int`, `_env_int_with_fallback`, `_env_multiline`, `_optional_env_str`, `_parse_int_csv`, `_parse_symbols`, `env_float`, `env_str`, `fmt_pct`, `require_env`
- Possibly unused: `_optional_env_str`

### `spot_turbo_bot.core.exchange_info`
- Path: `spot_turbo_bot\core\exchange_info.py`
- Imports: `__future__`, `spot_turbo_bot.core.config`, `typing`
- Imported by: `spot_turbo_bot.runners.one_sec_aggregator`, `spot_turbo_bot.runners.one_sec_runner`, `spot_turbo_bot.runners.start`, `spot_turbo_bot.trading.trading_engine`, `spot_turbo_bot.trading.trading_rules`, `spot_turbo_bot.trading.triggers`
- Defines: classes=`ExchangeInfo`, functions=`_decimals_from_step_like`, `round_down`, `round_to_step`, `round_to_tick`
- Possibly unused: _none_

### `spot_turbo_bot.core.logger`
- Path: `spot_turbo_bot\core\logger.py`
- Imports: `__future__`, `datetime`, `spot_turbo_bot.core.config`
- Imported by: `spot_turbo_bot.infra.binance_http`, `spot_turbo_bot.market.context`, `spot_turbo_bot.market.depth_ws`, `spot_turbo_bot.runners.diag`, `spot_turbo_bot.runners.one_sec_aggregator`, `spot_turbo_bot.runners.one_sec_runner`, `spot_turbo_bot.runners.runtime_tasks`, `spot_turbo_bot.runners.start`, `spot_turbo_bot.trading.trading_engine`
- Defines: functions=`_now_ksa_str`, `dbg`, `dbg_ws`, `log_boot`, `log_buy`, `log_daily_reset`, `log_line`, `log_sell`
- Possibly unused: `_now_ksa_str`, `dbg_ws`

### `spot_turbo_bot.core.state`
- Path: `spot_turbo_bot\core\state.py`
- Imports: `__future__`, `dataclasses`, `datetime`, `spot_turbo_bot.core.config`, `spot_turbo_bot.core.utils_time`, `typing`
- Imported by: `spot_turbo_bot.runners.runtime_tasks`, `spot_turbo_bot.runners.start`, `spot_turbo_bot.trading.trading_engine`, `spot_turbo_bot.trading.trading_rules`
- Defines: classes=`GlobalState`, `Position`, `SlotState`, functions=`ensure_runtime_dir`, `load_state`, `save_state`
- Possibly unused: _none_

### `spot_turbo_bot.core.utils_time`
- Path: `spot_turbo_bot\core\utils_time.py`
- Imports: `__future__`, `datetime`, `spot_turbo_bot.core.config`
- Imported by: `spot_turbo_bot.core.state`, `spot_turbo_bot.runners.runtime_tasks`, `spot_turbo_bot.trading.trading_engine`
- Defines: functions=`next_reset_dt_ksa`, `now_ksa`, `now_utc`, `seconds_until`
- Possibly unused: `now_utc`

### `spot_turbo_bot.infra`
- Path: `spot_turbo_bot\infra\__init__.py`
- Imports: _none_
- Imported by: _none_
- Defines: _none_
- Possibly unused: _none_

### `spot_turbo_bot.infra.binance_http`
- Path: `spot_turbo_bot\infra\binance_http.py`
- Imports: `__future__`, `decimal`, `spot_turbo_bot.core.config`, `spot_turbo_bot.core.logger`, `typing`, `urllib.parse`
- Imported by: `spot_turbo_bot.runners.one_sec_aggregator`, `spot_turbo_bot.runners.start`, `spot_turbo_bot.runners.user_stream`, `spot_turbo_bot.trading.trading_engine`, `spot_turbo_bot.trading.triggers`
- Defines: classes=`BinanceHTTP`, `BinanceHTTPError`, functions=`_normalize_credential`, async=`_raise_or_json`
- Possibly unused: _none_

### `spot_turbo_bot.infra.nats_adapter`
- Path: `spot_turbo_bot\infra\nats_adapter.py`
- Imports: `__future__`, `dataclasses`, `nats.aio.client`, `nkeys`, `spot_turbo_bot.core.config`, `typing`
- Imported by: `spot_turbo_bot.runners.one_sec_aggregator`, `spot_turbo_bot.runners.one_sec_runner`
- Defines: classes=`NATSConnectConfig`, `NATSOneSecAdapter`, `OneSecondEvent`, functions=`_build_auth_details`, `_build_connect_config`, `_build_tls_context`, `_make_signature_cb`, `get_nats_connect_config`, async=`_noop_async`
- Possibly unused: _none_

### `spot_turbo_bot.market`
- Path: `spot_turbo_bot\market\__init__.py`
- Imports: _none_
- Imported by: `spot_turbo_bot.runners.runtime_tasks`, `spot_turbo_bot.runners.start`, `spot_turbo_bot.trading.trading_engine`
- Defines: _none_
- Possibly unused: _none_

### `spot_turbo_bot.market.context`
- Path: `spot_turbo_bot\market\context.py`
- Imports: `spot_turbo_bot.core.config`, `spot_turbo_bot.core.logger`, `spot_turbo_bot.market.depth_ws`, `spot_turbo_bot.market.micro_stats`, `spot_turbo_bot.reports.report_writer`, `typing`
- Imported by: `spot_turbo_bot.runners.start`
- Defines: functions=`build_symbols_meta`, `setup`, async=`start_services`, `stop_services`
- Possibly unused: `setup`, `start_services`, `stop_services`

### `spot_turbo_bot.market.depth_baseline`
- Path: `spot_turbo_bot\market\depth_baseline.py`
- Imports: _none_
- Imported by: _none_
- Defines: functions=`pick_bucket`
- Possibly unused: `pick_bucket`

### `spot_turbo_bot.market.depth_ws`
- Path: `spot_turbo_bot\market\depth_ws.py`
- Imports: `__future__`, `spot_turbo_bot.core.config`, `spot_turbo_bot.core.logger`, `typing`
- Imported by: `spot_turbo_bot.market.context`
- Defines: classes=`Book`, `DepthWS`
- Possibly unused: _none_

### `spot_turbo_bot.market.impact_estimator`
- Path: `spot_turbo_bot\market\impact_estimator.py`
- Imports: `spot_turbo_bot.core.config`, `statistics`
- Imported by: `spot_turbo_bot.trading.trading_engine`
- Defines: functions=`_density_min`, `_log`, `_safe`, `estimate_fused`, `floor_to_step`, `floor_to_tick`, `impact_1pct_estimate`, `impact_1pct_lb`
- Possibly unused: `_density_min`

### `spot_turbo_bot.market.micro_stats`
- Path: `spot_turbo_bot\market\micro_stats.py`
- Imports: `spot_turbo_bot.core.config`
- Imported by: `spot_turbo_bot.market.context`
- Defines: classes=`MicroStats`
- Possibly unused: _none_

### `spot_turbo_bot.reports`
- Path: `spot_turbo_bot\reports\__init__.py`
- Imports: _none_
- Imported by: _none_
- Defines: _none_
- Possibly unused: _none_

### `spot_turbo_bot.reports.report_writer`
- Path: `spot_turbo_bot\reports\report_writer.py`
- Imports: `zoneinfo`
- Imported by: `spot_turbo_bot.market.context`, `spot_turbo_bot.runners.start`
- Defines: classes=`DailyTextReport`, functions=`_day_key`, `_hms`, `_ksa_now`
- Possibly unused: _none_

### `spot_turbo_bot.runners`
- Path: `spot_turbo_bot\runners\__init__.py`
- Imports: _none_
- Imported by: _none_
- Defines: _none_
- Possibly unused: _none_

### `spot_turbo_bot.runners.boot_lint`
- Path: `spot_turbo_bot\runners\boot_lint.py`
- Imports: `__future__`, `spot_turbo_bot.core.config`, `typing`
- Imported by: `spot_turbo_bot.runners.start`
- Defines: functions=`run_boot_lint`
- Possibly unused: _none_

### `spot_turbo_bot.runners.diag`
- Path: `spot_turbo_bot\runners\diag.py`
- Imports: `__future__`, `spot_turbo_bot.core.config`, `spot_turbo_bot.core.logger`, `typing`
- Imported by: `spot_turbo_bot.runners.user_stream`, `spot_turbo_bot.trading.triggers`
- Defines: classes=`StallWatchdog`, functions=`touch_activity`
- Possibly unused: `StallWatchdog`

### `spot_turbo_bot.runners.one_sec_aggregator`
- Path: `spot_turbo_bot\runners\one_sec_aggregator.py`
- Imports: `__future__`, `dataclasses`, `nats.aio.client`, `spot_turbo_bot.core.config`, `spot_turbo_bot.core.exchange_info`, `spot_turbo_bot.core.logger`, `spot_turbo_bot.infra.binance_http`, `spot_turbo_bot.infra.nats_adapter`, `typing`
- Imported by: _none_
- Defines: classes=`OneSecAggregator`, `SymbolHistory`, `SymbolState`, functions=`_chunked`, `_parse_args`, `main`, async=`_async_main`, `_resolve_symbols`
- Possibly unused: _none_

### `spot_turbo_bot.runners.one_sec_runner`
- Path: `spot_turbo_bot\runners\one_sec_runner.py`
- Imports: `__future__`, `spot_turbo_bot.core.config`, `spot_turbo_bot.core.exchange_info`, `spot_turbo_bot.core.logger`, `spot_turbo_bot.infra.nats_adapter`, `spot_turbo_bot.trading.trading_engine`, `typing`
- Imported by: `spot_turbo_bot.runners.start`
- Defines: classes=`OneSecRunner`
- Possibly unused: _none_

### `spot_turbo_bot.runners.runtime_tasks`
- Path: `spot_turbo_bot\runners\runtime_tasks.py`
- Imports: `__future__`, `datetime`, `spot_turbo_bot.core.logger`, `spot_turbo_bot.core.state`, `spot_turbo_bot.core.utils_time`, `spot_turbo_bot.market`, `spot_turbo_bot.trading.trading_engine`
- Imported by: `spot_turbo_bot.runners.start`
- Defines: classes=`DailyResetScheduler`
- Possibly unused: _none_

### `spot_turbo_bot.runners.start`
- Path: `spot_turbo_bot\runners\start.py`
- Imports: `__future__`, `decimal`, `spot_turbo_bot.core.config`, `spot_turbo_bot.core.exchange_info`, `spot_turbo_bot.core.logger`, `spot_turbo_bot.core.state`, `spot_turbo_bot.infra.binance_http`, `spot_turbo_bot.market`, `spot_turbo_bot.market.context`, `spot_turbo_bot.reports.report_writer`, `spot_turbo_bot.runners.boot_lint`, `spot_turbo_bot.runners.one_sec_runner`, `spot_turbo_bot.runners.runtime_tasks`, `spot_turbo_bot.runners.user_stream`, `spot_turbo_bot.trading.trading_engine`, `typing`
- Imported by: _none_
- Defines: functions=`_calc_proceeds_and_price_from_exec`, async=`main`
- Possibly unused: _none_

### `spot_turbo_bot.runners.user_stream`
- Path: `spot_turbo_bot\runners\user_stream.py`
- Imports: `__future__`, `spot_turbo_bot.core.config`, `spot_turbo_bot.infra.binance_http`, `spot_turbo_bot.runners.diag`, `typing`
- Imported by: `spot_turbo_bot.runners.start`
- Defines: classes=`UserStreamManager`
- Possibly unused: _none_

### `spot_turbo_bot.trading`
- Path: `spot_turbo_bot\trading\__init__.py`
- Imports: _none_
- Imported by: _none_
- Defines: _none_
- Possibly unused: _none_

### `spot_turbo_bot.trading.exit_v2`
- Path: `spot_turbo_bot\trading\exit_v2.py`
- Imports: `__future__`, `dataclasses`, `typing`
- Imported by: `spot_turbo_bot.trading.trading_engine`
- Defines: classes=`ExitV2Controller`, `ExitV2Params`, `ExitV2State`
- Possibly unused: _none_

### `spot_turbo_bot.trading.trading_engine`
- Path: `spot_turbo_bot\trading\trading_engine.py`
- Imports: `__future__`, `dataclasses`, `datetime`, `decimal`, `spot_turbo_bot.core.config`, `spot_turbo_bot.core.exchange_info`, `spot_turbo_bot.core.logger`, `spot_turbo_bot.core.state`, `spot_turbo_bot.core.utils_time`, `spot_turbo_bot.infra.binance_http`, `spot_turbo_bot.market`, `spot_turbo_bot.market.impact_estimator`, `spot_turbo_bot.trading.exit_v2`, `spot_turbo_bot.trading.trading_rules`, `typing`
- Imported by: `spot_turbo_bot.runners.one_sec_runner`, `spot_turbo_bot.runners.runtime_tasks`, `spot_turbo_bot.runners.start`, `spot_turbo_bot.trading.triggers`
- Defines: classes=`BuyRequest`, `TradingEngine`, functions=`_ceil_to_tick`, `_floor_to_step`, `_now_ms`, `compute_safe_quote`
- Possibly unused: _none_

### `spot_turbo_bot.trading.trading_rules`
- Path: `spot_turbo_bot\trading\trading_rules.py`
- Imports: `__future__`, `dataclasses`, `spot_turbo_bot.core.config`, `spot_turbo_bot.core.exchange_info`, `spot_turbo_bot.core.state`, `typing`
- Imported by: `spot_turbo_bot.trading.trading_engine`
- Defines: classes=`TradingRules`
- Possibly unused: _none_

### `spot_turbo_bot.trading.triggers`
- Path: `spot_turbo_bot\trading\triggers.py`
- Imports: `__future__`, `dataclasses`, `spot_turbo_bot.core.config`, `spot_turbo_bot.core.exchange_info`, `spot_turbo_bot.infra.binance_http`, `spot_turbo_bot.runners.diag`, `spot_turbo_bot.trading.trading_engine`, `typing`
- Imported by: _none_
- Defines: classes=`PriceSnapshot`, `TriggerRunner`
- Possibly unused: `TriggerRunner`
