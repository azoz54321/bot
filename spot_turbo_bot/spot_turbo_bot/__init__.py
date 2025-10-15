from __future__ import annotations

import sys
from importlib import import_module
from types import ModuleType
from typing import Dict


_PACKAGES = ("core", "infra", "market", "reports", "runners", "trading")
_LEGACY_MODULES: Dict[str, str] = {
    # Core
    "config": "core.config",
    "logger": "core.logger",
    "utils_time": "core.utils_time",
    "exchange_info": "core.exchange_info",
    "state": "core.state",
    # Infrastructure
    "binance_http": "infra.binance_http",
    "nats_adapter": "infra.nats_adapter",
    # Market data / stats
    "depth_ws": "market.depth_ws",
    "depth_baseline": "market.depth_baseline",
    "micro_stats": "market.micro_stats",
    "impact_estimator": "market.impact_estimator",
    "context": "market.context",
    # Reporting
    "report_writer": "reports.report_writer",
    # Runners / orchestration
    "start": "runners.start",
    "one_sec_runner": "runners.one_sec_runner",
    "user_stream": "runners.user_stream",
    "runtime_tasks": "runners.runtime_tasks",
    "boot_lint": "runners.boot_lint",
    "diag": "runners.diag",
    # Trading logic
    "trading_engine": "trading.trading_engine",
    "trading_rules": "trading.trading_rules",
    "exit_v2": "trading.exit_v2",
    "triggers": "trading.triggers",
}


def _expose_package(name: str) -> ModuleType:
    module = import_module(f".{name}", __name__)
    setattr(sys.modules[__name__], name, module)
    sys.modules[f"{__name__}.{name}"] = module
    return module


def _expose_legacy(name: str, target: str) -> ModuleType:
    module = import_module(f".{target}", __name__)
    setattr(sys.modules[__name__], name, module)
    sys.modules[f"{__name__}.{name}"] = module
    return module


__all__ = list(_PACKAGES) + list(_LEGACY_MODULES.keys())

for _pkg in _PACKAGES:
    _expose_package(_pkg)

for _name, _target in _LEGACY_MODULES.items():
    _expose_legacy(_name, _target)
