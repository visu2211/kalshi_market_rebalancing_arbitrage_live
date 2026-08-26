"""Loads config/settings.yaml (strategy/risk parameters, safe to commit)
and .env (KALSHI_API_KEY_ID/KALSHI_PRIVATE_KEY_PATH for prod WS auth, and
KALSHI_DEMO_API_KEY_ID/KALSHI_DEMO_PRIVATE_KEY_PATH for demo-account order
placement — never committed) into one Settings object every other module
reads from.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Optional

import yaml
from dotenv import load_dotenv

DEFAULT_SETTINGS_PATH = Path(__file__).resolve().parent / "settings.yaml"


@dataclass(frozen=True)
class KalshiSettings:
    base_url: str
    ws_url: str
    request_timeout_s: float
    max_retries: int
    api_key_id: Optional[str]
    private_key_path: Optional[str]


@dataclass(frozen=True)
class ScannerSettings:
    min_outcome_legs: int
    max_outcome_legs: int
    min_depth_contracts: Decimal
    rescan_interval_s: int
    orderbook_depth: int
    max_ws_legs: int


@dataclass(frozen=True)
class ArbSettings:
    buy_basket_max_cost_usd: Decimal


@dataclass(frozen=True)
class LoggingSettings:
    level: str
    opportunities_log_path: Path
    status_path: Path


@dataclass(frozen=True)
class ExecutionSettings:
    enabled: bool                          # explicit arm switch, default false — see execution/executor.py
    demo_base_url: str
    demo_api_key_id: Optional[str]
    demo_private_key_path: Optional[str]
    orders_log_path: Path
    circuit_breaker_max_mismatches: int


@dataclass(frozen=True)
class Settings:
    kalshi: KalshiSettings
    scanner: ScannerSettings
    arb: ArbSettings
    logging: LoggingSettings
    execution: ExecutionSettings


def load_settings(path: Path = DEFAULT_SETTINGS_PATH) -> Settings:
    load_dotenv()  # populates os.environ from .env if present; no-op otherwise
    raw = yaml.safe_load(path.read_text())

    project_root = path.resolve().parent.parent

    kalshi_raw = raw["kalshi"]
    scanner_raw = raw["scanner"]
    arb_raw = raw["arb"]
    logging_raw = raw["logging"]
    execution_raw = raw["execution"]

    def _resolve(rel_or_abs: str) -> Path:
        p = Path(rel_or_abs)
        return p if p.is_absolute() else project_root / p

    opportunities_log_path = _resolve(logging_raw["opportunities_log_path"])
    status_path = _resolve(logging_raw["status_path"])
    orders_log_path = _resolve(execution_raw["orders_log_path"])

    return Settings(
        kalshi=KalshiSettings(
            base_url=kalshi_raw["base_url"],
            ws_url=kalshi_raw["ws_url"],
            request_timeout_s=float(kalshi_raw["request_timeout_s"]),
            max_retries=int(kalshi_raw["max_retries"]),
            api_key_id=os.environ.get("KALSHI_API_KEY_ID") or None,
            private_key_path=os.environ.get("KALSHI_PRIVATE_KEY_PATH") or None,
        ),
        scanner=ScannerSettings(
            min_outcome_legs=int(scanner_raw["min_outcome_legs"]),
            max_outcome_legs=int(scanner_raw["max_outcome_legs"]),
            min_depth_contracts=Decimal(str(scanner_raw["min_depth_contracts"])),
            rescan_interval_s=int(scanner_raw["rescan_interval_s"]),
            orderbook_depth=int(scanner_raw["orderbook_depth"]),
            max_ws_legs=int(scanner_raw["max_ws_legs"]),
        ),
        arb=ArbSettings(
            buy_basket_max_cost_usd=Decimal(str(arb_raw["buy_basket_max_cost_usd"])),
        ),
        logging=LoggingSettings(
            level=logging_raw["level"],
            opportunities_log_path=opportunities_log_path,
            status_path=status_path,
        ),
        execution=ExecutionSettings(
            enabled=bool(execution_raw["enabled"]),
            demo_base_url=execution_raw["demo_base_url"],
            demo_api_key_id=os.environ.get("KALSHI_DEMO_API_KEY_ID") or None,
            demo_private_key_path=os.environ.get("KALSHI_DEMO_PRIVATE_KEY_PATH") or None,
            orders_log_path=orders_log_path,
            circuit_breaker_max_mismatches=int(execution_raw["circuit_breaker_max_mismatches"]),
        ),
    )
