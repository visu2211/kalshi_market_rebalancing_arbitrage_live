"""Loads config/settings.yaml (strategy/risk parameters, safe to commit)
and .env (KALSHI_API_KEY_ID/KALSHI_PRIVATE_KEY_PATH for prod WS auth, and
KALSHI_DEMO_API_KEY_ID/KALSHI_DEMO_PRIVATE_KEY_PATH for demo-account order
placement — never committed) into one Settings object every other module
reads from.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Mapping, Optional
from urllib.parse import urlparse

import yaml
from dotenv import load_dotenv

DEFAULT_SETTINGS_PATH = Path(__file__).resolve().parent / "settings.yaml"


class ConfigurationError(ValueError):
    """Raised when configuration is unsafe, missing, or malformed."""


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
    max_quote_age_ms: int


@dataclass(frozen=True)
class ArbSettings:
    buy_basket_max_cost_usd: Decimal
    buy_all_no_min_edge_usd: Decimal   # minimum required (N-1 - cost) edge for a buy_all_no basket — see scanner/basket.py's compute_no_opportunity. Default 0.02, symmetric with 1 - buy_basket_max_cost_usd


@dataclass(frozen=True)
class RelationshipsSettings:
    trust_auto_detected: bool                      # default True — auto-detected (mutually_exclusive+MECNET) events keep trading exactly as before
    manual_config_path: Optional[Path]              # config/relationships.yaml — safe to commit, no secrets
    verified_event_tickers: frozenset[str]          # loaded from manual_config_path; empty when unset, so scanner.relationships.build_relationships is a no-op wrapper by default


@dataclass(frozen=True)
class LoggingSettings:
    level: str
    opportunities_log_path: Path
    status_path: Path


@dataclass(frozen=True)
class ExecutionSettings:
    enabled: bool                          # explicit arm switch, default false — see execution/executor.py
    target: str                            # "demo" (paper money) or "prod" (REAL MONEY — see execution/executor.py)
    demo_base_url: str
    demo_api_key_id: Optional[str]
    demo_private_key_path: Optional[str]
    orders_log_path: Path
    risk_state_path: Path
    circuit_breaker_max_mismatches: int
    max_order_size_contracts: Decimal      # caps every leg's requested size regardless of the opportunity's computed max_size — the deliberate ramp-up lever for real money
    allow_no_direction: bool               # default False — gates BasketOpportunity.direction=="buy_all_no" out of execute_opportunity entirely; economically the same trade as the deleted sell_basket direction, see execution/executor.py and scanner/basket.py's compute_no_opportunity
    prod_enabled: bool
    require_rest_confirmation: bool    # re-read every leg's book from Kalshi before submitting
    prod_unconfirmed_orders_acknowledged: bool   # explicit opt-in to run prod WITHOUT that re-read
    max_gross_exposure_usd: Decimal
    max_event_exposure_usd: Decimal
    max_daily_notional_usd: Decimal
    max_daily_loss_usd: Decimal
    max_open_orders: int
    max_orders_per_minute: int
    max_baskets_per_session: int
    max_baskets_per_day: int
    event_cooldown_s: int
    max_unwind_loss_usd: Decimal
    min_edge_stability_ticks: int          # default 1 (no filter) — require the WS edge to persist this many consecutive ticks on the same event before it is executable. Filters flicker edges without adding a REST roundtrip.
    min_seconds_to_event_close: int        # default 0 (no filter) — reject candidates whose nearest leg close_time is within this many seconds. Prevents submitting into a market that already settled, where unwind cannot fill (Nov 4 incident).
    process_lock_path: Path
    alerts_log_path: Path
    alert_webhook_url: Optional[str]
    telegram_bot_token: Optional[str]
    telegram_chat_id: Optional[str]


@dataclass(frozen=True)
class Settings:
    kalshi: KalshiSettings
    scanner: ScannerSettings
    arb: ArbSettings
    relationships: RelationshipsSettings
    logging: LoggingSettings
    execution: ExecutionSettings


def load_settings(path: Path = DEFAULT_SETTINGS_PATH) -> Settings:
    load_dotenv()  # populates os.environ from .env if present; no-op otherwise
    raw = yaml.safe_load(path.read_text())
    if not isinstance(raw, Mapping):
        raise ConfigurationError("settings file must contain a YAML mapping")

    def _section(name: str) -> Mapping[str, Any]:
        value = raw.get(name)
        if not isinstance(value, Mapping):
            raise ConfigurationError(f"{name} must be a mapping")
        return value

    def _optional_section(name: str) -> Mapping[str, Any]:
        """Like _section, but a missing key defaults to {} instead of
        raising — for new, additive sections that must not break a
        settings.yaml written before they existed."""
        value = raw.get(name)
        if value is None:
            return {}
        if not isinstance(value, Mapping):
            raise ConfigurationError(f"{name} must be a mapping")
        return value

    def _required(section: Mapping[str, Any], key: str, prefix: str) -> Any:
        if key not in section:
            raise ConfigurationError(f"missing required setting: {prefix}.{key}")
        return section[key]

    def _bool(value: Any, name: str) -> bool:
        if type(value) is not bool:
            raise ConfigurationError(f"{name} must be a YAML boolean (true or false, without quotes)")
        return value

    def _int(value: Any, name: str, *, minimum: int = 0) -> int:
        if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
            raise ConfigurationError(f"{name} must be an integer >= {minimum}")
        return value

    def _decimal(value: Any, name: str, *, minimum: str = "0", maximum: str | None = None,
                 strictly_positive: bool = False) -> Decimal:
        try:
            result = Decimal(str(value))
        except (InvalidOperation, ValueError):
            raise ConfigurationError(f"{name} must be a decimal number") from None
        lower = Decimal(minimum)
        if not result.is_finite() or result < lower or (strictly_positive and result == 0):
            op = ">" if strictly_positive else ">="
            raise ConfigurationError(f"{name} must be finite and {op} {minimum}")
        if maximum is not None and result > Decimal(maximum):
            raise ConfigurationError(f"{name} must be <= {maximum}")
        return result

    def _url(value: Any, name: str, schemes: set[str]) -> str:
        if not isinstance(value, str):
            raise ConfigurationError(f"{name} must be a URL string")
        parsed = urlparse(value)
        if parsed.scheme not in schemes or not parsed.netloc or parsed.query or parsed.fragment:
            raise ConfigurationError(f"{name} must be an absolute {sorted(schemes)} URL without query/fragment")
        return value.rstrip("/")

    project_root = path.resolve().parent.parent

    kalshi_raw = _section("kalshi")
    scanner_raw = _section("scanner")
    arb_raw = _section("arb")
    relationships_raw = _optional_section("relationships")
    logging_raw = _section("logging")
    execution_raw = _section("execution")

    target = _required(execution_raw, "target", "execution")
    if target not in {"demo", "prod"}:
        raise ConfigurationError("execution.target must be exactly 'demo' or 'prod'")
    enabled = _bool(_required(execution_raw, "enabled", "execution"), "execution.enabled")
    prod_enabled = _bool(execution_raw.get("prod_enabled", False), "execution.prod_enabled")
    # Default False — same "ship disabled, ramp up manually" posture as
    # max_order_size_contracts. buy_all_no is economically the same trade as
    # the deleted sell_basket direction; see execution/executor.py.
    allow_no_direction = _bool(execution_raw.get("allow_no_direction", False), "execution.allow_no_direction")
    # Defaults to True: bypassing it is an explicit, deliberate opt-out, and it
    # is refused outright for production below.
    require_rest_confirmation = _bool(
        execution_raw.get("require_rest_confirmation", True), "execution.require_rest_confirmation"
    )
    # Separate, distinctly-named acknowledgement so prod can never lose the
    # confirmation step as a side effect of toggling one boolean.
    prod_unconfirmed_ack = _bool(
        execution_raw.get("prod_unconfirmed_orders_acknowledged", False),
        "execution.prod_unconfirmed_orders_acknowledged",
    )
    min_legs = _int(_required(scanner_raw, "min_outcome_legs", "scanner"), "scanner.min_outcome_legs", minimum=2)
    max_legs = _int(_required(scanner_raw, "max_outcome_legs", "scanner"), "scanner.max_outcome_legs", minimum=2)
    if min_legs > max_legs:
        raise ConfigurationError("scanner.min_outcome_legs must be <= scanner.max_outcome_legs")

    api_key_id = os.environ.get("KALSHI_API_KEY_ID") or None
    private_key_path = os.environ.get("KALSHI_PRIVATE_KEY_PATH") or None
    demo_api_key_id = os.environ.get("KALSHI_DEMO_API_KEY_ID") or None
    demo_private_key_path = os.environ.get("KALSHI_DEMO_PRIVATE_KEY_PATH") or None
    if enabled and target == "prod":
        if not require_rest_confirmation and not prod_unconfirmed_ack:
            # Real money normally never submits on locally-reconstructed prices.
            # Overriding it takes a second, differently-named flag on purpose:
            # one toggle should not be able to remove the only check that reads
            # the exchange's own book.
            raise ConfigurationError(
                "production execution without execution.require_rest_confirmation also requires "
                "execution.prod_unconfirmed_orders_acknowledged: true — orders would be priced off "
                "the local WS book alone, which measured 7-16c away from Kalshi's own book"
            )
        if not prod_enabled:
            raise ConfigurationError("production execution requires execution.prod_enabled: true")
        if os.environ.get("KALSHI_PROD_TRADING_ENABLED") != "true":
            raise ConfigurationError("production execution requires KALSHI_PROD_TRADING_ENABLED=true")
        expected_key = os.environ.get("KALSHI_EXPECTED_PROD_API_KEY_ID")
        if not expected_key or expected_key != api_key_id:
            raise ConfigurationError(
                "KALSHI_EXPECTED_PROD_API_KEY_ID must exactly match KALSHI_API_KEY_ID for production execution"
            )
        if not api_key_id or not private_key_path:
            raise ConfigurationError("production execution requires production API credentials")
    if enabled and target == "demo" and (not demo_api_key_id or not demo_private_key_path):
        raise ConfigurationError("demo execution requires demo API credentials")

    def _resolve(rel_or_abs: str) -> Path:
        p = Path(rel_or_abs)
        return p if p.is_absolute() else project_root / p

    trust_auto_detected = _bool(
        relationships_raw.get("trust_auto_detected", True), "relationships.trust_auto_detected"
    )
    manual_config_path_raw = relationships_raw.get("manual_config_path")
    manual_config_path = _resolve(manual_config_path_raw) if manual_config_path_raw else None
    verified_event_tickers: frozenset[str] = frozenset()
    if manual_config_path is not None:
        # Fail closed: a configured-but-missing/malformed manual relationship
        # file must not silently degrade to "no manual verification" — that
        # would look identical to trust_auto_detected's own default and hide
        # a real config mistake.
        if not manual_config_path.is_file():
            raise ConfigurationError(f"relationships.manual_config_path not found: {manual_config_path}")
        manual_raw = yaml.safe_load(manual_config_path.read_text()) or {}
        if not isinstance(manual_raw, Mapping):
            raise ConfigurationError(f"{manual_config_path} must contain a YAML mapping")
        tickers = manual_raw.get("verified_event_tickers", [])
        if not isinstance(tickers, list) or not all(isinstance(t, str) and t for t in tickers):
            raise ConfigurationError(
                f"{manual_config_path}: verified_event_tickers must be a list of non-empty strings"
            )
        verified_event_tickers = frozenset(tickers)

    opportunities_log_path = _resolve(logging_raw["opportunities_log_path"])
    status_path = _resolve(logging_raw["status_path"])
    orders_log_path = _resolve(execution_raw["orders_log_path"])
    risk_state_path = _resolve(_required(execution_raw, "risk_state_path", "execution"))
    process_lock_path = _resolve(_required(execution_raw, "process_lock_path", "execution"))
    alerts_log_path = _resolve(_required(execution_raw, "alerts_log_path", "execution"))
    alert_webhook_url = os.environ.get("KALSHI_ALERT_WEBHOOK_URL") or None
    if alert_webhook_url is not None:
        alert_webhook_url = _url(alert_webhook_url, "KALSHI_ALERT_WEBHOOK_URL", {"https"})
    telegram_bot_token = os.environ.get("TELEGRAM_BOT_TOKEN") or None
    telegram_chat_id = os.environ.get("TELEGRAM_CHAT_ID") or None
    if telegram_bot_token and not re.fullmatch(r"\d+:[A-Za-z0-9_-]+", telegram_bot_token):
        raise ConfigurationError("TELEGRAM_BOT_TOKEN has an invalid format")
    if telegram_chat_id and not re.fullmatch(r"-?\d+", telegram_chat_id):
        raise ConfigurationError("TELEGRAM_CHAT_ID must be an integer chat identifier")
    if bool(telegram_bot_token) != bool(telegram_chat_id):
        raise ConfigurationError("TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID must be configured together")
    if enabled and target == "prod" and not (alert_webhook_url or telegram_bot_token):
        raise ConfigurationError(
            "production execution requires Telegram credentials or KALSHI_ALERT_WEBHOOK_URL for incident notification"
        )

    return Settings(
        kalshi=KalshiSettings(
            base_url=_url(_required(kalshi_raw, "base_url", "kalshi"), "kalshi.base_url", {"https"}),
            ws_url=_url(_required(kalshi_raw, "ws_url", "kalshi"), "kalshi.ws_url", {"wss"}),
            request_timeout_s=float(_decimal(_required(kalshi_raw, "request_timeout_s", "kalshi"), "kalshi.request_timeout_s", strictly_positive=True)),
            max_retries=_int(_required(kalshi_raw, "max_retries", "kalshi"), "kalshi.max_retries", minimum=0),
            api_key_id=api_key_id,
            private_key_path=private_key_path,
        ),
        scanner=ScannerSettings(
            min_outcome_legs=min_legs,
            max_outcome_legs=max_legs,
            min_depth_contracts=_decimal(_required(scanner_raw, "min_depth_contracts", "scanner"), "scanner.min_depth_contracts", strictly_positive=True),
            rescan_interval_s=_int(_required(scanner_raw, "rescan_interval_s", "scanner"), "scanner.rescan_interval_s", minimum=1),
            orderbook_depth=_int(_required(scanner_raw, "orderbook_depth", "scanner"), "scanner.orderbook_depth", minimum=1),
            max_ws_legs=_int(_required(scanner_raw, "max_ws_legs", "scanner"), "scanner.max_ws_legs", minimum=1),
            max_quote_age_ms=_int(_required(scanner_raw, "max_quote_age_ms", "scanner"), "scanner.max_quote_age_ms", minimum=1),
        ),
        arb=ArbSettings(
            buy_basket_max_cost_usd=_decimal(_required(arb_raw, "buy_basket_max_cost_usd", "arb"), "arb.buy_basket_max_cost_usd", strictly_positive=True, maximum="1"),
            buy_all_no_min_edge_usd=_decimal(arb_raw.get("buy_all_no_min_edge_usd", "0.02"), "arb.buy_all_no_min_edge_usd", strictly_positive=True),
        ),
        relationships=RelationshipsSettings(
            trust_auto_detected=trust_auto_detected,
            manual_config_path=manual_config_path,
            verified_event_tickers=verified_event_tickers,
        ),
        logging=LoggingSettings(
            level=str(_required(logging_raw, "level", "logging")).upper(),
            opportunities_log_path=opportunities_log_path,
            status_path=status_path,
        ),
        execution=ExecutionSettings(
            enabled=enabled,
            target=target,
            demo_base_url=_url(_required(execution_raw, "demo_base_url", "execution"), "execution.demo_base_url", {"https"}),
            demo_api_key_id=demo_api_key_id,
            demo_private_key_path=demo_private_key_path,
            orders_log_path=orders_log_path,
            risk_state_path=risk_state_path,
            circuit_breaker_max_mismatches=_int(_required(execution_raw, "circuit_breaker_max_mismatches", "execution"), "execution.circuit_breaker_max_mismatches", minimum=1),
            max_order_size_contracts=_decimal(_required(execution_raw, "max_order_size_contracts", "execution"), "execution.max_order_size_contracts", strictly_positive=True),
            allow_no_direction=allow_no_direction,
            prod_enabled=prod_enabled,
            require_rest_confirmation=require_rest_confirmation,
            prod_unconfirmed_orders_acknowledged=prod_unconfirmed_ack,
            max_gross_exposure_usd=_decimal(_required(execution_raw, "max_gross_exposure_usd", "execution"), "execution.max_gross_exposure_usd", strictly_positive=True),
            max_event_exposure_usd=_decimal(_required(execution_raw, "max_event_exposure_usd", "execution"), "execution.max_event_exposure_usd", strictly_positive=True),
            max_daily_notional_usd=_decimal(_required(execution_raw, "max_daily_notional_usd", "execution"), "execution.max_daily_notional_usd", strictly_positive=True),
            max_daily_loss_usd=_decimal(_required(execution_raw, "max_daily_loss_usd", "execution"), "execution.max_daily_loss_usd", strictly_positive=True),
            max_open_orders=_int(_required(execution_raw, "max_open_orders", "execution"), "execution.max_open_orders", minimum=1),
            max_orders_per_minute=_int(_required(execution_raw, "max_orders_per_minute", "execution"), "execution.max_orders_per_minute", minimum=1),
            max_baskets_per_session=_int(_required(execution_raw, "max_baskets_per_session", "execution"), "execution.max_baskets_per_session", minimum=1),
            max_baskets_per_day=_int(_required(execution_raw, "max_baskets_per_day", "execution"), "execution.max_baskets_per_day", minimum=1),
            event_cooldown_s=_int(_required(execution_raw, "event_cooldown_s", "execution"), "execution.event_cooldown_s", minimum=1),
            max_unwind_loss_usd=_decimal(_required(execution_raw, "max_unwind_loss_usd", "execution"), "execution.max_unwind_loss_usd", strictly_positive=True, maximum="1"),
            min_edge_stability_ticks=_int(execution_raw.get("min_edge_stability_ticks", 1), "execution.min_edge_stability_ticks", minimum=1),
            min_seconds_to_event_close=_int(execution_raw.get("min_seconds_to_event_close", 0), "execution.min_seconds_to_event_close", minimum=0),
            process_lock_path=process_lock_path,
            alerts_log_path=alerts_log_path,
            alert_webhook_url=alert_webhook_url,
            telegram_bot_token=telegram_bot_token,
            telegram_chat_id=telegram_chat_id,
        ),
    )
