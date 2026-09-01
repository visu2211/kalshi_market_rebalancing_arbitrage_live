from __future__ import annotations

from copy import deepcopy
from pathlib import Path

import pytest
import yaml

from config.loader import ConfigurationError, load_settings


BASE_CONFIG = {
    "kalshi": {
        "base_url": "https://api.example.test/v2",
        "ws_url": "wss://api.example.test/ws/v2",
        "request_timeout_s": 10,
        "max_retries": 3,
    },
    "scanner": {
        "min_outcome_legs": 2,
        "max_outcome_legs": 6,
        "min_depth_contracts": 1,
        "rescan_interval_s": 60,
        "orderbook_depth": 5,
        "max_ws_legs": 100,
        "max_quote_age_ms": 1000,
    },
    "arb": {"buy_basket_max_cost_usd": 0.98},
    "logging": {
        "level": "INFO",
        "opportunities_log_path": "data/opportunities.jsonl",
        "status_path": "data/status.json",
    },
    "execution": {
        "enabled": False,
        "target": "demo",
        "prod_enabled": False,
        "demo_base_url": "https://demo.example.test/v2",
        "orders_log_path": "data/orders.jsonl",
        "risk_state_path": "data/risk_state.json",
        "circuit_breaker_max_mismatches": 2,
        "max_order_size_contracts": 1,
        "max_gross_exposure_usd": 25,
        "max_event_exposure_usd": 5,
        "max_daily_notional_usd": 50,
        "max_daily_loss_usd": 5,
        "max_open_orders": 12,
        "max_orders_per_minute": 20,
        "max_baskets_per_session": 1,
        "max_baskets_per_day": 1,
        "event_cooldown_s": 86400,
        "max_unwind_loss_usd": 0.10,
        "process_lock_path": "data/execution.lock",
        "alerts_log_path": "data/alerts.jsonl",
    },
}


def _write(tmp_path: Path, config: dict) -> Path:
    path = tmp_path / "config" / "settings.yaml"
    path.parent.mkdir()
    path.write_text(yaml.safe_dump(config))
    return path


def test_rejects_quoted_boolean(tmp_path: Path) -> None:
    config = deepcopy(BASE_CONFIG)
    config["execution"]["enabled"] = "false"
    with pytest.raises(ConfigurationError, match="YAML boolean"):
        load_settings(_write(tmp_path, config))


def test_rejects_unknown_target(tmp_path: Path) -> None:
    config = deepcopy(BASE_CONFIG)
    config["execution"]["target"] = "production"
    with pytest.raises(ConfigurationError, match="exactly 'demo' or 'prod'"):
        load_settings(_write(tmp_path, config))


def test_rejects_invalid_ranges(tmp_path: Path) -> None:
    config = deepcopy(BASE_CONFIG)
    config["scanner"]["max_quote_age_ms"] = 0
    with pytest.raises(ConfigurationError, match="max_quote_age_ms"):
        load_settings(_write(tmp_path, config))


def test_prod_requires_all_independent_interlocks(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config = deepcopy(BASE_CONFIG)
    config["execution"].update(enabled=True, target="prod", prod_enabled=True)
    monkeypatch.setenv("KALSHI_API_KEY_ID", "expected-account")
    monkeypatch.setenv("KALSHI_PRIVATE_KEY_PATH", "/keys/prod.pem")
    monkeypatch.setenv("KALSHI_PROD_TRADING_ENABLED", "true")
    monkeypatch.setenv("KALSHI_EXPECTED_PROD_API_KEY_ID", "wrong-account")
    with pytest.raises(ConfigurationError, match="exactly match"):
        load_settings(_write(tmp_path, config))


def test_prod_loads_only_when_account_guard_matches(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config = deepcopy(BASE_CONFIG)
    config["execution"].update(enabled=True, target="prod", prod_enabled=True)
    monkeypatch.setenv("KALSHI_API_KEY_ID", "expected-account")
    monkeypatch.setenv("KALSHI_PRIVATE_KEY_PATH", "/keys/prod.pem")
    monkeypatch.setenv("KALSHI_PROD_TRADING_ENABLED", "true")
    monkeypatch.setenv("KALSHI_EXPECTED_PROD_API_KEY_ID", "expected-account")
    monkeypatch.setenv("KALSHI_ALERT_WEBHOOK_URL", "https://alerts.example.test/kalshi")
    settings = load_settings(_write(tmp_path, config))
    assert settings.execution.target == "prod"
    assert settings.execution.risk_state_path == tmp_path / "data/risk_state.json"


def test_telegram_credentials_must_be_configured_together(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "123456:valid-looking-token")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "")
    with pytest.raises(ConfigurationError, match="configured together"):
        load_settings(_write(tmp_path, deepcopy(BASE_CONFIG)))


def test_telegram_chat_id_must_be_numeric(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "123456:valid-looking-token")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "not-a-chat")
    with pytest.raises(ConfigurationError, match="integer chat identifier"):
        load_settings(_write(tmp_path, deepcopy(BASE_CONFIG)))


def test_telegram_credentials_load_without_exposing_values(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "123456:valid-looking-token")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "-100123456789")
    settings = load_settings(_write(tmp_path, deepcopy(BASE_CONFIG)))
    assert settings.execution.telegram_bot_token is not None
    assert settings.execution.telegram_chat_id == "-100123456789"


def test_prod_refuses_to_load_without_rest_confirmation(tmp_path: Path) -> None:
    """Real money must never be submitted on locally-reconstructed prices."""
    config = deepcopy(BASE_CONFIG)
    config["execution"].update({"enabled": True, "target": "prod", "prod_enabled": True,
                                "require_rest_confirmation": False})
    with pytest.raises(ConfigurationError, match="require_rest_confirmation"):
        load_settings(_write(tmp_path, config))


def test_demo_may_bypass_rest_confirmation(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("KALSHI_DEMO_API_KEY_ID", "demo-key")
    monkeypatch.setenv("KALSHI_DEMO_PRIVATE_KEY_PATH", "/tmp/demo.pem")
    config = deepcopy(BASE_CONFIG)
    config["execution"].update({"enabled": True, "target": "demo", "require_rest_confirmation": False})
    settings = load_settings(_write(tmp_path, config))
    assert settings.execution.require_rest_confirmation is False


def test_rest_confirmation_defaults_to_required_when_absent(tmp_path: Path) -> None:
    config = deepcopy(BASE_CONFIG)
    config["execution"].pop("require_rest_confirmation", None)
    assert load_settings(_write(tmp_path, config)).execution.require_rest_confirmation is True


def test_allow_no_direction_defaults_false(tmp_path: Path) -> None:
    """BASE_CONFIG has no allow_no_direction key — the same 'ship disabled
    by default' posture as max_order_size_contracts must hold even for a
    settings.yaml written before this flag existed."""
    settings = load_settings(_write(tmp_path, deepcopy(BASE_CONFIG)))
    assert settings.execution.allow_no_direction is False


def test_allow_no_direction_can_be_explicitly_enabled(tmp_path: Path) -> None:
    config = deepcopy(BASE_CONFIG)
    config["execution"]["allow_no_direction"] = True
    settings = load_settings(_write(tmp_path, config))
    assert settings.execution.allow_no_direction is True


def test_relationships_section_absent_defaults_to_trust_auto_detected(tmp_path: Path) -> None:
    """BASE_CONFIG has no `relationships` key at all — this is the exact
    shape of every settings.yaml written before this section existed, and
    must load with today's unchanged behavior rather than raising."""
    settings = load_settings(_write(tmp_path, deepcopy(BASE_CONFIG)))
    assert settings.relationships.trust_auto_detected is True
    assert settings.relationships.manual_config_path is None
    assert settings.relationships.verified_event_tickers == frozenset()


def test_relationships_manual_config_path_must_exist(tmp_path: Path) -> None:
    config = deepcopy(BASE_CONFIG)
    config["relationships"] = {"manual_config_path": "config/relationships.yaml"}
    with pytest.raises(ConfigurationError, match="manual_config_path not found"):
        load_settings(_write(tmp_path, config))


def test_relationships_manual_config_loads_verified_tickers(tmp_path: Path) -> None:
    config = deepcopy(BASE_CONFIG)
    config["relationships"] = {"trust_auto_detected": False, "manual_config_path": "config/relationships.yaml"}
    settings_path = _write(tmp_path, config)
    (settings_path.parent / "relationships.yaml").write_text(
        yaml.safe_dump({"verified_event_tickers": ["KXFED-1", "KXNBA-1"]})
    )
    settings = load_settings(settings_path)
    assert settings.relationships.trust_auto_detected is False
    assert settings.relationships.verified_event_tickers == frozenset({"KXFED-1", "KXNBA-1"})


def test_relationships_manual_config_rejects_non_string_tickers(tmp_path: Path) -> None:
    config = deepcopy(BASE_CONFIG)
    config["relationships"] = {"manual_config_path": "config/relationships.yaml"}
    settings_path = _write(tmp_path, config)
    (settings_path.parent / "relationships.yaml").write_text(
        yaml.safe_dump({"verified_event_tickers": ["KXFED-1", 123]})
    )
    with pytest.raises(ConfigurationError, match="verified_event_tickers"):
        load_settings(settings_path)
