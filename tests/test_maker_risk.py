"""execution.maker.risk — persistent state, no network."""

import json
from decimal import Decimal
from pathlib import Path

import pytest

from execution.maker.risk import MakerRiskState, MakerRiskStateError


def test_load_missing_returns_empty(tmp_path: Path):
    state = MakerRiskState.load(tmp_path / "nope.json", "maker-v1")
    assert state.strategy_id == "maker-v1"
    assert state.daily_basket_attempts == 0
    assert state.tripped is False


def test_save_then_load_roundtrip(tmp_path: Path):
    path = tmp_path / "state.json"
    state = MakerRiskState.empty("maker-v1")
    state.record_basket_attempt("EVT")
    state.trip("test incident")
    state.save(path)

    loaded = MakerRiskState.load(path, "maker-v1")
    assert loaded.daily_basket_attempts == 1
    assert "EVT" in loaded.attempted_events_at
    assert loaded.tripped is True
    assert loaded.tripped_reason == "test incident"


def test_wrong_strategy_id_refuses_load(tmp_path: Path):
    path = tmp_path / "state.json"
    MakerRiskState.empty("maker-v2").save(path)
    with pytest.raises(MakerRiskStateError, match="belongs to strategy_id"):
        MakerRiskState.load(path, "maker-v1")


def test_corrupt_file_raises(tmp_path: Path):
    path = tmp_path / "bad.json"
    path.write_text("not json {{{")
    with pytest.raises(MakerRiskStateError):
        MakerRiskState.load(path, "maker-v1")


def test_event_cooldown():
    state = MakerRiskState.empty("maker-v1")
    assert state.in_event_cooldown("EVT", 60) is False
    state.record_basket_attempt("EVT", now=None)
    # Just recorded -> still in cooldown.
    assert state.in_event_cooldown("EVT", 60) is True
    # Pretend plenty of time has passed.
    assert state.in_event_cooldown("EVT", 60, now=state.attempted_events_at["EVT"] + 61) is False


def test_trip_is_idempotent():
    state = MakerRiskState.empty("maker-v1")
    state.trip("first")
    state.trip("second")
    assert state.tripped_reason == "first"


def test_reset_breaker_clears_flag():
    state = MakerRiskState.empty("maker-v1")
    state.trip("boom")
    state.reset_breaker()
    assert state.tripped is False
    assert state.tripped_reason is None
