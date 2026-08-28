from datetime import datetime, timezone
from decimal import Decimal

import pytest

from execution.risk import RiskState, RiskStateError


NOW = datetime(2026, 8, 26, tzinfo=timezone.utc)


def test_risk_state_round_trip(tmp_path):
    path = tmp_path / "risk.json"
    state = RiskState.empty(NOW)
    state.trip("stop")
    state.daily_notional_usd = Decimal("12.34")
    state.event_exposure_usd["E"] = Decimal("4.50")
    state.believed_positions["M"] = Decimal("1")
    state.persist(path)
    loaded = RiskState.load(path, NOW)
    assert loaded.tripped and loaded.tripped_reason == "stop"
    assert loaded.daily_notional_usd == Decimal("12.34")
    assert loaded.gross_exposure_usd == Decimal("4.50")
    assert loaded.believed_positions == {"M": Decimal("1")}


def test_corrupt_state_fails_closed(tmp_path):
    path = tmp_path / "risk.json"
    path.write_text("not json")
    with pytest.raises(RiskStateError):
        RiskState.load(path, NOW)


def test_day_roll_resets_daily_counters_but_not_exposure():
    state = RiskState.empty(NOW)
    state.daily_notional_usd = Decimal("10")
    state.daily_realized_loss_usd = Decimal("2")
    state.event_exposure_usd["E"] = Decimal("3")
    state.roll_day(datetime(2026, 8, 27, tzinfo=timezone.utc))
    assert state.daily_notional_usd == 0
    assert state.daily_realized_loss_usd == 0
    assert state.event_exposure_usd == {"E": Decimal("3")}
