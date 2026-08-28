"""execution.capital — pure ranking/allocation math, no network."""

from datetime import datetime, timedelta, timezone
from decimal import Decimal

from execution.capital import (
    ExecutionCandidate,
    estimated_capital_required,
    hourly_return_rate,
    hours_to_settlement,
    is_settlement_overdue,
    select_executable,
)
from scanner.basket import BasketOpportunity

NOW = datetime(2026, 8, 26, 12, 0, 0, tzinfo=timezone.utc)


def _opportunity(event_ticker, basket_price, edge, max_size, legs=("A", "B")) -> BasketOpportunity:
    return BasketOpportunity(
        event_ticker=event_ticker,
        direction="buy_basket",
        legs=list(legs),
        leg_quotes=[],
        basket_price_usd=Decimal(str(basket_price)),
        edge_usd=Decimal(str(edge)),
        max_size=Decimal(str(max_size)),
    )


def _candidate(event_ticker, basket_price, edge, max_size, hours_out) -> ExecutionCandidate:
    return ExecutionCandidate(
        event_ticker=event_ticker,
        opportunity=_opportunity(event_ticker, basket_price, edge, max_size),
        expected_settlement_time=NOW + timedelta(hours=hours_out),
    )


def test_hours_to_settlement_basic():
    later = NOW + timedelta(hours=3)
    assert hours_to_settlement(NOW, later) == Decimal("3")


def test_hours_to_settlement_floors_at_min_for_imminent_or_past_settlement():
    assert hours_to_settlement(NOW, NOW) == Decimal("0.01")
    assert hours_to_settlement(NOW, NOW - timedelta(hours=1)) == Decimal("0.01")


def test_estimated_capital_required_is_price_times_size():
    opp = _opportunity("EVT", "0.93", "0.07", "39")
    assert estimated_capital_required(opp) == Decimal("36.27")


def test_hourly_return_rate_favors_faster_settlement_at_equal_edge():
    opp = _opportunity("EVT", "0.93", "0.07", "39")
    fast = hourly_return_rate(opp, hours=Decimal("1"))
    slow = hourly_return_rate(opp, hours=Decimal("10"))
    assert fast > slow
    assert fast == slow * 10


def test_select_executable_picks_all_when_capital_is_plenty():
    candidates = [_candidate("A", "0.93", "0.07", "10", hours_out=2), _candidate("B", "0.95", "0.05", "10", hours_out=2)]
    selected = select_executable(candidates, available_capital=Decimal("1000"), now=NOW)
    assert {c.event_ticker for c in selected} == {"A", "B"}


def test_select_executable_prefers_faster_settling_over_bigger_raw_edge():
    # BIG has a bigger dollar edge (0.07 vs 0.03) but settles in 20h; FAST
    # is smaller but settles in 1h — per-hour return should favor FAST.
    big = _candidate("BIG", "0.93", "0.07", "10", hours_out=20)   # 0.07/0.93/20 = 0.00376/hr
    fast = _candidate("FAST", "0.97", "0.03", "10", hours_out=1)  # 0.03/0.97/1  = 0.0309/hr

    # Capital only covers one of them: BIG costs 9.30, FAST costs 9.70.
    selected = select_executable([big, fast], available_capital=Decimal("9.70"), now=NOW)
    assert [c.event_ticker for c in selected] == ["FAST"]


def test_select_executable_skips_unaffordable_but_backfills_cheaper_lower_ranked():
    expensive_best = _candidate("EXPENSIVE", "0.90", "0.10", "1000", hours_out=1)   # huge cost, best rate
    cheap_worse = _candidate("CHEAP", "0.99", "0.01", "5", hours_out=5)             # tiny cost, worse rate

    selected = select_executable([expensive_best, cheap_worse], available_capital=Decimal("10"), now=NOW)
    assert [c.event_ticker for c in selected] == ["CHEAP"]


def test_select_executable_returns_empty_when_nothing_fits():
    candidates = [_candidate("A", "0.93", "0.07", "1000", hours_out=1)]
    assert select_executable(candidates, available_capital=Decimal("1"), now=NOW) == []


def test_select_executable_spends_within_budget_not_over():
    a = _candidate("A", "1.00", "0.10", "10", hours_out=1)   # costs 10.00
    b = _candidate("B", "1.00", "0.09", "10", hours_out=1)   # costs 10.00, ranked below A
    selected = select_executable([a, b], available_capital=Decimal("15"), now=NOW)
    # only A fits alone in the 15 budget (10 spent, 5 left isn't enough for B's 10)
    assert [c.event_ticker for c in selected] == ["A"]


# --- overdue-settlement demotion (added 2026-08-28) --------------------------
# hours_to_settlement floors a NEGATIVE remaining time at _MIN_HOURS, so an
# event that expired hours ago scored as the fastest possible capital turnover.
# Under max_baskets_per_day=1 that ordering is the whole decision.


def test_overdue_event_no_longer_outranks_a_better_live_edge():
    """Both shapes were really detected 2026-08-28 minutes apart: the overdue
    basket had a 5c edge, the live one 8c, and the overdue one won by 130x."""
    overdue = _candidate("OVERDUE", 0.95, 0.05, 1, hours_out=-20)
    live = _candidate("LIVE", 0.92, 0.08, 1, hours_out=2.4)
    order = [c.event_ticker for c in select_executable([overdue, live], Decimal(100), NOW)]
    assert order == ["LIVE", "OVERDUE"]


def test_overdue_events_are_demoted_not_excluded():
    """A complete basket still pays $1 whenever settlement lands, so an overdue
    candidate stays eligible when it is the only one on offer."""
    overdue = _candidate("OVERDUE", 0.95, 0.05, 1, hours_out=-20)
    assert [c.event_ticker for c in select_executable([overdue], Decimal(100), NOW)] == ["OVERDUE"]


def test_overdue_candidates_still_rank_against_each_other_by_return():
    weak = _candidate("WEAK", 0.97, 0.03, 1, hours_out=-20)
    strong = _candidate("STRONG", 0.90, 0.10, 1, hours_out=-5)
    order = [c.event_ticker for c in select_executable([weak, strong], Decimal(100), NOW)]
    assert order == ["STRONG", "WEAK"]


def test_live_candidates_still_rank_by_capital_velocity():
    """Original behaviour must survive: at equal edge, sooner settlement wins."""
    slow = _candidate("SLOW", 0.95, 0.05, 1, hours_out=8)
    fast = _candidate("FAST", 0.95, 0.05, 1, hours_out=1)
    order = [c.event_ticker for c in select_executable([slow, fast], Decimal(100), NOW)]
    assert order == ["FAST", "SLOW"]


def test_settlement_exactly_now_counts_as_overdue():
    assert is_settlement_overdue(_candidate("X", 0.95, 0.05, 1, hours_out=0), NOW) is True
    assert is_settlement_overdue(_candidate("Y", 0.95, 0.05, 1, hours_out=0.001), NOW) is False
