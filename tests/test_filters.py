"""scanner.filters.filter_events — synthetic event payloads shaped like
Kalshi's live GET /events?with_nested_markets=true response (field names
confirmed live 2026-08-25/26, see filters.py's docstring). No network.

_live variant: the time bound is "expected to settle by midnight UTC
tonight," read from expected_expiration_time (falling back to close_time)
— not a configurable N-day horizon. See filters.py's module docstring for
why: close_time/expiration_time is Kalshi's outer administrative deadline
(accounting for e.g. a sports match being postponed up to two weeks), not
when the event is actually expected to resolve."""

from datetime import datetime, timezone
from decimal import Decimal

from scanner.filters import EventCandidate, filter_events, has_recent_activity, select_by_volume

NOW = datetime(2026, 8, 25, 12, 0, 0, tzinfo=timezone.utc)


def _market(
    ticker,
    status="active",
    expected_expiration_time="2026-08-25T23:00:00Z",
    close_time=None,
    volume_24h_fp="10.00",
):
    market = {"ticker": ticker, "status": status, "volume_24h_fp": volume_24h_fp}
    if expected_expiration_time is not None:
        market["expected_expiration_time"] = expected_expiration_time
    if close_time is not None:
        market["close_time"] = close_time
    return market


def _event(event_ticker, mutually_exclusive, markets, title="Some event"):
    return {"event_ticker": event_ticker, "title": title, "mutually_exclusive": mutually_exclusive, "markets": markets}


def test_keeps_mutually_exclusive_event_settling_later_today():
    events = [_event("KXFED-26SEP", True, [_market("A"), _market("B"), _market("C")])]
    candidates = filter_events(events, min_outcome_legs=2, max_outcome_legs=6, now=NOW)

    assert len(candidates) == 1
    assert candidates[0].event_ticker == "KXFED-26SEP"
    assert candidates[0].leg_tickers == ["A", "B", "C"]
    assert candidates[0].leg_volume_24h["A"] == Decimal("10.00")


def test_drops_non_mutually_exclusive_event():
    events = [_event("KXNOTME-1", False, [_market("A"), _market("B")])]
    assert filter_events(events, 2, 6, now=NOW) == []


def test_drops_event_below_min_legs():
    events = [_event("KXONE-1", True, [_market("A")])]
    assert filter_events(events, 2, 6, now=NOW) == []


def test_drops_event_above_max_legs():
    events = [_event("KXMANY-1", True, [_market(f"L{i}") for i in range(7)])]
    assert filter_events(events, 2, 6, now=NOW) == []


def test_finalized_legs_excluded_from_count_and_output():
    events = [_event("KXPARTIAL-1", True, [
        _market("A", status="active"),
        _market("B", status="active"),
        _market("C", status="finalized"),  # already resolved, shouldn't count as an open leg
    ])]
    candidates = filter_events(events, min_outcome_legs=2, max_outcome_legs=6, now=NOW)

    assert len(candidates) == 1
    assert candidates[0].leg_tickers == ["A", "B"]


def test_drops_event_settling_tomorrow():
    events = [_event("KXTOMORROW-1", True, [
        _market("A", expected_expiration_time="2026-08-26T00:30:00Z"),
        _market("B", expected_expiration_time="2026-08-26T00:30:00Z"),
    ])]
    assert filter_events(events, 2, 6, now=NOW) == []


def test_keeps_event_settling_at_the_last_instant_of_today():
    events = [_event("KXLASTCALL-1", True, [
        _market("A", expected_expiration_time="2026-08-25T23:59:59Z"),
        _market("B", expected_expiration_time="2026-08-25T23:59:59Z"),
    ])]
    candidates = filter_events(events, 2, 6, now=NOW)
    assert len(candidates) == 1


def test_expected_settlement_time_is_max_across_open_legs():
    events = [_event("KXFED-26SEP", True, [
        _market("A", expected_expiration_time="2026-08-25T18:00:00Z"),
        _market("B", expected_expiration_time="2026-08-25T22:00:00Z"),
    ])]
    candidates = filter_events(events, 2, 6, now=NOW)

    assert candidates[0].expected_settlement_time == datetime(2026, 8, 25, 22, 0, tzinfo=timezone.utc)


def test_keeps_event_settling_today_despite_far_out_close_time():
    """Regression test for the real KXATPCHALLENGERMATCH-26AUG26MENWEH case
    (verified live 2026-08-26): a same-day sports match whose close_time/
    expiration_time is weeks out (Kalshi's postponement-safety deadline)
    but whose expected_expiration_time is today. Reading close_time here
    was silently dropping every such match."""
    events = [_event("KXATPCHALLENGERMATCH-26AUG26MENWEH", True, [
        _market("MEN", expected_expiration_time="2026-08-25T13:50:00Z", close_time="2026-09-09T10:50:00Z"),
        _market("WEH", expected_expiration_time="2026-08-25T13:50:00Z", close_time="2026-09-09T10:50:00Z"),
    ])]
    candidates = filter_events(events, 2, 6, now=NOW)

    assert len(candidates) == 1
    assert candidates[0].expected_settlement_time == datetime(2026, 8, 25, 13, 50, tzinfo=timezone.utc)


def test_falls_back_to_close_time_when_expected_expiration_time_missing():
    events = [_event("KXNOEXPECTED-1", True, [
        _market("A", expected_expiration_time=None, close_time="2026-08-25T20:00:00Z"),
        _market("B", expected_expiration_time=None, close_time="2026-08-25T20:00:00Z"),
    ])]
    candidates = filter_events(events, 2, 6, now=NOW)

    assert len(candidates) == 1
    assert candidates[0].expected_settlement_time == datetime(2026, 8, 25, 20, 0, tzinfo=timezone.utc)


def test_drops_genuinely_multi_day_event_even_via_expected_expiration_time():
    """Regression test for the KXTESTMATCH-26AUG230030INDSRI case (verified
    live 2026-08-26): a multi-day test cricket match where
    expected_expiration_time itself is days out, not just close_time — this
    one should stay dropped, since it genuinely won't settle today."""
    events = [_event("KXTESTMATCH-26AUG230030INDSRI", True, [
        _market("IND", expected_expiration_time="2026-08-28T04:30:00Z", close_time="2026-08-30T04:30:00Z"),
        _market("SRI", expected_expiration_time="2026-08-28T04:30:00Z", close_time="2026-08-30T04:30:00Z"),
        _market("TIE", expected_expiration_time="2026-08-28T04:30:00Z", close_time="2026-08-30T04:30:00Z"),
    ])]
    assert filter_events(events, 2, 6, now=NOW) == []


def _candidate(event_ticker, leg_volumes: dict) -> EventCandidate:
    """leg_volumes: ticker -> volume (or None for "no data"). Constructs an
    EventCandidate directly, bypassing filter_events, for has_recent_activity
    and select_by_volume unit tests that don't care about the time/structural
    filtering above."""
    return EventCandidate(
        event_ticker=event_ticker,
        title=event_ticker,
        leg_tickers=list(leg_volumes.keys()),
        leg_volume_24h={t: (Decimal(str(v)) if v is not None else None) for t, v in leg_volumes.items()},
        expected_settlement_time=NOW,
    )


def test_has_recent_activity_true_when_every_leg_has_volume():
    c = _candidate("EVT-1", {"A": 100, "B": 50})
    assert has_recent_activity(c) is True


def test_has_recent_activity_false_when_any_leg_has_zero_volume():
    c = _candidate("EVT-1", {"A": 100, "B": 0})
    assert has_recent_activity(c) is False


def test_has_recent_activity_false_when_any_leg_has_missing_volume():
    c = _candidate("EVT-1", {"A": 100, "B": None})
    assert has_recent_activity(c) is False


def test_select_by_volume_ranks_highest_total_volume_first():
    low = _candidate("LOW", {"A": 10, "B": 10})
    high = _candidate("HIGH", {"A": 1000, "B": 1000})
    mid = _candidate("MID", {"A": 100, "B": 100})

    selected = select_by_volume([low, high, mid], max_legs=100)
    assert [c.event_ticker for c in selected] == ["HIGH", "MID", "LOW"]


def test_select_by_volume_respects_leg_budget_never_splitting_an_event():
    # 3-leg + 3-leg + 3-leg = 9 legs total; budget only fits two of them.
    a = _candidate("A", {"A1": 300, "A2": 300, "A3": 300})  # highest volume, 3 legs
    b = _candidate("B", {"B1": 200, "B2": 200, "B3": 200})  # mid volume, 3 legs
    c = _candidate("C", {"C1": 100, "C2": 100, "C3": 100})  # lowest volume, 3 legs

    selected = select_by_volume([a, b, c], max_legs=6)
    assert {s.event_ticker for s in selected} == {"A", "B"}
    assert sum(len(s.leg_tickers) for s in selected) == 6


def test_select_by_volume_backfills_smaller_event_that_still_fits():
    # A big 6-leg event exhausts most of the budget; a smaller low-volume
    # event that still fits in the remainder should be included rather than
    # leaving the budget partially unused.
    big = _candidate("BIG", {f"L{i}": 1000 for i in range(6)})       # 6 legs, highest volume
    small = _candidate("SMALL", {"A": 1, "B": 1})                     # 2 legs, lowest volume — but fits

    selected = select_by_volume([big, small], max_legs=8)
    assert {s.event_ticker for s in selected} == {"BIG", "SMALL"}


def test_select_by_volume_ties_broken_deterministically_by_ticker():
    a = _candidate("ZEBRA", {"A": 50})
    b = _candidate("ALPHA", {"A": 50})
    selected = select_by_volume([a, b], max_legs=10)
    assert [c.event_ticker for c in selected] == ["ALPHA", "ZEBRA"]
