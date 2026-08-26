"""stream.monitor.LocalBook / handle_message — synthetic WS messages shaped
like Kalshi's live orderbook_snapshot/orderbook_delta payloads (field names
and delta-is-relative semantics confirmed live 2026-08-26, see
LocalBook's docstring and kalshi/ws_client.py's docstring). No network."""

import json
from decimal import Decimal

from execution.executor import AtomicExecutionResult, LegExecutionResult
from kalshi.client import ExchangeError, OrderAction
from stream.monitor import (
    _BALANCE_CACHE_TTL_S,
    LocalBook,
    _BalanceCache,
    _CircuitBreaker,
    _reconcile_positions,
    _update_believed_positions,
    _write_scanner_status,
    handle_message,
)


class _FakeBalanceClient:
    def __init__(self, values):
        self.values = list(values)
        self.calls = 0

    async def get_balance(self):
        value = self.values[self.calls]
        self.calls += 1
        return value


def test_apply_snapshot_then_best_yes_quote():
    book = LocalBook()
    book.apply_snapshot(
        yes_levels=[(Decimal("0.40"), Decimal("100")), (Decimal("0.35"), Decimal("50"))],
        no_levels=[(Decimal("0.55"), Decimal("80")), (Decimal("0.50"), Decimal("20"))],
    )
    best_ask, ask_size, best_bid, bid_size = book.best_yes_quote()
    assert best_ask == Decimal("0.45")   # 1 - best NO bid (0.55)
    assert ask_size == Decimal("80")
    assert best_bid == Decimal("0.40")   # best YES bid
    assert bid_size == Decimal("100")


def test_best_yes_quote_none_when_one_side_empty():
    book = LocalBook()
    book.apply_snapshot(yes_levels=[(Decimal("0.40"), Decimal("100"))], no_levels=[])
    assert book.best_yes_quote() is None


def test_apply_delta_is_relative_not_absolute():
    """The real bug: delta_fp is a signed change to apply to the existing
    level, not the new absolute size — the original code treated it as
    absolute (book[price] = size)."""
    book = LocalBook()
    book.apply_snapshot(yes_levels=[(Decimal("0.40"), Decimal("100"))], no_levels=[(Decimal("0.55"), Decimal("50"))])

    book.apply_delta("yes", Decimal("0.40"), Decimal("25"))   # +25
    assert book.yes_bids[Decimal("0.40")] == Decimal("125")

    book.apply_delta("yes", Decimal("0.40"), Decimal("-1000"))  # observed-live-style large negative delta
    assert Decimal("0.40") not in book.yes_bids  # nets to <= 0, level removed


def test_apply_delta_creates_new_level_from_zero():
    book = LocalBook()
    book.apply_delta("no", Decimal("0.62"), Decimal("30"))
    assert book.no_bids[Decimal("0.62")] == Decimal("30")


def test_handle_message_orderbook_snapshot_uses_dollars_fp_keys():
    books = {"TICK-A": LocalBook()}
    msg = {
        "type": "orderbook_snapshot",
        "msg": {
            "market_ticker": "TICK-A",
            "yes_dollars_fp": [["0.4000", "100.00"]],
            "no_dollars_fp": [["0.5500", "80.00"]],
        },
    }
    changed = handle_message(msg, books)
    assert changed == "TICK-A"
    assert books["TICK-A"].yes_bids[Decimal("0.4000")] == Decimal("100.00")
    assert books["TICK-A"].no_bids[Decimal("0.5500")] == Decimal("80.00")


def test_handle_message_orderbook_snapshot_missing_side_key_means_empty():
    """Observed live: a side with zero resting levels is omitted entirely,
    not sent as an empty list."""
    books = {"TICK-A": LocalBook()}
    msg = {"type": "orderbook_snapshot", "msg": {"market_ticker": "TICK-A", "no_dollars_fp": [["0.5500", "80.00"]]}}
    handle_message(msg, books)
    assert books["TICK-A"].yes_bids == {}
    assert books["TICK-A"].no_bids[Decimal("0.5500")] == Decimal("80.00")


def test_handle_message_orderbook_delta_applies_relative_change():
    books = {"TICK-A": LocalBook()}
    books["TICK-A"].apply_snapshot(yes_levels=[(Decimal("0.40"), Decimal("100"))], no_levels=[(Decimal("0.55"), Decimal("50"))])

    msg = {
        "type": "orderbook_delta",
        "msg": {"market_ticker": "TICK-A", "side": "yes", "price_dollars": "0.4000", "delta_fp": "-999.00"},
    }
    changed = handle_message(msg, books)
    assert changed == "TICK-A"
    assert Decimal("0.4000") not in books["TICK-A"].yes_bids


def test_handle_message_orderbook_delta_missing_price_field_is_skipped_not_raised():
    """Regression test for the actual bug: the original parser read a
    nonexistent `price` field and would raise decimal.InvalidOperation on
    Decimal(str(None)) — the very first real delta message would have
    crashed the whole WS loop. Now it logs and skips instead."""
    books = {"TICK-A": LocalBook()}
    msg = {"type": "orderbook_delta", "msg": {"market_ticker": "TICK-A", "side": "yes", "delta_fp": "10.00"}}
    changed = handle_message(msg, books)
    assert changed is None
    assert books["TICK-A"].yes_bids == {}


def test_handle_message_orderbook_delta_missing_delta_field_is_skipped_not_raised():
    books = {"TICK-A": LocalBook()}
    msg = {"type": "orderbook_delta", "msg": {"market_ticker": "TICK-A", "side": "yes", "price_dollars": "0.40"}}
    changed = handle_message(msg, books)
    assert changed is None


def test_handle_message_orderbook_delta_unknown_side_is_skipped():
    books = {"TICK-A": LocalBook()}
    msg = {
        "type": "orderbook_delta",
        "msg": {"market_ticker": "TICK-A", "side": "maybe", "price_dollars": "0.40", "delta_fp": "10"},
    }
    assert handle_message(msg, books) is None


def test_handle_message_control_messages_return_none():
    books: dict = {}
    assert handle_message({"type": "subscribed", "msg": {}}, books) is None
    assert handle_message({"type": "error", "msg": {}}, books) is None


def test_handle_message_ignores_untracked_ticker():
    books = {"TICK-A": LocalBook()}
    msg = {"type": "orderbook_delta", "msg": {"market_ticker": "TICK-B", "side": "yes", "price_dollars": "0.40", "delta_fp": "10"}}
    assert handle_message(msg, books) is None


def test_write_scanner_status_writes_readable_json(tmp_path):
    path = tmp_path / "scanner_status.json"
    _write_scanner_status(path, phase="streaming", ws_shortlist_events=5, watched_events=[{"event_ticker": "A"}])

    data = json.loads(path.read_text())
    assert data["phase"] == "streaming"
    assert data["ws_shortlist_events"] == 5
    assert data["watched_events"] == [{"event_ticker": "A"}]
    assert "updated_at" in data


def test_write_scanner_status_overwrites_previous_content(tmp_path):
    path = tmp_path / "scanner_status.json"
    _write_scanner_status(path, phase="idle_no_candidates")
    _write_scanner_status(path, phase="streaming", ws_shortlist_events=3)

    data = json.loads(path.read_text())
    assert data["phase"] == "streaming"
    assert data["ws_shortlist_events"] == 3


def test_write_scanner_status_serializes_decimal_volume(tmp_path):
    path = tmp_path / "scanner_status.json"
    _write_scanner_status(path, watched_events=[{"event_ticker": "A", "volume_24h": Decimal("1234.56")}])

    data = json.loads(path.read_text())
    assert data["watched_events"][0]["volume_24h"] == "1234.56"


async def test_balance_cache_reuses_value_within_ttl(monkeypatch):
    cache = _BalanceCache()
    client = _FakeBalanceClient([Decimal("100"), Decimal("200")])
    clock = [1000.0]
    monkeypatch.setattr("stream.monitor.time.monotonic", lambda: clock[0])

    assert await cache.get(client) == Decimal("100")
    clock[0] += _BALANCE_CACHE_TTL_S - 0.5   # still within TTL
    assert await cache.get(client) == Decimal("100")
    assert client.calls == 1   # second call served from cache, no second get_balance()


async def test_balance_cache_refetches_after_ttl_expires(monkeypatch):
    cache = _BalanceCache()
    client = _FakeBalanceClient([Decimal("100"), Decimal("200")])
    clock = [1000.0]
    monkeypatch.setattr("stream.monitor.time.monotonic", lambda: clock[0])

    assert await cache.get(client) == Decimal("100")
    clock[0] += _BALANCE_CACHE_TTL_S + 0.5   # past TTL
    assert await cache.get(client) == Decimal("200")
    assert client.calls == 2


def test_circuit_breaker_trips_after_max_consecutive_mismatches():
    breaker = _CircuitBreaker(max_mismatches=2)
    assert breaker.tripped is False

    breaker.record(mismatched=True)
    assert breaker.tripped is False   # only 1 so far

    breaker.record(mismatched=True)
    assert breaker.tripped is True
    assert breaker.tripped_reason is not None


def test_circuit_breaker_resets_streak_on_a_matched_execution():
    breaker = _CircuitBreaker(max_mismatches=2)
    breaker.record(mismatched=True)
    breaker.record(mismatched=False)   # a clean fill resets the streak
    breaker.record(mismatched=True)
    assert breaker.tripped is False   # only 1 consecutive since the reset


def test_circuit_breaker_stays_tripped_once_tripped():
    breaker = _CircuitBreaker(max_mismatches=1)
    breaker.record(mismatched=True)
    assert breaker.tripped is True
    reason_at_trip = breaker.tripped_reason

    breaker.record(mismatched=False)   # doesn't un-trip it
    assert breaker.tripped is True
    assert breaker.tripped_reason == reason_at_trip


def test_circuit_breaker_never_trips_below_the_threshold():
    breaker = _CircuitBreaker(max_mismatches=3)
    for _ in range(2):
        breaker.record(mismatched=True)
    assert breaker.tripped is False


def test_circuit_breaker_force_trip_bypasses_the_counter():
    breaker = _CircuitBreaker(max_mismatches=10)   # would never trip via record() alone in this test
    breaker.force_trip("confirmed-failed unwind")
    assert breaker.tripped is True
    assert breaker.tripped_reason == "confirmed-failed unwind"


def test_circuit_breaker_force_trip_is_idempotent():
    breaker = _CircuitBreaker(max_mismatches=10)
    breaker.force_trip("first reason")
    breaker.force_trip("second reason")   # shouldn't overwrite the original
    assert breaker.tripped_reason == "first reason"


# ---- _BalanceCache.reserve() ----

async def test_balance_cache_reserve_reduces_available_within_the_same_window(monkeypatch):
    cache = _BalanceCache()
    client = _FakeBalanceClient([Decimal("100")])
    clock = [1000.0]
    monkeypatch.setattr("stream.monitor.time.monotonic", lambda: clock[0])

    assert await cache.get(client) == Decimal("100")
    cache.reserve(Decimal("30"))
    clock[0] += 1   # still within TTL
    assert await cache.get(client) == Decimal("70")   # 100 - 30 reserved, no new fetch


async def test_balance_cache_reservation_clears_on_refresh(monkeypatch):
    cache = _BalanceCache()
    client = _FakeBalanceClient([Decimal("100"), Decimal("50")])
    clock = [1000.0]
    monkeypatch.setattr("stream.monitor.time.monotonic", lambda: clock[0])

    await cache.get(client)
    cache.reserve(Decimal("30"))
    clock[0] += _BALANCE_CACHE_TTL_S + 0.5   # past TTL — triggers a fresh fetch
    # the fresh fetch (50) already reflects prior spending, so the stale
    # 30 reservation from before the refresh must not be double-subtracted
    assert await cache.get(client) == Decimal("50")


# ---- position reconciliation ----

def _leg_result(ticker: str, filled: str) -> LegExecutionResult:
    from kalshi.client import OrderResult, OrderStatus
    return LegExecutionResult(
        ticker=ticker, action=OrderAction.BUY, limit_price=Decimal("0.50"), size=Decimal(filled),
        order_result=OrderResult(
            order_id=f"o-{ticker}", client_order_id="c", status=OrderStatus.FILLED,
            filled_size=Decimal(filled), remaining_size=Decimal(0), average_fill_price=None,
        ),
    )


def test_update_believed_positions_adds_buy_fills():
    believed: dict[str, Decimal] = {}
    result = AtomicExecutionResult(leg_results=[_leg_result("A", "45"), _leg_result("B", "45")])
    _update_believed_positions(believed, result)
    assert believed == {"A": Decimal("45"), "B": Decimal("45")}


def test_update_believed_positions_subtracts_unwind_fills():
    believed = {"A": Decimal("45")}
    result = AtomicExecutionResult(leg_results=[], unwind_results=[_leg_result("A", "20")], mismatched=True)
    _update_believed_positions(believed, result)
    assert believed == {"A": Decimal("25")}


def test_update_believed_positions_accumulates_across_multiple_calls():
    believed: dict[str, Decimal] = {}
    _update_believed_positions(believed, AtomicExecutionResult(leg_results=[_leg_result("A", "10")]))
    _update_believed_positions(believed, AtomicExecutionResult(leg_results=[_leg_result("A", "5")]))
    assert believed == {"A": Decimal("15")}


class _FakePositionsClient:
    def __init__(self, positions: dict[str, Decimal]):
        self._positions = positions

    async def get_positions(self):
        return self._positions


async def test_reconcile_positions_no_mismatches_when_everything_matches():
    believed = {"A": Decimal("45"), "B": Decimal("10")}
    client = _FakePositionsClient({"A": Decimal("45"), "B": Decimal("10")})
    assert await _reconcile_positions(client, believed) == {}


async def test_reconcile_positions_detects_a_mismatch():
    believed = {"A": Decimal("45")}
    client = _FakePositionsClient({"A": Decimal("30")})   # Kalshi says 30, we think 45
    mismatches = await _reconcile_positions(client, believed)
    assert mismatches == {"A": (Decimal("45"), Decimal("30"))}


async def test_reconcile_positions_detects_a_position_we_didnt_know_about():
    believed: dict[str, Decimal] = {}
    client = _FakePositionsClient({"A": Decimal("10")})   # Kalshi shows a position we have no record of
    mismatches = await _reconcile_positions(client, believed)
    assert mismatches == {"A": (Decimal("0"), Decimal("10"))}


async def test_reconcile_positions_returns_empty_on_check_failure_rather_than_raising():
    class _FailingClient:
        async def get_positions(self):
            raise ExchangeError("network blip")

    assert await _reconcile_positions(_FailingClient(), {"A": Decimal("45")}) == {}
