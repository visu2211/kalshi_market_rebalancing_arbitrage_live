"""stream.monitor.LocalBook / handle_message — synthetic WS messages shaped
like Kalshi's live orderbook_snapshot/orderbook_delta payloads (field names
and delta-is-relative semantics confirmed live 2026-08-26, see
LocalBook's docstring and kalshi/ws_client.py's docstring). No network."""

import json
from datetime import datetime, timezone
from decimal import Decimal
from copy import deepcopy
from types import SimpleNamespace

import pytest

from execution.executor import AtomicExecutionResult, LegExecutionResult
from kalshi.client import ExchangeError, OrderAction, Orderbook, OrderbookLevel, RateLimitError, Side
from kalshi.ws_client import WS_RECONNECTED_MESSAGE_TYPE
from scanner.basket import BasketOpportunity, LegQuote
from execution.risk import RiskState
from execution.store import OrderStore
from scanner.relationships import Relationship
from stream.monitor import (
    _BALANCE_CACHE_TTL_S,
    run_stage3,
    LocalBook,
    _BalanceCache,
    _cap_opportunity_size,
    _CircuitBreaker,
    _confirm_opportunity_via_rest,
    _reconcile_positions,
    _realized_unwind_loss,
    _reprice_event,
    _unmatched_entry_exposure,
    _update_believed_positions,
    _write_scanner_status,
    handle_message,
)


def _stage1_settings(tmp_path):
    """Real Settings via the loader, pointed at a tmp dir — run_stage3 reads
    risk_state_path/status_path/rescan_interval_s, not just a couple of fields."""
    import yaml
    from config.loader import load_settings
    from tests.test_config_loader import BASE_CONFIG

    config = deepcopy(BASE_CONFIG)
    config["scanner"]["rescan_interval_s"] = 60
    config["logging"]["status_path"] = str(tmp_path / "status.json")
    config["logging"]["opportunities_log_path"] = str(tmp_path / "opportunities.jsonl")
    config["execution"]["risk_state_path"] = str(tmp_path / "risk_state.json")
    config["execution"]["orders_log_path"] = str(tmp_path / "orders.jsonl")
    config["execution"]["alerts_log_path"] = str(tmp_path / "alerts.jsonl")
    config["execution"]["process_lock_path"] = str(tmp_path / "execution.lock")
    path = tmp_path / "settings.yaml"
    path.write_text(yaml.safe_dump(config))
    return load_settings(path)


async def _noop_sleep(seconds):
    return None


def _confirm_settings(max_order_size="1", buy_max="0.98", min_depth="25"):
    """_confirm_opportunity_via_rest only reads these three values; a stub
    keeps the test from restating the whole Settings tree."""
    return SimpleNamespace(
        arb=SimpleNamespace(buy_basket_max_cost_usd=Decimal(buy_max)),
        scanner=SimpleNamespace(min_depth_contracts=Decimal(min_depth)),
        execution=SimpleNamespace(max_order_size_contracts=Decimal(max_order_size)),
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


# --- depth-aware accessors (2026 merge — read path only, storage unchanged) --


def _depth_book():
    book = LocalBook()
    book.apply_snapshot(
        yes_levels=[(Decimal("0.40"), Decimal("100")), (Decimal("0.38"), Decimal("50")), (Decimal("0.35"), Decimal("20"))],
        no_levels=[(Decimal("0.55"), Decimal("80")), (Decimal("0.58"), Decimal("30")), (Decimal("0.60"), Decimal("10"))],
    )
    return book


def test_yes_ask_levels_walks_no_bids_ascending_price():
    book = _depth_book()
    levels = book.yes_ask_levels(depth=3)
    # no_bids 0.55/0.58/0.60 -> synthesized YES asks 0.45/0.42/0.40, cheapest first.
    assert [l.price for l in levels] == [Decimal("0.40"), Decimal("0.42"), Decimal("0.45")]
    assert [l.size for l in levels] == [Decimal("10"), Decimal("30"), Decimal("80")]


def test_yes_bid_levels_walks_yes_bids_descending_price():
    book = _depth_book()
    levels = book.yes_bid_levels(depth=3)
    assert [l.price for l in levels] == [Decimal("0.40"), Decimal("0.38"), Decimal("0.35")]
    assert [l.size for l in levels] == [Decimal("100"), Decimal("50"), Decimal("20")]


def test_yes_ask_levels_respects_depth_limit():
    book = _depth_book()
    assert len(book.yes_ask_levels(depth=1)) == 1
    assert len(book.yes_ask_levels(depth=2)) == 2
    assert len(book.yes_ask_levels(depth=100)) == 3   # never more than what's actually resting


def test_yes_ask_levels_empty_when_no_bids_empty():
    book = LocalBook()
    book.apply_snapshot(yes_levels=[(Decimal("0.40"), Decimal("100"))], no_levels=[])
    assert book.yes_ask_levels(depth=5) == []


def test_yes_ask_and_bid_levels_at_depth_one_match_best_yes_quote():
    """The acceptance gate for depth-aware pricing: at depth=1 the new
    accessors must return exactly the same top-of-book price/size
    best_yes_quote already returns, so scanner.basket.compute_opportunity_depth_aware
    can reproduce compute_opportunity's output byte-for-byte at depth=1."""
    book = _depth_book()
    best_ask, ask_size, best_bid, bid_size = book.best_yes_quote()

    ask_levels = book.yes_ask_levels(depth=1)
    bid_levels = book.yes_bid_levels(depth=1)
    assert (ask_levels[0].price, ask_levels[0].size) == (best_ask, ask_size)
    assert (bid_levels[0].price, bid_levels[0].size) == (best_bid, bid_size)


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


def test_apply_delta_before_snapshot_is_ignored():
    book = LocalBook()
    book.apply_delta("no", Decimal("0.62"), Decimal("30"))
    assert book.no_bids == {}


def test_book_freshness_expires(monkeypatch):
    clock = [100.0]
    monkeypatch.setattr("stream.monitor.time.monotonic", lambda: clock[0])
    book = LocalBook()
    book.apply_snapshot([(Decimal("0.40"), Decimal("1"))], [(Decimal("0.50"), Decimal("1"))])
    assert book.is_fresh(1.0)
    clock[0] += 1.1
    assert not book.is_fresh(1.0)


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


def test_realized_unwind_loss_uses_actual_prices_and_fees():
    buy = _leg_result("A", "2")
    buy.limit_price = Decimal("0.60")
    buy.order_result.average_fill_price = Decimal("0.59")
    buy.order_result.average_fee_paid = Decimal("0.01")
    sell = _leg_result("A", "2")
    sell.action = OrderAction.SELL
    sell.order_result.average_fill_price = Decimal("0.50")
    sell.order_result.average_fee_paid = Decimal("0.01")
    result = AtomicExecutionResult(leg_results=[buy], unwind_results=[sell], mismatched=True)
    assert _realized_unwind_loss(result) == Decimal("0.22")


def test_unmatched_entry_exposure_counts_only_unsold_excess():
    buy_a = _leg_result("A", "3")
    buy_b = _leg_result("B", "1")
    sell_a = _leg_result("A", "1")
    sell_a.action = OrderAction.SELL
    result = AtomicExecutionResult(leg_results=[buy_a, buy_b], unwind_results=[sell_a], mismatched=True, unwind_confirmed=False)
    assert _unmatched_entry_exposure(result) == Decimal("0.50")


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


async def test_reconcile_positions_ignores_unrelated_account_position():
    believed: dict[str, Decimal] = {}
    client = _FakePositionsClient({"A": Decimal("10")})   # Kalshi shows a position we have no record of
    mismatches = await _reconcile_positions(client, believed)
    assert mismatches == {}


async def test_reconcile_positions_returns_empty_on_check_failure_rather_than_raising():
    class _FailingClient:
        async def get_positions(self):
            raise ExchangeError("network blip")

    assert await _reconcile_positions(_FailingClient(), {"A": Decimal("45")}) == {}


# ---- _cap_opportunity_size ----
# execution.max_order_size_contracts is the deliberate ramp-up lever for
# real money (2026-08-26) — these confirm it actually constrains what
# gets executed, and that everything else about the opportunity (price,
# legs, edge) is untouched.

def _opportunity(max_size: str) -> BasketOpportunity:
    legs = [LegQuote(ticker="A", best_ask=Decimal("0.40"), ask_size=Decimal(max_size), best_bid=Decimal("0.38"), bid_size=Decimal(max_size))]
    return BasketOpportunity(
        event_ticker="EVT", direction="buy_basket", legs=["A"], leg_quotes=legs,
        basket_price_usd=Decimal("0.96"), edge_usd=Decimal("0.04"), max_size=Decimal(max_size),
    )


def test_cap_opportunity_size_reduces_a_larger_max_size():
    capped = _cap_opportunity_size(_opportunity("900"), max_order_size_contracts=Decimal("1"))
    assert capped.max_size == Decimal("1")


def test_cap_opportunity_size_leaves_a_smaller_max_size_untouched():
    opp = _opportunity("1")
    capped = _cap_opportunity_size(opp, max_order_size_contracts=Decimal("5"))
    assert capped.max_size == Decimal("1")
    assert capped is opp   # unchanged input returned as-is, not a needless copy


def test_cap_opportunity_size_does_not_mutate_other_fields():
    capped = _cap_opportunity_size(_opportunity("900"), max_order_size_contracts=Decimal("1"))
    assert capped.event_ticker == "EVT"
    assert capped.basket_price_usd == Decimal("0.96")
    assert capped.edge_usd == Decimal("0.04")
    assert capped.legs == ["A"]


def test_cap_opportunity_size_exact_match_returns_same_size():
    capped = _cap_opportunity_size(_opportunity("1"), max_order_size_contracts=Decimal("1"))
    assert capped.max_size == Decimal("1")


# --- reconnect book invalidation (added 2026-08-27) -------------------------
# LocalBook.invalidate + kalshi.ws_client's WS_RECONNECTED_MESSAGE_TYPE.
# snapshot_received used to survive a reconnect, so a delta arriving before
# that ticker's replacement snapshot mutated the previous connection's book.


def test_invalidate_clears_every_level_and_unsets_snapshot_received():
    book = LocalBook()
    book.apply_snapshot(yes_levels=[(Decimal("0.51"), Decimal("800"))],
                        no_levels=[(Decimal("0.48"), Decimal("900"))])
    book.invalidate()
    assert book.yes_bids == {}
    assert book.no_bids == {}
    assert book.snapshot_received is False
    assert book.best_yes_quote() is None
    assert book.is_fresh(1.0) is False


def test_delta_after_invalidate_cannot_resurrect_a_stale_book():
    """The regression: without invalidate(), this delta lands on the previous
    connection's levels and — because apply_delta refreshes the timestamp —
    the resulting garbage quote also passes the is_fresh staleness gate."""
    book = LocalBook()
    book.apply_snapshot(yes_levels=[(Decimal("0.51"), Decimal("800"))],
                        no_levels=[(Decimal("0.48"), Decimal("900"))])
    assert book.best_yes_quote()[0] == Decimal("0.52")   # sane pre-reconnect ask

    book.invalidate()
    book.apply_delta("no", Decimal("0.70"), Decimal("5000"))   # would synthesize a 0.30 ask

    assert book.no_bids == {}
    assert book.best_yes_quote() is None
    assert book.is_fresh(1.0) is False


def test_reconnect_sentinel_is_not_mistaken_for_a_book_message():
    books = {"T": LocalBook()}
    assert handle_message({"type": WS_RECONNECTED_MESSAGE_TYPE}, books) is None
    assert books["T"].snapshot_received is False


# --- REST confirmation before submission (added 2026-08-27) -----------------
# _confirm_opportunity_via_rest re-reads Kalshi's own book for every leg
# immediately before submission, so an edge that exists only in local WS state
# cannot reach the exchange.


class _FakeBookClient:
    """get_orderbook stand-in returning canned top-of-book per ticker.
    `error_tickers` raise ExchangeError instead."""

    def __init__(self, quotes, error_tickers=frozenset()):
        self.quotes = quotes          # ticker -> (ask, ask_size, bid, bid_size)
        self.error_tickers = error_tickers
        self.calls = []

    async def get_orderbook(self, market_id, side=None, depth=None):
        self.calls.append(market_id)
        if market_id in self.error_tickers:
            raise ExchangeError(f"boom {market_id}")
        ask, ask_size, bid, bid_size = self.quotes[market_id]
        return Orderbook(
            market_id=market_id, side=Side.YES,
            bids=[OrderbookLevel(price=Decimal(bid), size=Decimal(bid_size))],
            asks=[OrderbookLevel(price=Decimal(ask), size=Decimal(ask_size))],
            timestamp=datetime.now(timezone.utc),
        )


def _ws_opportunity(basket_price="0.74", legs=(("A", "0.30", 5000, "0.51", 800),
                                               ("B", "0.44", 100, "0.43", 100))):
    quotes = [LegQuote(ticker=t, best_ask=Decimal(a), ask_size=Decimal(asz),
                       best_bid=Decimal(b), bid_size=Decimal(bsz)) for t, a, asz, b, bsz in legs]
    return BasketOpportunity(
        event_ticker="KXTEST", direction="buy_basket", legs=[q.ticker for q in quotes],
        leg_quotes=quotes, basket_price_usd=Decimal(basket_price),
        edge_usd=Decimal(1) - Decimal(basket_price), max_size=Decimal("100"),
    )


@pytest.mark.asyncio
async def test_rest_confirmation_rejects_a_phantom_ws_edge():
    """WS says the basket costs $0.74; Kalshi's own book says $1.01."""
    settings = _confirm_settings()
    client = _FakeBookClient({"A": ("0.52", "5000", "0.51", "800"),
                              "B": ("0.49", "100", "0.48", "100")})
    assert await _confirm_opportunity_via_rest(client, _ws_opportunity(), settings) is None
    assert client.calls == ["A", "B"]


@pytest.mark.asyncio
async def test_rest_confirmation_returns_capped_opportunity_priced_off_rest_quotes():
    settings = _confirm_settings()
    client = _FakeBookClient({"A": ("0.04", "5000", "0.03", "800"),
                              "B": ("0.90", "100", "0.89", "100")})
    confirmed = await _confirm_opportunity_via_rest(client, _ws_opportunity(), settings)

    assert confirmed is not None
    assert [q.best_ask for q in confirmed.leg_quotes] == [Decimal("0.04"), Decimal("0.90")]
    assert confirmed.max_size == Decimal("1")   # capped to max_order_size_contracts
    assert confirmed.basket_price_usd == Decimal("0.96")   # 0.94 + 2c of size-1 fees


@pytest.mark.asyncio
async def test_rest_confirmation_returns_none_when_a_leg_fetch_fails():
    settings = _confirm_settings()
    client = _FakeBookClient({"A": ("0.04", "5000", "0.03", "800"),
                              "B": ("0.90", "100", "0.89", "100")},
                             error_tickers=frozenset({"B"}))
    assert await _confirm_opportunity_via_rest(client, _ws_opportunity(), settings) is None


@pytest.mark.asyncio
async def test_rest_confirmation_rejects_an_incoherent_rest_book():
    settings = _confirm_settings()
    client = _FakeBookClient({"A": ("0.04", "5000", "0.30", "800"),   # bid > ask
                              "B": ("0.90", "100", "0.89", "100")})
    assert await _confirm_opportunity_via_rest(client, _ws_opportunity(), settings) is None


# --- Stage 1 resilience (added 2026-08-27) ----------------------------------
# run_stage3's Stage 1 REST pass was unguarded: data/scanner.log records a run
# ending on `RateLimitError: Kalshi rate limit hit on GET /events`. A detection
# loop has to outlive a transient exchange fault.


class _StopLoop(Exception):
    """Sentinel to break run_stage3's `while True` inside a test."""


class _FakeEventsClient:
    """get_all_events replays `outcomes` in order: an exception is raised, a
    list is returned. Exhausting the script raises _StopLoop to end the loop."""

    def __init__(self, outcomes):
        self.outcomes = list(outcomes)
        self.calls = 0

    @property
    def has_signing_credentials(self):
        return False   # keeps the loop out of the WS path; Stage 1 still runs

    async def get_all_events(self, status=None, with_nested_markets=False):
        self.calls += 1
        if not self.outcomes:
            raise _StopLoop
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


@pytest.mark.asyncio
async def test_stage1_rate_limit_does_not_end_the_loop(tmp_path, monkeypatch):
    settings = _stage1_settings(tmp_path)
    monkeypatch.setattr("stream.monitor.asyncio.sleep", _noop_sleep)
    client = _FakeEventsClient([
        RateLimitError("Kalshi rate limit hit on GET /events"),
        ExchangeError("Kalshi server error on GET /events: 502"),
        [],                      # recovers: a clean (empty) Stage 1 pass
    ])

    with pytest.raises(_StopLoop):
        await run_stage3(client, ws_client=None, settings=settings, execution_client=None)

    assert client.calls == 4   # 2 failures survived, 1 clean pass, then the sentinel
    status = json.loads(settings.logging.status_path.read_text())
    assert status["phase"] in ("idle_no_candidates", "idle_stage1_error")


@pytest.mark.asyncio
async def test_stage1_backoff_escalates_then_resets_after_a_clean_pass(tmp_path, monkeypatch):
    settings = _stage1_settings(tmp_path)
    slept: list[float] = []

    async def record_sleep(seconds):
        slept.append(seconds)

    monkeypatch.setattr("stream.monitor.asyncio.sleep", record_sleep)
    client = _FakeEventsClient([
        ExchangeError("boom 1"),
        ExchangeError("boom 2"),
        ExchangeError("boom 3"),
        [],                      # clean pass resets the backoff
        ExchangeError("boom 4"),
    ])

    with pytest.raises(_StopLoop):
        await run_stage3(client, ws_client=None, settings=settings, execution_client=None)

    # 5, 10, 20 escalating; then the empty-candidate cycle sleeps the rescan
    # interval; then the post-recovery failure starts over at the floor.
    assert slept[:3] == [5, 10, 20]
    assert slept[4] == 5


@pytest.mark.asyncio
async def test_stage1_backoff_is_capped_at_the_rescan_interval(tmp_path, monkeypatch):
    settings = _stage1_settings(tmp_path)
    slept: list[float] = []

    async def record_sleep(seconds):
        slept.append(seconds)

    monkeypatch.setattr("stream.monitor.asyncio.sleep", record_sleep)
    client = _FakeEventsClient([ExchangeError(f"boom {i}") for i in range(8)])

    with pytest.raises(_StopLoop):
        await run_stage3(client, ws_client=None, settings=settings, execution_client=None)

    assert max(slept) == settings.scanner.rescan_interval_s
    assert slept[-1] == settings.scanner.rescan_interval_s


def _fresh_book(ask=Decimal("0.25"), size=Decimal("100")):
    """A coherent, fully-populated, just-snapshotted book: best_yes_quote()
    returns (1-ask, size, ask-0.05, size) — i.e. best_ask == 1-ask."""
    book = LocalBook()
    book.apply_snapshot(
        yes_levels=[(ask - Decimal("0.05"), size)],
        no_levels=[(Decimal(1) - ask, size)],
    )
    return book


@pytest.mark.asyncio
async def test_reprice_event_dispatches_to_every_relationship_owning_the_ticker(tmp_path):
    """Regression test for the fan-out bug: event_by_ticker used to be a
    strict dict[str, EventCandidate] built via {t: c for c in shortlist for
    t in c.leg_tickers} — if a ticker belonged to two relationships, the
    second construction silently clobbered the first, and _reprice_event's
    single .get(ticker) lookup could only ever re-price one of them. Ticker
    "X" here belongs to both relationships; both must be re-priced off one
    book update, and neither's opportunity may be lost or overwritten by
    the other's."""
    settings = _stage1_settings(tmp_path)   # BASE_CONFIG: buy_basket_max_cost_usd=0.98, min_depth_contracts=1

    r1 = Relationship(
        event_ticker="EVT-R1", title="R1", leg_tickers=["X", "Y"],
        leg_volume_24h={}, expected_settlement_time=datetime.now(timezone.utc),
    )
    r2 = Relationship(
        event_ticker="EVT-R2", title="R2", leg_tickers=["X", "Z"],
        leg_volume_24h={}, expected_settlement_time=datetime.now(timezone.utc),
    )
    relationships_by_ticker = {"X": [r1, r2], "Y": [r1], "Z": [r2]}
    relationship_by_event_ticker = {r1.event_ticker: r1, r2.event_ticker: r2}

    # best_ask == 0.30 on every leg (well under the 0.98 threshold once
    # summed across 2 legs plus fees), coherent, fresh, ample depth.
    books = {"X": _fresh_book(), "Y": _fresh_book(), "Z": _fresh_book()}
    open_opportunities: dict = {}
    detection_diagnostics = {
        "fully_priced_evaluations": 0,
        "stale_book_rejections": 0,
        "incomplete_book_rejections": 0,
        "incoherent_book_rejections": 0,
        "insufficient_depth_rejections": 0,
        "price_threshold_rejections": 0,
        "closest_baskets": {},
    }

    await _reprice_event(
        "X", books, relationships_by_ticker, relationship_by_event_ticker, settings,
        open_opportunities, executed_opportunities=set(), execution_client=None, market_data_client=None,
        balance_cache=_BalanceCache(), circuit_breaker=_CircuitBreaker(max_mismatches=1),
        believed_positions={}, risk_state=RiskState.empty(datetime.now(timezone.utc)),
        detection_diagnostics=detection_diagnostics,
    )

    assert set(open_opportunities) == {"EVT-R1", "EVT-R2"}
    assert set(open_opportunities["EVT-R1"].legs) == {"X", "Y"}
    assert set(open_opportunities["EVT-R2"].legs) == {"X", "Z"}


@pytest.mark.asyncio
async def test_reprice_event_records_detected_opportunities_to_the_store_when_given(tmp_path):
    """Proves the Phase E wiring actually reaches OrderStore.record_opportunity
    from the live detection path (_reprice_event -> _reprice_relationship),
    not just that the two compile together — store=None (every test above
    this one) must remain unaffected."""
    settings = _stage1_settings(tmp_path)
    r1 = Relationship(
        event_ticker="EVT-R1", title="R1", leg_tickers=["X", "Y"],
        leg_volume_24h={}, expected_settlement_time=datetime.now(timezone.utc),
    )
    relationships_by_ticker = {"X": [r1], "Y": [r1]}
    relationship_by_event_ticker = {r1.event_ticker: r1}
    books = {"X": _fresh_book(), "Y": _fresh_book()}
    open_opportunities: dict = {}
    detection_diagnostics = {
        "fully_priced_evaluations": 0, "stale_book_rejections": 0, "incomplete_book_rejections": 0,
        "incoherent_book_rejections": 0, "insufficient_depth_rejections": 0, "price_threshold_rejections": 0,
        "closest_baskets": {},
    }
    store = OrderStore.open(tmp_path / "orders.db")

    await _reprice_event(
        "X", books, relationships_by_ticker, relationship_by_event_ticker, settings,
        open_opportunities, executed_opportunities=set(), execution_client=None, market_data_client=None,
        balance_cache=_BalanceCache(), circuit_breaker=_CircuitBreaker(max_mismatches=1),
        believed_positions={}, risk_state=RiskState.empty(datetime.now(timezone.utc)),
        detection_diagnostics=detection_diagnostics, store=store,
    )

    assert "EVT-R1" in open_opportunities
    recorded = store.get_opportunities("EVT-R1")
    assert len(recorded) == 1
    assert recorded[0].direction == "buy_basket"
    assert set(recorded[0].legs) == {"X", "Y"}


@pytest.mark.asyncio
async def test_edge_stability_counter_increments_each_tick_and_clears_on_close(tmp_path):
    """Each WS tick that keeps the edge open increments the counter; once
    the opportunity closes (edge dies), the counter is cleared so a new
    open starts from 1 again. _reconcile_execution gates on this value to
    filter one-tick flicker edges."""
    settings = _stage1_settings(tmp_path)
    r1 = Relationship(
        event_ticker="EVT-R1", title="R1", leg_tickers=["X", "Y"],
        leg_volume_24h={}, expected_settlement_time=datetime.now(timezone.utc),
    )
    relationships_by_ticker = {"X": [r1], "Y": [r1]}
    relationship_by_event_ticker = {r1.event_ticker: r1}
    books = {"X": _fresh_book(), "Y": _fresh_book()}
    open_opportunities: dict = {}
    detection_diagnostics = {
        "fully_priced_evaluations": 0, "stale_book_rejections": 0, "incomplete_book_rejections": 0,
        "incoherent_book_rejections": 0, "insufficient_depth_rejections": 0, "price_threshold_rejections": 0,
        "closest_baskets": {},
        "edge_stability_ticks": {"buy_basket": {}, "buy_all_no": {}},
    }

    async def rp():
        await _reprice_event(
            "X", books, relationships_by_ticker, relationship_by_event_ticker, settings,
            open_opportunities, executed_opportunities=set(), execution_client=None, market_data_client=None,
            balance_cache=_BalanceCache(), circuit_breaker=_CircuitBreaker(max_mismatches=1),
            believed_positions={}, risk_state=RiskState.empty(datetime.now(timezone.utc)),
            detection_diagnostics=detection_diagnostics,
        )

    await rp()
    await rp()
    await rp()
    assert detection_diagnostics["edge_stability_ticks"]["buy_basket"]["EVT-R1"] == 3

    # Collapse the book so no opportunity remains, then re-tick: counter clears.
    books["X"] = _fresh_book(ask=Decimal("0.99"))
    await rp()
    assert "EVT-R1" not in detection_diagnostics["edge_stability_ticks"]["buy_basket"]
    assert "EVT-R1" not in open_opportunities

    # Restore and re-open: counter starts at 1 again, not continues from 3.
    books["X"] = _fresh_book()
    await rp()
    assert detection_diagnostics["edge_stability_ticks"]["buy_basket"]["EVT-R1"] == 1
