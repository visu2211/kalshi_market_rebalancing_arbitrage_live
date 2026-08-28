"""execution.executor — no network. A fake client stand-in records every
place_orders_batch()/cancel_order()/get_order() call, so these tests
verify order construction, batching, the unwind-on-mismatch safety logic,
and unwind confirmation without hitting Kalshi.

buy_basket only — sell_basket was removed 2026-08-26 (see
execution/executor.py's and scanner/basket.py's docstrings), so every
opportunity here is a BUY and every unwind is a SELL.

_live variant: also verifies the no-slippage IOC-style expiration_ts is
set on every leg, and — added 2026-08-26 after a real incident (see
execution/executor.py's module docstring) — that unevenly-filled legs
trigger an immediate offsetting unwind rather than being left as a naked
position, and that the unwind is polled to confirmation rather than just
fired and trusted."""

from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional

import pytest

from execution.executor import STRATEGY_CLIENT_ORDER_ID_PREFIX, UnresolvedSubmissionError, compute_unwind_legs, estimate_worst_case_unwind_loss, execute_opportunity
from kalshi.client import AmbiguousSubmissionError, ExchangeError, KalshiClient, OrderAction, OrderResult, OrderStatus, Orderbook, OrderbookLevel, Side, TimeInForce
from scanner.basket import LegQuote, compute_opportunity

BUY_MAX = Decimal("0.98")
MIN_DEPTH = Decimal("25")


def _leg(ticker, ask, ask_size, bid, bid_size) -> LegQuote:
    return LegQuote(ticker=ticker, best_ask=Decimal(ask), ask_size=Decimal(ask_size), best_bid=Decimal(bid), bid_size=Decimal(bid_size))


class FakeKalshiClient:
    """Records every place_orders_batch() call (one entry per batch, each
    a list of the OrderRequests submitted together), every cancel_order()
    call, and every get_order() call.

    `fills` maps ticker -> filled size for the FIRST batch (the original
    basket buy); `unwind_fills` maps ticker -> filled size for the SECOND
    batch (the unwind sell), if one happens — both default to a clean
    full fill. `reject_tickers` makes a ticker come back REJECTED with 0
    filled instead, on either batch. `confirm_after_polls` maps ticker ->
    how many get_order() polls before it reports fully filled (simulates
    a delayed fill); omitted means it never changes from what the batch
    response already said, letting _confirm_unwind's timeout be
    exercised."""

    def __init__(
        self,
        fills: Optional[dict[str, Decimal]] = None,
        unwind_fills: Optional[dict[str, Decimal]] = None,
        unwind_fill_batches: Optional[list[dict[str, Decimal]]] = None,
        book_bids: Optional[dict[str, list[tuple[Decimal, Decimal]]]] = None,
        reject_tickers: frozenset[str] = frozenset(),
        confirm_after_polls: Optional[dict[str, int]] = None,
    ):
        self.batch_calls: list[list] = []
        self.cancel_calls: list[str] = []
        self._canceled: set[str] = set()
        self.get_order_calls: list[str] = []
        self._fills = fills or {}
        self._unwind_fills = unwind_fills or {}
        self._unwind_fill_batches = unwind_fill_batches
        self._book_bids = book_bids or {}
        self._reject_tickers = reject_tickers
        self._confirm_after_polls = confirm_after_polls or {}
        self._poll_counts: dict[str, int] = {}
        self._orders_by_id: dict[str, tuple[str, Decimal, OrderResult]] = {}   # order_id -> (ticker, initial_size, last_result)

    async def place_orders_batch(self, orders):
        self.batch_calls.append(list(orders))
        is_unwind_batch = len(self.batch_calls) > 1
        if is_unwind_batch and self._unwind_fill_batches is not None:
            stage = len(self.batch_calls) - 2
            fill_source = self._unwind_fill_batches[stage] if stage < len(self._unwind_fill_batches) else {}
        else:
            fill_source = self._unwind_fills if is_unwind_batch else self._fills

        results = []
        for order in orders:
            if order.market_id in self._reject_tickers:
                result = OrderResult(
                    order_id="", client_order_id=order.client_order_id or "cid",
                    status=OrderStatus.REJECTED, filled_size=Decimal(0), remaining_size=order.size,
                    average_fill_price=None, raw={"error": {"code": "insufficient_balance", "message": "insufficient_balance"}},
                )
                results.append(result)
                continue
            filled = fill_source.get(order.market_id, order.size)
            remaining = order.size - filled
            status = KalshiClient._derive_order_status(filled, remaining)
            order_id = f"order-{order.market_id}-{len(self.batch_calls)}"
            result = OrderResult(
                order_id=order_id, client_order_id=order.client_order_id or "cid",
                status=status, filled_size=filled, remaining_size=remaining, average_fill_price=None,
            )
            results.append(result)
            self._orders_by_id[order_id] = (order.market_id, order.size, result)
        return results

    async def cancel_order(self, order_id):
        self.cancel_calls.append(order_id)
        # A real exchange reports no remaining size once a cancel succeeds.
        # The fake used to leave the pre-cancel snapshot in place, which hid
        # the "order still live after cancel" condition entirely.
        self._canceled.add(order_id)

    async def get_order(self, order_id):
        self.get_order_calls.append(order_id)
        ticker, initial_size, last_result = self._orders_by_id[order_id]
        polls_needed = self._confirm_after_polls.get(ticker)
        self._poll_counts[order_id] = self._poll_counts.get(order_id, 0) + 1
        if polls_needed is not None and self._poll_counts[order_id] >= polls_needed:
            return OrderResult(
                order_id=order_id, client_order_id=last_result.client_order_id,
                status=OrderStatus.FILLED, filled_size=initial_size, remaining_size=Decimal(0), average_fill_price=None,
            )
        if order_id in self._canceled:
            return OrderResult(
                order_id=order_id, client_order_id=last_result.client_order_id,
                status=OrderStatus.FILLED if last_result.filled_size > 0 else OrderStatus.CANCELED,
                filled_size=last_result.filled_size, remaining_size=Decimal(0),
                average_fill_price=last_result.average_fill_price,
                average_fee_paid=last_result.average_fee_paid,
            )
        return last_result

    async def get_orderbook(self, ticker, side=Side.YES, depth=None):
        levels = self._book_bids.get(ticker, [(Decimal("0.01"), Decimal("100000"))])
        return Orderbook(
            market_id=ticker,
            side=side,
            bids=[OrderbookLevel(price=price, size=size) for price, size in levels],
            asks=[],
            timestamp=datetime.now(timezone.utc),
        )


@pytest.fixture
def buy_basket_opportunity():
    legs = [
        _leg("CUT50", "0.04", 120, "0.02", 120),
        _leg("CUT25", "0.55", 340, "0.53", 340),
        _leg("NOCHG", "0.28", 500, "0.26", 500),
        _leg("HIKE25", "0.03", 45, "0.01", 45),
    ]
    return compute_opportunity("KXFED-26SEP", legs, BUY_MAX, MIN_DEPTH)


# ---- compute_unwind_legs (pure) ----

def test_compute_unwind_legs_no_unwind_when_all_equal():
    assert compute_unwind_legs({"A": Decimal("45"), "B": Decimal("45"), "C": Decimal("45")}) == []


def test_compute_unwind_legs_no_unwind_when_nothing_filled():
    assert compute_unwind_legs({"A": Decimal("0"), "B": Decimal("0")}) == []


def test_compute_unwind_legs_full_unwind_when_one_leg_got_zero():
    unwind = compute_unwind_legs({"A": Decimal("323"), "B": Decimal("30"), "C": Decimal("0")})
    by_ticker = {u.ticker: u.excess_size for u in unwind}
    assert by_ticker == {"A": Decimal("323"), "B": Decimal("30")}   # C excluded — 0 excess, already at the floor


def test_compute_unwind_legs_partial_unwind_to_common_floor():
    unwind = compute_unwind_legs({"A": Decimal("100"), "B": Decimal("80"), "C": Decimal("60")})
    by_ticker = {u.ticker: u.excess_size for u in unwind}
    assert by_ticker == {"A": Decimal("40"), "B": Decimal("20")}   # C is the floor, excluded


def test_compute_unwind_legs_empty_input():
    assert compute_unwind_legs({}) == []


# ---- execute_opportunity: basic placement ----

@pytest.mark.asyncio
async def test_buy_basket_places_all_legs_in_one_batch_call(buy_basket_opportunity):
    client = FakeKalshiClient()   # default: every leg fills fully
    result = await execute_opportunity(client, buy_basket_opportunity)

    assert len(client.batch_calls) == 1   # one batched request, not N sequential ones
    orders = client.batch_calls[0]
    assert [o.market_id for o in orders] == ["CUT50", "CUT25", "NOCHG", "HIKE25"]
    assert all(o.action == OrderAction.BUY for o in orders)
    assert [o.limit_price for o in orders] == [Decimal("0.04"), Decimal("0.55"), Decimal("0.28"), Decimal("0.03")]
    assert all(o.size == buy_basket_opportunity.max_size == Decimal("45") for o in orders)
    assert all(o.client_order_id.startswith(f"{STRATEGY_CLIENT_ORDER_ID_PREFIX}entry-") for o in orders)
    assert len({o.client_order_id for o in orders}) == len(orders)

    assert len(result.leg_results) == 4
    assert all(r.order_result is not None and r.error is None for r in result.leg_results)
    assert result.mismatched is False
    assert result.unwind_confirmed is True   # nothing to unwind — vacuously confirmed
    assert result.unwind_results == []
    assert client.cancel_calls == []   # nothing left resting, nothing to cancel


@pytest.mark.asyncio
async def test_every_entry_leg_uses_native_fok_without_expiration(buy_basket_opportunity):
    client = FakeKalshiClient()
    await execute_opportunity(client, buy_basket_opportunity)

    orders = client.batch_calls[0]
    for order in orders:
        assert order.time_in_force == TimeInForce.FILL_OR_KILL
        assert order.expiration_ts is None


@pytest.mark.asyncio
async def test_even_fills_across_all_legs_needs_no_unwind(buy_basket_opportunity):
    client = FakeKalshiClient()   # every leg fills fully and evenly
    result = await execute_opportunity(client, buy_basket_opportunity)

    assert result.mismatched is False
    assert result.unwind_confirmed is True
    assert result.unwind_results == []
    assert len(client.batch_calls) == 1   # no second (unwind) batch call
    assert client.cancel_calls == []


@pytest.mark.asyncio
async def test_unfilled_remainder_is_canceled_immediately(buy_basket_opportunity):
    client = FakeKalshiClient(fills={"CUT50": Decimal("20")})   # partially filled, 25 left resting
    result = await execute_opportunity(client, buy_basket_opportunity)

    cut50_result = next(r for r in result.leg_results if r.ticker == "CUT50")
    assert cut50_result.order_result.order_id in client.cancel_calls
    # After a successful cancel the reconciled state must show nothing live —
    # the 20 that filled, and no remainder still working at the exchange.
    assert cut50_result.order_result.filled_size == Decimal("20")
    assert cut50_result.order_result.remaining_size == Decimal("0")
    assert cut50_result.terminal_verified is True


# ---- execute_opportunity: unwind-on-mismatch ----

@pytest.mark.asyncio
async def test_one_leg_rejection_triggers_full_unwind_of_the_others(buy_basket_opportunity):
    """Regression test for the real incident: FAI never filled while FAT
    and TIE did — here NOCHG is rejected while the other 3 legs fill.
    Every leg that DID fill must now get sold back (floor = 0), and the
    unwind confirms fully filled (the fake client's default sell fill is
    immediate and complete)."""
    client = FakeKalshiClient(reject_tickers=frozenset({"NOCHG"}))
    result = await execute_opportunity(client, buy_basket_opportunity)

    # original batch: all 4 legs still attempted despite the rejection
    assert len(client.batch_calls[0]) == 4
    rejected = [r for r in result.leg_results if r.error is not None]
    assert len(rejected) == 1
    assert rejected[0].ticker == "NOCHG"

    # unwind: the 3 legs that DID fill get sold back, opposite action,
    # aggressive price, at their full filled size (floor is 0)
    assert result.mismatched is True
    assert result.unwind_confirmed is True
    assert len(client.batch_calls) == 2
    unwind_orders = client.batch_calls[1]
    assert {o.market_id for o in unwind_orders} == {"CUT50", "CUT25", "HIKE25"}
    assert all(o.action == OrderAction.SELL for o in unwind_orders)   # opposite of the original BUY
    expected_bids = {q.ticker: q.best_bid for q in buy_basket_opportunity.leg_quotes}
    assert all(o.limit_price == expected_bids[o.market_id] for o in unwind_orders)
    assert all(o.size == Decimal("45") for o in unwind_orders)   # full filled amount, since floor is 0
    assert all(o.client_order_id.startswith(f"{STRATEGY_CLIENT_ORDER_ID_PREFIX}unwind-") for o in unwind_orders)


def test_worst_case_unwind_loss_requires_full_exit_depth(buy_basket_opportunity):
    shallow = buy_basket_opportunity.leg_quotes[0]
    object.__setattr__(shallow, "bid_size", Decimal("1"))
    assert estimate_worst_case_unwind_loss(buy_basket_opportunity) is None


def test_worst_case_unwind_loss_includes_spread_and_both_fees(buy_basket_opportunity):
    loss = estimate_worst_case_unwind_loss(buy_basket_opportunity)
    assert loss is not None and loss > Decimal(0)


@pytest.mark.asyncio
async def test_uneven_partial_fills_unwind_only_the_excess_above_the_floor(buy_basket_opportunity):
    # CUT25 only partially fills (30 of 45); the other three fill fully.
    client = FakeKalshiClient(fills={"CUT25": Decimal("30")})
    result = await execute_opportunity(client, buy_basket_opportunity)

    assert result.mismatched is True
    assert result.unwind_confirmed is True
    unwind_orders = client.batch_calls[1]
    unwind_by_ticker = {o.market_id: o.size for o in unwind_orders}
    # floor is 30 (CUT25's fill) — the other three each unwind their excess of 15
    assert unwind_by_ticker == {"CUT50": Decimal("15"), "NOCHG": Decimal("15"), "HIKE25": Decimal("15")}
    assert "CUT25" not in unwind_by_ticker   # already at the floor, no excess


# ---- execute_opportunity: unwind confirmation ----

@pytest.mark.asyncio
async def test_unwind_confirmed_true_when_it_fills_after_a_couple_polls(buy_basket_opportunity, monkeypatch):
    # Speed up the polling loop for the test rather than waiting out the real interval.
    monkeypatch.setattr("execution.executor._UNWIND_CONFIRM_POLL_INTERVAL_S", 0.001)
    monkeypatch.setattr("execution.executor._UNWIND_CONFIRM_TIMEOUT_S", 1.0)

    # NOCHG rejected -> the other 3 unwind. CUT50's unwind sell doesn't
    # fill immediately (rests), but confirms filled by the 2nd poll.
    client = FakeKalshiClient(
        reject_tickers=frozenset({"NOCHG"}),
        unwind_fills={"CUT50": Decimal("0")},
        confirm_after_polls={"CUT50": 2},
    )
    result = await execute_opportunity(client, buy_basket_opportunity)

    assert result.mismatched is True
    assert result.unwind_confirmed is True
    assert client.get_order_calls   # it actually had to poll


@pytest.mark.asyncio
async def test_unwind_confirmed_false_when_it_never_fills_within_timeout(buy_basket_opportunity, monkeypatch):
    monkeypatch.setattr("execution.executor._UNWIND_CONFIRM_POLL_INTERVAL_S", 0.001)
    monkeypatch.setattr("execution.executor._UNWIND_CONFIRM_TIMEOUT_S", 0.01)

    # CUT50's unwind sell never fills (confirm_after_polls omitted for it) —
    # a genuinely stuck flatten attempt, the worst-case scenario.
    client = FakeKalshiClient(reject_tickers=frozenset({"NOCHG"}), unwind_fills={"CUT50": Decimal("0")})
    result = await execute_opportunity(client, buy_basket_opportunity)

    assert result.mismatched is True
    assert result.unwind_confirmed is False
    stuck_id = "order-CUT50-2"
    assert stuck_id in client.cancel_calls


@pytest.mark.asyncio
async def test_refreshed_book_fallback_flattens_remaining_exposure(buy_basket_opportunity, monkeypatch):
    monkeypatch.setattr("execution.executor._UNWIND_CONFIRM_POLL_INTERVAL_S", 0.001)
    monkeypatch.setattr("execution.executor._UNWIND_CONFIRM_TIMEOUT_S", 0.005)
    client = FakeKalshiClient(
        reject_tickers=frozenset({"NOCHG"}),
        unwind_fill_batches=[{"CUT50": Decimal("0")}, {}],
        book_bids={"CUT50": [(Decimal("0.015"), Decimal("45"))]},
    )

    result = await execute_opportunity(client, buy_basket_opportunity)

    assert result.unwind_confirmed is True
    assert result.refreshed_book_fallback_used is True
    assert result.emergency_unwind_used is False
    assert result.unwind_attempt_count == 2
    assert client.batch_calls[2][0].limit_price == Decimal("0.015")
    assert result.unwind_results[-1].execution_stage == "unwind-refresh"


@pytest.mark.asyncio
async def test_emergency_one_cent_fallback_is_last_resort(buy_basket_opportunity, monkeypatch):
    monkeypatch.setattr("execution.executor._UNWIND_CONFIRM_POLL_INTERVAL_S", 0.001)
    monkeypatch.setattr("execution.executor._UNWIND_CONFIRM_TIMEOUT_S", 0.005)
    client = FakeKalshiClient(
        reject_tickers=frozenset({"NOCHG"}),
        unwind_fill_batches=[{"CUT50": Decimal("0")}, {"CUT50": Decimal("0")}, {}],
        book_bids={"CUT50": [(Decimal("0.015"), Decimal("45"))]},
    )

    result = await execute_opportunity(client, buy_basket_opportunity)

    assert result.unwind_confirmed is True
    assert result.emergency_unwind_used is True
    assert result.unwind_attempt_count == 3
    assert client.batch_calls[3][0].limit_price == Decimal("0.01")
    assert result.unwind_results[-1].execution_stage == "unwind-emergency"


# --- ambiguous submission handling (added 2026-08-28) ------------------------
# A lost response does NOT mean nothing happened. Collapsing it into zero fills
# made a lost entry look like a clean no-fill while real positions sat open,
# and let a lost unwind be followed by a second unwind that oversells.


class _AmbiguousClient(FakeKalshiClient):
    """Raises AmbiguousSubmissionError on the first batch, then answers the
    client_order_id lookup with `resolved` (ticker -> filled size). A ticker
    absent from `resolved` was genuinely never accepted."""

    def __init__(self, resolved=None, lookup_fails=False, **kw):
        super().__init__(**kw)
        self._resolved = resolved or {}
        self._lookup_fails = lookup_fails
        self.lookup_calls = 0
        self._submitted: list = []

    async def place_orders_batch(self, orders):
        # Only the ENTRY batch goes ambiguous, so each test isolates one
        # ambiguous stage; later unwind batches behave normally.
        if self._submitted:
            return await super().place_orders_batch(orders)
        self._submitted = list(orders)
        raise AmbiguousSubmissionError(
            "transport failure on POST /portfolio/events/orders/batched",
            client_order_ids=[o.client_order_id for o in orders],
        )

    async def resolve_client_order_ids(self, client_order_ids):
        self.lookup_calls += 1
        if self._lookup_fails:
            raise ExchangeError("lookup unavailable")
        out = {}
        for order in self._submitted:
            size = self._resolved.get(order.market_id)
            if size is None:
                continue
            resolved_result = OrderResult(
                order_id=f"resolved-{order.market_id}", client_order_id=order.client_order_id,
                status=OrderStatus.FILLED if size >= order.size else OrderStatus.PARTIALLY_FILLED,
                filled_size=Decimal(size), remaining_size=Decimal(0), average_fill_price=order.limit_price,
            )
            # register so the follow-up verification read can find it
            self._orders_by_id[resolved_result.order_id] = (order.market_id, order.size, resolved_result)
            out[order.client_order_id] = resolved_result
        return out


@pytest.mark.asyncio
async def test_lost_entry_response_is_resolved_not_assumed_empty(buy_basket_opportunity):
    """The exchange DID fill three legs; only the answer was lost. The real
    fills must surface, and the uneven basket must unwind rather than being
    reported as a clean no-fill."""
    client = _AmbiguousClient(resolved={"CUT50": 45, "CUT25": 45, "NOCHG": 45})
    result = await execute_opportunity(client, buy_basket_opportunity)

    assert client.lookup_calls == 1
    filled = {r.ticker: (r.order_result.filled_size if r.order_result else Decimal(0)) for r in result.leg_results}
    assert filled["CUT50"] == Decimal("45")
    assert filled["HIKE25"] == Decimal("0")     # genuinely never accepted
    assert result.mismatched is True            # NOT silently "clean"


@pytest.mark.asyncio
async def test_lost_response_with_no_orders_accepted_is_a_true_no_fill(buy_basket_opportunity):
    client = _AmbiguousClient(resolved={})
    result = await execute_opportunity(client, buy_basket_opportunity)
    assert client.lookup_calls == 1
    assert all(r.order_result is None for r in result.leg_results)
    assert result.mismatched is False           # every leg confirmed absent → truthfully clean


@pytest.mark.asyncio
async def test_unresolvable_submission_halts_instead_of_guessing(buy_basket_opportunity):
    """If the lookup itself fails we still don't know — execution must stop
    rather than submit another stage against an unknown position."""
    client = _AmbiguousClient(lookup_fails=True)
    with pytest.raises(UnresolvedSubmissionError) as excinfo:
        await execute_opportunity(client, buy_basket_opportunity)
    assert len(excinfo.value.leg_results) == 4
    assert all("AMBIGUOUS" in (r.error or "") for r in excinfo.value.leg_results)


class _UncancellableClient(FakeKalshiClient):
    """An entry leg stays live and the cancel fails — the exact shape that used
    to be reported as a clean, non-mismatched basket."""

    async def cancel_order(self, order_id):
        self.cancel_calls.append(order_id)
        raise ExchangeError("cancel rejected")


@pytest.mark.asyncio
async def test_entry_with_uncancellable_live_order_halts(buy_basket_opportunity):
    client = _UncancellableClient(fills={"CUT50": Decimal("0"), "CUT25": Decimal("0"),
                                         "NOCHG": Decimal("0"), "HIKE25": Decimal("0")})
    with pytest.raises(UnresolvedSubmissionError) as excinfo:
        await execute_opportunity(client, buy_basket_opportunity)
    assert "unverified or non-terminal" in str(excinfo.value)
