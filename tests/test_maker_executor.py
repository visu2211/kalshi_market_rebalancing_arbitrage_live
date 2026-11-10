"""execution.maker.executor — no network. A FakeKalshiClient mirrors
the taker test's pattern, extended to model resting GTC orders (status
stays OPEN with remaining_size > 0 after the batch response, until the
monitor decides to cancel them)."""

from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path

import pytest

from config.loader import MakerExecutionSettings
from execution.maker.executor import (
    STRATEGY_CLIENT_ORDER_ID_PREFIX,
    execute_opportunity_maker,
    manage_fill_event,
)
from execution.maker.inventory import InventoryActionKind, MakerInventoryBook
from execution.maker.risk import MakerRiskState
from kalshi.client import KalshiClient, OrderResult, OrderStatus, Side
from scanner.basket import BasketOpportunity, LegQuote


# ---- Fixtures ----

def _leg(ticker, ask, ask_size, bid, bid_size) -> LegQuote:
    return LegQuote(
        ticker=ticker,
        best_ask=Decimal(ask), ask_size=Decimal(ask_size),
        best_bid=Decimal(bid), bid_size=Decimal(bid_size),
    )


def _make_settings(tmp_path: Path, enabled: bool = True) -> MakerExecutionSettings:
    return MakerExecutionSettings(
        enabled=enabled,
        target="demo",
        prod_enabled=False,
        target_margin_per_basket=Decimal("0.03"),
        max_inventory_per_event_usd=Decimal("5"),
        quote_offset_ticks=1,
        requote_threshold_ticks=2,
        max_resting_order_age_s=300,
        flatten_on_leg_fill=True,
        max_concurrent_quoted_events=3,
        strategy_id="maker-v1",
        risk_state_path=tmp_path / "risk.json",
        orders_log_path=tmp_path / "orders.jsonl",
        order_store_path=tmp_path / "orders.sqlite3",
        process_lock_path=tmp_path / "lock",
    )


def _mkopp(direction="buy_basket", event="EVT") -> BasketOpportunity:
    # Mids: A=0.40, B=0.20, C=0.20 -> sum = 0.80. With 1-tick offset
    # each quote is at mid - 0.01: 0.39, 0.19, 0.19 (sum 0.77). Fees on a
    # size-1 basket ~ 0.06 total. Net cost 0.83, margin 0.17 — well above
    # the 3c threshold.
    legs = [
        _leg("A", "0.45", 100, "0.35", 100),
        _leg("B", "0.25", 100, "0.15", 100),
        _leg("C", "0.25", 100, "0.15", 100),
    ]
    return BasketOpportunity(
        event_ticker=event,
        direction=direction,
        legs=[l.ticker for l in legs],
        leg_quotes=legs,
        basket_price_usd=Decimal("1.10"),
        edge_usd=Decimal(0),
        max_size=Decimal(1),
    )


class FakeKalshiClient:
    """Records every place_orders_batch / cancel_order / get_order call.

    `batch_outcomes[i]` is a dict ticker -> (status, filled_size) applied
    to the i-th batch call (first one = the initial quote batch). The
    default outcome for a maker quote is OPEN with 0 filled, size
    remaining — i.e. the resting-order state."""

    def __init__(self, batch_outcomes=None, reject_tickers=frozenset()):
        self.batch_calls = []
        self.cancel_calls = []
        self._batch_outcomes = batch_outcomes or []
        self._reject_tickers = reject_tickers

    async def place_orders_batch(self, orders):
        self.batch_calls.append(list(orders))
        outcome = (
            self._batch_outcomes[len(self.batch_calls) - 1]
            if len(self.batch_calls) - 1 < len(self._batch_outcomes)
            else {}
        )
        results = []
        for o in orders:
            if o.market_id in self._reject_tickers:
                results.append(OrderResult(
                    order_id="", client_order_id=o.client_order_id or "cid",
                    status=OrderStatus.REJECTED,
                    filled_size=Decimal(0), remaining_size=o.size,
                    average_fill_price=None,
                    raw={"error": {"code": "insufficient_balance",
                                   "message": "insufficient_balance"}},
                ))
                continue
            status, filled = outcome.get(o.market_id, (OrderStatus.OPEN, Decimal(0)))
            remaining = o.size - filled
            results.append(OrderResult(
                order_id=f"order-{o.market_id}-{len(self.batch_calls)}",
                client_order_id=o.client_order_id or "cid",
                status=status, filled_size=filled,
                remaining_size=remaining, average_fill_price=None,
            ))
        return results

    async def cancel_order(self, order_id):
        self.cancel_calls.append(order_id)


# ---- quote + submit path ----

@pytest.mark.asyncio
async def test_submits_batch_when_margin_clears(tmp_path):
    client = FakeKalshiClient()
    settings = _make_settings(tmp_path)
    risk = MakerRiskState.empty("maker-v1")
    inv = MakerInventoryBook(
        flatten_on_leg_fill=True,
        max_inventory_per_event_usd=settings.max_inventory_per_event_usd,
    )

    result = await execute_opportunity_maker(
        client, _mkopp(),
        settings=settings, risk=risk, inventory=inv,
    )

    assert result.refused_reason is None
    assert len(client.batch_calls) == 1
    assert len(result.leg_results) == 3
    # Every leg went out as a GTC BUY at mid - 1 tick
    prices = {o.market_id: o.limit_price for o in client.batch_calls[0]}
    assert prices == {"A": Decimal("0.39"), "B": Decimal("0.19"), "C": Decimal("0.19")}
    # All submitted with maker prefix
    for o in client.batch_calls[0]:
        assert o.client_order_id.startswith(STRATEGY_CLIENT_ORDER_ID_PREFIX)
    # Risk state recorded an attempt
    assert risk.daily_basket_attempts == 1
    assert "EVT" in risk.attempted_events_at


@pytest.mark.asyncio
async def test_refuses_when_disabled(tmp_path):
    client = FakeKalshiClient()
    settings = _make_settings(tmp_path, enabled=False)
    risk = MakerRiskState.empty("maker-v1")
    inv = MakerInventoryBook(flatten_on_leg_fill=True,
                             max_inventory_per_event_usd=Decimal("5"))

    result = await execute_opportunity_maker(
        client, _mkopp(), settings=settings, risk=risk, inventory=inv,
    )
    assert result.refused_reason == "maker_disabled"
    assert client.batch_calls == []


@pytest.mark.asyncio
async def test_refuses_when_breaker_tripped(tmp_path):
    client = FakeKalshiClient()
    settings = _make_settings(tmp_path)
    risk = MakerRiskState.empty("maker-v1")
    risk.trip("test")
    inv = MakerInventoryBook(flatten_on_leg_fill=True,
                             max_inventory_per_event_usd=Decimal("5"))

    result = await execute_opportunity_maker(
        client, _mkopp(), settings=settings, risk=risk, inventory=inv,
    )
    assert result.refused_reason.startswith("breaker_tripped")
    assert client.batch_calls == []


@pytest.mark.asyncio
async def test_refuses_when_event_in_cooldown(tmp_path):
    client = FakeKalshiClient()
    settings = _make_settings(tmp_path)
    risk = MakerRiskState.empty("maker-v1")
    risk.record_basket_attempt("EVT")   # fresh cooldown entry
    inv = MakerInventoryBook(flatten_on_leg_fill=True,
                             max_inventory_per_event_usd=Decimal("5"))

    result = await execute_opportunity_maker(
        client, _mkopp(), settings=settings, risk=risk, inventory=inv,
        event_cooldown_s=3600,
    )
    assert result.refused_reason == "event_cooldown"
    assert client.batch_calls == []


@pytest.mark.asyncio
async def test_refuses_when_inventory_cap_would_exceed(tmp_path):
    client = FakeKalshiClient()
    settings = _make_settings(tmp_path)
    settings = MakerExecutionSettings(
        **{**settings.__dict__, "max_inventory_per_event_usd": Decimal("0.01")}
    )
    risk = MakerRiskState.empty("maker-v1")
    inv = MakerInventoryBook(flatten_on_leg_fill=True,
                             max_inventory_per_event_usd=settings.max_inventory_per_event_usd)

    result = await execute_opportunity_maker(
        client, _mkopp(), settings=settings, risk=risk, inventory=inv,
    )
    assert result.refused_reason == "inventory_cap_exceeded"
    assert client.batch_calls == []


# ---- manage_fill_event ----

@pytest.mark.asyncio
async def test_manage_fill_flattens_on_leg_fill():
    client = FakeKalshiClient(batch_outcomes=[
        # Second batch (the flatten sell): fills at $0.35.
        {"A": (OrderStatus.FILLED, Decimal(1))},
    ])
    inv = MakerInventoryBook(flatten_on_leg_fill=True,
                             max_inventory_per_event_usd=Decimal("5"))
    inv.open_event("EVT", ["A", "B", "C"], Decimal(1))

    unwinds = await manage_fill_event(
        client, "EVT", "A",
        cumulative_filled_delta=Decimal(1),
        fill_price=Decimal("0.44"),
        remaining_size=Decimal(0),
        resting_order_ids={"B": "order-B-1", "C": "order-C-1"},
        inventory=inv,
        exit_bids={"A": Decimal("0.35")},
    )

    # Should have canceled B and C
    assert sorted(client.cancel_calls) == ["order-B-1", "order-C-1"]
    # And fired an IOC sell for A's excess (1 contract above the 0 floor)
    assert len(client.batch_calls) == 1
    sell = client.batch_calls[0][0]
    assert sell.market_id == "A"
    assert sell.action.value == "sell"
    assert sell.limit_price == Decimal("0.35")
    assert len(unwinds) == 1
    # Event should now be closed in the inventory book.
    assert "EVT" not in inv.snapshot()


@pytest.mark.asyncio
async def test_manage_fill_basket_complete_no_cancel_no_sell():
    # flatten_on_leg_fill=False so the first fill is NO_ACTION and the
    # second fill brings the basket to parity (all at target_size).
    client = FakeKalshiClient()
    inv = MakerInventoryBook(flatten_on_leg_fill=False,
                             max_inventory_per_event_usd=Decimal("5"))
    inv.open_event("EVT", ["A", "B"], Decimal(1))

    unwinds1 = await manage_fill_event(
        client, "EVT", "A",
        cumulative_filled_delta=Decimal(1),
        fill_price=Decimal("0.44"),
        remaining_size=Decimal(0),
        resting_order_ids={"B": "order-B-1"},
        inventory=inv,
        exit_bids={"A": Decimal("0.4")},
    )
    assert unwinds1 == []
    assert client.cancel_calls == []
    assert client.batch_calls == []

    unwinds2 = await manage_fill_event(
        client, "EVT", "B",
        cumulative_filled_delta=Decimal(1),
        fill_price=Decimal("0.24"),
        remaining_size=Decimal(0),
        resting_order_ids={},
        inventory=inv,
        exit_bids={"B": Decimal("0.2")},
    )
    assert unwinds2 == []
    # Basket complete -> event removed
    assert "EVT" not in inv.snapshot()
