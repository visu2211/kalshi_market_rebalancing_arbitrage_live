"""execution.maker.quoter — pure, no network. Verifies the mid-price
prior, the per-tick offset arithmetic, the aggregate-margin gate, and
the symmetry between buy_basket (YES-side) and buy_all_no (NO-side)
quoting.
"""

from decimal import Decimal

from execution.maker.quoter import (
    TICK,
    MakerQuote,
    mid_drift_ticks,
    quote_basket,
)
from kalshi.client import OrderAction, Side
from scanner.basket import BasketOpportunity, LegQuote, compute_no_opportunity


BUY_MAX = Decimal("0.98")
MIN_DEPTH = Decimal("25")


def _leg(ticker, ask, ask_size, bid, bid_size) -> LegQuote:
    return LegQuote(
        ticker=ticker,
        best_ask=Decimal(ask), ask_size=Decimal(ask_size),
        best_bid=Decimal(bid), bid_size=Decimal(bid_size),
    )


def _basket_opp(event, legs, direction="buy_basket", max_size=Decimal(1)) -> BasketOpportunity:
    """Build a BasketOpportunity directly, bypassing compute_opportunity's
    taker-side cost gate — the whole point of the maker is to quote when
    the taker's gate (sum(asks) < buy_basket_max_cost_usd) doesn't fire."""
    return BasketOpportunity(
        event_ticker=event,
        direction=direction,
        legs=[l.ticker for l in legs],
        leg_quotes=legs,
        basket_price_usd=sum((l.best_ask for l in legs), Decimal(0)),
        edge_usd=Decimal(0),
        max_size=max_size,
    )


# ---- buy_basket (YES-side) ----

def test_quote_basket_posts_at_mid_minus_offset():
    legs = [
        _leg("A", "0.50", 100, "0.40", 100),    # mid = 0.45
        _leg("B", "0.30", 100, "0.20", 100),    # mid = 0.25
        _leg("C", "0.30", 100, "0.20", 100),    # mid = 0.25
    ]
    opp = _basket_opp("EVT", legs)

    q = quote_basket(opp, quote_offset_ticks=1,
                     target_margin_per_basket=Decimal("0.001"),
                     size=Decimal(1))
    assert q is not None
    assert q.direction == "buy_basket"
    prices = {lq.ticker: lq.price for lq in q.leg_quotes}
    # Expected: mid - 1 tick (= 0.01) for each
    assert prices["A"] == Decimal("0.44")
    assert prices["B"] == Decimal("0.24")
    assert prices["C"] == Decimal("0.24")
    assert all(lq.side == Side.YES for lq in q.leg_quotes)
    assert all(lq.action == OrderAction.BUY for lq in q.leg_quotes)


def test_quote_basket_rejects_when_margin_insufficient():
    # Mids sum tight to 1 — after fees there's no 3c margin, so quote_basket
    # must return None regardless of offset.
    legs = [
        _leg("A", "0.50", 100, "0.48", 100),    # mid = 0.49
        _leg("B", "0.50", 100, "0.48", 100),    # mid = 0.49
    ]
    opp = _basket_opp("EVT", legs)

    # Offset 1 tick on each -> sum = 0.96; 1 - 0.96 = 0.04 gross margin.
    # Taker fees on a size-1 at $0.48 are ~0.03-0.04 per leg, so net margin is
    # negative. Requesting 3c target margin -> None.
    q = quote_basket(opp, quote_offset_ticks=1,
                     target_margin_per_basket=Decimal("0.03"),
                     size=Decimal(1))
    assert q is None


def test_quote_basket_accepts_when_offset_widens_margin():
    legs = [
        _leg("A", "0.50", 100, "0.48", 100),    # mid = 0.49
        _leg("B", "0.50", 100, "0.48", 100),    # mid = 0.49
    ]
    opp = _basket_opp("EVT", legs)

    # Widen the offset — each quote at 0.39 ->sum 0.78, margin ~0.22 after fees.
    q = quote_basket(opp, quote_offset_ticks=10,
                     target_margin_per_basket=Decimal("0.05"),
                     size=Decimal(1))
    assert q is not None
    prices = {lq.ticker: lq.price for lq in q.leg_quotes}
    assert prices["A"] == Decimal("0.39")
    assert prices["B"] == Decimal("0.39")
    assert q.expected_margin_usd > Decimal("0.05")


def test_quote_basket_rejects_offset_pushing_price_below_zero():
    # A leg with a mid near zero plus a 5-tick offset would produce a
    # negative quote — not a valid Kalshi price. The basket must be
    # rejected rather than emitted.
    legs = [
        _leg("A", "0.03", 100, "0.01", 100),    # mid = 0.02
        _leg("B", "0.95", 100, "0.93", 100),    # mid = 0.94
    ]
    opp = _basket_opp("EVT", legs)

    q = quote_basket(opp, quote_offset_ticks=5,
                     target_margin_per_basket=Decimal("0.001"),
                     size=Decimal(1))
    assert q is None


def test_quote_basket_rejects_crossed_book():
    # Crossed book is a corrupt WS state (see scanner.basket.incoherent_legs).
    # quote_basket must refuse exactly like compute_opportunity does.
    legs = [
        _leg("A", "0.40", 100, "0.50", 100),    # ask < bid — crossed
        _leg("B", "0.50", 100, "0.48", 100),
    ]
    # Can't construct via compute_opportunity (it will return None), so
    # fabricate a BasketOpportunity directly.
    from scanner.basket import BasketOpportunity
    opp = BasketOpportunity(
        event_ticker="EVT",
        direction="buy_basket",
        legs=["A", "B"],
        leg_quotes=legs,
        basket_price_usd=Decimal("0.98"),
        edge_usd=Decimal("0.02"),
        max_size=Decimal(1),
    )
    q = quote_basket(opp, quote_offset_ticks=1,
                     target_margin_per_basket=Decimal("0.01"),
                     size=Decimal(1))
    assert q is None


# ---- buy_all_no (NO-side) ----

def test_quote_basket_buy_all_no_synthesizes_no_side():
    # buy_all_no fixture mirrors tests/test_executor.py's: sum(bids)=1.20,
    # which gives real negative-cost edge on the NO basket.
    legs = [
        _leg("A", "0.32", 120, "0.30", 120),
        _leg("B", "0.32", 340, "0.30", 340),
        _leg("C", "0.32", 500, "0.30", 500),
        _leg("D", "0.32", 45, "0.30", 45),
    ]
    opp = compute_no_opportunity("EVT", legs, Decimal("0.02"), Decimal(45))
    assert opp is not None, "buy_all_no opportunity must materialize for the quoter test"

    q = quote_basket(opp, quote_offset_ticks=1,
                     target_margin_per_basket=Decimal("0.03"),
                     size=Decimal(1))
    assert q is not None
    assert q.direction == "buy_all_no"
    # Each leg's YES mid = (0.32 + 0.30)/2 = 0.31 -> NO mid = 0.69 -> NO quote = 0.68
    for lq in q.leg_quotes:
        assert lq.side == Side.NO
        assert lq.action == OrderAction.BUY
        assert lq.price == Decimal("0.68")


# ---- mid_drift_ticks ----

def test_mid_drift_ticks_counts_whole_ticks():
    assert mid_drift_ticks(Decimal("0.50"), Decimal("0.50")) == 0
    assert mid_drift_ticks(Decimal("0.50"), Decimal("0.51")) == 1
    assert mid_drift_ticks(Decimal("0.51"), Decimal("0.50")) == 1   # absolute
    assert mid_drift_ticks(Decimal("0.50"), Decimal("0.53")) == 3
    # Fractional tick rounded to nearest whole
    assert mid_drift_ticks(Decimal("0.500"), Decimal("0.504")) == 0
    assert mid_drift_ticks(Decimal("0.500"), Decimal("0.506")) == 1
