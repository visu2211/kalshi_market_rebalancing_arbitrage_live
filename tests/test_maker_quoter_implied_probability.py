"""Verify the implied-probability pricing prior normalizes Σ p_i to 1
across the basket and quotes further below overpriced legs than the
mid prior would.
"""

from decimal import Decimal

import pytest

from execution.maker.quoter import quote_basket
from kalshi.client import OrderAction, Side
from scanner.basket import BasketOpportunity, LegQuote


def _leg(ticker, ask, ask_size, bid, bid_size) -> LegQuote:
    return LegQuote(
        ticker=ticker,
        best_ask=Decimal(ask), ask_size=Decimal(ask_size),
        best_bid=Decimal(bid), bid_size=Decimal(bid_size),
    )


def _basket_opp(event, legs, direction="buy_basket", max_size=Decimal(1)) -> BasketOpportunity:
    return BasketOpportunity(
        event_ticker=event, direction=direction,
        legs=[l.ticker for l in legs], leg_quotes=legs,
        basket_price_usd=sum((l.best_ask for l in legs), Decimal(0)),
        edge_usd=Decimal(0), max_size=max_size,
    )


def test_implied_probability_normalizes_to_one():
    # Mildly overpriced basket: mids sum to 1.05 (5% bid-ask spread tax).
    legs = [
        _leg("A", "0.36", 100, "0.34", 100),    # mid 0.35
        _leg("B", "0.36", 100, "0.34", 100),    # mid 0.35
        _leg("C", "0.36", 100, "0.34", 100),    # mid 0.35
    ]
    opp = _basket_opp("EVT", legs)
    # Mid prior at offset=5: quotes 0.30 each (sum 0.90). Fees ~$0.06. Cost 0.96. Margin 0.04 > target.
    mid_quote = quote_basket(opp, quote_offset_ticks=5,
                             target_margin_per_basket=Decimal("0.01"),
                             size=Decimal(1), pricing_prior="mid")
    assert mid_quote is not None
    assert {lq.price for lq in mid_quote.leg_quotes} == {Decimal("0.30")}

    # Implied-prob: each normalized to 0.35/1.05 = 0.3333..., quote 0.2833 each.
    ip_quote = quote_basket(opp, quote_offset_ticks=5,
                            target_margin_per_basket=Decimal("0.01"),
                            size=Decimal(1), pricing_prior="implied_probability")
    assert ip_quote is not None
    # Every leg should price below the mid-prior quote (further from overpriced mid).
    for ip_lq, mid_lq in zip(ip_quote.leg_quotes, mid_quote.leg_quotes):
        assert ip_lq.price < mid_lq.price, (
            f"implied-prob quote {ip_lq.price} should be lower than mid quote {mid_lq.price}"
        )
    # And Σ of the fair-prob references should sum to 1 (modulo rounding)
    total_fair = sum((lq.mid for lq in ip_quote.leg_quotes), Decimal(0))
    assert abs(total_fair - Decimal(1)) < Decimal("0.0001")


def test_implied_probability_matches_mid_when_sum_is_one():
    # Perfectly priced basket where mids already sum to exactly 1 —
    # implied-probability shouldn't shift anything. Wide offset to make
    # the mid-prior margin clear despite fees.
    legs = [
        _leg("A", "0.21", 100, "0.19", 100),    # mid 0.20
        _leg("B", "0.31", 100, "0.29", 100),    # mid 0.30
        _leg("C", "0.51", 100, "0.49", 100),    # mid 0.50
    ]
    opp = _basket_opp("EVT", legs)
    mid_q = quote_basket(opp, quote_offset_ticks=10, target_margin_per_basket=Decimal("0.01"),
                        size=Decimal(1), pricing_prior="mid")
    ip_q = quote_basket(opp, quote_offset_ticks=10, target_margin_per_basket=Decimal("0.01"),
                       size=Decimal(1), pricing_prior="implied_probability")
    assert mid_q is not None and ip_q is not None
    for mid_lq, ip_lq in zip(mid_q.leg_quotes, ip_q.leg_quotes):
        # Allow tiny Decimal rounding; shouldn't differ by more than a sub-tick.
        assert abs(mid_lq.price - ip_lq.price) < Decimal("0.0001")


def test_implied_probability_buy_all_no_side_is_no():
    # For buy_all_no the IP prior produces a NO-side BUY quote. Whether
    # IP's quote is above or below the mid prior depends on which side is
    # overpriced — tested here only that the side is correct and the
    # normalization math is applied (fair YES probs sum to 1 across the
    # basket, so fair NO probs sum to N-1).
    legs = [
        _leg("A", "0.28", 100, "0.26", 100),    # mid 0.27
        _leg("B", "0.28", 100, "0.26", 100),    # mid 0.27
        _leg("C", "0.28", 100, "0.26", 100),    # mid 0.27
        _leg("D", "0.28", 100, "0.26", 100),    # mid 0.27, Σ = 1.08
    ]
    opp = _basket_opp("EVT", legs, direction="buy_all_no")
    # offset=5 needed so the mid+fees math clears the margin gate for IP too
    # (IP pushes the NO quote to 0.74 vs mid prior's 0.72 for this basket —
    # IP makes the quote LESS aggressive when the YES side is overpriced
    # because the NO side was already underpriced).
    ip_q = quote_basket(opp, quote_offset_ticks=5, target_margin_per_basket=Decimal("0.001"),
                       size=Decimal(1), pricing_prior="implied_probability")
    assert ip_q is not None
    for lq in ip_q.leg_quotes:
        assert lq.side == Side.NO
    # lq.mid for buy_all_no carries the NO-side fair value (1 - normalized_yes).
    # Σ should equal N - 1 within rounding.
    total_no_fair = sum((lq.mid for lq in ip_q.leg_quotes), Decimal(0))
    expected = Decimal(len(legs) - 1)
    assert abs(total_no_fair - expected) < Decimal("0.0001"), (
        f"Σ no_fair = {total_no_fair}, expected ~{expected}"
    )


def test_unknown_prior_raises():
    legs = [_leg("A", "0.5", 100, "0.4", 100)]
    opp = _basket_opp("EVT", legs)
    with pytest.raises(ValueError, match="unknown pricing_prior"):
        quote_basket(opp, quote_offset_ticks=1,
                     target_margin_per_basket=Decimal("0.001"),
                     size=Decimal(1), pricing_prior="black_scholes")
