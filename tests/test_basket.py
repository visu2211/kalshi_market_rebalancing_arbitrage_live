"""scanner.basket.compute_opportunity — no network. Numbers mirror the
worked example from planning: a 4-leg event with asks 0.04/0.55/0.28/0.03
(sizes 120/340/500/45) totals $0.90 + $0.06 fees = $0.96, under the $0.98
buy threshold, with a 45-contract depth floor.

buy_basket only — sell_basket was removed 2026-08-26 (see scanner/basket.py's
and README.md's docstrings)."""

from decimal import Decimal

from scanner.basket import LegQuote, compute_opportunity

BUY_MAX = Decimal("0.98")
MIN_DEPTH = Decimal("25")


def _leg(ticker, ask, ask_size, bid, bid_size) -> LegQuote:
    return LegQuote(ticker=ticker, best_ask=Decimal(ask), ask_size=Decimal(ask_size), best_bid=Decimal(bid), bid_size=Decimal(bid_size))


def test_buy_basket_opportunity_detected_with_correct_edge_and_size():
    legs = [
        _leg("CUT50", "0.04", 120, "0.02", 120),
        _leg("CUT25", "0.55", 340, "0.53", 340),
        _leg("NOCHG", "0.28", 500, "0.26", 500),
        _leg("HIKE25", "0.03", 45, "0.01", 45),
    ]
    opp = compute_opportunity("KXFED-26SEP", legs, BUY_MAX, MIN_DEPTH)

    assert opp is not None
    assert opp.direction == "buy_basket"
    assert opp.basket_price_usd == Decimal("0.96")
    assert opp.edge_usd == Decimal("0.04")
    assert opp.max_size == Decimal("45")
    assert opp.legs == ["CUT50", "CUT25", "NOCHG", "HIKE25"]
    assert opp.leg_quotes == legs


def test_buy_basket_rejected_when_thinnest_leg_below_depth_floor():
    legs = [
        _leg("CUT50", "0.04", 120, "0.02", 120),
        _leg("CUT25", "0.55", 340, "0.53", 340),
        _leg("NOCHG", "0.28", 500, "0.26", 500),
        _leg("HIKE25", "0.03", 10, "0.01", 10),  # below MIN_DEPTH=25
    ]
    assert compute_opportunity("KXFED-26SEP", legs, BUY_MAX, MIN_DEPTH) is None


def test_fairly_priced_basket_is_not_an_opportunity():
    # sum(asks) = 1.00, plus fees -> well above BUY_MAX. Fair/normal
    # spread, no buy-side edge.
    legs = [
        _leg("A", "0.25", 100, "0.24", 100),
        _leg("B", "0.25", 100, "0.24", 100),
        _leg("C", "0.25", 100, "0.24", 100),
        _leg("D", "0.25", 100, "0.24", 100),
    ]
    assert compute_opportunity("KXTEST-2", legs, BUY_MAX, MIN_DEPTH) is None


def test_overpriced_basket_is_not_flagged_even_though_bids_would_have_been_a_sell_opportunity():
    # sum(bids) = 1.20 would have cleared the old sell_basket threshold —
    # confirms that direction is simply gone, not just unreachable.
    legs = [
        _leg("A", "0.32", 100, "0.30", 100),
        _leg("B", "0.32", 100, "0.30", 100),
        _leg("C", "0.32", 100, "0.30", 100),
        _leg("D", "0.32", 100, "0.30", 100),
    ]
    assert compute_opportunity("KXTEST-1", legs, BUY_MAX, MIN_DEPTH) is None


def test_empty_legs_returns_none():
    assert compute_opportunity("KXTEST-3", [], BUY_MAX, MIN_DEPTH) is None
