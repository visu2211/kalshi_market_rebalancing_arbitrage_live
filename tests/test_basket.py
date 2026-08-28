"""scanner.basket.compute_opportunity — no network. Numbers mirror the
worked example from planning: a 4-leg event with asks 0.04/0.55/0.28/0.03
(sizes 120/340/500/45) totals $0.90 + $0.06 fees = $0.96, under the $0.98
buy threshold, with a 45-contract depth floor.

buy_basket only — sell_basket was removed 2026-08-26 (see scanner/basket.py's
and README.md's docstrings)."""

from decimal import Decimal

from scanner.basket import LegQuote, compute_opportunity, incoherent_legs

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


def test_basket_uses_aggregate_fee_for_requested_execution_size():
    legs = [
        _leg("A", "0.04", 100, "0.03", 100),
        _leg("B", "0.90", 100, "0.89", 100),
    ]
    opp = compute_opportunity(
        "KXTEST-SIZED", legs, Decimal("0.96"), Decimal(1), contract_count=Decimal(10)
    )
    assert opp is not None
    # A: 3 cents total; B: 7 cents total. Per contract cost = .94 + .01.
    assert opp.basket_price_usd == Decimal("0.95")


# --- corrupt local book rejection (added 2026-08-27) -------------------------
# scanner.basket.incoherent_legs: a locked or crossed book is an instant free
# round-trip, so a live exchange never shows one — seeing it means the local
# book is wrong, not that an edge exists. This is what produced the phantom
# 3-34c "opportunities" in data/opportunities.jsonl.


def test_crossed_leg_is_rejected_even_when_the_basket_looks_cheap():
    """The exact phantom shape: one corrupt leg with an impossibly good ask
    (below its own bid) drags a normal basket under the threshold."""
    legs = [
        _leg("A", "0.30", 5000, "0.51", 800),   # crossed: bid 0.51 > ask 0.30
        _leg("B", "0.49", 100, "0.48", 100),
    ]
    assert sum(l.best_ask for l in legs) < BUY_MAX      # would otherwise flag
    assert compute_opportunity("KXPHANTOM", legs, BUY_MAX, MIN_DEPTH) is None


def test_locked_leg_is_rejected():
    legs = [
        _leg("A", "0.40", 100, "0.40", 100),   # locked: bid == ask
        _leg("B", "0.45", 100, "0.44", 100),
    ]
    assert compute_opportunity("KXLOCKED", legs, BUY_MAX, MIN_DEPTH) is None


def test_out_of_range_price_is_rejected_rather_than_raising():
    """best_ask of 0 is reachable when a synthesized ask comes from a 1.00 NO
    bid; taker_fee raises on it, which would kill the WS loop mid-tick."""
    legs = [
        _leg("A", "0", 100, "0", 100),
        _leg("B", "0.45", 100, "0.44", 100),
    ]
    assert compute_opportunity("KXZERO", legs, BUY_MAX, MIN_DEPTH) is None


def test_incoherent_legs_names_only_the_offending_legs():
    good = _leg("GOOD", "0.49", 100, "0.48", 100)
    crossed = _leg("CROSSED", "0.30", 100, "0.51", 100)
    locked = _leg("LOCKED", "0.40", 100, "0.40", 100)
    assert incoherent_legs([good, crossed, locked]) == [crossed, locked]
    assert incoherent_legs([good]) == []


def test_coherent_basket_still_detected():
    """Guard against the new check being too strict: a normal one-cent-spread
    book must still pass."""
    legs = [
        _leg("A", "0.04", 120, "0.03", 120),
        _leg("B", "0.90", 100, "0.89", 100),
    ]
    assert compute_opportunity("KXOK", legs, BUY_MAX, MIN_DEPTH) is not None
