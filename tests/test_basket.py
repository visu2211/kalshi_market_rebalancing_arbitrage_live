"""scanner.basket.compute_opportunity / compute_no_opportunity — no
network. Numbers mirror the worked example from planning: a 4-leg event
with asks 0.04/0.55/0.28/0.03 (sizes 120/340/500/45) totals $0.90 + $0.06
fees = $0.96, under the $0.98 buy threshold, with a 45-contract depth
floor.

Two directions: buy_basket (compute_opportunity) and buy_all_no
(compute_no_opportunity) — see scanner/basket.py's module docstring for
why buy_all_no reproduces the sell_basket direction removed 2026-08-26."""

from decimal import Decimal

from kalshi.client import OrderbookLevel
from scanner.basket import (
    DepthLegQuote,
    LegQuote,
    compute_no_opportunity,
    compute_opportunity,
    compute_opportunity_depth_aware,
    incoherent_depth_legs,
    incoherent_legs,
)
from scanner.fees import taker_fee

BUY_MAX = Decimal("0.98")
MIN_DEPTH = Decimal("25")
MIN_EDGE = Decimal("0.02")   # buy_all_no's minimum required edge — symmetric with 1 - BUY_MAX


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


# --- buy_all_no (2026 merge — reproduces the deleted sell_basket direction) --


def test_buy_all_no_opportunity_detected_with_correct_edge_and_size():
    # sum(bids) = 1.20 across 4 legs — the bid side is overpriced relative
    # to the (N-1)=3 dollar payout for buying one NO on every leg.
    legs = [
        _leg("CUT50", "0.32", 120, "0.30", 120),
        _leg("CUT25", "0.32", 340, "0.30", 340),
        _leg("NOCHG", "0.32", 500, "0.30", 500),
        _leg("HIKE25", "0.32", 45, "0.30", 45),
    ]
    opp = compute_no_opportunity("KXFED-26SEP", legs, MIN_EDGE, MIN_DEPTH)

    assert opp is not None
    assert opp.direction == "buy_all_no"
    # no_ask = 1 - 0.30 = 0.70 per leg; taker_fee(0.70, 1) = 2 cents.
    assert opp.basket_price_usd == Decimal("2.88")
    assert opp.edge_usd == Decimal("0.12")
    assert opp.max_size == Decimal("45")   # min(bid_size), the thinnest leg
    assert opp.legs == ["CUT50", "CUT25", "NOCHG", "HIKE25"]
    assert opp.leg_quotes == legs


def test_buy_all_no_rejected_when_thinnest_leg_below_depth_floor():
    legs = [
        _leg("CUT50", "0.32", 120, "0.30", 120),
        _leg("CUT25", "0.32", 340, "0.30", 340),
        _leg("NOCHG", "0.32", 500, "0.30", 500),
        _leg("HIKE25", "0.32", 10, "0.30", 10),  # below MIN_DEPTH=25
    ]
    assert compute_no_opportunity("KXFED-26SEP", legs, MIN_EDGE, MIN_DEPTH) is None


def test_fairly_priced_basket_is_not_a_no_opportunity():
    # sum(bids) = 0.96 — a normal, slightly-underpriced (bookmaker-margin)
    # bid side. edge = sum(bids) - 1 - fees is negative regardless of leg
    # count (the N-1 payout and the N-leg no_ask sum always cancel to this —
    # see test_no_direction_edge_matches_hand_computed_sell_basket_formula).
    legs = [
        _leg("A", "0.25", 100, "0.24", 100),
        _leg("B", "0.25", 100, "0.24", 100),
        _leg("C", "0.25", 100, "0.24", 100),
        _leg("D", "0.25", 100, "0.24", 100),
    ]
    assert compute_no_opportunity("KXTEST-2", legs, MIN_EDGE, MIN_DEPTH) is None


def test_underpriced_basket_is_not_a_no_opportunity_even_though_asks_would_have_been_a_buy_opportunity():
    # The exact legs from test_buy_basket_opportunity_detected... — a real
    # buy_basket edge on the ask side must not spuriously register as a
    # buy_all_no edge on the bid side too; the two directions are priced
    # off opposite sides of the book and are independent.
    legs = [
        _leg("CUT50", "0.04", 120, "0.02", 120),
        _leg("CUT25", "0.55", 340, "0.53", 340),
        _leg("NOCHG", "0.28", 500, "0.26", 500),
        _leg("HIKE25", "0.03", 45, "0.01", 45),
    ]
    assert compute_opportunity("KXFED-26SEP", legs, BUY_MAX, MIN_DEPTH) is not None
    assert compute_no_opportunity("KXFED-26SEP", legs, MIN_EDGE, MIN_DEPTH) is None


def test_no_direction_empty_legs_returns_none():
    assert compute_no_opportunity("KXTEST-3", [], MIN_EDGE, MIN_DEPTH) is None


def test_no_direction_rejects_crossed_leg_same_as_buy_basket():
    """incoherent_legs guards both sides of the book symmetrically — no
    direction-specific change was needed for it (see compute_no_opportunity's
    docstring)."""
    legs = [
        _leg("A", "0.30", 5000, "0.51", 800),   # crossed: bid 0.51 > ask 0.30
        _leg("B", "0.49", 100, "0.48", 100),
    ]
    assert compute_no_opportunity("KXPHANTOM", legs, MIN_EDGE, MIN_DEPTH) is None


def test_no_direction_edge_matches_hand_computed_sell_basket_formula():
    """Direct check of the equivalence claimed in the module/function
    docstrings: buying one NO on every leg reproduces the deleted
    sell_basket direction's edge exactly. Payout (N-1) and the N-leg
    no_ask sum (N - sum(best_bid_i)) always cancel N, leaving
    edge = sum(best_bid_i) - 1 - fees regardless of leg count — and
    because Kalshi's fee formula depends only on price*(1-price), which is
    symmetric under price -> 1-price, taker_fee(no_ask_i) == taker_fee(best_bid_i)
    exactly, so fees can be hand-computed directly off best_bid too."""
    legs = [
        _leg("A", "0.47", 200, "0.45", 200),
        _leg("B", "0.42", 200, "0.40", 200),
        _leg("C", "0.37", 200, "0.35", 200),
    ]
    opp = compute_no_opportunity("KXSYM-1", legs, Decimal("0"), MIN_DEPTH)
    assert opp is not None

    hand_computed_edge = sum((leg.best_bid for leg in legs), start=Decimal(0)) - Decimal(1)
    hand_computed_edge -= sum((taker_fee(leg.best_bid, Decimal(1)) for leg in legs), start=Decimal(0))
    assert opp.edge_usd == hand_computed_edge
    assert opp.edge_usd == Decimal("0.14")


# --- compute_opportunity_depth_aware (2026 merge) ----------------------------
# LocalBook already stores the full price ladder; these tests cover the read
# path that walks it. The equivalence test below is the actual acceptance
# gate for scanner.max_book_depth_levels == 1 — not "looks right" but a
# byte-for-byte comparison against compute_opportunity's own output.


def test_depth_aware_matches_compute_opportunity_at_depth_one():
    flat_legs = [
        _leg("CUT50", "0.04", 120, "0.02", 120),
        _leg("CUT25", "0.55", 340, "0.53", 340),
        _leg("NOCHG", "0.28", 500, "0.26", 500),
        _leg("HIKE25", "0.03", 45, "0.01", 45),
    ]
    depth_legs = [
        DepthLegQuote(
            ticker=leg.ticker,
            ask_levels=[OrderbookLevel(leg.best_ask, leg.ask_size)],
            bid_levels=[OrderbookLevel(leg.best_bid, leg.bid_size)],
        )
        for leg in flat_legs
    ]

    flat = compute_opportunity("KXFED-26SEP", flat_legs, BUY_MAX, MIN_DEPTH)
    depth = compute_opportunity_depth_aware("KXFED-26SEP", depth_legs, BUY_MAX, MIN_DEPTH)

    assert flat is not None and depth is not None
    assert depth.direction == flat.direction
    assert depth.basket_price_usd == flat.basket_price_usd
    assert depth.edge_usd == flat.edge_usd
    assert depth.max_size == flat.max_size
    assert depth.legs == flat.legs


def test_depth_aware_walks_multiple_levels_when_top_level_insufficient():
    # Leg A's top level (0.10 x 5) can't cover the requested 10 contracts on
    # its own; the walk must span into its second level (0.12 x 20) rather
    # than pricing all 10 at the top price alone.
    legs = [
        DepthLegQuote(
            ticker="A",
            ask_levels=[OrderbookLevel(Decimal("0.10"), Decimal("5")), OrderbookLevel(Decimal("0.12"), Decimal("20"))],
            bid_levels=[OrderbookLevel(Decimal("0.08"), Decimal("50"))],
        ),
        DepthLegQuote(
            ticker="B",
            ask_levels=[OrderbookLevel(Decimal("0.20"), Decimal("100"))],
            bid_levels=[OrderbookLevel(Decimal("0.18"), Decimal("50"))],
        ),
    ]
    opp = compute_opportunity_depth_aware("KXDEPTH-1", legs, Decimal("1.0"), Decimal("1"), contract_count=Decimal("10"))

    assert opp is not None
    # A: 5@0.10 (fee 0.04) + 5@0.12 (fee 0.04) = 1.18, /10 = 0.118
    # B: 10@0.20 (fee 0.12) = 2.12, /10 = 0.212
    assert opp.basket_price_usd == Decimal("0.330")
    assert opp.max_size == Decimal("25")   # 5+20 walked on A, the thinner total depth
    assert opp.legs == ["A", "B"]


def test_depth_aware_returns_none_when_requested_quantity_exceeds_walked_depth():
    legs = [
        DepthLegQuote(
            ticker="A",
            ask_levels=[OrderbookLevel(Decimal("0.10"), Decimal("5")), OrderbookLevel(Decimal("0.12"), Decimal("20"))],
            bid_levels=[OrderbookLevel(Decimal("0.08"), Decimal("50"))],
        ),
        DepthLegQuote(
            ticker="B",
            ask_levels=[OrderbookLevel(Decimal("0.20"), Decimal("100"))],
            bid_levels=[OrderbookLevel(Decimal("0.18"), Decimal("50"))],
        ),
    ]
    # A only has 25 total across both levels; 30 requested.
    assert compute_opportunity_depth_aware(
        "KXDEPTH-2", legs, Decimal("1.0"), Decimal("1"), contract_count=Decimal("30")
    ) is None


def test_depth_aware_rejects_leg_coherent_at_top_but_crossed_deeper():
    """The exact shape a stale/corrupted deeper level would produce: level 1
    (0.30) is fine against the 0.29 bid, but level 2 (0.20) is not — exactly
    what incoherent_legs' top-of-book-only check would miss."""
    legs = [
        DepthLegQuote(
            ticker="A",
            ask_levels=[OrderbookLevel(Decimal("0.30"), Decimal("10")), OrderbookLevel(Decimal("0.20"), Decimal("50"))],
            bid_levels=[OrderbookLevel(Decimal("0.29"), Decimal("50"))],
        ),
        DepthLegQuote(
            ticker="B",
            ask_levels=[OrderbookLevel(Decimal("0.49"), Decimal("100"))],
            bid_levels=[OrderbookLevel(Decimal("0.48"), Decimal("100"))],
        ),
    ]
    assert compute_opportunity_depth_aware("KXPHANTOM-DEEP", legs, Decimal("1.0"), Decimal("1")) is None


def test_incoherent_depth_legs_names_only_offending_legs():
    good = DepthLegQuote(
        "GOOD", ask_levels=[OrderbookLevel(Decimal("0.49"), Decimal("100"))],
        bid_levels=[OrderbookLevel(Decimal("0.48"), Decimal("100"))],
    )
    crossed_deep = DepthLegQuote(
        "CROSSEDDEEP",
        ask_levels=[OrderbookLevel(Decimal("0.30"), Decimal("10")), OrderbookLevel(Decimal("0.20"), Decimal("50"))],
        bid_levels=[OrderbookLevel(Decimal("0.29"), Decimal("50"))],
    )
    empty = DepthLegQuote("EMPTY", ask_levels=[], bid_levels=[OrderbookLevel(Decimal("0.10"), Decimal("10"))])
    assert incoherent_depth_legs([good, crossed_deep, empty]) == [crossed_deep, empty]
    assert incoherent_depth_legs([good]) == []
