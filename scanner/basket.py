"""Core intra-market rebalancing arb math.

For a mutually exclusive event with legs L1..Ln (exactly one resolves YES),
buying YES on every leg pays out exactly $1 regardless of outcome. So:

    buy_cost = sum(ask_i) + sum(fee(ask_i))   -- flag if < arb.buy_basket_max_cost_usd

The max size of the basket trade is capped by its thinnest leg — a
Stage 2/3 shortlist candidate must clear scanner.min_depth_contracts on
every leg, not just on average.

Two directions: buy_basket (compute_opportunity, above) and buy_all_no
(compute_no_opportunity, below). sell_basket — selling YES on every leg
when sum(bids) is overpriced — was removed 2026-08-26 as never verified
live; buy_all_no is economically that same trade (Kalshi's fee formula
depends only on price*(1-price), symmetric under price -> 1-price, so
pricing a NO-basket off the bid side reproduces sell_basket's edge
exactly — see compute_no_opportunity's docstring) reintroduced behind
execution.allow_no_direction (default False, see execution/executor.py)
rather than shipped armed by default.

Depth-aware pricing (compute_opportunity_depth_aware, below): compute_opportunity
above prices only the top of book. When scanner.max_book_depth_levels > 1,
stream/monitor.py's LocalBook.yes_ask_levels/yes_bid_levels supply more than
one price level per leg, and compute_opportunity_depth_aware walks them to
find the true cost of filling a requested quantity that may exceed what's
resting at the best price alone. At max_book_depth_levels == 1 it reproduces
compute_opportunity's output exactly — see that function's docstring and
tests/test_basket.py's equivalence test.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Optional

from kalshi.client import OrderbookLevel
from scanner.fees import taker_fee


@dataclass(frozen=True)
class LegQuote:
    ticker: str
    best_ask: Decimal
    ask_size: Decimal
    best_bid: Decimal
    bid_size: Decimal


def incoherent_legs(legs: list[LegQuote]) -> list[LegQuote]:
    """Returns the legs whose quotes a real order book cannot produce.

    A locked or crossed book (best_bid >= best_ask) is a free instant
    round-trip, so it never survives on a live exchange. Seeing one does not
    mean an edge exists — it means the book the quote came from is wrong.
    stream/monitor.py maintains its books from incremental WS deltas, and a
    delta applied to a level left over from a previous connection produces
    exactly this shape: an impossibly good synthesized ask, usually carrying
    impossibly large size. Those corrupt quotes are also what defeat the
    downstream unwind-loss guard, since a crossed book makes
    execution/executor.py's estimate_worst_case_unwind_loss read as a
    *negative* spread and so approve the trade it should block.

    Prices outside (0, 1) are structurally impossible for a $1 binary too,
    and would otherwise raise out of taker_fee in the middle of a WS tick
    rather than just dropping the basket.
    """
    return [
        leg for leg in legs
        if not (
            Decimal(0) < leg.best_ask < Decimal(1)
            and Decimal(0) < leg.best_bid < Decimal(1)
            and leg.best_bid < leg.best_ask
        )
    ]


@dataclass(frozen=True)
class BasketOpportunity:
    event_ticker: str
    direction: str          # "buy_basket" or "buy_all_no" — threaded through logging/JSONL/the dashboard/execution/executor.py as the field that decides which side each leg trades
    legs: list[str]
    leg_quotes: list[LegQuote]   # same legs, full quotes — execution/executor.py uses these for order price/size
    basket_price_usd: Decimal   # total cost per contract to acquire the complete basket (buy_cost for buy_basket, sum of synthesized NO asks for buy_all_no)
    edge_usd: Decimal           # guaranteed payout minus basket_price_usd, per contract — payout is $1 for buy_basket, (len(legs)-1) dollars for buy_all_no
    max_size: Decimal           # min(available size) across legs
    fee_multiplier: Decimal = Decimal(1)


def compute_opportunity(
    event_ticker: str,
    legs: list[LegQuote],
    buy_basket_max_cost_usd: Decimal,
    min_depth_contracts: Decimal,
    contract_count: Decimal = Decimal(1),
    fee_multiplier: Decimal = Decimal(1),
) -> Optional[BasketOpportunity]:
    """Returns a BasketOpportunity if the fee-adjusted buy cost clears
    buy_basket_max_cost_usd *and* the depth floor, else None."""
    if not legs:
        return None

    if contract_count <= 0:
        raise ValueError("contract_count must be positive")

    # Structurally impossible quotes are a corrupt local book, never an edge —
    # see incoherent_legs. Checked before the fee math both because it is the
    # cheaper rejection and because taker_fee raises on an out-of-range price.
    if incoherent_legs(legs):
        return None

    # Fees are rounded once per leg for the complete execution quantity.
    # basket_price_usd remains a per-contract figure for compatibility with
    # the capital and execution layers.
    buy_cost = sum(
        (
            leg.best_ask
            + taker_fee(leg.best_ask, contract_count, fee_multiplier) / contract_count
            for leg in legs
        ),
        start=Decimal(0),
    )
    if buy_cost >= buy_basket_max_cost_usd:
        return None

    max_size = min(leg.ask_size for leg in legs)
    if max_size < min_depth_contracts:
        return None

    return BasketOpportunity(
        event_ticker=event_ticker,
        direction="buy_basket",
        legs=[leg.ticker for leg in legs],
        leg_quotes=list(legs),
        basket_price_usd=buy_cost,
        edge_usd=Decimal(1) - buy_cost,
        max_size=max_size,
        fee_multiplier=fee_multiplier,
    )


def compute_no_opportunity(
    event_ticker: str,
    legs: list[LegQuote],
    min_edge_usd: Decimal,
    min_depth_contracts: Decimal,
    contract_count: Decimal = Decimal(1),
    fee_multiplier: Decimal = Decimal(1),
) -> Optional[BasketOpportunity]:
    """Returns a BasketOpportunity (direction="buy_all_no") if buying one
    NO contract on every leg of this N-leg exhaustive partition clears
    min_edge_usd after fees, else None.

    Exactly one leg's YES resolves true, so exactly N-1 legs' NO resolves
    true — buying one NO on every leg guarantees a payout of (N-1) dollars,
    not $1. Kalshi's book is bid-only per side, so a NO-basket's ask is
    synthesized the same way compute_opportunity already synthesizes a YES
    ask off the opposite side's bid: no_ask = 1 - best_bid. LegQuote needs
    no new fields for this — best_bid/bid_size (already populated for the
    YES-direction coherence guard) are exactly the price and size this
    direction consumes.

    Economically this reproduces the sell_basket direction this repo built
    once and deliberately deleted 2026-08-26 as never validated live (see
    module docstring): Kalshi's fee formula depends only on
    price*(1-price), which is symmetric under price -> 1-price, so
    taker_fee(1 - best_bid_i, ...) == taker_fee(best_bid_i, ...) exactly,
    and the resulting edge — sum(best_bid_i) - (N-1) - fees — is identical
    either way. Kept as a function separate from compute_opportunity
    (rather than a direction branch inside it) so each side's math and
    tests stay independently reasoned about; incoherent_legs already
    guards both sides of the book symmetrically and needs no change here.
    """
    if not legs:
        return None

    if contract_count <= 0:
        raise ValueError("contract_count must be positive")

    if incoherent_legs(legs):
        return None

    payout = Decimal(len(legs) - 1)
    no_ask = {leg.ticker: Decimal(1) - leg.best_bid for leg in legs}
    cost = sum(
        (
            no_ask[leg.ticker]
            + taker_fee(no_ask[leg.ticker], contract_count, fee_multiplier) / contract_count
            for leg in legs
        ),
        start=Decimal(0),
    )
    edge = payout - cost
    if edge <= min_edge_usd:
        return None

    max_size = min(leg.bid_size for leg in legs)
    if max_size < min_depth_contracts:
        return None

    return BasketOpportunity(
        event_ticker=event_ticker,
        direction="buy_all_no",
        legs=[leg.ticker for leg in legs],
        leg_quotes=list(legs),
        basket_price_usd=cost,
        edge_usd=edge,
        max_size=max_size,
        fee_multiplier=fee_multiplier,
    )


@dataclass(frozen=True)
class DepthLegQuote:
    ticker: str
    ask_levels: list[OrderbookLevel]   # ascending price — best (cheapest) ask first
    bid_levels: list[OrderbookLevel]   # descending price — best (highest) bid first


def incoherent_depth_legs(legs: list[DepthLegQuote]) -> list[DepthLegQuote]:
    """Depth-aware equivalent of incoherent_legs: every walked ask level
    must be a structurally possible price, and the deepest walked ask must
    still clear the best bid. A book that looks coherent at the top level
    but crosses several levels deep is exactly the corrupted-post-reconnect
    shape incoherent_legs exists to catch (see that function's docstring)
    — just invisible if only level 1 is checked."""
    bad = []
    for leg in legs:
        if not leg.ask_levels or not leg.bid_levels:
            bad.append(leg)
            continue
        best_bid = leg.bid_levels[0].price
        coherent = (
            Decimal(0) < best_bid < Decimal(1)
            and all(Decimal(0) < lvl.price < Decimal(1) for lvl in leg.ask_levels)
            and all(lvl.price > best_bid for lvl in leg.ask_levels)
        )
        if not coherent:
            bad.append(leg)
    return bad


def compute_opportunity_depth_aware(
    event_ticker: str,
    legs: list[DepthLegQuote],
    buy_basket_max_cost_usd: Decimal,
    min_depth_contracts: Decimal,
    contract_count: Decimal = Decimal(1),
    fee_multiplier: Decimal = Decimal(1),
) -> Optional[BasketOpportunity]:
    """Depth-aware sibling of compute_opportunity: walks each leg's ask
    ladder (as many levels as DepthLegQuote.ask_levels carries — see
    stream/monitor.py's LocalBook.yes_ask_levels) to price contract_count
    contracts, spanning more than one price level when the best level alone
    can't cover it, rather than assuming the whole quantity fills at the
    best price. Fee is computed per walked price tranche and summed, never
    once at a blended price — Kalshi's fee is quadratic in price, so pricing
    the fee off a blended average would not match what walking the book
    actually costs.

    Equivalence: when every leg's ask_levels has exactly one entry
    (scanner.max_book_depth_levels == 1) and contract_count does not exceed
    that single level's size for any leg — the calling convention
    stream/monitor.py already follows, capping contract_count to
    min(ask_size) before calling compute_opportunity today — this produces
    byte-identical basket_price_usd/edge_usd/max_size to compute_opportunity.
    See tests/test_basket.py's explicit equivalence test.

    Returns None (rather than pricing a partial fill as if it were
    complete) if the walked levels cannot cover contract_count on some leg
    — that leg's remaining ask_levels were exhausted first.
    """
    if not legs:
        return None
    if contract_count <= 0:
        raise ValueError("contract_count must be positive")
    if incoherent_depth_legs(legs):
        return None

    buy_cost = Decimal(0)
    for leg in legs:
        remaining = contract_count
        leg_cost = Decimal(0)
        for level in leg.ask_levels:
            if remaining <= 0:
                break
            qty = min(level.size, remaining)
            if qty <= 0:
                continue
            leg_cost += level.price * qty + taker_fee(level.price, qty, fee_multiplier)
            remaining -= qty
        if remaining > 0:
            return None   # requested quantity exceeds depth within the walked levels
        buy_cost += leg_cost / contract_count
    if buy_cost >= buy_basket_max_cost_usd:
        return None

    max_size = min(
        sum((lvl.size for lvl in leg.ask_levels), start=Decimal(0))
        for leg in legs
    )
    if max_size < min_depth_contracts:
        return None

    return BasketOpportunity(
        event_ticker=event_ticker,
        direction="buy_basket",
        legs=[leg.ticker for leg in legs],
        leg_quotes=[
            LegQuote(
                ticker=leg.ticker,
                best_ask=leg.ask_levels[0].price,
                ask_size=leg.ask_levels[0].size,
                best_bid=leg.bid_levels[0].price,
                bid_size=leg.bid_levels[0].size,
            )
            for leg in legs
        ],
        basket_price_usd=buy_cost,
        edge_usd=Decimal(1) - buy_cost,
        max_size=max_size,
        fee_multiplier=fee_multiplier,
    )
