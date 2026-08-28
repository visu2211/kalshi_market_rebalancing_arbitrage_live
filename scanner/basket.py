"""Core intra-market rebalancing arb math.

For a mutually exclusive event with legs L1..Ln (exactly one resolves YES),
buying YES on every leg pays out exactly $1 regardless of outcome. So:

    buy_cost = sum(ask_i) + sum(fee(ask_i))   -- flag if < arb.buy_basket_max_cost_usd

The max size of the basket trade is capped by its thinnest leg — a
Stage 2/3 shortlist candidate must clear scanner.min_depth_contracts on
every leg, not just on average.

buy_basket ONLY (sell_basket removed 2026-08-26): the opposite direction —
selling YES on every leg when sum(bids) is overpriced — was never verified
live (every real opportunity observed was buy_basket) and would have
doubled the surface area of execution/executor.py's unwind logic for a
path that had never actually been exercised. Deliberately dropped rather
than shipped untested — see README.md's "What's deliberately not
implemented" section.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Optional

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
    direction: str          # always "buy_basket" — kept as a field (rather than removed) since it's threaded through logging/JSONL/the dashboard as a label
    legs: list[str]
    leg_quotes: list[LegQuote]   # same legs, full quotes — execution/executor.py uses these for order price/size
    basket_price_usd: Decimal   # buy_cost
    edge_usd: Decimal           # distance from $1.00, per contract
    max_size: Decimal           # min(ask size) across legs
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
