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


@dataclass(frozen=True)
class BasketOpportunity:
    event_ticker: str
    direction: str          # always "buy_basket" — kept as a field (rather than removed) since it's threaded through logging/JSONL/the dashboard as a label
    legs: list[str]
    leg_quotes: list[LegQuote]   # same legs, full quotes — execution/executor.py uses these for order price/size
    basket_price_usd: Decimal   # buy_cost
    edge_usd: Decimal           # distance from $1.00, per contract
    max_size: Decimal           # min(ask size) across legs


def compute_opportunity(
    event_ticker: str,
    legs: list[LegQuote],
    buy_basket_max_cost_usd: Decimal,
    min_depth_contracts: Decimal,
) -> Optional[BasketOpportunity]:
    """Returns a BasketOpportunity if the fee-adjusted buy cost clears
    buy_basket_max_cost_usd *and* the depth floor, else None."""
    if not legs:
        return None

    buy_cost = sum((leg.best_ask + taker_fee(leg.best_ask) for leg in legs), start=Decimal(0))
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
    )
