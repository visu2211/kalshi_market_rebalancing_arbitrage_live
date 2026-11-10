"""Maker quoter: given a BasketOpportunity's leg quotes, decides where to
rest buy orders and whether the aggregate margin clears
target_margin_per_basket. Pure — no I/O, no state.

v1 pricing prior: per-leg mid-price. For each leg
    mid_i   = (best_bid + best_ask) / 2
    quote_i = mid_i - quote_offset_ticks * tick
and the basket is quoted only if, assuming every quote fills,
    sum(quote_i) + estimated_entry_fees  <  1 - target_margin_per_basket
                                                   (for buy_basket)
    sum(no_quote_i) + estimated_entry_fees  <  (N - 1) - target_margin_per_basket
                                                   (for buy_all_no)
where for buy_all_no the NO-side rest price is synthesized symmetrically to
scanner.basket.compute_no_opportunity's YES-bid math — no new fields on
LegQuote needed.

Smarter priors (rolling-average mid, normalized-to-sum-1 priors) can
replace _target_quote without touching any other module in this package.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Optional

from kalshi.client import OrderAction, Side
from scanner.basket import BasketOpportunity, LegQuote, incoherent_legs
from scanner.fees import taker_fee

# Kalshi binary markets price in whole cents, so one tick is $0.01.
# Kept as a module-level constant rather than threaded as a parameter so
# the only knob is quote_offset_ticks — number of ticks, not tick size.
TICK = Decimal("0.01")


@dataclass(frozen=True)
class LegQuoteTarget:
    """One leg's intended resting order. `price` is what we'd POST; `mid`
    is the mid-market used to derive it (carried so order_manager can
    detect drift and decide to requote without recomputing the prior)."""

    ticker: str
    side: Side                          # YES for buy_basket, NO for buy_all_no
    action: OrderAction                 # always BUY in maker v1
    price: Decimal
    size: Decimal
    mid: Decimal
    entry_fee: Decimal                  # fee we'd pay if this quote fills at `price` at `size`


@dataclass(frozen=True)
class MakerQuote:
    """The full basket's target quotes, plus the expected economics IF
    every leg fills. Emitted only when the basket clears the target
    margin — otherwise quote_basket returns None."""

    event_ticker: str
    direction: str                       # mirrors BasketOpportunity.direction
    leg_quotes: list[LegQuoteTarget]
    expected_basket_cost_usd: Decimal    # sum(quote prices) plus per-leg entry fees, per contract
    expected_payout_usd: Decimal         # $1 for buy_basket, (N-1) for buy_all_no
    expected_margin_usd: Decimal         # payout - cost (per contract)
    size: Decimal                        # same size on every leg


def _mid(leg: LegQuote) -> Decimal:
    """Simple mid-price prior. If the book is crossed/locked, the caller
    already rejected via incoherent_legs — so this assumes a sane book."""
    return (leg.best_bid + leg.best_ask) / Decimal(2)


def _target_quote(direction: str, leg: LegQuote, offset_ticks: int) -> tuple[Side, Decimal, Decimal]:
    """Return (side_to_buy, target_price, reference_mid) for a resting
    BUY on this leg given the basket direction.

    - buy_basket: we want YES on this leg. Mid is of the YES side. Rest a
      BUY YES at mid_yes - offset.
    - buy_all_no: we want NO on this leg. Mid is of the NO side, which
      Kalshi doesn't quote directly — synthesize it the same way
      scanner.basket.compute_no_opportunity synthesizes the NO ask:
      mid_no = 1 - mid_yes. Rest a BUY NO at mid_no - offset.
    """
    mid_yes = _mid(leg)
    offset = Decimal(offset_ticks) * TICK
    if direction == "buy_basket":
        return Side.YES, mid_yes - offset, mid_yes
    elif direction == "buy_all_no":
        mid_no = Decimal(1) - mid_yes
        return Side.NO, mid_no - offset, mid_no
    raise ValueError(f"unknown direction: {direction}")


def quote_basket(
    opportunity: BasketOpportunity,
    *,
    quote_offset_ticks: int,
    target_margin_per_basket: Decimal,
    size: Decimal,
    fee_multiplier: Optional[Decimal] = None,
) -> Optional[MakerQuote]:
    """Compute target maker quotes for `opportunity`. Returns None (no
    orders will be posted) if the resulting basket's expected margin
    doesn't clear `target_margin_per_basket`, or if the prior leg book is
    structurally impossible (see scanner.basket.incoherent_legs — same
    guard as the taker uses).

    `size` is applied uniformly to every leg. Caller is expected to have
    already respected max_inventory_per_event_usd + any max-size cap.

    `fee_multiplier` defaults to the opportunity's own multiplier — only
    overridden for symmetric testing. Fees are per-leg quadratic the same
    way scanner.basket prices them for the taker side.
    """
    if quote_offset_ticks < 1:
        raise ValueError("quote_offset_ticks must be >= 1")
    if size <= 0:
        raise ValueError("size must be positive")
    if not opportunity.leg_quotes:
        return None
    if incoherent_legs(opportunity.leg_quotes):
        return None

    fee_mult = fee_multiplier if fee_multiplier is not None else opportunity.fee_multiplier

    leg_quotes: list[LegQuoteTarget] = []
    total_price = Decimal(0)
    total_fee = Decimal(0)
    for leg in opportunity.leg_quotes:
        side, price, mid = _target_quote(opportunity.direction, leg, quote_offset_ticks)
        # A quote whose price fell off the tradeable (0, 1) range (e.g. a
        # near-zero mid that the offset would push below $0.01) is
        # structurally impossible — Kalshi would reject it at submission.
        # Drop the basket rather than ship a quote that can't exist.
        if not (Decimal(0) < price < Decimal(1)):
            return None
        fee = taker_fee(price, size, fee_mult)
        leg_quotes.append(LegQuoteTarget(
            ticker=leg.ticker, side=side, action=OrderAction.BUY,
            price=price, size=size, mid=mid, entry_fee=fee,
        ))
        total_price += price
        total_fee += fee

    expected_cost_per_contract = total_price + (total_fee / size)
    if opportunity.direction == "buy_basket":
        payout = Decimal(1)
    else:
        payout = Decimal(len(opportunity.leg_quotes) - 1)
    margin = payout - expected_cost_per_contract

    if margin < target_margin_per_basket:
        return None

    return MakerQuote(
        event_ticker=opportunity.event_ticker,
        direction=opportunity.direction,
        leg_quotes=leg_quotes,
        expected_basket_cost_usd=expected_cost_per_contract,
        expected_payout_usd=payout,
        expected_margin_usd=margin,
        size=size,
    )


def mid_drift_ticks(previous_mid: Decimal, current_mid: Decimal) -> int:
    """How many ticks (always non-negative) the mid has moved since we
    posted. Ignores direction — the order_manager decides whether a drift
    is adverse based on whether we rest on the bid or ask side.

    Integer rounding is intentional: tick-aligned Kalshi books don't have
    a meaningful notion of "0.7 ticks," so a requote decision should fire
    only when the mid has drifted a whole tick."""
    diff = abs(current_mid - previous_mid)
    return int((diff / TICK).quantize(Decimal(1)))
