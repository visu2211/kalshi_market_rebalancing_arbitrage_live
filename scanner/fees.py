"""Kalshi's parabolic taker fee schedule.

    fee_cents = ceil(0.07 * P * (1-P) * 100)

where P is the fill price as a USD probability (0 < P < 1). Fee peaks
around P=0.50 (max theoretical liquidity/risk to the exchange) and shrinks
toward the tails — a $0.03 or $0.97 fill costs ~1 cent, a $0.50 fill costs
~2 cents, per contract.
"""

from __future__ import annotations

from decimal import ROUND_CEILING, Decimal

_CENT = Decimal("0.01")
_FEE_RATE = Decimal("0.07")
_HUNDRED = Decimal("100")


def taker_fee(price: Decimal) -> Decimal:
    """Per-contract taker fee in USD for a fill at `price`."""
    if not (Decimal(0) < price < Decimal(1)):
        raise ValueError(f"taker_fee: price must be strictly between 0 and 1, got {price}")
    raw_cents = _FEE_RATE * price * (Decimal(1) - price) * _HUNDRED
    fee_cents = raw_cents.to_integral_value(rounding=ROUND_CEILING)
    return fee_cents * _CENT
