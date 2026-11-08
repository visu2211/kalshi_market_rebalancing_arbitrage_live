
"""Capital-aware execution ranking — when multiple detected opportunities
are open at once and available demo-account capital can't fund all of
them, decides which to execute and in what order.

Per the project owner's choice (2026-08-26): rank by return per dollar per
hour of capital lock-up, not raw dollar edge. A basket that returns 2% in
1 hour is a better use of limited capital than one that returns 3% but
ties the money up for 10 hours — the faster one frees capital sooner to
redeploy into whatever opens next, within the same trading day. Hours to
settlement comes from Stage 1's EventCandidate.expected_settlement_time —
the same field that decided which events are same-day in the first place
(scanner/filters.py) — so an opportunity right at the end of the day is
treated as maximally time-constrained, not undefined/infinite.

Every opportunity is a buy_basket (sell_basket was removed 2026-08-26 —
see scanner/basket.py and README.md), so estimated_capital_required is
exact: basket_price_usd is already sum(ask_i) + fees, so
basket_price_usd * max_size is literally the dollar cost to buy every leg
at max_size.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from scanner.basket import BasketOpportunity

_MIN_HOURS = Decimal("0.01")   # floor so an opportunity settling right now doesn't divide by ~0


@dataclass(frozen=True)
class ExecutionCandidate:
    event_ticker: str
    opportunity: BasketOpportunity
    expected_settlement_time: datetime


def hours_to_settlement(now: datetime, expected_settlement_time: datetime) -> Decimal:
    """Hours until capital committed to this event frees up.

    CAUTION: the _MIN_HOURS floor also swallows NEGATIVE deltas, so an event
    already past its settlement returns the floor and reads as maximally fast.
    That is meaningless for an overdue market — see _rank_key, which is why
    ranking must not consult this number alone.
    """
    delta_hours = Decimal(str((expected_settlement_time - now).total_seconds() / 3600))
    return max(delta_hours, _MIN_HOURS)


def estimated_capital_required(opportunity: BasketOpportunity) -> Decimal:
    return opportunity.basket_price_usd * opportunity.max_size


def hourly_return_rate(opportunity: BasketOpportunity, hours: Decimal) -> Decimal:
    """Edge as a fraction of capital committed, per hour that capital is
    locked up. Used only to rank competing candidates against each other,
    not as a literal APR."""
    return (opportunity.edge_usd / opportunity.basket_price_usd) / hours


def is_settlement_overdue(candidate: ExecutionCandidate, now: datetime) -> bool:
    """True once an event is past its own expected settlement but still open.

    Kalshi keeps these tradeable while settlement is pending, so they reach the
    ranker like any other candidate.
    """
    return candidate.expected_settlement_time <= now


def _rank_key(candidate: ExecutionCandidate, now: datetime) -> tuple[int, Decimal]:
    """Sort key: live candidates first, then by capital velocity within each tier.

    The tier exists because hourly_return_rate cannot be trusted for an overdue
    event. hours_to_settlement floors a NEGATIVE remaining time at _MIN_HOURS,
    so an event that expired hours ago reads as "capital frees in 36 seconds" —
    the best score obtainable — when the truth is the reverse: settlement lag on
    an overdue market is unknown and empirically long (observed live: expired
    20h earlier and still pending), making it the slowest way to tie up capital,
    not the fastest. Unranked, a 5c overdue basket outranked an 8c live one by
    130x, and under max_baskets_per_day=1 that ordering IS the decision.

    Overdue candidates stay eligible — a complete basket still pays $1 whenever
    settlement lands, so this is deliberately a demotion, not a ban. They simply
    stop pre-empting live candidates on a scoring artifact.
    """
    rate = hourly_return_rate(candidate.opportunity, hours_to_settlement(now, candidate.expected_settlement_time))
    return (0 if is_settlement_overdue(candidate, now) else 1, rate)


def select_executable(
    candidates: list[ExecutionCandidate],
    available_capital: Decimal,
    now: datetime,
) -> list[ExecutionCandidate]:
    """Ranks `candidates` by _rank_key (live before overdue, then by capital
    velocity within each tier) and greedily selects as many as fit within
    available_capital. A candidate
    that doesn't fit is skipped rather than sized down (every selected
    basket still fills at its full priced max_size), and a cheaper,
    lower-ranked candidate after it that DOES fit is still taken —
    knapsack-greedy, not exact-optimal, but a reasonable approximation for
    the small number of concurrently-open opportunities this realistically
    ever sees at once."""
    ranked = sorted(candidates, key=lambda c: _rank_key(c, now), reverse=True)
    selected: list[ExecutionCandidate] = []
    remaining = available_capital
    for candidate in ranked:
        cost = estimated_capital_required(candidate.opportunity)
        if cost > remaining:
            continue
        selected.append(candidate)
        remaining -= cost
    return selected
