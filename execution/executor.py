"""Places the legs of a detected BasketOpportunity against Kalshi's DEMO
account — never prod. Detection (scanner/, stream/monitor.py) runs against
real prod order books, because that's the only place there's a real book to
find edges in; Kalshi's demo environment is a separate, effectively empty
paper-trading book (confirmed in the sibling arb-bot project: its demo
account had a $0 balance and no observed counterparties). So orders placed
here are very likely to rest unfilled, or even get rejected outright with
insufficient_balance until the demo account is funded via Kalshi's demo UI
— that's expected, not a bug in this code.

_live variant — no-slippage fill: opportunity.max_size and each leg's price
already come from scanner.basket.compute_opportunity, which caps size to
exactly what's resting at the observed best price on the thinnest leg (see
that module's docstring) — never a deeper, worse-priced book level. On top
of that, every order here is placed with a short expiration_ts (see
_IOC_WINDOW_S and kalshi.client.KalshiClient.place_order's docstring): it
either matches immediately against the depth it was priced against, or
auto-cancels, so a leg can never sit on the book and fill later at a price
that's moved away from the one the opportunity was priced at.

Buy-basket only (rebuilt 2026-08-26 after a real incident): every
BasketOpportunity is a buy_basket now — see scanner/basket.py and
README.md's "What's deliberately not implemented" for why the opposite
(sell_basket) direction was dropped rather than shipped. That
simplification is why unwinding is always a SELL below, never a BUY-to-cover.

Batched, atomic-attempt, unwind-on-mismatch, confirmed: every leg used to
be placed sequentially, one round-trip at a time, fire-and-forget. Against
a funded demo account with real fills, that surfaced a serious problem —
detection prices come from prod's real book, but fills happen against
demo's separate, mostly-empty one, so legs fill asymmetrically instead of
together. One live match (KXSAUDIPLGAME-26AUG26FAIFAT, a 3-way
FAI/FAT/TIE basket) filled FAT and TIE repeatedly across several reopened
opportunities while FAI never filled at all, leaving a real $100+ naked,
unhedged position instead of the promised risk-free basket. Three changes
fix this:

  1. All legs are now submitted in one place_orders_batch() call instead
     of N sequential ones — this removes the network-round-trip-sized
     window during which the book could move between leg 1 and leg N.
     It does NOT manufacture liquidity that isn't there; if a leg
     genuinely has no resting demo depth, batching alone can't fill it.
  2. After the batch comes back, compute_unwind_legs() checks whether
     every leg actually filled to the same size. If not — even if only
     one leg got zero fill — every leg that DID fill above that common
     floor gets an immediate offsetting SELL (aggressively priced at
     _UNWIND_AGGRESSIVE_SELL_PRICE to guarantee crossing, at the cost of
     a second round of taker fees) to bring the position back down to a
     matched, genuinely risk-free basket (or fully flat, if the floor is
     zero). This is the "legging risk" pattern real arb desks fall back
     to when true atomic multi-leg execution isn't available at the
     exchange level — Kalshi has no combo-order type for a whole basket,
     so reactive unwind is the correct tier here, not a workaround.
  3. The unwind isn't just fired and trusted — _confirm_unwind polls each
     unwind order (KalshiClient.get_order) until it's fully filled or
     _UNWIND_CONFIRM_TIMEOUT_S elapses. An aggressive price guarantees
     crossing IF there's any resting liquidity on the other side at all,
     but not otherwise — a leg with truly no counterparty on either side
     would leave the unwind order resting indefinitely, unconfirmed, if
     nobody ever checked back. AtomicExecutionResult.unwind_confirmed
     tells the caller whether the flatten is actually verified complete,
     not just attempted — stream/monitor.py trips the circuit breaker
     immediately (not on the normal mismatch-counting schedule) on a
     confirmed-failed unwind, since that's strictly worse than an
     ordinary mismatch: a naked position AND a failed attempt to fix it.

Any unfilled remainder is also canceled immediately (KalshiClient.cancel_order)
rather than left to expire on its own via expiration_ts, so the fill
picture used for the unwind decision can't shift under it afterward.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Optional

from kalshi.client import (
    ExchangeError,
    KalshiClient,
    OrderAction,
    OrderRequest,
    OrderResult,
    OrderStatus,
    OrderType,
    Side,
)
from scanner.basket import BasketOpportunity

_IOC_WINDOW_S = 2   # auto-cancel any unfilled remainder this many seconds out, as a backstop behind the explicit cancel below

# Priced at the extreme low end of Kalshi's 0.01-0.99 range on purpose —
# the goal is guaranteed crossing against any resting liquidity to
# eliminate naked exposure as fast as possible, not price optimization.
# Accepting a worse price here is the deliberate cost of insurance against
# open-ended directional risk.
_UNWIND_AGGRESSIVE_SELL_PRICE = Decimal("0.01")

_UNWIND_CONFIRM_POLL_INTERVAL_S = 0.5
_UNWIND_CONFIRM_TIMEOUT_S = 5.0


@dataclass
class LegExecutionResult:
    ticker: str
    action: OrderAction
    limit_price: Decimal
    size: Decimal
    order_result: Optional[OrderResult]   # None if the request itself was rejected/errored
    error: Optional[str] = None           # set iff order_result is None, or order_result.status == REJECTED


@dataclass
class UnwindLeg:
    ticker: str
    excess_size: Decimal


@dataclass
class AtomicExecutionResult:
    leg_results: list[LegExecutionResult]           # the basket's original per-leg placement results
    unwind_results: list[LegExecutionResult] = field(default_factory=list)   # offsetting SELL orders placed to flatten a mismatch, if any
    mismatched: bool = False                         # True iff the legs filled unevenly and an unwind was attempted — see module docstring
    unwind_confirmed: bool = True                    # False iff an unwind was attempted but didn't confirm fully filled within the timeout — see _confirm_unwind


def compute_unwind_legs(filled_sizes: dict[str, Decimal]) -> list[UnwindLeg]:
    """Given each leg's actual filled size (0 for a leg that was rejected
    or never filled), returns which legs need an offsetting SELL and how
    much, to bring every leg down to the common minimum fill across the
    whole basket — the only quantity at which the basket is genuinely
    risk-free. A basket where every leg filled equally (including the
    all-zero case, where nothing filled at all) needs no unwind."""
    if not filled_sizes:
        return []
    floor = min(filled_sizes.values())
    return [
        UnwindLeg(ticker=ticker, excess_size=size - floor)
        for ticker, size in filled_sizes.items()
        if size > floor
    ]


def _format_order_error(raw: dict) -> str:
    error = raw.get("error")
    if isinstance(error, dict):
        return f"{error.get('code', 'unknown_error')}: {error.get('message', '')}".strip(": ")
    return str(error)


async def _place_batch(
    demo_client: KalshiClient,
    orders: list[OrderRequest],
    action: OrderAction,
) -> list[LegExecutionResult]:
    """Places `orders` in one batch call and maps the result back to
    LegExecutionResult per order, whether the failure was request-level
    (ExchangeError on the whole call) or per-order (a REJECTED
    OrderResult inside an otherwise-successful batch response)."""
    try:
        order_results = await demo_client.place_orders_batch(orders)
    except ExchangeError as exc:
        return [
            LegExecutionResult(ticker=o.market_id, action=action, limit_price=o.limit_price, size=o.size, order_result=None, error=str(exc))
            for o in orders
        ]

    return [
        LegExecutionResult(
            ticker=order.market_id,
            action=action,
            limit_price=order.limit_price,
            size=order.size,
            order_result=result,
            error=_format_order_error(result.raw) if result.status == OrderStatus.REJECTED else None,
        )
        for order, result in zip(orders, order_results)
    ]


async def _cancel_unfilled_remainders(demo_client: KalshiClient, leg_results: list[LegExecutionResult]) -> None:
    for lr in leg_results:
        r = lr.order_result
        if r is not None and r.status in (OrderStatus.OPEN, OrderStatus.PARTIALLY_FILLED) and r.remaining_size > 0:
            try:
                await demo_client.cancel_order(r.order_id)
            except ExchangeError:
                pass   # already filled or canceled by the time we got here — not fatal, fill picture is already locked in


async def _confirm_unwind(demo_client: KalshiClient, unwind_results: list[LegExecutionResult]) -> bool:
    """Polls every unwind order that didn't come back immediately fully
    filled, until each one confirms fully filled or _UNWIND_CONFIRM_TIMEOUT_S
    elapses. Returns True iff every unwind order is confirmed fully
    filled — False is a genuine, confirmed failure to flatten the
    mismatch (not just "we didn't check"), which the caller
    (stream/monitor.py) escalates harder than an ordinary mismatch."""
    pending = {
        lr.order_result.order_id: lr
        for lr in unwind_results
        if lr.order_result is not None and lr.order_result.remaining_size > 0
    }
    if not pending:
        return all(lr.order_result is not None and lr.order_result.remaining_size <= 0 for lr in unwind_results)

    deadline = time.monotonic() + _UNWIND_CONFIRM_TIMEOUT_S
    while pending and time.monotonic() < deadline:
        await asyncio.sleep(_UNWIND_CONFIRM_POLL_INTERVAL_S)
        for order_id in list(pending):
            try:
                result = await demo_client.get_order(order_id)
            except ExchangeError:
                continue   # transient — try again next poll, until the deadline
            if result.remaining_size <= 0:
                del pending[order_id]

    return not pending


async def execute_opportunity(demo_client: KalshiClient, opportunity: BasketOpportunity) -> AtomicExecutionResult:
    """Places every leg of `opportunity` in one batched BUY call, cancels
    any unfilled remainder immediately, and — if the legs filled unevenly
    — fires an immediate offsetting SELL to bring the position back to a
    matched, risk-free basket (or fully flat), then polls to confirm that
    unwind actually completed. See module docstring.

    Every leg is bought at its leg_quote.best_ask (the same price
    scanner.basket used to compute the edge, so a fill — if the demo book
    ever has a counterparty — would realize close to the priced edge), at
    opportunity.max_size contracts each (equal quantity across the basket
    — that's what makes the $1 payout/cost guarantee hold *if* every leg
    actually fills to that size, which this function no longer just
    assumes)."""
    expiration_ts = int(time.time()) + _IOC_WINDOW_S

    orders = [
        OrderRequest(
            market_id=leg.ticker,
            side=Side.YES,
            action=OrderAction.BUY,
            order_type=OrderType.LIMIT,
            size=opportunity.max_size,
            limit_price=leg.best_ask,
            expiration_ts=expiration_ts,
        )
        for leg in opportunity.leg_quotes
    ]

    leg_results = await _place_batch(demo_client, orders, OrderAction.BUY)
    await _cancel_unfilled_remainders(demo_client, leg_results)

    filled_sizes = {lr.ticker: (lr.order_result.filled_size if lr.order_result else Decimal(0)) for lr in leg_results}
    unwind_legs = compute_unwind_legs(filled_sizes)
    if not unwind_legs:
        return AtomicExecutionResult(leg_results=leg_results, unwind_results=[], mismatched=False, unwind_confirmed=True)

    unwind_orders = [
        OrderRequest(
            market_id=leg.ticker, side=Side.YES, action=OrderAction.SELL, order_type=OrderType.LIMIT,
            size=leg.excess_size, limit_price=_UNWIND_AGGRESSIVE_SELL_PRICE,
        )
        for leg in unwind_legs
    ]
    unwind_results = await _place_batch(demo_client, unwind_orders, OrderAction.SELL)
    unwind_confirmed = await _confirm_unwind(demo_client, unwind_results)

    return AtomicExecutionResult(leg_results=leg_results, unwind_results=unwind_results, mismatched=True, unwind_confirmed=unwind_confirmed)
