"""Places the legs of a detected BasketOpportunity against whichever
account settings.execution.target configures — "demo" (paper money) or
"prod" (REAL MONEY, the real Kalshi account). Detection (scanner/,
stream/monitor.py) always runs against real prod order books regardless of
target, because that's the only place there's a real book to find edges
in — but for a long time execution here was hard-coded to demo only,
deliberately, because demo's order book turned out to be a separate,
mostly-empty paper-trading book almost entirely disconnected from prod's
real liquidity (confirmed in the sibling arb-bot project, and again at
length in this project — see the "batched, atomic-attempt" section below).
So orders placed against demo are very likely to rest unfilled, or fill
asymmetrically across a basket's legs — expected demo behavior, not a bug.

target == "prod" (added 2026-08-26, after the demo-liquidity problem below
made clear that demo can validate detection and the safety systems here
but never profitability) is REAL MONEY. VERIFIED LIVE 2026-08-26: the same
KALSHI_API_KEY_ID/KALSHI_PRIVATE_KEY_PATH already used for detection does
have order-placement permission on prod (tested with a deliberately
non-marketable $0.01 limit order, immediately canceled, on a real open
market — accepted and canceled cleanly, balance unaffected). Every safety
system below (batching, unwind, confirmation, reconciliation, the circuit
breaker) applies identically regardless of target — none of it is
demo-specific. The one additional guardrail specific to real money:
execution.max_order_size_contracts (see stream/monitor.py's
_cap_opportunity_size) caps every leg's requested size well below what an
opportunity could support, independent of and in addition to everything
below — started at 1 contract deliberately, meant to be ramped up
manually, not automatically.

_live variant — no-slippage fill: opportunity.max_size and each leg's price
already come from scanner.basket.compute_opportunity, which caps size to
exactly what's resting at the observed best price on the thinnest leg (see
that module's docstring) — never a deeper, worse-priced book level. On top
of that, every entry order uses Kalshi's native fill_or_kill time in force:
each individual leg fills completely or is killed. This prevents partial
fills within one leg, but does not make separate legs atomic.

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
     floor gets an immediate offsetting SELL at the last observed bid. If
     residual exposure remains, the executor refreshes the live book and
     crosses the displayed depth, then submits a final $0.01 IOC attempt.
     This prioritizes flattening over the original loss bound. A successful unwind brings exposure to a
     matched, fixed-payout basket (or fully flat, if the floor is
     zero). This is the "legging risk" pattern real arb desks fall back
     to when true atomic multi-leg execution isn't available at the
     exchange level — Kalshi has no combo-order type for a whole basket,
     so reactive unwind is the correct tier here, not a workaround.
  3. The unwind isn't just fired and trusted — every stage is reconciled
     against Kalshi before the residual is computed. Even a $0.01 sell
     cannot fill without a buyer. AtomicExecutionResult.unwind_confirmed
     tells the caller whether the flatten is actually verified complete,
     not just attempted — stream/monitor.py trips the circuit breaker
     immediately (not on the normal mismatch-counting schedule) on a
     confirmed-failed unwind, since that's strictly worse than an
     ordinary mismatch: a naked position AND a failed attempt to fix it.

Every placement is fetched back from the exchange before exposure is
calculated. Any anomalous live remainder is explicitly canceled and fetched
again, so the unwind decision uses the final post-cancel fill picture.
"""

from __future__ import annotations

import asyncio
import time
import uuid

import structlog
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Optional

from kalshi.client import (
    AmbiguousSubmissionError,
    ExchangeError,
    KalshiClient,
    OrderAction,
    OrderRequest,
    OrderResult,
    OrderStatus,
    OrderType,
    Side,
    TimeInForce,
)
from scanner.basket import BasketOpportunity
from scanner.fees import taker_fee

_UNWIND_CONFIRM_POLL_INTERVAL_S = 0.5
_UNWIND_CONFIRM_TIMEOUT_S = 5.0
_UNWIND_BOOK_DEPTH = 100
_EMERGENCY_UNWIND_SELL_PRICE = Decimal("0.01")

logger = structlog.get_logger(__name__)

STRATEGY_CLIENT_ORDER_ID_PREFIX = "kmra-"


def _new_client_order_id(kind: str) -> str:
    """Durable ownership marker used by startup/open-order recovery."""
    return f"{STRATEGY_CLIENT_ORDER_ID_PREFIX}{kind}-{uuid.uuid4()}"


@dataclass
class LegExecutionResult:
    ticker: str
    action: OrderAction
    limit_price: Decimal
    size: Decimal
    order_result: Optional[OrderResult]   # None if the request itself was rejected/errored
    error: Optional[str] = None           # set iff order_result is None, or order_result.status == REJECTED
    execution_stage: str = "entry"
    # False when we could not confirm this order reached a terminal state —
    # a failed verification read, a failed cancel, or a live remainder that
    # survived cancellation. An unverified leg may still be working at the
    # exchange, so its fill count is a guess and must not be treated as final.
    terminal_verified: bool = True


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
    decision_at_ms: int = 0
    entry_request_started_at_ms: int = 0
    entry_response_received_at_ms: int = 0
    entry_exchange_ts_ms: list[int] = field(default_factory=list)
    unwind_request_started_at_ms: int | None = None
    unwind_response_received_at_ms: int | None = None
    estimated_worst_unwind_loss_usd: Decimal = Decimal(0)
    refreshed_book_fallback_used: bool = False
    emergency_unwind_used: bool = False
    unwind_attempt_count: int = 0


def estimate_worst_case_unwind_loss(opportunity: BasketOpportunity) -> Decimal | None:
    """Conservative loss if every proposed leg had to be sold immediately.

    Returns None when the displayed best bid cannot absorb the full proposed
    size on any leg.  Summing every leg is stricter than the most common
    mismatch, but safely covers any subset of entry legs filling.
    """
    total = Decimal(0)
    for leg in opportunity.leg_quotes:
        if leg.bid_size < opportunity.max_size or leg.best_bid <= 0:
            return None
        entry_fee = taker_fee(leg.best_ask, opportunity.max_size, opportunity.fee_multiplier)
        exit_fee = taker_fee(leg.best_bid, opportunity.max_size, opportunity.fee_multiplier)
        total += (leg.best_ask - leg.best_bid) * opportunity.max_size + entry_fee + exit_fee
    return max(total, Decimal(0))


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


def _remaining_unwind_legs(original: list[UnwindLeg], results: list[LegExecutionResult]) -> list[UnwindLeg]:
    sold: dict[str, Decimal] = {}
    for result in results:
        if result.order_result is not None:
            sold[result.ticker] = sold.get(result.ticker, Decimal(0)) + result.order_result.filled_size
    return [
        UnwindLeg(leg.ticker, leg.excess_size - sold.get(leg.ticker, Decimal(0)))
        for leg in original
        if leg.excess_size > sold.get(leg.ticker, Decimal(0))
    ]


def _price_for_bid_depth(book, size: Decimal) -> Decimal | None:
    """Lowest bid needed to sell ``size`` against the current REST book."""
    cumulative = Decimal(0)
    for level in book.bids:
        cumulative += level.size
        if cumulative >= size:
            return level.price
    return None


async def _submit_unwind_stage(
    client: KalshiClient,
    legs: list[UnwindLeg],
    prices: dict[str, Decimal],
    kind: str,
) -> list[LegExecutionResult]:
    orders = [
        OrderRequest(
            market_id=leg.ticker, side=Side.YES, action=OrderAction.SELL,
            order_type=OrderType.LIMIT, size=leg.excess_size,
            limit_price=prices[leg.ticker], client_order_id=_new_client_order_id(kind),
            time_in_force=TimeInForce.IMMEDIATE_OR_CANCEL,
        )
        for leg in legs if leg.ticker in prices
    ]
    if not orders:
        return []
    results = await _place_batch(client, orders, OrderAction.SELL)
    for result in results:
        result.execution_stage = kind
    await _reconcile_final_states(client, results)
    await _confirm_unwind(client, results)
    return results


def _format_order_error(raw: dict) -> str:
    error = raw.get("error")
    if isinstance(error, dict):
        return f"{error.get('code', 'unknown_error')}: {error.get('message', '')}".strip(": ")
    return str(error)


class UnresolvedSubmissionError(Exception):
    """An ambiguous submission could not be resolved against the exchange.

    Distinct from AmbiguousSubmissionError, which only says the response was
    lost. This says we then failed to find out what happened — so the true
    position is unknown and no further stage may be submitted. The caller
    halts and trips the durable breaker rather than guessing.
    """

    def __init__(self, message: str, leg_results: list["LegExecutionResult"],
                 salvage_results: list["LegExecutionResult"] | None = None) -> None:
        super().__init__(message)
        self.leg_results = leg_results
        # Offsetting sells fired for legs whose fills WERE verified, before halting.
        self.salvage_results = list(salvage_results or [])


async def _resolve_ambiguous_batch(
    client: KalshiClient,
    orders: list[OrderRequest],
    action: OrderAction,
    exc: AmbiguousSubmissionError,
) -> list[LegExecutionResult]:
    """Settle a lost batch response by looking up the ids we submitted.

    An id Kalshi returns tells us exactly what that order did, and the real
    fill counts flow on as if the response had never been lost. An id Kalshi
    has no record of was genuinely never accepted, so zero fills is truthful
    for that leg specifically. If the lookup itself fails we still do not know,
    and raise UnresolvedSubmissionError rather than inventing an answer.
    """
    submitted = {o.client_order_id: o for o in orders if o.client_order_id}
    try:
        resolved = await client.resolve_client_order_ids(list(submitted))
    except ExchangeError as lookup_exc:
        unknown = [
            LegExecutionResult(
                ticker=o.market_id, action=action, limit_price=o.limit_price, size=o.size,
                order_result=None,
                error=f"AMBIGUOUS — submission unresolved: {exc}; lookup also failed: {lookup_exc}",
            )
            for o in orders
        ]
        raise UnresolvedSubmissionError(
            f"could not determine the fate of {len(orders)} submitted order(s): {lookup_exc}", unknown
        ) from lookup_exc

    results: list[LegExecutionResult] = []
    for order in orders:
        found = resolved.get(order.client_order_id) if order.client_order_id else None
        results.append(LegExecutionResult(
            ticker=order.market_id, action=action, limit_price=order.limit_price, size=order.size,
            order_result=found,
            error=None if found is not None else f"not accepted by the exchange (resolved after: {exc})",
        ))
    logger.warning(
        "ambiguous_submission_resolved",
        submitted=len(orders), found_at_exchange=len(resolved), error=str(exc),
    )
    return results


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
    except AmbiguousSubmissionError as exc:
        # The exchange may have applied this batch and only lost the answer.
        # Treating that as "no fills" is what makes a lost entry response look
        # like a clean no-fill while real positions sit open, and what lets a
        # lost unwind response be followed by a second unwind that oversells.
        # Resolve it by the ids we submitted before anyone decides anything.
        return await _resolve_ambiguous_batch(demo_client, orders, action, exc)
    except ExchangeError as exc:
        # A clean rejection (4xx/429) — the exchange definitively did not
        # accept these, so zero fills is the truthful reading.
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
                pass


_ORDER_READ_RETRIES = 3
_ORDER_READ_BACKOFF_S = 0.25


async def _read_order_authoritative(
    client: KalshiClient, order_id: str, client_order_id: str | None
) -> tuple[OrderResult | None, str | None]:
    """Read one order's terminal state, tolerating a transient post-fill gap.

    VERIFIED LIVE 2026-08-28: immediately after a real prod fill,
    GET /portfolio/orders/{id} returned 404 for an order that had definitively
    filled — the same id resolved normally moments later, and get_orders()
    listed it correctly the whole time. Treating that single 404 as
    "unverifiable" halted execution before the unwind could flatten a filled
    leg, leaving a real naked position. So: retry, then fall back to looking the
    order up by client_order_id, and only report failure when both are
    exhausted. Returns (result, error) with exactly one of them set.
    """
    last: str | None = None
    for attempt in range(_ORDER_READ_RETRIES):
        try:
            return await client.get_order(order_id), None
        except ExchangeError as exc:
            last = str(exc)
            if attempt + 1 < _ORDER_READ_RETRIES:
                await asyncio.sleep(_ORDER_READ_BACKOFF_S)

    # Same handle the ambiguity path uses; it reads every status, so an order
    # that filled and closed is still found.
    if client_order_id:
        try:
            found = await client.resolve_client_order_ids([client_order_id])
        except ExchangeError as exc:
            return None, f"{last}; client_order_id lookup also failed: {exc}"
        if client_order_id in found:
            logger.warning(
                "order_read_recovered_via_client_order_id",
                order_id=order_id, client_order_id=client_order_id, after=last,
            )
            return found[client_order_id], None
    return None, last


async def _reconcile_final_states(
    client: KalshiClient,
    leg_results: list[LegExecutionResult],
) -> None:
    """Replace placement snapshots with authoritative terminal states.

    Native IOC/FOK responses are documented as final, but this extra read is
    intentional defense in depth. If the exchange ever reports a live
    remainder, cancel it and read once more so exposure is calculated from
    the post-cancel state rather than a stale placement response.
    """
    for leg in leg_results:
        placed = leg.order_result
        if placed is None or not placed.order_id or placed.status == OrderStatus.REJECTED:
            continue
        final, read_error = await _read_order_authoritative(
            client, placed.order_id, placed.client_order_id
        )
        if final is None:
            leg.error = f"unable to verify final order state: {read_error}"
            leg.terminal_verified = False
            continue

        if final.remaining_size > 0:
            try:
                await client.cancel_order(final.order_id)
            except ExchangeError as exc:
                # Swallowing this used to let an entry be reported clean while
                # a live buy order was still resting at the exchange.
                leg.error = f"unable to cancel live remainder: {exc}"
                leg.terminal_verified = False
            post, post_error = await _read_order_authoritative(
                client, final.order_id, final.client_order_id
            )
            if post is None:
                leg.error = f"unable to verify post-cancel order state: {post_error}"
                leg.terminal_verified = False
            else:
                final = post
                if final.remaining_size > 0:
                    leg.error = f"order still live after cancel: {final.remaining_size} remaining"
                    leg.terminal_verified = False

        # Placement V2 currently carries execution economics that Get Order
        # may omit. Preserve them while taking fill/remaining from the final read.
        if final.average_fill_price is None:
            final.average_fill_price = placed.average_fill_price
        if final.average_fee_paid is None:
            final.average_fee_paid = placed.average_fee_paid
        leg.order_result = final


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
        if lr.order_result is not None and lr.order_result.filled_size < lr.size
    }
    if not pending:
        return all(lr.order_result is not None and lr.order_result.filled_size >= lr.size for lr in unwind_results)

    deadline = time.monotonic() + _UNWIND_CONFIRM_TIMEOUT_S
    while pending and time.monotonic() < deadline:
        await asyncio.sleep(_UNWIND_CONFIRM_POLL_INTERVAL_S)
        for order_id in list(pending):
            try:
                result = await demo_client.get_order(order_id)
            except ExchangeError:
                continue   # transient — try again next poll, until the deadline
            pending[order_id].order_result = result
            if result.filled_size >= pending[order_id].size:
                del pending[order_id]

    # Never leave a failed emergency order resting after confirmation ends.
    for order_id in list(pending):
        try:
            await demo_client.cancel_order(order_id)
        except ExchangeError:
            pass
        try:
            pending[order_id].order_result = await demo_client.get_order(order_id)
        except ExchangeError:
            pass

    return not pending and all(
        lr.order_result is not None and lr.order_result.filled_size >= lr.size
        for lr in unwind_results
    )


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
    decision_at_ms = int(time.time() * 1000)
    estimated_unwind_loss = estimate_worst_case_unwind_loss(opportunity) or Decimal(0)
    orders = [
        OrderRequest(
            market_id=leg.ticker,
            side=Side.YES,
            action=OrderAction.BUY,
            order_type=OrderType.LIMIT,
            size=opportunity.max_size,
            limit_price=leg.best_ask,
            client_order_id=_new_client_order_id("entry"),
            time_in_force=TimeInForce.FILL_OR_KILL,
        )
        for leg in opportunity.leg_quotes
    ]

    entry_started_ms = int(time.time() * 1000)
    leg_results = await _place_batch(demo_client, orders, OrderAction.BUY)
    entry_received_ms = int(time.time() * 1000)
    await _reconcile_final_states(demo_client, leg_results)

    # An unverified leg's fill count is a guess. Deciding "no unwind needed"
    # from guesses is how an entry got reported clean while live buy orders
    # were still resting. Halt instead: the caller cancels by client id,
    # reconciles positions, and trips the durable breaker.
    unverified = [lr for lr in leg_results if not lr.terminal_verified]
    if unverified:
        # Flatten what we DO know about before halting. VERIFIED LIVE
        # 2026-08-28: raising here without this left a real, known 1-contract
        # prod fill naked, because the halt preceded the unwind entirely. A
        # confirmed fill left open is worse than one extra sell.
        #
        # Legs we could NOT verify are deliberately left alone — selling an
        # unconfirmed quantity risks overselling, which is the failure this
        # halt exists to prevent in the first place.
        salvage_legs = [
            UnwindLeg(lr.ticker, lr.order_result.filled_size)
            for lr in leg_results
            if lr.terminal_verified and lr.order_result is not None and lr.order_result.filled_size > 0
        ]
        salvage_results: list[LegExecutionResult] = []
        if salvage_legs:
            logger.warning(
                "salvage_unwind_before_halt",
                tickers=[l.ticker for l in salvage_legs],
                reason="entry state partly unverified; flattening the legs whose fills are confirmed",
            )
            observed_prices = {q.ticker: q.best_bid for q in opportunity.leg_quotes}
            salvage_results = await _submit_unwind_stage(
                demo_client, salvage_legs, observed_prices, "unwind-salvage",
            )
        raise UnresolvedSubmissionError(
            "entry left "
            + str(len(unverified))
            + " leg(s) in an unverified or non-terminal state: "
            + "; ".join(f"{lr.ticker}: {lr.error}" for lr in unverified),
            leg_results,
            salvage_results,
        )

    filled_sizes = {lr.ticker: (lr.order_result.filled_size if lr.order_result else Decimal(0)) for lr in leg_results}
    unwind_legs = compute_unwind_legs(filled_sizes)
    if not unwind_legs:
        return AtomicExecutionResult(
            leg_results=leg_results, unwind_results=[], mismatched=False, unwind_confirmed=True,
            decision_at_ms=decision_at_ms, entry_request_started_at_ms=entry_started_ms,
            entry_response_received_at_ms=entry_received_ms,
            entry_exchange_ts_ms=[int(lr.order_result.raw["ts_ms"]) for lr in leg_results if lr.order_result and lr.order_result.raw.get("ts_ms")],
            estimated_worst_unwind_loss_usd=estimated_unwind_loss,
        )

    unwind_started_ms = int(time.time() * 1000)
    observed_prices = {q.ticker: q.best_bid for q in opportunity.leg_quotes}
    unwind_results = await _submit_unwind_stage(demo_client, unwind_legs, observed_prices, "unwind")
    unwind_attempt_count = 1

    remaining = _remaining_unwind_legs(unwind_legs, unwind_results)
    refreshed_fallback_used = bool(remaining)
    if remaining:
        refreshed_prices: dict[str, Decimal] = {}
        for leg in remaining:
            try:
                book = await demo_client.get_orderbook(leg.ticker, side=Side.YES, depth=_UNWIND_BOOK_DEPTH)
            except ExchangeError:
                continue
            price = _price_for_bid_depth(book, leg.excess_size)
            if price is not None:
                refreshed_prices[leg.ticker] = price
        refreshed_results = await _submit_unwind_stage(
            demo_client, remaining, refreshed_prices, "unwind-refresh",
        )
        if refreshed_results:
            unwind_attempt_count += 1
            unwind_results.extend(refreshed_results)

    remaining = _remaining_unwind_legs(unwind_legs, unwind_results)
    emergency_used = bool(remaining)
    if remaining:
        emergency_prices = {leg.ticker: _EMERGENCY_UNWIND_SELL_PRICE for leg in remaining}
        emergency_results = await _submit_unwind_stage(
            demo_client, remaining, emergency_prices, "unwind-emergency",
        )
        unwind_attempt_count += 1
        unwind_results.extend(emergency_results)

    unwind_received_ms = int(time.time() * 1000)
    unwind_confirmed = not _remaining_unwind_legs(unwind_legs, unwind_results)

    return AtomicExecutionResult(
        leg_results=leg_results, unwind_results=unwind_results, mismatched=True, unwind_confirmed=unwind_confirmed,
        decision_at_ms=decision_at_ms, entry_request_started_at_ms=entry_started_ms,
        entry_response_received_at_ms=entry_received_ms,
        entry_exchange_ts_ms=[int(lr.order_result.raw["ts_ms"]) for lr in leg_results if lr.order_result and lr.order_result.raw.get("ts_ms")],
        unwind_request_started_at_ms=unwind_started_ms, unwind_response_received_at_ms=unwind_received_ms,
        estimated_worst_unwind_loss_usd=estimated_unwind_loss,
        refreshed_book_fallback_used=refreshed_fallback_used,
        emergency_unwind_used=emergency_used,
        unwind_attempt_count=unwind_attempt_count,
    )
