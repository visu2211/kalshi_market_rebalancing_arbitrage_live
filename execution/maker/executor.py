"""Top-level maker executor — the module stream/maker_monitor.py calls
when a BasketOpportunity's maker quote clears the margin threshold.

Taker is one atomic batch + an unwind reconciliation; maker is three
loosely-coupled phases that happen at different wall-clock times:

  1. QUOTE + SUBMIT  (execute_opportunity_maker, in this module)
     Compute target prices via quoter.quote_basket, run pre-submit
     guards (breaker, cooldown, inventory cap, daily count, enabled
     flag), place the full basket of GTC orders in one batch, and
     return. Does NOT wait for fills — maker orders are supposed to
     rest.

  2. FILL REACT      (manage_fill_event, in this module)
     The monitor forwards each WS order_update / fill message in.
     manage_fill_event feeds it to MakerInventoryBook.record_fill and
     carries out whatever InventoryAction the book returns — cancels
     any still-resting legs, fires an IOC sell to flatten excess.

  3. CLEANUP / AGE   (sweep_stale_orders, in this module)
     Periodic pass: any resting order older than
     max_resting_order_age_s or whose current mid has drifted past
     requote_threshold_ticks is canceled. Not implemented yet —
     stubbed so the monitor loop can schedule it; v1 relies on the
     monitor calling cancel_order directly.

execute_opportunity_maker's signature intentionally parallels
execution.executor.execute_opportunity (client, opportunity, kwargs,
returning a result dataclass) so the monitor's dispatch at the clean
seam looks symmetric. Store writes mirror the taker's record_submitted
→ record_result discipline: every order lands in the store BEFORE
place_orders_batch returns, so a crash between submission and response
can be reconciled.
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Optional

import structlog

from config.loader import MakerExecutionSettings
from execution.maker.inventory import (
    InventoryAction,
    InventoryActionKind,
    MakerInventoryBook,
)
from execution.maker.quoter import MakerQuote, quote_basket
from execution.maker.risk import MakerRiskState
from execution.store import OrderStore, PendingOrder
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

logger = structlog.get_logger(__name__)

STRATEGY_CLIENT_ORDER_ID_PREFIX = "kmrm-"   # "kalshi market rebalancing MAKER", distinct from taker's "kmra-"
_FLATTEN_SELL_PRICE_FLOOR = Decimal("0.01")   # emergency unwind floor — matches execution/executor.py's _EMERGENCY_UNWIND_SELL_PRICE


def _new_client_order_id(kind: str) -> str:
    return f"{STRATEGY_CLIENT_ORDER_ID_PREFIX}{kind}-{uuid.uuid4()}"


@dataclass
class MakerLegResult:
    """One leg's placement outcome. Narrower than the taker's
    LegExecutionResult because the maker's post-submit lifecycle is
    tracked by MakerInventoryBook and MakerOrderStore (via WS fill
    events), not inline here."""

    ticker: str
    side: Side
    limit_price: Decimal
    size: Decimal
    client_order_id: str
    order_result: Optional[OrderResult] = None    # None if request-level error or submission was refused
    error: Optional[str] = None


@dataclass
class MakerExecutionResult:
    event_ticker: str
    direction: str
    quote: Optional[MakerQuote]
    leg_results: list[MakerLegResult] = field(default_factory=list)
    refused_reason: Optional[str] = None           # set iff no orders were submitted
    decision_at_ms: int = 0
    submitted_at_ms: int = 0
    response_received_at_ms: int = 0


class MakerBreakerTrippedError(RuntimeError):
    """Raised if execute_opportunity_maker is called with a tripped
    risk state — mirrors how the taker's monitor halts on breaker."""


def _pre_submit_refusal(
    opportunity: BasketOpportunity,
    settings: MakerExecutionSettings,
    risk: MakerRiskState,
    inventory: MakerInventoryBook,
    event_cooldown_s: int,
    max_baskets_per_day: int,
    expected_cost_usd: Decimal,
) -> Optional[str]:
    """Pre-submit gates. Returns a short refusal reason string or None if
    the basket may proceed. Order mirrors the taker's: cheap checks
    first, expensive last."""
    if not settings.enabled:
        return "maker_disabled"
    if risk.tripped:
        return f"breaker_tripped: {risk.tripped_reason}"
    if risk.daily_basket_attempts >= max_baskets_per_day:
        return f"daily_basket_cap_reached ({risk.daily_basket_attempts}/{max_baskets_per_day})"
    if risk.in_event_cooldown(opportunity.event_ticker, event_cooldown_s):
        return "event_cooldown"
    if inventory.would_exceed_inventory_cap(opportunity.event_ticker, expected_cost_usd):
        return "inventory_cap_exceeded"
    return None


async def execute_opportunity_maker(
    client: KalshiClient,
    opportunity: BasketOpportunity,
    *,
    settings: MakerExecutionSettings,
    risk: MakerRiskState,
    inventory: MakerInventoryBook,
    store: Optional[OrderStore] = None,
    event_cooldown_s: int = 86400,
    max_baskets_per_day: int = 3,
) -> MakerExecutionResult:
    """Quote `opportunity` with the maker's pricing prior and post the
    full basket of GTC limit orders in one batch, if every gate passes.

    Does NOT wait for fills — maker orders are expected to rest. Fill
    handling happens in manage_fill_event, driven by WS messages the
    monitor forwards in.

    All pre-submit refusals return a result with `refused_reason` set and
    an empty leg_results list — the caller distinguishes those from a
    submission that happened + got rejected per-leg by the exchange
    (which comes back as a non-empty leg_results with per-leg error
    strings, matching the taker's shape).
    """
    decision_at_ms = int(time.time() * 1000)

    quote = quote_basket(
        opportunity,
        quote_offset_ticks=settings.quote_offset_ticks,
        target_margin_per_basket=settings.target_margin_per_basket,
        size=opportunity.max_size,
        pricing_prior=settings.pricing_prior,
    )
    if quote is None:
        return MakerExecutionResult(
            event_ticker=opportunity.event_ticker,
            direction=opportunity.direction,
            quote=None,
            refused_reason="margin_insufficient_or_prior_rejected",
            decision_at_ms=decision_at_ms,
        )

    expected_cost_usd = quote.expected_basket_cost_usd * quote.size
    refusal = _pre_submit_refusal(
        opportunity, settings, risk, inventory,
        event_cooldown_s=event_cooldown_s,
        max_baskets_per_day=max_baskets_per_day,
        expected_cost_usd=expected_cost_usd,
    )
    if refusal is not None:
        return MakerExecutionResult(
            event_ticker=opportunity.event_ticker,
            direction=opportunity.direction,
            quote=quote,
            refused_reason=refusal,
            decision_at_ms=decision_at_ms,
        )

    # Register inventory + risk-state BEFORE submission (persist-before-submit,
    # same discipline as the taker). An exchange-side "lost response" or crash
    # must leave behind durable breadcrumbs for reconciliation.
    inventory.open_event(
        opportunity.event_ticker,
        [lq.ticker for lq in quote.leg_quotes],
        quote.size,
        direction=opportunity.direction,
    )
    risk.record_basket_attempt(opportunity.event_ticker)

    orders: list[OrderRequest] = []
    pre_ids: dict[str, str] = {}
    for leg in quote.leg_quotes:
        coid = _new_client_order_id("quote")
        pre_ids[leg.ticker] = coid
        orders.append(OrderRequest(
            market_id=leg.ticker,
            side=leg.side,
            action=OrderAction.BUY,
            order_type=OrderType.LIMIT,
            size=leg.size,
            limit_price=leg.price,
            client_order_id=coid,
            time_in_force=TimeInForce.GOOD_TILL_CANCELED,
        ))

    if store is not None:
        store.record_submitted([
            PendingOrder(
                client_order_id=o.client_order_id or "",
                ticker=o.market_id, side=o.side.value, action=o.action.value,
                execution_stage="maker_quote",
                limit_price=o.limit_price, size=o.size,
            )
            for o in orders if o.client_order_id
        ])

    submitted_at_ms = int(time.time() * 1000)
    try:
        batch_results = await client.place_orders_batch(orders)
    except AmbiguousSubmissionError as exc:
        # Lost response on a maker submission is less dangerous than on
        # the taker (GTC orders rest openly rather than firing an
        # immediate cross), but we still don't KNOW whether the orders
        # are up. Mark every leg's result as pending so the caller can
        # reconcile by client_order_id via the existing
        # client.resolve_client_order_ids helper. Not swept here — the
        # monitor handles reconciliation when the submission dust settles.
        logger.warning("maker_ambiguous_submission", error=str(exc), event_ticker=opportunity.event_ticker)
        return MakerExecutionResult(
            event_ticker=opportunity.event_ticker,
            direction=opportunity.direction,
            quote=quote,
            leg_results=[
                MakerLegResult(
                    ticker=o.market_id, side=o.side, limit_price=o.limit_price,
                    size=o.size, client_order_id=o.client_order_id or "",
                    error=f"AMBIGUOUS — {exc}",
                ) for o in orders
            ],
            refused_reason=None,
            decision_at_ms=decision_at_ms,
            submitted_at_ms=submitted_at_ms,
            response_received_at_ms=int(time.time() * 1000),
        )
    except ExchangeError as exc:
        # Clean rejection — exchange definitively didn't accept the batch.
        return MakerExecutionResult(
            event_ticker=opportunity.event_ticker,
            direction=opportunity.direction,
            quote=quote,
            leg_results=[
                MakerLegResult(
                    ticker=o.market_id, side=o.side, limit_price=o.limit_price,
                    size=o.size, client_order_id=o.client_order_id or "",
                    error=str(exc),
                ) for o in orders
            ],
            refused_reason=None,
            decision_at_ms=decision_at_ms,
            submitted_at_ms=submitted_at_ms,
            response_received_at_ms=int(time.time() * 1000),
        )

    response_received_at_ms = int(time.time() * 1000)
    leg_results: list[MakerLegResult] = []
    for order, result in zip(orders, batch_results):
        leg_results.append(MakerLegResult(
            ticker=order.market_id, side=order.side, limit_price=order.limit_price,
            size=order.size, client_order_id=order.client_order_id or "",
            order_result=result,
            error=None if result.status != OrderStatus.REJECTED else _format_rejection(result),
        ))
        if store is not None and order.client_order_id:
            store.record_result(
                order.client_order_id,
                order_id=result.order_id or None,
                status=result.status.value,
                filled_size=result.filled_size,
                remaining_size=result.remaining_size,
                average_fill_price=result.average_fill_price,
                average_fee_paid=result.average_fee_paid,
                terminal_verified=(result.status in (OrderStatus.FILLED, OrderStatus.CANCELED, OrderStatus.REJECTED)),
            )

    return MakerExecutionResult(
        event_ticker=opportunity.event_ticker,
        direction=opportunity.direction,
        quote=quote,
        leg_results=leg_results,
        decision_at_ms=decision_at_ms,
        submitted_at_ms=submitted_at_ms,
        response_received_at_ms=response_received_at_ms,
    )


def _format_rejection(result: OrderResult) -> str:
    err = result.raw.get("error") if isinstance(result.raw, dict) else None
    if isinstance(err, dict):
        return f"{err.get('code', 'unknown_error')}: {err.get('message', '')}".strip(": ")
    return "rejected"


async def manage_fill_event(
    client: KalshiClient,
    event_ticker: str,
    ticker: str,
    *,
    cumulative_filled_delta: Decimal,
    fill_price: Decimal,
    remaining_size: Decimal,
    resting_order_ids: dict[str, str],      # ticker -> order_id, for cancellation of remaining resting legs
    inventory: MakerInventoryBook,
    exit_bids: dict[str, Decimal],          # best observable bid per ticker right now, for unwind price
    store: Optional[OrderStore] = None,
) -> list[MakerLegResult]:
    """Called by the monitor for each incremental fill arriving on WS.

    `cumulative_filled_delta` is how much more filled since last call for
    THIS ticker — not the cumulative total — because InventoryBook's
    record_fill expects deltas. The monitor tracks the per-order running
    total externally and subtracts.

    Returns the list of unwind-sell orders placed to flatten any excess
    exposure above the current floor (empty for NO_ACTION /
    BASKET_COMPLETE). The caller handles canceling the remaining resting
    legs through `resting_order_ids` as instructed by the action's
    `tickers_to_cancel` field.
    """
    action = inventory.record_fill(
        event_ticker, ticker,
        filled_size_delta=cumulative_filled_delta,
        fill_price=fill_price,
        remaining_size=remaining_size,
    )
    unwind_results: list[MakerLegResult] = []

    if action.kind == InventoryActionKind.BASKET_COMPLETE:
        logger.info("maker_basket_complete", event_ticker=event_ticker, floor_size=str(action.floor_size))
        inventory.close_event(event_ticker)
        return unwind_results

    if action.kind == InventoryActionKind.NO_ACTION:
        return unwind_results

    # CANCEL_REMAINING_AND_FLATTEN: first cancel the still-resting legs,
    # then fire an IOC sell per excess leg at the current observable bid
    # (falling back to the emergency floor if a bid isn't visible — this
    # is the same "take what you can get" shape execution/executor.py uses
    # for its emergency unwind).
    #
    # Direction-aware side selection: a buy_basket basket held YES on every
    # leg, so flatten sells YES at the YES bid. A buy_all_no basket held
    # NO on every leg, so flatten sells NO at the NO bid — which Kalshi
    # doesn't quote directly, so caller passes the synthesized NO bid
    # (1 - YES ask) in `exit_bids`. Same symmetry as the taker's
    # _position_side / _exit_price pair in execution/executor.py.
    inv_snapshot = inventory.snapshot().get(event_ticker)
    direction = inv_snapshot.direction if inv_snapshot else "buy_basket"
    flatten_side = Side.YES if direction == "buy_basket" else Side.NO

    for cancel_ticker in action.tickers_to_cancel:
        order_id = resting_order_ids.get(cancel_ticker)
        if order_id is None:
            continue
        try:
            await client.cancel_order(order_id)
        except ExchangeError as exc:
            logger.warning("maker_cancel_failed", ticker=cancel_ticker, order_id=order_id, error=str(exc))

    for excess_ticker, excess_size in action.unwind_excess.items():
        price = exit_bids.get(excess_ticker, _FLATTEN_SELL_PRICE_FLOOR)
        if price <= 0:
            price = _FLATTEN_SELL_PRICE_FLOOR
        coid = _new_client_order_id("flatten")
        order = OrderRequest(
            market_id=excess_ticker,
            side=flatten_side,
            action=OrderAction.SELL,
            order_type=OrderType.LIMIT,
            size=excess_size,
            limit_price=price,
            client_order_id=coid,
            time_in_force=TimeInForce.IMMEDIATE_OR_CANCEL,
        )
        if store is not None:
            store.record_submitted([PendingOrder(
                client_order_id=coid, ticker=excess_ticker, side=flatten_side.value,
                action=OrderAction.SELL.value, execution_stage="maker_flatten",
                limit_price=price, size=excess_size,
            )])
        try:
            results = await client.place_orders_batch([order])
            result = results[0] if results else None
        except (AmbiguousSubmissionError, ExchangeError) as exc:
            logger.warning("maker_flatten_failed", ticker=excess_ticker, error=str(exc))
            unwind_results.append(MakerLegResult(
                ticker=excess_ticker, side=flatten_side, limit_price=price,
                size=excess_size, client_order_id=coid, error=str(exc),
            ))
            continue
        unwind_results.append(MakerLegResult(
            ticker=excess_ticker, side=flatten_side, limit_price=price,
            size=excess_size, client_order_id=coid,
            order_result=result,
            error=None if result and result.status != OrderStatus.REJECTED else _format_rejection(result) if result else "no_result",
        ))
        if store is not None and result is not None:
            store.record_result(
                coid, order_id=result.order_id or None,
                status=result.status.value, filled_size=result.filled_size,
                remaining_size=result.remaining_size,
                average_fill_price=result.average_fill_price,
                average_fee_paid=result.average_fee_paid,
                terminal_verified=(result.status in (OrderStatus.FILLED, OrderStatus.CANCELED, OrderStatus.REJECTED)),
            )

    # Once flatten orders have fired, the basket is either flat (floor == 0)
    # or complete at the floor; either way we stop tracking this event here.
    # The monitor can re-quote it on the next detection cycle if the book
    # still qualifies and the event isn't in cooldown.
    inventory.close_event(event_ticker)
    return unwind_results
