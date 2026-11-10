"""Per-event inventory tracking for the maker strategy.

The taker's safety model is "all-or-nothing within one batch" — FOK means a
leg either fills to size or dies. Maker is the opposite: a resting order
can sit unfilled for minutes, fills partially, or fills completely
independently of its siblings. That means we always need an answer to
"given what filled so far, what's the current basket position and what
should we do next?"

This module is pure state math, no I/O. Fills are reported in by the
order_manager (which hears them off WS or get_order polls); actions are
suggested back out as InventoryAction values.

Terminology:
- "target size" = the uniform leg size the basket was quoted at
- "filled size" = contracts actually filled on this leg so far
- "floor" = min(filled sizes across all legs). A basket at the floor is
  risk-free (a complete basket at `floor` contracts); any excess above
  the floor is naked single-leg exposure.

Design decision: we pay to flatten excess, not to "complete" the basket
by crossing the unfilled legs' asks. Crossing the ask is the TAKER side's
job — if we wanted to take, we'd run the taker strategy. The maker's edge
disappears if we have to cross for the remaining legs: we'd be paying
spread + fees on both the resting leg (which we got) and the crossed legs
(which we chased), netting negative. Instead, excess on the filled legs
unwinds at the current bid, taking a bounded loss (bounded by the
per-tick + fee geometry, same logic as the taker's max_unwind_loss_usd).

flatten_on_leg_fill=True in the config: as soon as any leg fills, we
cancel the remaining resting legs and flatten the filled leg. This
sacrifices some would-be baskets (because the remaining legs might have
filled on their own moments later) in exchange for never carrying naked
single-leg exposure. v1 ships this way; a later release can experiment
with a "wait N seconds for the rest to fill before flattening" policy.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum


class InventoryActionKind(str, Enum):
    NO_ACTION = "no_action"                      # basket in equilibrium (all at floor)
    CANCEL_REMAINING_AND_FLATTEN = "cancel_remaining_and_flatten"
    BASKET_COMPLETE = "basket_complete"          # every leg filled to target_size, nothing to do


@dataclass(frozen=True)
class InventoryAction:
    kind: InventoryActionKind
    # Tickers that still have resting orders to cancel (empty for NO_ACTION/BASKET_COMPLETE).
    tickers_to_cancel: list[str] = field(default_factory=list)
    # Per-ticker excess (fill - floor) that should be sold back. Only
    # populated for CANCEL_REMAINING_AND_FLATTEN.
    unwind_excess: dict[str, Decimal] = field(default_factory=dict)
    floor_size: Decimal = Decimal(0)             # the common fill across the basket, only non-zero once at least one leg hit floor > 0


@dataclass
class EventInventory:
    """All maker-strategy state for one quoted event.

    Owned by MakerInventoryBook; reset when the event is dequeued."""

    event_ticker: str
    target_size: Decimal                          # uniform size every leg was quoted at
    leg_tickers: list[str]
    filled: dict[str, Decimal] = field(default_factory=dict)     # ticker -> contracts filled so far
    resting: set[str] = field(default_factory=set)               # tickers whose GTC order is still live
    notional_at_risk_usd: Decimal = Decimal(0)   # sum of (filled_i * limit_price_i); approximate exposure used for the per-event cap

    @property
    def floor(self) -> Decimal:
        """Common fill across the basket — the risk-free quantity. Equals
        min(filled over all legs), treating a missing/absent leg as 0."""
        return min((self.filled.get(t, Decimal(0)) for t in self.leg_tickers), default=Decimal(0))

    @property
    def excess_by_ticker(self) -> dict[str, Decimal]:
        """How much of each leg is above the common floor (i.e. naked
        single-leg exposure that needs to flatten before the basket is
        risk-free)."""
        f = self.floor
        return {
            t: self.filled.get(t, Decimal(0)) - f
            for t in self.leg_tickers
            if self.filled.get(t, Decimal(0)) > f
        }


class MakerInventoryBook:
    """All open maker-quoted events, keyed by event_ticker. One instance
    per running maker monitor; thread-unsafe (the monitor's own event loop
    serializes access, same as the taker's state)."""

    def __init__(self, *, flatten_on_leg_fill: bool, max_inventory_per_event_usd: Decimal):
        self._events: dict[str, EventInventory] = {}
        self._flatten_on_leg_fill = flatten_on_leg_fill
        self._max_inventory_usd = max_inventory_per_event_usd

    def open_event(
        self,
        event_ticker: str,
        leg_tickers: list[str],
        target_size: Decimal,
    ) -> EventInventory:
        """Begin tracking a newly-quoted event. Idempotent — a second
        call for an event already tracked returns the existing record
        unchanged (the order_manager may re-emit on requote)."""
        if event_ticker in self._events:
            return self._events[event_ticker]
        inv = EventInventory(
            event_ticker=event_ticker,
            target_size=target_size,
            leg_tickers=list(leg_tickers),
            resting=set(leg_tickers),
        )
        self._events[event_ticker] = inv
        return inv

    def close_event(self, event_ticker: str) -> None:
        """Forget this event entirely (basket completed or fully flattened).
        A no-op if we never tracked it."""
        self._events.pop(event_ticker, None)

    def record_fill(
        self,
        event_ticker: str,
        ticker: str,
        filled_size_delta: Decimal,
        fill_price: Decimal,
        remaining_size: Decimal,
    ) -> InventoryAction:
        """Record that `ticker` has filled an additional `filled_size_delta`
        contracts at `fill_price`, with `remaining_size` left resting (0
        means the GTC order is terminal). Returns the InventoryAction the
        order_manager should take.

        `filled_size_delta` is a DELTA (how much more than last time), not
        the total — mirrors how WS fill messages come through. The caller
        is responsible for mapping cumulative WS fill state into deltas
        (order_manager keeps the per-order cumulative total; this module
        doesn't need it).
        """
        inv = self._events.get(event_ticker)
        if inv is None:
            # A fill for an event we aren't tracking should never happen under
            # normal flow (it means the order_manager is forwarding fills for
            # something close_event already cleared). Caller decides how to
            # escalate; here we just emit NO_ACTION rather than re-opening the
            # book.
            return InventoryAction(kind=InventoryActionKind.NO_ACTION)

        inv.filled[ticker] = inv.filled.get(ticker, Decimal(0)) + filled_size_delta
        inv.notional_at_risk_usd += filled_size_delta * fill_price
        if remaining_size == 0 and ticker in inv.resting:
            inv.resting.discard(ticker)

        every_leg_full = all(
            inv.filled.get(t, Decimal(0)) >= inv.target_size for t in inv.leg_tickers
        )
        if every_leg_full:
            return InventoryAction(
                kind=InventoryActionKind.BASKET_COMPLETE,
                floor_size=inv.target_size,
            )

        if self._flatten_on_leg_fill:
            return InventoryAction(
                kind=InventoryActionKind.CANCEL_REMAINING_AND_FLATTEN,
                tickers_to_cancel=sorted(inv.resting),
                unwind_excess=inv.excess_by_ticker,
                floor_size=inv.floor,
            )

        return InventoryAction(kind=InventoryActionKind.NO_ACTION, floor_size=inv.floor)

    def would_exceed_inventory_cap(self, event_ticker: str, extra_usd: Decimal) -> bool:
        """Pre-quote check: would adding `extra_usd` of notional take this
        event over the cap? Called by the executor before posting a new
        basket of quotes. Treats an un-opened event as having 0 inventory."""
        current = self._events.get(event_ticker)
        base = current.notional_at_risk_usd if current else Decimal(0)
        return base + extra_usd > self._max_inventory_usd

    def snapshot(self) -> dict[str, EventInventory]:
        """Read-only view, for diagnostics/logging. Returns a shallow
        copy so the caller can't mutate internal state by accident."""
        return dict(self._events)
