"""Live loop for the maker-side strategy — the sibling of
stream/monitor.py, intentionally smaller.

Two concurrent async tasks, no shared mutable state between them except
through the RestingOrderTracker + MakerInventoryBook pair:

  public_ws_loop  — subscribes orderbook_delta for the shortlisted legs
                    (same stage-1 filter + volume shortlist as taker),
                    maintains per-ticker LocalBook via handle_message
                    (imported from stream.monitor), re-prices the
                    event's basket on each tick, and dispatches to
                    execute_opportunity_maker when the quoter clears.

  private_ws_loop — subscribes order_update + fill channels on a signed
                    WS with the demo/prod credentials, parses each fill
                    message into the inventory book via
                    manage_fill_event. Fully decoupled from the public
                    loop — a public tick re-pricing is independent of
                    a private fill arriving.

The taker's full run_stage3 includes a lot the maker doesn't need:
relationship management, circuit-breaker on stale WS state, position
reconciliation against /portfolio/positions. v1 ships without those —
all can be ported later as targeted additions. The lean v1 covers
detect → quote → rest → fill → flatten, which is enough to run against
prod with tiny caps and measure fill behavior.

Entrypoint: scripts/run_maker_scanner.py.
"""

from __future__ import annotations

import asyncio
import json
import signal
import time
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Optional

import structlog

from config.loader import Settings
from execution.maker.executor import (
    MakerExecutionResult,
    execute_opportunity_maker,
    manage_fill_event,
)
from execution.maker.inventory import MakerInventoryBook
from execution.maker.risk import MakerRiskState
from execution.maker.store import maker_order_store_open
from kalshi.client import KalshiClient, OrderStatus
from kalshi.ws_client import KalshiWSClient, WS_RECONNECTED_MESSAGE_TYPE
from scanner.basket import LegQuote, compute_no_opportunity, compute_opportunity
from scanner.filters import filter_events, has_recent_activity, select_by_volume
from stream.monitor import LocalBook, handle_message

logger = structlog.get_logger(__name__)

# Private WS fill message types. Kalshi's v2 public docs name the channels
# "order_update" and "fill"; the parser here takes a light touch — any field
# is optional, and a message that doesn't parse is logged and dropped rather
# than raised, so a schema surprise degrades gracefully (same posture as the
# taker's handle_message in stream/monitor.py).
_PRIVATE_CHANNELS = ("order_update", "fill")

_SWEEP_INTERVAL_S = 10


class RestingOrderTracker:
    """Maps (event, ticker) to the resting order's order_id + its
    cumulative filled_size so manage_fill_event can be called with
    deltas (which the MakerInventoryBook expects).

    Single-owner: the monitor's public_ws_loop writes on quote submit
    and private_ws_loop reads on every fill. Thread-unsafe; the asyncio
    event loop is the serialization boundary."""

    def __init__(self) -> None:
        self._order_ids: dict[tuple[str, str], str] = {}        # (event, ticker) -> order_id
        self._filled: dict[str, Decimal] = {}                   # order_id -> cumulative filled
        self._order_to_event_leg: dict[str, tuple[str, str]] = {}   # order_id -> (event, ticker)

    def record(self, event: str, ticker: str, order_id: str) -> None:
        if not order_id:
            return
        self._order_ids[(event, ticker)] = order_id
        self._filled[order_id] = Decimal(0)
        self._order_to_event_leg[order_id] = (event, ticker)

    def forget(self, event: str) -> None:
        for key in [k for k in self._order_ids if k[0] == event]:
            oid = self._order_ids.pop(key)
            self._filled.pop(oid, None)
            self._order_to_event_leg.pop(oid, None)

    def event_order_ids(self, event: str) -> dict[str, str]:
        return {t: oid for (e, t), oid in self._order_ids.items() if e == event}

    def event_for_order(self, order_id: str) -> Optional[tuple[str, str]]:
        return self._order_to_event_leg.get(order_id)

    def record_cumulative(self, order_id: str, cumulative_filled: Decimal) -> Decimal:
        """Takes a cumulative fill count on this order, returns the delta
        vs. last time. Handles the first-fill case (delta == cumulative)."""
        prev = self._filled.get(order_id, Decimal(0))
        delta = cumulative_filled - prev
        self._filled[order_id] = cumulative_filled
        return delta if delta > 0 else Decimal(0)


class MakerMonitor:
    """Lifetime + loops for one live maker-strategy process.

    Caller is responsible for the process lock (scripts/run_maker_scanner.py
    takes it before constructing) and for loading the Settings. The monitor
    owns the live clients, inventory book, risk state, and store."""

    def __init__(
        self,
        settings: Settings,
        public_client: KalshiClient,
        exec_client: KalshiClient,
        public_ws: KalshiWSClient,
        private_ws: KalshiWSClient,
        risk: MakerRiskState,
        inventory: MakerInventoryBook,
    ) -> None:
        self.settings = settings
        self.public_client = public_client
        self.exec_client = exec_client
        self.public_ws = public_ws
        self.private_ws = private_ws
        self.risk = risk
        self.inventory = inventory
        self.store = maker_order_store_open(settings.maker_execution)
        self.tracker = RestingOrderTracker()
        self.books: dict[str, LocalBook] = {}
        self.leg_to_event: dict[str, str] = {}
        self.event_to_legs: dict[str, list[str]] = {}
        self._shutdown = asyncio.Event()

    async def run(self) -> None:
        """Entrypoint. Returns cleanly when _shutdown is set (SIGINT/TERM
        or breaker trip)."""
        mk = self.settings.maker_execution
        logger.info("maker_monitor_start",
                    strategy_id=mk.strategy_id,
                    target=mk.target,
                    enabled=mk.enabled,
                    observe_only=mk.observe_only)
        if not mk.enabled and not mk.observe_only:
            logger.warning("maker_monitor_disabled — exiting without placing orders")
            return
        if self.risk.tripped:
            logger.error("maker_breaker_tripped_at_startup",
                         reason=self.risk.tripped_reason)
            return

        tasks = [
            asyncio.create_task(self._stage1_loop(), name="stage1"),
            asyncio.create_task(self._public_ws_loop(), name="public_ws"),
            asyncio.create_task(self._private_ws_loop(), name="private_ws"),
            asyncio.create_task(self._sweep_loop(), name="sweep"),
        ]
        try:
            await self._shutdown.wait()
        finally:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            self.risk.save(self.settings.maker_execution.risk_state_path)
            logger.info("maker_monitor_stop")

    def request_shutdown(self) -> None:
        self._shutdown.set()

    # ---- Stage 1 (periodic) ----

    async def _stage1_loop(self) -> None:
        interval = self.settings.scanner.rescan_interval_s
        while not self._shutdown.is_set():
            try:
                await self._refresh_shortlist()
            except Exception as exc:
                logger.error("maker_stage1_failed", error=str(exc), exc_info=True)
            try:
                await asyncio.wait_for(self._shutdown.wait(), timeout=interval)
            except asyncio.TimeoutError:
                pass

    async def _refresh_shortlist(self) -> None:
        raw_events = await self.public_client.get_events(status="open", with_nested_markets=True)
        candidates = filter_events(
            raw_events,
            min_outcome_legs=self.settings.scanner.min_outcome_legs,
            max_outcome_legs=self.settings.scanner.max_outcome_legs,
            blacklisted_series=self.settings.scanner.blacklisted_series,
        )
        active = [c for c in candidates if has_recent_activity(c)]
        shortlist = select_by_volume(active, max_legs=self.settings.scanner.max_ws_legs)
        self.event_to_legs = {c.event_ticker: list(c.leg_tickers) for c in shortlist}
        self.leg_to_event = {t: ev for ev, legs in self.event_to_legs.items() for t in legs}
        # Books for newly-added legs start empty; snapshot will arrive on re-subscribe.
        for leg in self.leg_to_event:
            self.books.setdefault(leg, LocalBook())
        # Legs that fell off the shortlist can be forgotten (frees state for an
        # event we're no longer watching). Any ongoing inventory on a dropped
        # event stays in the inventory book until its fills clear.
        dropped = [t for t in self.books if t not in self.leg_to_event]
        for t in dropped:
            self.books.pop(t, None)
        logger.info(
            "maker_stage1_complete",
            stage1_candidates=len(candidates),
            active_candidates=len(active),
            shortlist_events=len(shortlist),
            shortlist_legs=sum(len(c.leg_tickers) for c in shortlist),
        )

    # ---- Public WS ----

    async def _public_ws_loop(self) -> None:
        """Subscribes to orderbook_delta for the current shortlist. On
        each tick, re-prices the affected event and submits a maker
        quote if the quoter clears."""
        while not self._shutdown.is_set():
            tickers = list(self.leg_to_event)
            if not tickers:
                await asyncio.sleep(1)
                continue
            try:
                async for msg in self.public_ws.stream(tickers, channels=("orderbook_delta",)):
                    if msg.get("type") == WS_RECONNECTED_MESSAGE_TYPE:
                        for book in self.books.values():
                            book.invalidate()
                        continue
                    changed = handle_message(msg, self.books)
                    if changed is None:
                        continue
                    event_ticker = self.leg_to_event.get(changed)
                    if event_ticker:
                        await self._reprice(event_ticker)
                    if self._shutdown.is_set():
                        break
            except Exception as exc:
                logger.error("maker_public_ws_error", error=str(exc), exc_info=True)
                await asyncio.sleep(1)

    async def _reprice(self, event_ticker: str) -> None:
        legs = self._legs_for(event_ticker)
        if legs is None:
            return
        # Try both directions; maker's margin gate decides which (if any) to post.
        for detector in (compute_opportunity, compute_no_opportunity):
            opp = detector(
                event_ticker, legs,
                (self.settings.arb.buy_basket_max_cost_usd if detector is compute_opportunity
                 else self.settings.arb.buy_all_no_min_edge_usd),
                self.settings.scanner.min_depth_contracts,
            )
            if opp is None:
                continue
            # Don't re-quote an event that already has resting orders out.
            if self.tracker.event_order_ids(event_ticker):
                continue
            await self._quote_and_submit(opp)
            break   # at most one direction per re-price

    def _legs_for(self, event_ticker: str) -> Optional[list[LegQuote]]:
        tickers = self.event_to_legs.get(event_ticker)
        if not tickers:
            return None
        max_age_s = self.settings.scanner.max_quote_age_ms / 1000
        now_m = time.monotonic()
        legs: list[LegQuote] = []
        for t in tickers:
            book = self.books.get(t)
            if book is None or not book.snapshot_received or not book.is_fresh(max_age_s, now_m):
                return None
            q = book.best_yes_quote()
            if q is None:
                return None
            best_ask, ask_size, best_bid, bid_size = q
            legs.append(LegQuote(ticker=t, best_ask=best_ask, ask_size=ask_size, best_bid=best_bid, bid_size=bid_size))
        return legs

    async def _quote_and_submit(self, opportunity) -> None:
        if self.settings.maker_execution.observe_only:
            await self._observe_only_log(opportunity)
            return
        try:
            result: MakerExecutionResult = await execute_opportunity_maker(
                self.exec_client, opportunity,
                settings=self.settings.maker_execution,
                risk=self.risk,
                inventory=self.inventory,
                store=self.store,
                event_cooldown_s=self.settings.execution.event_cooldown_s,
                max_baskets_per_day=self.settings.execution.max_baskets_per_day,
            )
        except Exception as exc:
            logger.error("maker_execute_failed", event_ticker=opportunity.event_ticker,
                         error=str(exc), exc_info=True)
            return
        if result.refused_reason:
            logger.info("maker_refused", event_ticker=opportunity.event_ticker,
                        reason=result.refused_reason)
            return
        # Record each resting order the batch returned so the private WS loop
        # can tie fills back to events.
        for leg in result.leg_results:
            if leg.order_result and leg.order_result.order_id:
                self.tracker.record(opportunity.event_ticker, leg.ticker, leg.order_result.order_id)
        self.risk.save(self.settings.maker_execution.risk_state_path)
        logger.info(
            "maker_quote_submitted",
            event_ticker=opportunity.event_ticker,
            direction=opportunity.direction,
            legs=len(result.leg_results),
            rejected=sum(1 for lr in result.leg_results if lr.error),
            expected_margin_usd=str(result.quote.expected_margin_usd) if result.quote else "",
        )

    async def _observe_only_log(self, opportunity) -> None:
        """observe_only mode: run the quote math and append the result to
        a JSONL (never call the exchange). Caller analyzes the file
        later to measure fill rate / adverse selection against real
        forward book data.

        Writes to settings.maker_execution.orders_log_path with a
        maker_would_quote type tag so a later analyzer can walk the file
        and, for each row, check against stream history whether the quote
        would have filled before being canceled or out-of-the-money."""
        from execution.maker.quoter import quote_basket
        mk = self.settings.maker_execution
        quote = quote_basket(
            opportunity,
            quote_offset_ticks=mk.quote_offset_ticks,
            target_margin_per_basket=mk.target_margin_per_basket,
            size=opportunity.max_size,
        )
        if quote is None:
            return
        record = {
            "at": datetime.now(timezone.utc).isoformat(),
            "type": "maker_would_quote",
            "event_ticker": opportunity.event_ticker,
            "direction": opportunity.direction,
            "size": str(quote.size),
            "expected_basket_cost_usd": str(quote.expected_basket_cost_usd),
            "expected_margin_usd": str(quote.expected_margin_usd),
            "legs": [
                {
                    "ticker": lq.ticker,
                    "side": lq.side.value,
                    "price": str(lq.price),
                    "mid": str(lq.mid),
                    "ask_at_quote": str(self._ask_for(lq.ticker)),
                    "bid_at_quote": str(self._bid_for(lq.ticker)),
                }
                for lq in quote.leg_quotes
            ],
        }
        path = mk.orders_log_path
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a") as f:
            f.write(json.dumps(record) + "\n")
        logger.info(
            "maker_would_quote",
            event_ticker=opportunity.event_ticker,
            direction=opportunity.direction,
            margin_usd=str(quote.expected_margin_usd),
        )

    def _ask_for(self, ticker: str) -> Decimal:
        book = self.books.get(ticker)
        if book is None:
            return Decimal(0)
        q = book.best_yes_quote()
        return q[0] if q else Decimal(0)

    def _bid_for(self, ticker: str) -> Decimal:
        book = self.books.get(ticker)
        if book is None:
            return Decimal(0)
        q = book.best_yes_quote()
        return q[2] if q else Decimal(0)

    # ---- Private WS (fills) ----

    async def _private_ws_loop(self) -> None:
        """Subscribes to order_update + fill channels. Each message routes
        to manage_fill_event with the delta since the last report on this
        order_id.

        observe_only mode skips this entirely — there are no orders to
        track fills for.

        Kalshi's exact v2 private-channel shape is not canonically captured
        in this repo yet — the parsing below tolerates the fields it can
        find and logs anything it can't, so a schema surprise degrades
        gracefully. First live message will tell us whether this matches
        reality; if not, tune the field names here without touching the
        inventory or executor layers."""
        if self.settings.maker_execution.observe_only:
            logger.info("maker_private_ws_skipped_observe_only")
            await self._shutdown.wait()
            return
        while not self._shutdown.is_set():
            try:
                async for msg in self.private_ws.stream([], channels=_PRIVATE_CHANNELS):
                    if msg.get("type") == WS_RECONNECTED_MESSAGE_TYPE:
                        continue
                    msg_type = msg.get("type")
                    body = msg.get("msg", {}) or {}
                    order_id = str(body.get("order_id") or "")
                    if not order_id:
                        continue
                    located = self.tracker.event_for_order(order_id)
                    if located is None:
                        logger.debug("maker_private_msg_unknown_order",
                                     type=msg_type, order_id=order_id)
                        continue
                    event_ticker, ticker = located

                    if msg_type == "fill":
                        count_field = body.get("count_fp", body.get("count", 0))
                        price_field = body.get("price_dollars", body.get("price"))
                        cumulative = self._apply_fill_count(order_id, Decimal(str(count_field or 0)))
                        if cumulative is None:
                            continue
                        fill_price = Decimal(str(price_field or 0))
                        await manage_fill_event(
                            self.exec_client, event_ticker, ticker,
                            cumulative_filled_delta=cumulative,
                            fill_price=fill_price,
                            remaining_size=Decimal(str(body.get("remaining_count_fp", 0))),
                            resting_order_ids=self.tracker.event_order_ids(event_ticker),
                            inventory=self.inventory,
                            exit_bids=self._best_bids_for_event(event_ticker),
                            store=self.store,
                        )
                    elif msg_type == "order_update":
                        status = body.get("status")
                        if status in (OrderStatus.CANCELED.value, OrderStatus.FILLED.value, OrderStatus.REJECTED.value):
                            # Terminal: drop from tracker so we stop forwarding its fills.
                            self.tracker.forget(event_ticker)
                            self.inventory.close_event(event_ticker)
                    if self._shutdown.is_set():
                        break
            except Exception as exc:
                logger.error("maker_private_ws_error", error=str(exc), exc_info=True)
                await asyncio.sleep(1)

    def _apply_fill_count(self, order_id: str, new_cumulative: Decimal) -> Optional[Decimal]:
        """Return the delta since last time (or None if nothing changed /
        the order is unknown)."""
        delta = self.tracker.record_cumulative(order_id, new_cumulative)
        return delta if delta > 0 else None

    def _best_bids_for_event(self, event_ticker: str) -> dict[str, Decimal]:
        """YES bid per leg (for buy_basket flatten) — the maker_execution
        path currently passes this as `exit_bids` regardless of direction;
        buy_all_no flatten derives a NO bid from the taker's own symmetry
        (1 - YES ask) at the leg level, applied in executor's flatten
        branch. For v1 the monitor passes the YES bid; a later patch can
        pass the direction-correct bid from here instead."""
        bids: dict[str, Decimal] = {}
        for t in self.event_to_legs.get(event_ticker, []):
            book = self.books.get(t)
            if book is None:
                continue
            quote = book.best_yes_quote()
            if quote is None:
                continue
            bids[t] = quote[2]   # best_bid (YES side)
        return bids

    # ---- Periodic sweep ----

    async def _sweep_loop(self) -> None:
        """Future home of max_resting_order_age_s / requote_threshold_ticks
        enforcement. v1 is a stub that just keeps the task alive so the
        monitor's task set is stable; the first real sweep policy can
        land here without touching run()."""
        while not self._shutdown.is_set():
            try:
                await asyncio.wait_for(self._shutdown.wait(), timeout=_SWEEP_INTERVAL_S)
            except asyncio.TimeoutError:
                pass


def install_shutdown_handlers(monitor: MakerMonitor) -> None:
    """Translate SIGINT / SIGTERM into monitor.request_shutdown. Runs on
    the event loop the caller must already have running."""
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, monitor.request_shutdown)
        except NotImplementedError:
            # Windows / unusual loop; fall back to default behavior
            pass
