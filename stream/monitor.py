"""Stage 3 — WS streaming, continuously, over the highest-volume slice of
Stage 1's same-day universe.

_live variant: Stage 1 (scanner.filters) restricts the universe to
same-day-settling events before this stage ever sees them (see that
module's docstring).

WS subscription selection (VERIFIED LIVE 2026-08-26 this was broken):
earlier this stage built its subscription set from Stage 2
(scanner.orderbook_scan) — a REST pass that only kept events *already*
clearing the fee-adjusted edge threshold at the moment of that poll. That
meant Stage 3 could only ever watch an opportunity that happened to already
be open when a 5-minute-interval REST snapshot landed on it — structurally
incapable of catching one that opened and closed between polls, which
given how tight real edges are (observed: closest live basket was 3 cents
*above* the $1.00 threshold, not under it) is the only kind that actually
exists. Now it instead selects via scanner.filters.select_by_volume,
ranking Stage 1's same-day + has_recent_activity candidates by 24h volume
and filling settings.scanner.max_ws_legs (200) with the highest-volume
ones — computed entirely from Stage 1's free /events payload, no REST book
calls needed to decide what to watch. Every leg in that set then gets
watched continuously and re-priced on every WS tick, so an edge is caught
the moment it opens rather than only if it survives until the next poll.

Each cycle: Stage 1 runs to refresh the candidate pool, select_by_volume
picks the WS subscription set, then this module opens one WS subscription
over it and streams orderbook_delta ticks for settings.scanner.rescan_interval_s
before looping back to refresh Stage 1 and re-rank by volume. On every book
update it re-runs the same scanner.basket edge calc, and logs (console +
JSONL) on each open/close transition of a basket opportunity — not on
every tick, so a basket sitting just past the threshold doesn't spam the
log once per delta.

Execution (VERIFIED LIVE 2026-08-26, added after real demo capital was on
the line): a fresh/still-open opportunity triggers _reconcile_execution,
not a direct fire, when settings.execution.enabled and a demo_client are
provided. That function re-ranks every currently-open, not-yet-executed
opportunity by execution.capital.hourly_return_rate (edge per dollar per
hour of capital lock-up, not raw edge_usd — a basket settling sooner frees
capital to redeploy sooner) against the demo account's live balance
(KalshiClient.get_balance), and executes as many as currently fit, via
execution/executor.py's no-slippage IOC-style fill — see that module's and
execution/capital.py's docstrings.

See kalshi/ws_client.py's docstring for what's confirmed vs. assumed about
the WS message schema this module parses.
"""

from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Optional

import structlog

from config.loader import Settings
from execution.capital import ExecutionCandidate, estimated_capital_required, select_executable
from execution.executor import AtomicExecutionResult, LegExecutionResult, execute_opportunity
from kalshi.client import ExchangeError, KalshiClient
from kalshi.ws_client import KalshiWSClient
from scanner.basket import BasketOpportunity, LegQuote, compute_opportunity
from scanner.filters import EventCandidate, filter_events, has_recent_activity, select_by_volume

logger = structlog.get_logger(__name__)


def _parse_decimal(value) -> Optional[Decimal]:
    """Returns None (rather than raising) on a missing/unparseable value —
    handle_message logs and skips the message instead of crashing the
    whole WS loop on one bad field."""
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except InvalidOperation:
        return None


@dataclass
class LocalBook:
    """Locally maintained yes/no bid book for one ticker, built from WS
    snapshot/delta messages. Mirrors kalshi.client.get_orderbook's
    normalization: Kalshi's book is bid-only per side, so the YES ask is
    synthesized as 1 - best NO bid, and vice versa.

    VERIFIED LIVE 2026-08-26 against wss://api.elections.kalshi.com/trade-api/ws/v2:
    an orderbook_snapshot's levels arrive as `yes_dollars_fp`/`no_dollars_fp`
    (a side missing entirely, e.g. no resting YES bids at snapshot time, is
    just omitted — not sent as []), each a [price_dollars_str, size_fp_str]
    pair. An orderbook_delta's `delta_fp` is a *signed relative change* to
    apply to whatever's already resting at that price (observed a live
    -999.00), not the new absolute size — apply_delta below adds it to the
    existing level and drops the level if that nets to <=0. The original
    implementation here treated delta_fp as an absolute size and read
    nonexistent field names (`price`/`delta`/`yes`/`no` instead of
    `price_dollars`/`delta_fp`/`yes_dollars_fp`/`no_dollars_fp`) — it would
    have raised on the very first real delta message
    (Decimal(str(None)) on the missing `price` key) rather than just
    mispricing anything, so this was never actually exercised against a
    live stream before now."""

    yes_bids: dict[Decimal, Decimal] = field(default_factory=dict)  # price -> size
    no_bids: dict[Decimal, Decimal] = field(default_factory=dict)

    def apply_snapshot(self, yes_levels: list[tuple[Decimal, Decimal]], no_levels: list[tuple[Decimal, Decimal]]) -> None:
        self.yes_bids = {p: s for p, s in yes_levels if s > 0}
        self.no_bids = {p: s for p, s in no_levels if s > 0}

    def apply_delta(self, side: str, price: Decimal, size_delta: Decimal) -> None:
        book = self.yes_bids if side == "yes" else self.no_bids
        new_size = book.get(price, Decimal(0)) + size_delta
        if new_size <= 0:
            book.pop(price, None)
        else:
            book[price] = new_size

    def best_yes_quote(self) -> Optional[tuple[Decimal, Decimal, Decimal, Decimal]]:
        """Returns (best_ask, ask_size, best_bid, bid_size) for the YES
        side, or None if either side of the book is currently empty
        (no snapshot received yet, or all levels pulled)."""
        if not self.yes_bids or not self.no_bids:
            return None
        best_bid_price = max(self.yes_bids)
        best_no_bid_price = max(self.no_bids)
        return (
            Decimal(1) - best_no_bid_price,    # best_ask
            self.no_bids[best_no_bid_price],   # ask_size
            best_bid_price,                     # best_bid
            self.yes_bids[best_bid_price],      # bid_size
        )


def handle_message(msg: dict, books: dict[str, LocalBook]) -> Optional[str]:
    """Applies one WS message to `books` in place. Returns the ticker that
    changed (so the caller knows which event to re-price), or None for
    control messages / tickers we're not tracking / unparseable messages —
    all logged, never raised, so a schema surprise degrades gracefully."""
    msg_type = msg.get("type")
    body = msg.get("msg", {}) or {}
    ticker = body.get("market_ticker")

    if msg_type in ("subscribed", "error"):
        logger.info("ws_control_message", type=msg_type, msg=msg)
        return None

    if not ticker or ticker not in books:
        return None

    if msg_type == "orderbook_snapshot":
        yes_levels = [
            (p, s) for p, s in
            ((_parse_decimal(pr), _parse_decimal(sz)) for pr, sz in body.get("yes_dollars_fp", []))
            if p is not None and s is not None
        ]
        no_levels = [
            (p, s) for p, s in
            ((_parse_decimal(pr), _parse_decimal(sz)) for pr, sz in body.get("no_dollars_fp", []))
            if p is not None and s is not None
        ]
        books[ticker].apply_snapshot(yes_levels, no_levels)
        return ticker

    if msg_type == "orderbook_delta":
        side = body.get("side")
        if side not in ("yes", "no"):
            logger.warning("ws_delta_unknown_side", ticker=ticker, side=side)
            return None
        price = _parse_decimal(body.get("price_dollars"))
        size_delta = _parse_decimal(body.get("delta_fp"))
        if price is None or size_delta is None:
            logger.warning("ws_delta_missing_fields", ticker=ticker, body=body)
            return None
        books[ticker].apply_delta(side, price, size_delta)
        return ticker

    logger.debug("ws_message_unhandled", type=msg_type)
    return None


def _append_opportunity_jsonl(opportunity: BasketOpportunity, path: Path) -> None:
    record = {
        "detected_at": datetime.now(timezone.utc).isoformat(),
        "event_ticker": opportunity.event_ticker,
        "direction": opportunity.direction,
        "legs": opportunity.legs,
        "basket_price_usd": str(opportunity.basket_price_usd),
        "edge_usd": str(opportunity.edge_usd),
        "max_size": str(opportunity.max_size),
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a") as f:
        f.write(json.dumps(record) + "\n")


def _append_order_jsonl(opportunity: BasketOpportunity, leg_results: list[LegExecutionResult], path: Path, kind: str = "basket") -> None:
    record = {
        "placed_at": datetime.now(timezone.utc).isoformat(),
        "kind": kind,   # "basket" (the original attempt) or "unwind" (an offsetting flatten after a mismatch — see execution/executor.py)
        "event_ticker": opportunity.event_ticker,
        "direction": opportunity.direction,
        "legs": [
            {
                "ticker": r.ticker,
                "action": r.action.value,
                "limit_price": str(r.limit_price),
                "size": str(r.size),
                "order_id": r.order_result.order_id if r.order_result else None,
                "status": r.order_result.status.value if r.order_result else None,
                "filled_size": str(r.order_result.filled_size) if r.order_result else None,
                "error": r.error,
            }
            for r in leg_results
        ],
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a") as f:
        f.write(json.dumps(record) + "\n")


_HEARTBEAT_MIN_INTERVAL_S = 5   # throttle how often the status file is rewritten while streaming


def _write_scanner_status(path: Path, **fields: Any) -> None:
    """Overwrites data/scanner_status.json with the current state of this
    process — read by ui/app.py's Production Scanner Status section so the
    dashboard (a separate process) can show what's actually being watched
    and whether this loop is still alive, not just what's in the
    opportunities/orders JSONL history. Written atomically (temp file +
    rename) so a concurrent read never sees a half-written file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(".tmp")
    tmp_path.write_text(json.dumps({"updated_at": datetime.now(timezone.utc).isoformat(), **fields}, default=str))
    tmp_path.replace(path)


def _log_leg_results(event_ticker: str, leg_results: list[LegExecutionResult], placed_event: str, rejected_event: str) -> None:
    for r in leg_results:
        if r.order_result is not None:
            logger.info(
                placed_event,
                event_ticker=event_ticker,
                ticker=r.ticker,
                action=r.action.value,
                limit_price=str(r.limit_price),
                size=str(r.size),
                order_id=r.order_result.order_id,
                status=r.order_result.status.value,
                filled_size=str(r.order_result.filled_size),
            )
        else:
            logger.warning(
                rejected_event,
                event_ticker=event_ticker,
                ticker=r.ticker,
                action=r.action.value,
                limit_price=str(r.limit_price),
                size=str(r.size),
                error=r.error,
            )


async def _execute_and_log(opportunity: BasketOpportunity, demo_client: KalshiClient, settings: Settings) -> Optional[AtomicExecutionResult]:
    """Fires execution/executor.py's batched basket placement against the
    demo account and logs every leg's outcome (and, if the legs filled
    unevenly, every unwind leg's outcome too) — never raises back into
    _reprice_event, since a demo order attempt failing shouldn't interrupt
    detection. Returns the full AtomicExecutionResult (None if execution
    was skipped for missing credentials) so the caller can update believed
    positions and feed .mismatched / .unwind_confirmed to the circuit
    breaker — see execution/executor.py's module docstring for why both
    matter, not just whether it mismatched."""
    if not demo_client.has_signing_credentials:
        logger.warning(
            "execution_skipped_no_demo_credentials",
            event_ticker=opportunity.event_ticker,
            reason="KALSHI_DEMO_API_KEY_ID/KALSHI_DEMO_PRIVATE_KEY_PATH not configured",
        )
        return None

    result = await execute_opportunity(demo_client, opportunity)
    _log_leg_results(opportunity.event_ticker, result.leg_results, "demo_order_placed", "demo_order_rejected")
    _append_order_jsonl(opportunity, result.leg_results, settings.execution.orders_log_path, kind="basket")

    if result.mismatched:
        filled_sizes = {r.ticker: (str(r.order_result.filled_size) if r.order_result else "0") for r in result.leg_results}
        logger.error(
            "basket_mismatch_unwind",
            event_ticker=opportunity.event_ticker,
            reason="legs filled unevenly — unwinding the excess to avoid a naked position",
            filled_sizes=filled_sizes,
        )
        _log_leg_results(opportunity.event_ticker, result.unwind_results, "unwind_order_placed", "unwind_order_rejected")
        _append_order_jsonl(opportunity, result.unwind_results, settings.execution.orders_log_path, kind="unwind")
        if not result.unwind_confirmed:
            logger.error(
                "unwind_not_confirmed",
                event_ticker=opportunity.event_ticker,
                reason="unwind order(s) did not confirm fully filled within the timeout — a naked position may still be open",
            )

    return result


def _update_believed_positions(believed_positions: dict[str, Decimal], result: AtomicExecutionResult) -> None:
    """Keeps a running net-position estimate per ticker from our own fill
    responses — the buy side of every basket adds, the sell side of every
    unwind subtracts. This is what _reconcile_positions checks against
    Kalshi's actual records; it's deliberately just addition over
    responses we've already seen, not a query, so it stays cheap enough
    to update on every execution."""
    for lr in result.leg_results:
        if lr.order_result is not None:
            believed_positions[lr.ticker] = believed_positions.get(lr.ticker, Decimal(0)) + lr.order_result.filled_size
    for lr in result.unwind_results:
        if lr.order_result is not None:
            believed_positions[lr.ticker] = believed_positions.get(lr.ticker, Decimal(0)) - lr.order_result.filled_size


async def _reconcile_positions(demo_client: KalshiClient, believed_positions: dict[str, Decimal]) -> dict[str, tuple[Decimal, Decimal]]:
    """Compares our internally-tracked believed_positions (built purely
    from order-response bookkeeping in _update_believed_positions) against
    Kalshi's actual live positions (KalshiClient.get_positions). Returns
    {ticker: (believed, actual)} for every ticker where they disagree —
    empty means everything reconciles. This is what catches the risk a
    bookkeeping-only view can't: a cancel that raced a fill, an unwind
    order that filled later than _confirm_unwind's timeout waited for,
    anything else that would otherwise drift silently. Catches
    ExchangeError and returns {} (skips this pass) rather than raising —
    a failed reconciliation *check* shouldn't itself interrupt detection,
    though a real *mismatch* found by a successful check does trip the
    circuit breaker (see the caller in _stream_shortlist)."""
    try:
        actual = await demo_client.get_positions()
    except ExchangeError as exc:
        logger.warning("reconciliation_check_failed", error=str(exc))
        return {}

    mismatches: dict[str, tuple[Decimal, Decimal]] = {}
    for ticker in set(believed_positions) | set(actual):
        believed = believed_positions.get(ticker, Decimal(0))
        real = actual.get(ticker, Decimal(0))
        if believed != real:
            mismatches[ticker] = (believed, real)
    return mismatches


_BALANCE_CACHE_TTL_S = 3.0   # see _BalanceCache docstring


class _BalanceCache:
    """Short-TTL cache around KalshiClient.get_balance(), with a
    reservation ledger to close a cross-call race the plain cache alone
    left open. Without the cache, a live signed REST round-trip on every
    single WS tick that has an unexecuted opportunity pending would (a)
    add latency between "we decided to execute" and "the order is
    actually sent" — the book can move in that window, so this isn't free
    even though the fill itself stays no-slippage — and (b) block the
    single-threaded WS message loop from processing other ticks while the
    request is in flight.

    But a plain TTL cache has its own gap: two separate
    _reconcile_execution calls landing within the same TTL window (e.g.
    two different opportunities each triggering their own fresh-open
    call) would both read the SAME cached balance, neither aware of what
    the other just spent — collectively able to over-commit relative to
    what's actually available, even though each individual call correctly
    budgets within itself. reserve() fixes this: every executed
    candidate's estimated cost is subtracted from what get() reports for
    the rest of the current TTL window, so the second call sees the first
    call's spending even though the underlying cached balance hasn't
    actually been re-fetched yet. The reservation resets to zero exactly
    when the cache does refresh, since a fresh balance from Kalshi already
    reflects everything spent before that point."""

    def __init__(self) -> None:
        self._value: Optional[Decimal] = None
        self._checked_at = 0.0
        self._reserved = Decimal(0)

    async def get(self, client: KalshiClient) -> Decimal:
        now = time.monotonic()
        if self._value is None or now - self._checked_at >= _BALANCE_CACHE_TTL_S:
            self._value = await client.get_balance()
            self._checked_at = now
            self._reserved = Decimal(0)
        return self._value - self._reserved

    def reserve(self, amount: Decimal) -> None:
        self._reserved += amount


class _CircuitBreaker:
    """Auto-disables execution after repeated leg-mismatched baskets in a
    row — a safety net for exactly the kind of incident a human might not
    notice in time (see execution/executor.py's module docstring: 8
    repeated mismatched baskets on one volatile market accumulated a real
    $100+ naked position before a human caught it via the dashboard and
    manually disabled execution). Scoped for the life of the whole
    run_stage3 process — created once, before its while-True loop, not
    per rescan cycle or per WS reconnect — since a trip must survive
    Stage 1's 5-minute rescan; resetting it on rescan would silently
    re-arm execution right back into the same failure. Only a process
    restart (a deliberate human action, after investigating) clears it.
    A single non-mismatched (fully matched) execution resets the
    consecutive-mismatch streak — one bad fill on an otherwise-healthy
    market shouldn't halt everything."""

    def __init__(self, max_mismatches: int) -> None:
        self._max = max_mismatches
        self._consecutive_mismatches = 0
        self.tripped = False
        self.tripped_reason: Optional[str] = None

    def record(self, mismatched: bool) -> None:
        if self.tripped:
            return
        if mismatched:
            self._consecutive_mismatches += 1
            if self._consecutive_mismatches >= self._max:
                self.force_trip(
                    f"{self._consecutive_mismatches} consecutive baskets needed an unwind "
                    "(legs filled unevenly) — execution auto-disabled; investigate, then "
                    "restart the process to re-arm."
                )
        else:
            self._consecutive_mismatches = 0

    def force_trip(self, reason: str) -> None:
        """Trips immediately, bypassing the consecutive-mismatch counter
        — used for a failure worse than an ordinary mismatch: a confirmed-
        failed unwind (execution/executor.py's _confirm_unwind returning
        False — a naked position AND a failed attempt to fix it) or a
        position-reconciliation mismatch (our bookkeeping has diverged
        from what Kalshi's own records show)."""
        if self.tripped:
            return
        self.tripped = True
        self.tripped_reason = reason
        logger.error("circuit_breaker_tripped", reason=reason)


async def _reconcile_execution(
    open_opportunities: dict[str, BasketOpportunity],
    executed_opportunities: set[str],
    event_by_ticker: dict[str, EventCandidate],
    demo_client: KalshiClient,
    settings: Settings,
    balance_cache: _BalanceCache,
    circuit_breaker: _CircuitBreaker,
    believed_positions: dict[str, Decimal],
) -> None:
    """Capital-aware execution pass — re-ranks every currently-open,
    not-yet-executed opportunity by execution.capital.hourly_return_rate
    against the demo account's balance (via balance_cache — see its
    docstring for why this isn't a fresh REST call every time), and
    executes as many as fit. Called both on every fresh open transition
    (_reprice_event) and off the streaming heartbeat (_stream_shortlist),
    so an opportunity skipped earlier for insufficient capital gets
    reconsidered if balance frees up later rather than only once. See
    execution/capital.py's docstring for why per-hour return (not raw
    edge_usd) is the ranking key. Stops immediately (before even checking
    balance) if circuit_breaker.tripped — see that class's docstring."""
    if circuit_breaker.tripped:
        return
    if not demo_client.has_signing_credentials:
        return

    unexecuted = [t for t in open_opportunities if t not in executed_opportunities]
    if not unexecuted:
        return

    candidates: list[ExecutionCandidate] = []
    for event_ticker in unexecuted:
        opportunity = open_opportunities[event_ticker]
        # BasketOpportunity.legs holds leg tickers; event_by_ticker is
        # keyed by leg ticker, so any one leg resolves back to the event's
        # EventCandidate (for its expected_settlement_time).
        event_candidate = event_by_ticker.get(opportunity.legs[0])
        if event_candidate is None:
            continue
        candidates.append(ExecutionCandidate(event_ticker, opportunity, event_candidate.expected_settlement_time))
    if not candidates:
        return

    try:
        available = await balance_cache.get(demo_client)
    except ExchangeError as exc:
        logger.warning("capital_check_failed", error=str(exc))
        return

    selected = select_executable(candidates, available, datetime.now(timezone.utc))
    skipped = {c.event_ticker for c in candidates} - {c.event_ticker for c in selected}
    for event_ticker in skipped:
        logger.info("execution_skipped_insufficient_capital", event_ticker=event_ticker, available_capital=str(available))

    for candidate in selected:
        executed_opportunities.add(candidate.event_ticker)
        balance_cache.reserve(estimated_capital_required(candidate.opportunity))   # see _BalanceCache's docstring for the cross-call race this closes

        result = await _execute_and_log(candidate.opportunity, demo_client, settings)
        if result is None:
            continue   # skipped for missing credentials — nothing to reconcile or feed the breaker
        _update_believed_positions(believed_positions, result)

        if result.mismatched and not result.unwind_confirmed:
            circuit_breaker.force_trip(
                f"unwind for {candidate.event_ticker} did not confirm fully filled within the timeout — "
                "a naked position may still be open; investigate immediately, then restart the process to re-arm."
            )
        else:
            circuit_breaker.record(result.mismatched)
        if circuit_breaker.tripped:
            break   # don't fire the rest of this pass's selections after tripping


async def _reprice_event(
    ticker: str,
    books: dict[str, LocalBook],
    event_by_ticker: dict[str, EventCandidate],
    settings: Settings,
    open_opportunities: dict[str, BasketOpportunity],
    executed_opportunities: set[str],
    demo_client: Optional[KalshiClient],
    balance_cache: _BalanceCache,
    circuit_breaker: _CircuitBreaker,
    believed_positions: dict[str, Decimal],
) -> None:
    """Re-runs the basket edge calc for the event that owns `ticker` and
    logs an open/close transition if the opportunity state changed. A fresh
    close->open transition also triggers a capital-aware execution pass
    (_reconcile_execution), if armed (settings.execution.enabled and
    demo_client provided)."""
    candidate = event_by_ticker.get(ticker)
    if candidate is None:
        return

    legs: list[LegQuote] = []
    for leg_ticker in candidate.leg_tickers:
        quote = books[leg_ticker].best_yes_quote()
        if quote is None:
            return  # book not fully populated yet for this event — wait for more ticks
        best_ask, ask_size, best_bid, bid_size = quote
        legs.append(LegQuote(ticker=leg_ticker, best_ask=best_ask, ask_size=ask_size, best_bid=best_bid, bid_size=bid_size))

    opportunity = compute_opportunity(
        event_ticker=candidate.event_ticker,
        legs=legs,
        buy_basket_max_cost_usd=settings.arb.buy_basket_max_cost_usd,
        min_depth_contracts=settings.scanner.min_depth_contracts,
    )

    was_open = candidate.event_ticker in open_opportunities
    if opportunity is not None:
        open_opportunities[candidate.event_ticker] = opportunity
        if not was_open:
            logger.info(
                "opportunity_detected",
                stage="stage3",
                event_ticker=opportunity.event_ticker,
                direction=opportunity.direction,
                legs=opportunity.legs,
                basket_price_usd=str(opportunity.basket_price_usd),
                edge_usd=str(opportunity.edge_usd),
                max_size=str(opportunity.max_size),
            )
            _append_opportunity_jsonl(opportunity, settings.logging.opportunities_log_path)
        if settings.execution.enabled and demo_client is not None:
            await _reconcile_execution(
                open_opportunities, executed_opportunities, event_by_ticker, demo_client, settings,
                balance_cache, circuit_breaker, believed_positions,
            )
    elif was_open:
        del open_opportunities[candidate.event_ticker]
        executed_opportunities.discard(candidate.event_ticker)
        logger.info("opportunity_closed", event_ticker=candidate.event_ticker)


_POSITION_RECONCILE_INTERVAL_S = 30   # throttled well below the 5s heartbeat — positions only change when we execute, this is a safety check, not a hot path


async def _stream_shortlist(
    ws_client: KalshiWSClient,
    shortlist: list[EventCandidate],
    settings: Settings,
    demo_client: Optional[KalshiClient],
    status: dict[str, Any],
    circuit_breaker: _CircuitBreaker,
    believed_positions: dict[str, Decimal],
) -> None:
    books: dict[str, LocalBook] = {t: LocalBook() for c in shortlist for t in c.leg_tickers}
    event_by_ticker: dict[str, EventCandidate] = {t: c for c in shortlist for t in c.leg_tickers}
    open_opportunities: dict[str, BasketOpportunity] = {}
    executed_opportunities: set[str] = set()
    balance_cache = _BalanceCache()
    last_heartbeat = 0.0
    last_reconciled = 0.0
    last_reconciliation_mismatches: dict[str, tuple[Decimal, Decimal]] = {}

    async for msg in ws_client.stream(list(books.keys())):
        ticker = handle_message(msg, books)
        if ticker is not None:
            await _reprice_event(
                ticker, books, event_by_ticker, settings, open_opportunities, executed_opportunities,
                demo_client, balance_cache, circuit_breaker, believed_positions,
            )

        now = time.monotonic()
        if now - last_heartbeat >= _HEARTBEAT_MIN_INTERVAL_S:
            last_heartbeat = now
            if settings.execution.enabled and demo_client is not None:
                # Retries any opportunity still open but skipped earlier for
                # insufficient capital, in case balance freed up since —
                # _reprice_event above only re-evaluates on a tick for one
                # of THIS opportunity's own legs, so a long-open, still-
                # unexecuted candidate could otherwise go unreconsidered
                # between its own ticks.
                await _reconcile_execution(
                    open_opportunities, executed_opportunities, event_by_ticker, demo_client, settings,
                    balance_cache, circuit_breaker, believed_positions,
                )

                if now - last_reconciled >= _POSITION_RECONCILE_INTERVAL_S:
                    last_reconciled = now
                    mismatches = await _reconcile_positions(demo_client, believed_positions)
                    last_reconciliation_mismatches = mismatches
                    if mismatches:
                        logger.error(
                            "position_reconciliation_mismatch",
                            mismatches={t: {"believed": str(b), "actual": str(a)} for t, (b, a) in mismatches.items()},
                        )
                        circuit_breaker.force_trip(
                            f"position reconciliation found {len(mismatches)} ticker(s) where our records "
                            "disagree with Kalshi's — our bookkeeping can no longer be trusted; investigate "
                            "immediately, then restart the process to re-arm."
                        )

            _write_scanner_status(
                settings.logging.status_path,
                **(status | {
                    "ws_connected": True,
                    "open_opportunities": len(open_opportunities),
                    "pending_execution": len(open_opportunities) - len(executed_opportunities),
                    "circuit_breaker_tripped": circuit_breaker.tripped,
                    "circuit_breaker_reason": circuit_breaker.tripped_reason,
                    "position_reconciliation_mismatches": {
                        t: {"believed": str(b), "actual": str(a)} for t, (b, a) in last_reconciliation_mismatches.items()
                    },
                    "phase": "streaming",
                }),
            )


async def run_stage3(
    client: KalshiClient,
    ws_client: KalshiWSClient,
    settings: Settings,
    demo_client: Optional[KalshiClient] = None,
) -> None:
    """Runs forever: refreshes Stage 1 + the volume ranking every
    settings.scanner.rescan_interval_s and streams Stage 3 continuously
    over whatever the top settings.scanner.max_ws_legs by volume are in
    between (see module docstring for why this replaced the old
    Stage-2-gated shortlist). demo_client, when provided (see
    scripts/run_scanner.py — only constructed when settings.execution.enabled),
    is where a freshly detected opportunity's basket gets placed — see
    _execute_and_log."""
    circuit_breaker = _CircuitBreaker(settings.execution.circuit_breaker_max_mismatches)   # created once — see that class's docstring for why it must survive rescans
    believed_positions: dict[str, Decimal] = {}   # created once, same reason — net position is a running total across the whole process, not per rescan cycle

    while True:
        raw_events = await client.get_all_events(status="open", with_nested_markets=True)
        candidates = filter_events(
            raw_events,
            min_outcome_legs=settings.scanner.min_outcome_legs,
            max_outcome_legs=settings.scanner.max_outcome_legs,
        )
        active_candidates = [c for c in candidates if has_recent_activity(c)]
        shortlist = select_by_volume(active_candidates, max_legs=settings.scanner.max_ws_legs)
        logger.info(
            "stage1_complete",
            stage1_candidates=len(candidates),
            active_candidates=len(active_candidates),
            ws_shortlist=len(shortlist),
            ws_legs=sum(len(c.leg_tickers) for c in shortlist),
        )

        status: dict[str, Any] = {
            "stage1_candidates": len(candidates),
            "active_candidates": len(active_candidates),
            "ws_shortlist_events": len(shortlist),
            "ws_shortlist_legs": sum(len(c.leg_tickers) for c in shortlist),
            "watched_events": [
                {
                    "event_ticker": c.event_ticker,
                    "title": c.title,
                    "legs": len(c.leg_tickers),
                    "volume_24h": sum((c.leg_volume_24h.get(t) or 0) for t in c.leg_tickers),
                }
                for c in shortlist
            ],
            "execution_enabled": settings.execution.enabled,
            "rescan_interval_s": settings.scanner.rescan_interval_s,
            "ws_connected": False,
            "open_opportunities": 0,
            "circuit_breaker_tripped": circuit_breaker.tripped,
            "circuit_breaker_reason": circuit_breaker.tripped_reason,
            "position_reconciliation_mismatches": {},
        }

        if not shortlist:
            logger.info("stage3_idle", reason="no_same_day_active_events", next_rescan_s=settings.scanner.rescan_interval_s)
            _write_scanner_status(settings.logging.status_path, **status, phase="idle_no_candidates")
            await asyncio.sleep(settings.scanner.rescan_interval_s)
            continue

        if not client.has_signing_credentials:
            # Without signing credentials there's no REST fallback detection
            # anymore (Stage 2's REST book-check is no longer in this loop —
            # see module docstring), so this cycle detects nothing at all.
            # Rather than crash (KalshiWSClient would raise AuthError on
            # connect), log and keep looping at the normal cadence in case
            # credentials get configured later without a restart.
            logger.warning(
                "stage3_ws_skipped_no_credentials",
                reason="KALSHI_API_KEY_ID/KALSHI_PRIVATE_KEY_PATH not configured",
                shortlist_size=len(shortlist),
                next_rescan_s=settings.scanner.rescan_interval_s,
            )
            _write_scanner_status(settings.logging.status_path, **status, phase="idle_no_credentials")
            await asyncio.sleep(settings.scanner.rescan_interval_s)
            continue

        num_tickers = sum(len(c.leg_tickers) for c in shortlist)
        logger.info("stage3_streaming", num_events=len(shortlist), num_tickers=num_tickers)
        _write_scanner_status(settings.logging.status_path, **status, phase="connecting")
        try:
            await asyncio.wait_for(
                _stream_shortlist(ws_client, shortlist, settings, demo_client, status, circuit_breaker, believed_positions),
                timeout=settings.scanner.rescan_interval_s,
            )
        except asyncio.TimeoutError:
            pass
        logger.info("stage3_rescan", reason="rescan_interval_elapsed")
