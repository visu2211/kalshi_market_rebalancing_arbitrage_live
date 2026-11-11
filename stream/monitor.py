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
and filling settings.scanner.max_ws_legs with the highest-volume
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
not a direct fire, when settings.execution.enabled and an execution_client
are provided. execution_client is whichever account settings.execution.target
configures — "demo" (paper money) or "prod" (REAL MONEY on the real Kalshi
account, VERIFIED LIVE 2026-08-26 that the prod key has order-placement
permission — see scripts/run_scanner.py and execution/executor.py's module
docstring). _reconcile_execution re-ranks every currently-open, not-yet-
executed opportunity by execution.capital.hourly_return_rate (edge per
dollar per hour of capital lock-up, not raw edge_usd — a basket settling
sooner frees capital to redeploy sooner) against that account's live
balance (KalshiClient.get_balance), caps each one's size to
settings.execution.max_order_size_contracts (the deliberate ramp-up lever
— see execution/capital.py's docstring), and executes as many as currently
fit, via execution/executor.py's no-slippage IOC-style fill — see that
module's and execution/capital.py's docstrings.

See kalshi/ws_client.py's docstring for what's confirmed vs. assumed about
the WS message schema this module parses.
"""

from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Optional

import structlog
import httpx

from config.loader import Settings
from execution.capital import ExecutionCandidate, estimated_capital_required, select_executable
from execution.executor import (
    STRATEGY_CLIENT_ORDER_ID_PREFIX,
    UnresolvedSubmissionError,
    AtomicExecutionResult,
    LegExecutionResult,
    execute_opportunity,
    estimate_worst_case_unwind_loss,
    _exit_price,
)
from execution.risk import RiskState, RiskStateError
from execution.store import OrderStore
from kalshi.client import ExchangeError, KalshiClient, OrderbookLevel, OrderStatus, Side
from kalshi.ws_client import WS_RECONNECTED_MESSAGE_TYPE, KalshiWSClient
from scanner.basket import BasketOpportunity, LegQuote, compute_no_opportunity, compute_opportunity, incoherent_legs
from scanner.fees import taker_fee
from scanner.filters import EventCandidate, filter_events, has_recent_activity, select_by_volume
from scanner.relationships import Relationship, build_relationships

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

    VERIFIED LIVE 2026-08-26 (and re-confirmed 2026-08-27 against the
    currently configured wss://external-api-ws.kalshi.com host):
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
    snapshot_received: bool = False
    updated_at_monotonic: float = 0.0

    def apply_snapshot(self, yes_levels: list[tuple[Decimal, Decimal]], no_levels: list[tuple[Decimal, Decimal]]) -> None:
        self.yes_bids = {p: s for p, s in yes_levels if s > 0}
        self.no_bids = {p: s for p, s in no_levels if s > 0}
        self.snapshot_received = True
        self.updated_at_monotonic = time.monotonic()

    def invalidate(self) -> None:
        """Drops every locally-held level and un-sets snapshot_received.

        Called for all books whenever the WS connection is re-established
        (kalshi/ws_client.py's WS_RECONNECTED_MESSAGE_TYPE). Without this,
        snapshot_received stayed True across a reconnect, so a delta arriving
        before that ticker's replacement snapshot was applied to the *previous*
        connection's book — and, because apply_delta refreshes
        updated_at_monotonic, the resulting garbage quote then passed the
        is_fresh staleness gate. That produced impossibly good synthesized asks
        with impossibly large size, which is what the live opportunity log
        shows: 10 unrelated events (ATP, WTA, ITF, MLB) all "opening" a 4-34c
        basket within the same 2 seconds on 2026-08-26T00:07:34.
        """
        self.yes_bids = {}
        self.no_bids = {}
        self.snapshot_received = False
        self.updated_at_monotonic = 0.0

    def apply_delta(self, side: str, price: Decimal, size_delta: Decimal) -> None:
        if not self.snapshot_received:
            return
        book = self.yes_bids if side == "yes" else self.no_bids
        new_size = book.get(price, Decimal(0)) + size_delta
        if new_size <= 0:
            book.pop(price, None)
        else:
            book[price] = new_size
        self.updated_at_monotonic = time.monotonic()

    def is_fresh(self, max_age_s: float, now: float | None = None) -> bool:
        now = time.monotonic() if now is None else now
        return self.snapshot_received and now - self.updated_at_monotonic <= max_age_s

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

    def yes_ask_levels(self, depth: int) -> list[OrderbookLevel]:
        """Walks the YES ask ladder — synthesized from no_bids exactly as
        best_yes_quote synthesizes the top level, 1 - price per level —
        ascending price (best/cheapest ask first), up to `depth` levels.
        The full ladder is already held in no_bids (apply_snapshot/apply_delta
        keep every level, not just the top); this is a read-path addition,
        not a storage change — see scanner.basket.compute_opportunity_depth_aware,
        which consumes this to price more than the best level."""
        return sorted(
            (OrderbookLevel(price=Decimal(1) - price, size=size) for price, size in self.no_bids.items()),
            key=lambda level: level.price,
        )[:depth]

    def yes_bid_levels(self, depth: int) -> list[OrderbookLevel]:
        """Walks the YES bid ladder (yes_bids, sent directly by Kalshi, no
        synthesis needed) descending price (best/highest bid first), up to
        `depth` levels."""
        return sorted(
            (OrderbookLevel(price=price, size=size) for price, size in self.yes_bids.items()),
            key=lambda level: level.price,
            reverse=True,
        )[:depth]


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
        # Per-leg quotes as observed at detection. Without these a flagged
        # basket is a single number with no way to see which leg carried the
        # edge, whether a leg was one-sided, or how thin the binding leg was —
        # all of which decide whether the basket was really executable.
        "leg_quotes": [
            {
                "ticker": q.ticker,
                "ask": str(q.best_ask),
                "ask_size": str(q.ask_size),
                "bid": str(q.best_bid),
                "bid_size": str(q.bid_size),
                "spread": str(q.best_ask - q.best_bid),
            }
            for q in opportunity.leg_quotes
        ],
        "sum_ask_usd": str(sum((q.best_ask for q in opportunity.leg_quotes), start=Decimal(0))),
        "fees_usd": str(
            opportunity.basket_price_usd - sum((q.best_ask for q in opportunity.leg_quotes), start=Decimal(0))
        ),
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a") as f:
        f.write(json.dumps(record) + "\n")


def _append_order_jsonl(
    opportunity: BasketOpportunity, leg_results: list[LegExecutionResult], path: Path, target: str, kind: str = "basket",
    execution_result: AtomicExecutionResult | None = None,
) -> None:
    matched_size = min(
        (r.order_result.filled_size if r.order_result else Decimal(0) for r in leg_results),
        default=Decimal(0),
    ) if kind == "basket" else Decimal(0)
    actual_entry_cost = Decimal(0)
    economics_complete = True
    if kind == "basket" and matched_size > 0:
        for leg in leg_results:
            result = leg.order_result
            if result is None or result.average_fill_price is None or result.average_fee_paid is None:
                economics_complete = False
                continue
            actual_entry_cost += (result.average_fill_price + result.average_fee_paid) * matched_size
    record = {
        "placed_at": datetime.now(timezone.utc).isoformat(),
        "kind": kind,   # "basket" (the original attempt) or "unwind" (an offsetting flatten after a mismatch — see execution/executor.py)
        "target": target,   # "demo" or "prod" (REAL MONEY) — settings.execution.target at the moment this was placed
        "event_ticker": opportunity.event_ticker,
        "direction": opportunity.direction,
        # Per-contract, as detected — the basket's actual realized profit is
        # this times however many contracts of every leg actually filled
        # (ui/app.py's "Trades actually filled" section does that math from
        # the per-leg filled_size below, since a partial/uneven fill means
        # the realized size can be less than what was requested here).
        "basket_price_usd": str(opportunity.basket_price_usd),
        "edge_usd_per_contract": str(opportunity.edge_usd),
        "actual_matched_size": str(matched_size) if kind == "basket" else None,
        "actual_matched_entry_cost_usd": str(actual_entry_cost) if kind == "basket" and economics_complete else None,
        "actual_locked_payout_usd": str(matched_size) if kind == "basket" and economics_complete else None,
        "actual_expected_profit_usd": str(matched_size - actual_entry_cost) if kind == "basket" and economics_complete else None,
        "actual_economics_complete": economics_complete if kind == "basket" else None,
        "telemetry": ({
            "decision_at_ms": execution_result.decision_at_ms,
            "entry_request_started_at_ms": execution_result.entry_request_started_at_ms,
            "entry_response_received_at_ms": execution_result.entry_response_received_at_ms,
            "entry_exchange_ts_ms": execution_result.entry_exchange_ts_ms,
            "unwind_request_started_at_ms": execution_result.unwind_request_started_at_ms,
            "unwind_response_received_at_ms": execution_result.unwind_response_received_at_ms,
            "estimated_worst_unwind_loss_usd": str(execution_result.estimated_worst_unwind_loss_usd),
            "refreshed_book_fallback_used": execution_result.refreshed_book_fallback_used,
            "emergency_unwind_used": execution_result.emergency_unwind_used,
            "unwind_attempt_count": execution_result.unwind_attempt_count,
        } if execution_result is not None else None),
        "legs": [
            {
                "ticker": r.ticker,
                "action": r.action.value,
                "limit_price": str(r.limit_price),
                "size": str(r.size),
                "order_id": r.order_result.order_id if r.order_result else None,
                "status": r.order_result.status.value if r.order_result else None,
                "filled_size": str(r.order_result.filled_size) if r.order_result else None,
                "average_fill_price": str(r.order_result.average_fill_price) if r.order_result and r.order_result.average_fill_price is not None else None,
                "average_fee_paid": str(r.order_result.average_fee_paid) if r.order_result and r.order_result.average_fee_paid is not None else None,
                "error": r.error,
                "execution_stage": r.execution_stage,
            }
            for r in leg_results
        ],
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a") as f:
        f.write(json.dumps(record) + "\n")


async def _emit_incident(settings: Settings, incident_type: str, **details: Any) -> bool:
    payload = {
        "occurred_at": datetime.now(timezone.utc).isoformat(),
        "type": incident_type,
        "target": settings.execution.target,
        **details,
    }
    path = settings.execution.alerts_log_path
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a") as f:
        f.write(json.dumps(payload, default=str) + "\n")
    delivered = True
    if settings.execution.alert_webhook_url:
        try:
            async with httpx.AsyncClient(timeout=5) as client:
                response = await client.post(settings.execution.alert_webhook_url, json=payload)
                response.raise_for_status()
        except (httpx.HTTPError, OSError):
            # Do not include the URL or response body: webhook URLs commonly
            # contain bearer secrets.
            logger.error("incident_webhook_failed", incident_type=incident_type)
            delivered = False
    if settings.execution.telegram_bot_token and settings.execution.telegram_chat_id:
        telegram_url = f"https://api.telegram.org/bot{settings.execution.telegram_bot_token}/sendMessage"
        details_text = json.dumps(details, default=str, sort_keys=True)
        message = (
            f"Kalshi arbitrage alert\n"
            f"Type: {incident_type}\n"
            f"Target: {settings.execution.target}\n"
            f"Time: {payload['occurred_at']}\n"
            f"Details: {details_text}"
        )[:4000]
        try:
            async with httpx.AsyncClient(timeout=5) as client:
                response = await client.post(
                    telegram_url,
                    json={"chat_id": settings.execution.telegram_chat_id, "text": message},
                )
                response.raise_for_status()
                body = response.json()
                if body.get("ok") is not True:
                    raise httpx.HTTPError("Telegram API returned ok=false")
        except (httpx.HTTPError, OSError, ValueError):
            # Never log the request URL: it embeds TELEGRAM_BOT_TOKEN.
            logger.error("incident_telegram_failed", incident_type=incident_type)
            delivered = False
    return delivered


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


def _log_entry_leg_outcomes(opportunity: BasketOpportunity, leg_results: list[LegExecutionResult]) -> None:
    """Per-leg diagnosis of an entry attempt.

    A basket-level "it mismatched" says nothing about WHY. Comparing the WS
    price submitted against what each leg actually did separates the two
    candidate explanations: a gap concentrated in ONE leg is a book-
    reconstruction problem on that market, while a gap spread across every leg
    is real movement, pointing at latency rather than data quality.

    VERIFIED LIVE 2026-08-28: the first real prod attempt filled 1 of 3 legs and
    the other two came back fill_or_kill_insufficient_resting_volume — the WS
    book displayed ask depth that could not fill a single contract. That is the
    kind of finding this line exists to surface. Runs after submission, so it
    cannot delay an order.
    """
    ws_by_ticker = {q.ticker: q for q in opportunity.leg_quotes}
    for lr in leg_results:
        q = ws_by_ticker.get(lr.ticker)
        r = lr.order_result
        fill_price = r.average_fill_price if r is not None else None
        logger.info(
            "entry_leg_outcome",
            event_ticker=opportunity.event_ticker,
            ticker=lr.ticker,
            ws_ask=str(q.best_ask) if q else None,
            ws_ask_size=str(q.ask_size) if q else None,
            ws_bid=str(q.best_bid) if q else None,
            submitted_limit=str(lr.limit_price),
            filled_size=str(r.filled_size) if r is not None else "0",
            fill_price=str(fill_price) if fill_price is not None else None,
            slippage_vs_ws=str(fill_price - q.best_ask) if (fill_price is not None and q is not None) else None,
            terminal_verified=lr.terminal_verified,
            outcome=("filled" if (r is not None and r.filled_size > 0)
                     else "killed_no_fill" if r is not None else "rejected"),
            error=lr.error,
        )


async def _execute_and_log(
    opportunity: BasketOpportunity, execution_client: KalshiClient, settings: Settings,
    store: Optional[OrderStore] = None,
) -> Optional[AtomicExecutionResult]:
    """Fires execution/executor.py's batched basket placement against
    whichever account settings.execution.target configures (demo or —
    REAL MONEY — prod) and logs every leg's outcome (and, if the legs
    filled unevenly, every unwind leg's outcome too) — never raises back
    into _reprice_event, since an order attempt failing shouldn't
    interrupt detection. Returns the full AtomicExecutionResult (None if
    execution was skipped for missing credentials) so the caller can
    update believed positions and feed .mismatched / .unwind_confirmed to
    the circuit breaker — see execution/executor.py's module docstring
    for why both matter, not just whether it mismatched. store, if given,
    is threaded straight to execute_opportunity — see that function's and
    execution/store.py's docstrings."""
    if not execution_client.has_signing_credentials:
        logger.warning(
            "execution_skipped_no_demo_credentials",
            event_ticker=opportunity.event_ticker,
            reason="KALSHI_DEMO_API_KEY_ID/KALSHI_DEMO_PRIVATE_KEY_PATH not configured",
        )
        return None

    try:
        result = await execute_opportunity(
            execution_client, opportunity,
            allow_no_direction=settings.execution.allow_no_direction,
            use_canary_submission=settings.execution.use_canary_submission,
            store=store,
        )
    except UnresolvedSubmissionError as exc:
        # The true position at the exchange is unknown. Do not submit another
        # stage, do not classify the basket. Cancel what we can identify,
        # record it, and let the caller trip the durable breaker.
        logger.error(
            "execution_state_unknown", event_ticker=opportunity.event_ticker, error=str(exc),
            reason="submission could not be reconciled against the exchange; execution halted",
        )
        # The diagnostic must run on THIS path too — the halt is exactly the
        # case where you most need to know which legs filled and at what price.
        _log_entry_leg_outcomes(opportunity, exc.leg_results)
        _append_order_jsonl(
            opportunity, exc.leg_results, settings.execution.orders_log_path,
            settings.execution.target, kind="basket",
        )
        if exc.salvage_results:
            _log_leg_results(opportunity.event_ticker, exc.salvage_results, "unwind_order_placed", "unwind_order_rejected")
            _append_order_jsonl(
                opportunity, exc.salvage_results, settings.execution.orders_log_path,
                settings.execution.target, kind="unwind",
            )
        await _emit_incident(
            settings, "execution_state_unknown", event_ticker=opportunity.event_ticker,
            error=str(exc),
            legs=[{"ticker": lr.ticker, "error": lr.error} for lr in exc.leg_results],
        )
        raise
    _log_leg_results(opportunity.event_ticker, result.leg_results, "demo_order_placed", "demo_order_rejected")

    # Per-leg diagnosis. A basket-level "it mismatched" says nothing about WHY.
    # Comparing the WS price we submitted at against what each leg actually did
    # separates the two candidate explanations: a gap concentrated in ONE leg is
    # a book-reconstruction problem on that market, while a gap spread evenly
    # across every leg is real market movement, pointing at latency rather than
    # data quality. Runs after submission, so it cannot delay the order.
    _log_entry_leg_outcomes(opportunity, result.leg_results)

    _append_order_jsonl(opportunity, result.leg_results, settings.execution.orders_log_path, settings.execution.target, kind="basket", execution_result=result)

    if result.mismatched:
        filled_sizes = {r.ticker: (str(r.order_result.filled_size) if r.order_result else "0") for r in result.leg_results}
        logger.error(
            "basket_mismatch_unwind",
            event_ticker=opportunity.event_ticker,
            reason="legs filled unevenly — unwinding the excess to avoid a naked position",
            filled_sizes=filled_sizes,
        )
        _log_leg_results(opportunity.event_ticker, result.unwind_results, "unwind_order_placed", "unwind_order_rejected")
        _append_order_jsonl(opportunity, result.unwind_results, settings.execution.orders_log_path, settings.execution.target, kind="unwind", execution_result=result)
        await _emit_incident(
            settings, "basket_mismatch_unwind", event_ticker=opportunity.event_ticker,
            unwind_confirmed=result.unwind_confirmed,
            estimated_worst_unwind_loss_usd=str(result.estimated_worst_unwind_loss_usd),
            realized_unwind_loss_usd=str(_realized_unwind_loss(result)),
            refreshed_book_fallback_used=result.refreshed_book_fallback_used,
            emergency_unwind_used=result.emergency_unwind_used,
            unwind_attempt_count=result.unwind_attempt_count,
        )
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


def _realized_unwind_loss(result: AtomicExecutionResult) -> Decimal:
    """Conservative realized loss for quantities bought and sold back.

    Positive values are losses; profits are floored at zero so an emergency
    round trip can never increase the configured daily loss budget.
    """
    buys = {lr.ticker: lr for lr in result.leg_results if lr.order_result is not None}
    loss = Decimal(0)
    for sell_leg in result.unwind_results:
        sell = sell_leg.order_result
        buy_leg = buys.get(sell_leg.ticker)
        if sell is None or buy_leg is None or sell.filled_size <= 0:
            continue
        buy = buy_leg.order_result
        assert buy is not None
        size = min(sell.filled_size, buy.filled_size)
        buy_price = buy.average_fill_price or buy_leg.limit_price
        sell_price = sell.average_fill_price or Decimal(0)
        buy_fee = buy.average_fee_paid or Decimal(0)
        sell_fee = sell.average_fee_paid or Decimal(0)
        loss += ((buy_price - sell_price) + buy_fee + sell_fee) * size
    return max(loss, Decimal(0))


def _unmatched_entry_exposure(result: AtomicExecutionResult) -> Decimal:
    """Capital still exposed in entry fills not offset by matched legs or sells."""
    if not result.leg_results:
        return Decimal(0)
    entry_fills = {
        leg.ticker: (leg.order_result.filled_size if leg.order_result is not None else Decimal(0))
        for leg in result.leg_results
    }
    common_fill = min(entry_fills.values(), default=Decimal(0))
    sold: dict[str, Decimal] = {}
    for leg in result.unwind_results:
        if leg.order_result is not None:
            sold[leg.ticker] = sold.get(leg.ticker, Decimal(0)) + leg.order_result.filled_size
    exposure = Decimal(0)
    for leg in result.leg_results:
        remaining_excess = max(entry_fills[leg.ticker] - common_fill - sold.get(leg.ticker, Decimal(0)), Decimal(0))
        if remaining_excess:
            fill_price = leg.order_result.average_fill_price if leg.order_result is not None else None
            exposure += remaining_excess * (fill_price or leg.limit_price)
    return exposure


async def _reconcile_positions(execution_client: KalshiClient, believed_positions: dict[str, Decimal]) -> dict[str, tuple[Decimal, Decimal]]:
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
        actual = await execution_client.get_positions()
    except ExchangeError as exc:
        logger.warning("reconciliation_check_failed", error=str(exc))
        return {}

    mismatches: dict[str, tuple[Decimal, Decimal]] = {}
    for ticker in set(believed_positions):
        believed = believed_positions.get(ticker, Decimal(0))
        real = actual.get(ticker, Decimal(0))
        if believed != real:
            mismatches[ticker] = (believed, real)
    return mismatches


async def _release_finalized_positions(
    execution_client: KalshiClient,
    actual: dict[str, Decimal],
    believed_positions: dict[str, Decimal],
    risk_state: RiskState,
) -> None:
    """Remove persisted positions only after the exchange says finalized."""
    for ticker, believed in list(believed_positions.items()):
        if believed == 0 or actual.get(ticker, Decimal(0)) != 0:
            continue
        market = await execution_client.get_market(ticker)
        if market.get("status") != "finalized":
            continue
        event = risk_state.ticker_to_event.pop(ticker, None)
        believed_positions.pop(ticker, None)
        if event and not any(mapped == event for mapped in risk_state.ticker_to_event.values()):
            risk_state.event_exposure_usd.pop(event, None)
    risk_state.believed_positions = dict(believed_positions)


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

    def __init__(self, max_mismatches: int, risk_state: RiskState | None = None, risk_state_path: Path | None = None) -> None:
        self._max = max_mismatches
        self._consecutive_mismatches = 0
        self._risk_state = risk_state
        self._risk_state_path = risk_state_path
        self.tripped = risk_state.tripped if risk_state is not None else False
        self.tripped_reason: Optional[str] = risk_state.tripped_reason if risk_state is not None else None

    def record(self, mismatched: bool) -> None:
        if self.tripped:
            return
        if mismatched:
            self._consecutive_mismatches += 1
            if self._consecutive_mismatches >= self._max:
                self.force_trip(
                    f"{self._consecutive_mismatches} consecutive baskets needed an unwind "
                    "(legs filled unevenly) — execution auto-disabled. The trip is DURABLE: "
                    "restarting reloads it. Investigate, then clear the breaker in "
                    "data/risk_state.json to re-arm."
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
        if self._risk_state is not None:
            self._risk_state.trip(reason)
            if self._risk_state_path is not None:
                self._risk_state.persist(self._risk_state_path)
        logger.error("circuit_breaker_tripped", reason=reason)


def _cap_opportunity_size(opportunity: BasketOpportunity, max_order_size_contracts: Decimal) -> BasketOpportunity:
    """Caps opportunity.max_size at settings.execution.max_order_size_contracts
    — the deliberate ramp-up lever for real money (2026-08-26). Applied
    here, before capital ranking (execution.capital), not just before
    firing the order — estimated_capital_required and hourly_return_rate
    both read max_size, so ranking against the UNcapped size would wildly
    overestimate the capital an order this small actually needs.

    Always <= opportunity.max_size, which scanner.basket.compute_opportunity
    already guarantees is >= scanner.min_depth_contracts —
    so as long as max_order_size_contracts is at or below that floor (e.g.
    the default of 1), the capped size is always covered by real observed
    depth on every leg, never a request for more than what's genuinely
    there."""
    capped_size = min(max_order_size_contracts, opportunity.max_size)
    if capped_size == opportunity.max_size:
        return opportunity
    return replace(opportunity, max_size=capped_size)


_REST_CONFIRM_DEPTH = 5      # only top-of-book is priced; matches scanner.orderbook_depth's intent
_REST_CONFIRM_TIMEOUT_S = 2.0  # hard ceiling on gathering every leg's book
_REST_CONFIRM_MAX_AGE_S = 1.0  # the independent pre-submit books must all arrive inside this tight window


async def _confirm_opportunity_via_rest(
    market_data_client: KalshiClient,
    opportunity: BasketOpportunity,
    settings: Settings,
) -> Optional[BasketOpportunity]:
    """Re-prices `opportunity` against a fresh REST book for every leg, and
    returns an equivalent BasketOpportunity built from those REST quotes
    (already size-capped) — or None if the edge does not survive.

    The WS book is an incrementally maintained local reconstruction; this is
    an independent read of the same book straight from Kalshi, so the two
    agreeing is real corroboration that the edge exists rather than that our
    local state drifted. It costs one round-trip per leg immediately before
    submission, which is the correct place to spend latency: an edge that
    cannot survive the time it takes to confirm it could not have survived
    the time it takes to fill it either.

    Orders are then placed off the *returned* quotes, not the WS ones, so the
    prices submitted are the prices just verified.
    """
    # Fetched concurrently under one deadline. Sequentially, a 6-leg basket's
    # first quote could be seconds old by submission. The order prices must
    # come from one tightly bounded confirmation window.
    started = time.monotonic()
    try:
        books = await asyncio.wait_for(
            asyncio.gather(*(
                market_data_client.get_orderbook(t, side=Side.YES, depth=_REST_CONFIRM_DEPTH)
                for t in opportunity.legs
            )),
            timeout=_REST_CONFIRM_TIMEOUT_S,
        )
    except asyncio.TimeoutError:
        logger.warning(
            "rest_confirmation_timeout", event_ticker=opportunity.event_ticker,
            timeout_s=_REST_CONFIRM_TIMEOUT_S,
        )
        return None
    except ExchangeError as exc:
        logger.warning("rest_confirmation_failed", event_ticker=opportunity.event_ticker, error=str(exc))
        return None

    elapsed = time.monotonic() - started
    if elapsed > _REST_CONFIRM_MAX_AGE_S:
        # Quotes that took too long to gather are no fresher than the WS book
        # they were meant to corroborate.
        logger.warning(
            "rest_confirmation_stale", event_ticker=opportunity.event_ticker,
            elapsed_s=round(elapsed, 3), limit_s=_REST_CONFIRM_MAX_AGE_S,
        )
        return None

    legs: list[LegQuote] = []
    for ticker, book in zip(opportunity.legs, books):
        if book.best_ask is None or book.best_bid is None or book.best_ask.size <= 0:
            logger.warning("rest_confirmation_incomplete_book", event_ticker=opportunity.event_ticker, ticker=ticker)
            return None
        legs.append(LegQuote(
            ticker=ticker,
            best_ask=book.best_ask.price,
            ask_size=book.best_ask.size,
            best_bid=book.best_bid.price,
            bid_size=book.best_bid.size,
        ))

    bad = incoherent_legs(legs)
    if bad:
        logger.warning(
            "rest_confirmation_incoherent_book", event_ticker=opportunity.event_ticker,
            tickers=[leg.ticker for leg in bad],
        )
        return None

    # Direction-aware confirmation: buy_basket entries price off YES asks
    # (leg.best_ask, min leg.ask_size); buy_all_no entries price off NO asks
    # synthesized as (1 - leg.best_bid) consuming leg.bid_size — the same
    # dispatch scanner/basket.py already makes between compute_opportunity and
    # compute_no_opportunity. Validating a buy_all_no detection with the
    # buy_basket math measured the wrong trade entirely and refused every
    # detection as "edge gone" with sum_ask ~= $1.02 (observed 2026-11-08).
    if opportunity.direction == "buy_all_no":
        confirm_size = min(settings.execution.max_order_size_contracts, min(leg.bid_size for leg in legs))
        confirmed = compute_no_opportunity(
            event_ticker=opportunity.event_ticker,
            legs=legs,
            min_edge_usd=settings.arb.buy_all_no_min_edge_usd,
            min_depth_contracts=settings.scanner.min_depth_contracts,
            contract_count=confirm_size,
            fee_multiplier=opportunity.fee_multiplier,
        )
    else:
        confirm_size = min(settings.execution.max_order_size_contracts, min(leg.ask_size for leg in legs))
        confirmed = compute_opportunity(
            event_ticker=opportunity.event_ticker,
            legs=legs,
            buy_basket_max_cost_usd=settings.arb.buy_basket_max_cost_usd,
            min_depth_contracts=settings.scanner.min_depth_contracts,
            contract_count=confirm_size,
            fee_multiplier=opportunity.fee_multiplier,
        )
    if confirmed is None:
        # Both prices fee-inclusive so they are directly comparable. Logging a
        # fee-inclusive WS price against a raw REST ask sum made a basket that
        # got CHEAPER on REST read as though the edge had evaporated.
        if opportunity.direction == "buy_all_no":
            rest_entry_prices = [Decimal(1) - leg.best_bid for leg in legs]
        else:
            rest_entry_prices = [leg.best_ask for leg in legs]
        rest_sum_ask = sum(rest_entry_prices, start=Decimal(0))
        rest_fees = sum(
            (taker_fee(p, confirm_size, opportunity.fee_multiplier) for p in rest_entry_prices),
            start=Decimal(0),
        )
        logger.warning(
            "rest_confirmation_edge_gone", event_ticker=opportunity.event_ticker,
            direction=opportunity.direction,
            ws_basket_price_usd=str(opportunity.basket_price_usd),
            rest_basket_price_usd=str(rest_sum_ask + rest_fees),
            rest_sum_ask_usd=str(rest_sum_ask),
            rest_fees_usd=str(rest_fees),
            threshold_usd=str(
                Decimal(len(legs) - 1) - settings.arb.buy_all_no_min_edge_usd
                if opportunity.direction == "buy_all_no"
                else settings.arb.buy_basket_max_cost_usd
            ),
        )
        return None

    return _cap_opportunity_size(confirmed, settings.execution.max_order_size_contracts)


async def _reconcile_execution(
    edge_stability_ticks: dict[str, list[Decimal]],
    open_opportunities: dict[str, BasketOpportunity],
    executed_opportunities: set[str],
    relationship_by_event_ticker: dict[str, Relationship],
    execution_client: KalshiClient,
    market_data_client: KalshiClient,
    settings: Settings,
    balance_cache: _BalanceCache,
    circuit_breaker: _CircuitBreaker,
    believed_positions: dict[str, Decimal],
    risk_state: RiskState,
    store: Optional[OrderStore] = None,
) -> None:
    """Capital-aware execution pass — re-ranks every currently-open,
    not-yet-executed opportunity by execution.capital.hourly_return_rate
    against the execution account's balance (via balance_cache — see its
    docstring for why this isn't a fresh REST call every time), caps each
    one's size to settings.execution.max_order_size_contracts, and
    executes as many as fit. Called both on every fresh open transition
    (_reprice_event) and off the streaming heartbeat (_stream_shortlist),
    so an opportunity skipped earlier for insufficient capital gets
    reconsidered if balance frees up later rather than only once. See
    execution/capital.py's docstring for why per-hour return (not raw
    edge_usd) is the ranking key. Stops immediately (before even checking
    balance) if circuit_breaker.tripped — see that class's docstring."""
    if circuit_breaker.tripped:
        return
    if not execution_client.has_signing_credentials:
        return

    unexecuted = [t for t in open_opportunities if t not in executed_opportunities]
    if not unexecuted:
        return

    candidates: list[ExecutionCandidate] = []
    for event_ticker in unexecuted:
        opportunity = _cap_opportunity_size(open_opportunities[event_ticker], settings.execution.max_order_size_contracts)
        # Looked up by the opportunity's own event_ticker, not by resolving
        # its first leg back to an owner — a leg can belong to more than one
        # relationship (see scanner/relationships.py), so deriving the event
        # indirectly via legs[0] would be ambiguous; event_ticker is not.
        relationship = relationship_by_event_ticker.get(event_ticker)
        if relationship is None:
            continue
        candidates.append(ExecutionCandidate(event_ticker, opportunity, relationship.expected_settlement_time))
    if not candidates:
        return

    try:
        available = await balance_cache.get(execution_client)
    except ExchangeError as exc:
        logger.warning("capital_check_failed", error=str(exc))
        return

    utc_now = datetime.now(timezone.utc)
    now_epoch = time.time()
    risk_state.roll_day(utc_now)
    recent_cutoff = time.time() - 60
    risk_state.recent_order_timestamps = [t for t in risk_state.recent_order_timestamps if t >= recent_cutoff]

    risk_eligible: list[ExecutionCandidate] = []
    for candidate in candidates:
        if risk_state.session_basket_attempts >= settings.execution.max_baskets_per_session:
            logger.warning("execution_skipped_session_basket_limit", limit=settings.execution.max_baskets_per_session)
            break
        if risk_state.daily_basket_attempts >= settings.execution.max_baskets_per_day:
            logger.warning("execution_skipped_daily_basket_limit", limit=settings.execution.max_baskets_per_day)
            break
        last_attempt = risk_state.attempted_events_at.get(candidate.event_ticker)
        if last_attempt is not None and now_epoch - last_attempt < settings.execution.event_cooldown_s:
            # Durable per-event cooldown is monotonic (grows with every WS
            # tick), so once rejected it stays rejected for this run. Mark
            # consumed to avoid logging dozens of times per second while the
            # book keeps ticking — same pattern as the close-time gate below.
            if candidate.event_ticker not in executed_opportunities:
                logger.warning("execution_skipped_event_cooldown", event_ticker=candidate.event_ticker)
                executed_opportunities.add(candidate.event_ticker)
            continue
        # Edge-stability filter. The WS detection may be a one-tick flicker that
        # dies before any order could reach the exchange (see 2026-11-08 LocalBook
        # diagnostic: WS and REST books were byte-identical; the problem was
        # edge lifetime < RTT). Require the edge to have persisted this many
        # consecutive WS ticks on this event before it is executable. Default 1
        # preserves prior behavior.
        history = edge_stability_ticks.get(candidate.event_ticker, [])
        observed_ticks = len(history)
        if observed_ticks < settings.execution.min_edge_stability_ticks:
            logger.info(
                "execution_skipped_edge_unstable", event_ticker=candidate.event_ticker,
                observed_ticks=observed_ticks, required=settings.execution.min_edge_stability_ticks,
            )
            continue
        # Non-shrinking-edge filter (added 2026-11-11). On 418 live detections
        # 78% of multi-tick bursts shrank by mean $0.04 — those are edges
        # dying while the scanner reprices. Reject when the window-end edge
        # is below the window-start edge; the market is correcting and our
        # FOK will land after the correction. Default off (backward compat).
        if settings.execution.require_non_shrinking_edge and len(history) >= 2:
            if history[-1] < history[0]:
                logger.info(
                    "execution_skipped_edge_shrinking", event_ticker=candidate.event_ticker,
                    edge_first=str(history[0]), edge_last=str(history[-1]),
                )
                continue
        # Close-time cutoff. Nov 4 incident: a basket executed 6+ hours after
        # the event had settled had leg BB reject FOK (nothing resting) while
        # leg TL filled — and then no unwind could fill because the market was
        # dead. Reject within min_seconds_to_event_close of settlement. Default
        # 0 preserves prior behavior.
        if settings.execution.min_seconds_to_event_close > 0:
            seconds_to_close = (candidate.expected_settlement_time - utc_now).total_seconds()
            if seconds_to_close < settings.execution.min_seconds_to_event_close:
                # Monotonic: once we are past the cutoff, we are not coming
                # back. Mark executed so this event is not reconsidered on
                # every WS tick for the remainder of the run — otherwise
                # dozens of these fire per second for a stale yesterday
                # event whose book still gets updates. Logged once per
                # session per event.
                if candidate.event_ticker not in executed_opportunities:
                    logger.info(
                        "execution_skipped_too_close_to_settle",
                        event_ticker=candidate.event_ticker,
                        seconds_to_close=seconds_to_close,
                        required=settings.execution.min_seconds_to_event_close,
                    )
                    executed_opportunities.add(candidate.event_ticker)
                continue
        worst_unwind_loss = estimate_worst_case_unwind_loss(candidate.opportunity)
        if worst_unwind_loss is None:
            logger.warning("execution_skipped_insufficient_exit_depth", event_ticker=candidate.event_ticker)
            continue
        if worst_unwind_loss > settings.execution.max_unwind_loss_usd:
            logger.warning(
                "execution_skipped_unwind_loss_limit", event_ticker=candidate.event_ticker,
                estimated_loss=str(worst_unwind_loss), limit=str(settings.execution.max_unwind_loss_usd),
            )
            continue
        cost = estimated_capital_required(candidate.opportunity)
        unwind_turnover = sum(
            _exit_price(candidate.opportunity.direction, leg) * candidate.opportunity.max_size
            for leg in candidate.opportunity.leg_quotes
        )
        projected_turnover = cost + unwind_turnover
        projected_event = risk_state.event_exposure_usd.get(candidate.event_ticker, Decimal(0)) + cost
        projected_gross = risk_state.gross_exposure_usd + cost
        projected_daily = risk_state.daily_notional_usd + projected_turnover
        # Entry plus observed-bid, refreshed-book, and emergency unwind batches.
        projected_orders = len(candidate.opportunity.legs) * 4
        if projected_event > settings.execution.max_event_exposure_usd:
            logger.warning("execution_skipped_event_exposure_limit", event_ticker=candidate.event_ticker)
            continue
        if projected_gross > settings.execution.max_gross_exposure_usd:
            logger.warning("execution_skipped_gross_exposure_limit", event_ticker=candidate.event_ticker)
            continue
        if projected_daily > settings.execution.max_daily_notional_usd:
            logger.warning("execution_skipped_daily_notional_limit", event_ticker=candidate.event_ticker)
            continue
        if risk_state.daily_realized_loss_usd >= settings.execution.max_daily_loss_usd:
            circuit_breaker.force_trip("maximum daily realized loss reached")
            break
        if len(risk_state.recent_order_timestamps) + projected_orders > settings.execution.max_orders_per_minute:
            logger.warning("execution_skipped_order_rate_limit", event_ticker=candidate.event_ticker)
            continue
        risk_eligible.append(candidate)

    selected = select_executable(risk_eligible, available, utc_now)
    # Diffed against risk_eligible, not candidates: a candidate already rejected
    # by a risk gate has logged its real reason, and diffing the full set gave it
    # a second, contradictory "insufficient capital" line. Observed live
    # 2026-08-28 — a basket refused for a $0.11 unwind estimate was also logged
    # as short of capital while $515.00 was available.
    skipped = {c.event_ticker for c in risk_eligible} - {c.event_ticker for c in selected}
    for event_ticker in skipped:
        logger.info("execution_skipped_insufficient_capital", event_ticker=event_ticker, available_capital=str(available))

    for candidate in selected:
        if risk_state.session_basket_attempts >= settings.execution.max_baskets_per_session:
            break
        if risk_state.daily_basket_attempts >= settings.execution.max_baskets_per_day:
            break
        # Marked consumed before confirmation, not after: _reconcile_execution
        # runs on every WS tick with an open opportunity, so retrying a failed
        # confirmation would issue one REST burst per tick against exactly the
        # corrupt-book case this exists to catch. The event is reconsidered
        # once the opportunity closes and genuinely re-opens (_reprice_event
        # discards it then).
        executed_opportunities.add(candidate.event_ticker)

        if settings.execution.require_rest_confirmation:
            opportunity = await _confirm_opportunity_via_rest(market_data_client, candidate.opportunity, settings)
            if opportunity is None:
                continue
        else:
            # Deliberate opt-out, refused outright for prod by config.loader.
            # Orders are priced off the local WS reconstruction alone, so a
            # corrupt book reaches the exchange instead of being caught. Only
            # defensible on demo, where the purpose is to learn what an order
            # actually does — a question no amount of confirmation can answer,
            # since an edge shorter than one round trip can never be
            # corroborated by a second round trip.
            opportunity = candidate.opportunity   # already size-capped
            logger.warning(
                "rest_confirmation_bypassed", event_ticker=candidate.event_ticker,
                target=settings.execution.target,
                basket_price_usd=str(opportunity.basket_price_usd),
                edge_usd=str(opportunity.edge_usd),
                reason="execution.require_rest_confirmation is false — submitting on WS prices",
            )

        # The pre-trade gates above ran on WS quotes. Re-run the one that
        # bounds real money on the confirmed quotes, because a crossed book
        # makes estimate_worst_case_unwind_loss read a negative spread and so
        # approve precisely the corrupt basket it is meant to reject.
        worst_unwind_loss = estimate_worst_case_unwind_loss(opportunity)
        if worst_unwind_loss is None or worst_unwind_loss > settings.execution.max_unwind_loss_usd:
            logger.warning(
                "execution_skipped_unwind_loss_limit_on_confirmation",
                event_ticker=candidate.event_ticker,
                estimated_loss=str(worst_unwind_loss),
                limit=str(settings.execution.max_unwind_loss_usd),
            )
            continue

        # Named neutrally and carrying `rest_confirmed`, because this point is
        # reached on BOTH branches above. Logging "rest_confirmed_opportunity"
        # unconditionally claimed a confirmation had happened even when it was
        # bypassed — which read, in the log, exactly like a confirmed edge.
        logger.info(
            "execution_candidate_accepted", event_ticker=candidate.event_ticker,
            rest_confirmed=settings.execution.require_rest_confirmation,
            ws_basket_price_usd=str(candidate.opportunity.basket_price_usd),
            confirmed_basket_price_usd=str(opportunity.basket_price_usd),
            confirmed_edge_usd=str(opportunity.edge_usd), size=str(opportunity.max_size),
        )

        cost = estimated_capital_required(opportunity)
        worst_unwind_turnover = sum(
            _exit_price(opportunity.direction, leg) * opportunity.max_size for leg in opportunity.leg_quotes
        )

        # The capital/exposure gates above ran on WS prices. Re-run them on the
        # confirmed prices actually being submitted — otherwise a basket that
        # got more expensive between detection and confirmation could commit
        # more capital than any limit approved.
        if cost > available:
            logger.warning(
                "execution_skipped_insufficient_capital_on_confirmation",
                event_ticker=candidate.event_ticker, cost=str(cost), available=str(available),
            )
            continue
        if risk_state.event_exposure_usd.get(candidate.event_ticker, Decimal(0)) + cost > settings.execution.max_event_exposure_usd:
            logger.warning("execution_skipped_event_exposure_limit_on_confirmation", event_ticker=candidate.event_ticker)
            continue
        if risk_state.gross_exposure_usd + cost > settings.execution.max_gross_exposure_usd:
            logger.warning("execution_skipped_gross_exposure_limit_on_confirmation", event_ticker=candidate.event_ticker)
            continue
        if risk_state.daily_notional_usd + cost + worst_unwind_turnover > settings.execution.max_daily_notional_usd:
            logger.warning("execution_skipped_daily_notional_limit_on_confirmation", event_ticker=candidate.event_ticker)
            continue

        balance_cache.reserve(cost)   # see _BalanceCache's docstring for the cross-call race this closes

        # Persist the attempt and conservative turnover before submission so
        # a crash after the exchange accepts the request cannot erase limits.
        risk_state.session_basket_attempts += 1
        risk_state.daily_basket_attempts += 1
        risk_state.attempted_events_at[candidate.event_ticker] = time.time()
        risk_state.ticker_to_event.update({ticker: candidate.event_ticker for ticker in opportunity.legs})
        for ticker in opportunity.legs:
            risk_state.believed_positions.setdefault(ticker, believed_positions.get(ticker, Decimal(0)))
        risk_state.daily_notional_usd += cost + worst_unwind_turnover
        risk_state.persist(settings.execution.risk_state_path)

        try:
            result = await _execute_and_log(opportunity, execution_client, settings, store)
        except UnresolvedSubmissionError as exc:
            # Unknown exchange state is strictly worse than a known mismatch:
            # positions may exist that our bookkeeping cannot see. Trip durably
            # and stop this pass — a human reconciles before anything re-arms.
            #
            # Record what we DO know first. VERIFIED LIVE 2026-08-28: halting
            # without this left the ledger reporting "flat" while a real
            # 1-contract prod position was open, so the 30s reconciliation had
            # to rediscover a fill we already had in hand.
            partial = AtomicExecutionResult(
                leg_results=exc.leg_results,
                unwind_results=exc.salvage_results,
                mismatched=True,
                unwind_confirmed=False,
            )
            _update_believed_positions(believed_positions, partial)
            risk_state.believed_positions = dict(believed_positions)
            risk_state.ticker_to_event.update(
                {ticker: candidate.event_ticker for ticker in opportunity.legs}
            )
            risk_state.daily_realized_loss_usd += _realized_unwind_loss(partial)
            circuit_breaker.force_trip(
                f"execution state unknown for {candidate.event_ticker}: {exc}"
            )
            risk_state.persist(settings.execution.risk_state_path)
            break
        if result is None:
            continue   # skipped for missing credentials — nothing to reconcile or feed the breaker
        _update_believed_positions(believed_positions, result)
        risk_state.daily_realized_loss_usd += _realized_unwind_loss(result)
        risk_state.recent_order_timestamps.extend([time.time()] * (len(result.leg_results) + len(result.unwind_results)))
        submitted_unwind_turnover = sum(lr.limit_price * lr.size for lr in result.unwind_results)
        risk_state.daily_notional_usd += max(submitted_unwind_turnover - worst_unwind_turnover, Decimal(0))
        common_fill = min(
            (lr.order_result.filled_size if lr.order_result is not None else Decimal(0) for lr in result.leg_results),
            default=Decimal(0),
        )
        matched_cost = opportunity.basket_price_usd * common_fill
        unmatched_exposure = _unmatched_entry_exposure(result)
        if matched_cost > 0 or unmatched_exposure > 0:
            risk_state.event_exposure_usd[candidate.event_ticker] = (
                risk_state.event_exposure_usd.get(candidate.event_ticker, Decimal(0)) + matched_cost + unmatched_exposure
            )
        risk_state.believed_positions = dict(believed_positions)
        risk_state.ticker_to_event.update({ticker: candidate.event_ticker for ticker in opportunity.legs})
        risk_state.persist(settings.execution.risk_state_path)

        if result.mismatched:
            reason = (
                f"unwind for {candidate.event_ticker} did not confirm fully filled; a naked position may remain"
                if not result.unwind_confirmed else
                f"first basket mismatch on {candidate.event_ticker}; canary policy halted execution"
            )
            circuit_breaker.force_trip(reason)
        else:
            circuit_breaker.record(False)
        if risk_state.daily_realized_loss_usd >= settings.execution.max_daily_loss_usd:
            circuit_breaker.force_trip("maximum daily realized loss reached")
        if circuit_breaker.tripped:
            await _emit_incident(
                settings, "circuit_breaker_tripped", event_ticker=candidate.event_ticker,
                reason=circuit_breaker.tripped_reason,
                daily_realized_loss_usd=str(risk_state.daily_realized_loss_usd),
            )
            break   # don't fire the rest of this pass's selections after tripping


async def _reprice_event(
    ticker: str,
    books: dict[str, LocalBook],
    relationships_by_ticker: dict[str, list[Relationship]],
    relationship_by_event_ticker: dict[str, Relationship],
    settings: Settings,
    open_opportunities: dict[str, BasketOpportunity],
    executed_opportunities: set[str],
    execution_client: Optional[KalshiClient],
    market_data_client: KalshiClient,
    balance_cache: _BalanceCache,
    circuit_breaker: _CircuitBreaker,
    believed_positions: dict[str, Decimal],
    risk_state: RiskState,
    detection_diagnostics: dict[str, Any],
    store: Optional[OrderStore] = None,
    open_no_opportunities: Optional[dict[str, BasketOpportunity]] = None,
    executed_no_opportunities: Optional[set[str]] = None,
) -> None:
    """Dispatches to every relationship that owns `ticker` — see
    scanner/relationships.py: a ticker can belong to more than one
    relationship (today, always exactly one, since only auto-detected
    exhaustive-partition relationships exist, but the dispatch must not
    assume that), so this re-prices each one rather than just the first.

    open_no_opportunities/executed_no_opportunities, if given, track
    buy_all_no detections in a separate namespace from the buy_basket ones
    in open_opportunities/executed_opportunities — a relationship's legs
    can carry an open opportunity in each direction at once, independently
    (see scanner/basket.py's compute_no_opportunity). None (the default)
    means buy_all_no detection is off entirely for this call."""
    for relationship in relationships_by_ticker.get(ticker, []):
        await _reprice_relationship(
            relationship, books, relationship_by_event_ticker, settings, open_opportunities,
            executed_opportunities, execution_client, market_data_client, balance_cache,
            circuit_breaker, believed_positions, risk_state, detection_diagnostics, store,
            open_no_opportunities, executed_no_opportunities,
        )


async def _reprice_relationship(
    relationship: Relationship,
    books: dict[str, LocalBook],
    relationship_by_event_ticker: dict[str, Relationship],
    settings: Settings,
    open_opportunities: dict[str, BasketOpportunity],
    executed_opportunities: set[str],
    execution_client: Optional[KalshiClient],
    market_data_client: KalshiClient,
    balance_cache: _BalanceCache,
    circuit_breaker: _CircuitBreaker,
    believed_positions: dict[str, Decimal],
    risk_state: RiskState,
    detection_diagnostics: dict[str, Any],
    store: Optional[OrderStore] = None,
    open_no_opportunities: Optional[dict[str, BasketOpportunity]] = None,
    executed_no_opportunities: Optional[set[str]] = None,
) -> None:
    """Re-runs the basket edge calc for one relationship and logs an
    open/close transition if the opportunity state changed. A fresh
    close->open transition also triggers a capital-aware execution pass
    (_reconcile_execution), if armed (settings.execution.enabled and
    execution_client provided). Formerly the single-candidate body of
    _reprice_event, extracted so that function can dispatch to every
    relationship a changed ticker belongs to.

    When open_no_opportunities is given (not None), also runs
    compute_no_opportunity on the same legs and tracks it independently —
    a relationship can have an open buy_basket opportunity, an open
    buy_all_no opportunity, both, or neither at once; the two directions
    price off opposite sides of the book and never conflict."""
    candidate = relationship

    legs: list[LegQuote] = []
    quote_now = time.monotonic()
    max_quote_age_s = settings.scanner.max_quote_age_ms / 1000
    for leg_ticker in candidate.leg_tickers:
        book = books[leg_ticker]
        if not book.snapshot_received:
            detection_diagnostics["incomplete_book_rejections"] += 1
            return
        if not book.is_fresh(max_quote_age_s, quote_now):
            detection_diagnostics["stale_book_rejections"] += 1
            return
        quote = book.best_yes_quote()
        if quote is None:
            detection_diagnostics["incomplete_book_rejections"] += 1
            return  # book not fully populated yet for this event — wait for more ticks
        best_ask, ask_size, best_bid, bid_size = quote
        legs.append(LegQuote(ticker=leg_ticker, best_ask=best_ask, ask_size=ask_size, best_bid=best_bid, bid_size=bid_size))

    # compute_opportunity rejects these on its own; logging here is what makes
    # local book corruption visible instead of silently indistinguishable from
    # "no edge right now". A steady trickle is normal mid-update churn; a burst
    # across many unrelated events is the reconnect-staleness signature.
    bad = incoherent_legs(legs)
    if bad:
        detection_diagnostics["incoherent_book_rejections"] += 1
        logger.warning(
            "incoherent_book_rejected", event_ticker=candidate.event_ticker,
            legs=[
                {"ticker": leg.ticker, "best_ask": str(leg.best_ask), "best_bid": str(leg.best_bid)}
                for leg in bad
            ],
        )
        return

    contract_count = min(settings.execution.max_order_size_contracts, min(leg.ask_size for leg in legs))
    basket_cost = sum(
        (
            leg.best_ask
            + taker_fee(leg.best_ask, contract_count, candidate.fee_multiplier) / contract_count
            for leg in legs
        ),
        start=Decimal(0),
    )
    minimum_depth = min(leg.ask_size for leg in legs)
    detection_diagnostics["fully_priced_evaluations"] += 1
    closest = detection_diagnostics["closest_baskets"]
    closest[candidate.event_ticker] = {
        "event_ticker": candidate.event_ticker,
        "basket_cost_usd": str(basket_cost),
        "distance_to_threshold_usd": str(basket_cost - settings.arb.buy_basket_max_cost_usd),
        "minimum_ask_depth": str(minimum_depth),
        "legs": len(legs),
    }
    if len(closest) > 10:
        worst = max(closest, key=lambda event: Decimal(closest[event]["basket_cost_usd"]))
        del closest[worst]

    opportunity = compute_opportunity(
        event_ticker=candidate.event_ticker,
        legs=legs,
        buy_basket_max_cost_usd=settings.arb.buy_basket_max_cost_usd,
        min_depth_contracts=settings.scanner.min_depth_contracts,
        contract_count=contract_count,
        fee_multiplier=candidate.fee_multiplier,
    )
    if opportunity is None:
        if minimum_depth < settings.scanner.min_depth_contracts:
            detection_diagnostics["insufficient_depth_rejections"] += 1
        elif basket_cost >= settings.arb.buy_basket_max_cost_usd:
            detection_diagnostics["price_threshold_rejections"] += 1

    was_open = candidate.event_ticker in open_opportunities
    stability_buy = detection_diagnostics.setdefault("edge_stability_ticks", {"buy_basket": {}, "buy_all_no": {}})["buy_basket"]
    if opportunity is not None:
        open_opportunities[candidate.event_ticker] = opportunity
        hist = stability_buy.setdefault(candidate.event_ticker, [])
        hist.append(opportunity.edge_usd)
        # Cap the history to the stability window so stale deep-history doesn't
        # dominate the non-shrinking check — we only care about the current window.
        if len(hist) > settings.execution.min_edge_stability_ticks:
            del hist[0:len(hist) - settings.execution.min_edge_stability_ticks]
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
            if store is not None:
                store.record_opportunity(opportunity)
        if settings.execution.enabled and execution_client is not None:
            await _reconcile_execution(
                stability_buy,
                open_opportunities, executed_opportunities, relationship_by_event_ticker, execution_client,
                market_data_client, settings, balance_cache, circuit_breaker, believed_positions,
                risk_state, store,
            )
    elif was_open:
        del open_opportunities[candidate.event_ticker]
        executed_opportunities.discard(candidate.event_ticker)
        stability_buy.pop(candidate.event_ticker, None)
        logger.info("opportunity_closed", event_ticker=candidate.event_ticker)

    if open_no_opportunities is not None:
        no_opportunity = compute_no_opportunity(
            event_ticker=candidate.event_ticker,
            legs=legs,
            min_edge_usd=settings.arb.buy_all_no_min_edge_usd,
            min_depth_contracts=settings.scanner.min_depth_contracts,
            contract_count=contract_count,
            fee_multiplier=candidate.fee_multiplier,
        )
        was_no_open = candidate.event_ticker in open_no_opportunities
        stability_no = detection_diagnostics.setdefault("edge_stability_ticks", {"buy_basket": {}, "buy_all_no": {}})["buy_all_no"]
        if no_opportunity is not None:
            open_no_opportunities[candidate.event_ticker] = no_opportunity
            hist_no = stability_no.setdefault(candidate.event_ticker, [])
            hist_no.append(no_opportunity.edge_usd)
            if len(hist_no) > settings.execution.min_edge_stability_ticks:
                del hist_no[0:len(hist_no) - settings.execution.min_edge_stability_ticks]
            if not was_no_open:
                logger.info(
                    "opportunity_detected",
                    stage="stage3",
                    event_ticker=no_opportunity.event_ticker,
                    direction=no_opportunity.direction,
                    legs=no_opportunity.legs,
                    basket_price_usd=str(no_opportunity.basket_price_usd),
                    edge_usd=str(no_opportunity.edge_usd),
                    max_size=str(no_opportunity.max_size),
                )
                _append_opportunity_jsonl(no_opportunity, settings.logging.opportunities_log_path)
                if store is not None:
                    store.record_opportunity(no_opportunity)
            if settings.execution.enabled and execution_client is not None and executed_no_opportunities is not None:
                await _reconcile_execution(
                    stability_no,
                    open_no_opportunities, executed_no_opportunities, relationship_by_event_ticker, execution_client,
                    market_data_client, settings, balance_cache, circuit_breaker, believed_positions,
                    risk_state, store,
                )
        elif was_no_open:
            del open_no_opportunities[candidate.event_ticker]
            if executed_no_opportunities is not None:
                executed_no_opportunities.discard(candidate.event_ticker)
            stability_no.pop(candidate.event_ticker, None)
            logger.info("opportunity_closed", event_ticker=candidate.event_ticker)


_POSITION_RECONCILE_INTERVAL_S = 30   # throttled well below the 5s heartbeat — positions only change when we execute, this is a safety check, not a hot path
_STAGE1_MIN_BACKOFF_S = 5             # first retry delay after a failed Stage 1 REST pass; doubles up to scanner.rescan_interval_s


async def _stream_shortlist(
    ws_client: KalshiWSClient,
    shortlist: list[EventCandidate],
    settings: Settings,
    execution_client: Optional[KalshiClient],
    market_data_client: KalshiClient,
    status: dict[str, Any],
    circuit_breaker: _CircuitBreaker,
    believed_positions: dict[str, Decimal],
    risk_state: RiskState,
    store: Optional[OrderStore] = None,
) -> None:
    relationships = build_relationships(
        shortlist,
        settings.relationships.verified_event_tickers,
        trust_auto_detected=settings.relationships.trust_auto_detected,
    )
    if store is not None:
        # One row per relationship per rescan cycle — a queryable history of
        # what was auto-detected vs. manually verified over time, not just
        # the current set. See execution/store.py's record_relationships.
        store.record_relationships(relationships)
    books: dict[str, LocalBook] = {t: LocalBook() for r in relationships for t in r.leg_tickers}
    # Fan-out index: a leg ticker can belong to more than one relationship
    # (not possible yet — only one auto-detected exhaustive-partition
    # relationship exists per event today — but a strict 1:1 dict here
    # would silently clobber the moment a second relationship type or
    # direction is added, exactly the bug this replaces). event_ticker
    # itself stays a unique relationship identity, so that index stays 1:1.
    relationships_by_ticker: dict[str, list[Relationship]] = {}
    for r in relationships:
        for t in r.leg_tickers:
            relationships_by_ticker.setdefault(t, []).append(r)
    relationship_by_event_ticker: dict[str, Relationship] = {r.event_ticker: r for r in relationships}
    open_opportunities: dict[str, BasketOpportunity] = {}
    executed_opportunities: set[str] = set()
    open_no_opportunities: dict[str, BasketOpportunity] = {}
    executed_no_opportunities: set[str] = set()
    balance_cache = _BalanceCache()
    last_heartbeat = 0.0
    last_reconciled = 0.0
    last_reconciliation_mismatches: dict[str, tuple[Decimal, Decimal]] = {}
    detection_diagnostics: dict[str, Any] = {
        "fully_priced_evaluations": 0,
        "stale_book_rejections": 0,
        "incomplete_book_rejections": 0,
        "incoherent_book_rejections": 0,
        "insufficient_depth_rejections": 0,
        "price_threshold_rejections": 0,
        "closest_baskets": {},
        # Per-direction, per-event consecutive-tick counts the edge has
        # remained priced-through. Carried in diagnostics so no new signatures
        # need threading; _reconcile_execution gates on
        # settings.execution.min_edge_stability_ticks reading these.
        "edge_stability_ticks": {"buy_basket": {}, "buy_all_no": {}},
    }

    async for msg in ws_client.stream(list(books.keys())):
        if msg.get("type") == WS_RECONNECTED_MESSAGE_TYPE:
            # Connection-scoped state only. Every book is rebuilt from the
            # replacement snapshots this subscribe is about to deliver, and
            # any opportunity still "open" was priced off books that no
            # longer exist — see LocalBook.invalidate. executed_opportunities
            # is deliberately NOT cleared: keeping an event marked consumed
            # can only suppress an order, never cause one.
            for book in books.values():
                book.invalidate()
            open_opportunities.clear()
            open_no_opportunities.clear()
            logger.info("ws_books_invalidated", reason="reconnected", tickers=len(books))
            continue

        ticker = handle_message(msg, books)
        if ticker is not None:
            await _reprice_event(
                ticker, books, relationships_by_ticker, relationship_by_event_ticker, settings,
                open_opportunities, executed_opportunities, execution_client, market_data_client,
                balance_cache, circuit_breaker, believed_positions, risk_state, detection_diagnostics,
                store, open_no_opportunities, executed_no_opportunities,
            )

        now = time.monotonic()
        if now - last_heartbeat >= _HEARTBEAT_MIN_INTERVAL_S:
            last_heartbeat = now
            if settings.execution.enabled and execution_client is not None:
                # Retries any opportunity still open but skipped earlier for
                # insufficient capital, in case balance freed up since —
                # _reprice_event above only re-evaluates on a tick for one
                # of THIS opportunity's own legs, so a long-open, still-
                # unexecuted candidate could otherwise go unreconsidered
                # between its own ticks.
                stability_map = detection_diagnostics["edge_stability_ticks"]
                await _reconcile_execution(
                    stability_map["buy_basket"],
                    open_opportunities, executed_opportunities, relationship_by_event_ticker, execution_client,
                    market_data_client, settings, balance_cache, circuit_breaker, believed_positions,
                    risk_state, store,
                )
                await _reconcile_execution(
                    stability_map["buy_all_no"],
                    open_no_opportunities, executed_no_opportunities, relationship_by_event_ticker, execution_client,
                    market_data_client, settings, balance_cache, circuit_breaker, believed_positions,
                    risk_state, store,
                )

                if now - last_reconciled >= _POSITION_RECONCILE_INTERVAL_S:
                    last_reconciled = now
                    try:
                        # Enforced continuously, not only at startup: an
                        # anomalous live IOC/FOK order used to be able to sit
                        # undetected until the next restart.
                        owned = await execution_client.get_orders(
                            client_order_id_prefix=STRATEGY_CLIENT_ORDER_ID_PREFIX
                        )
                        live = [
                            o for o in owned
                            if o.order_id and o.status in (OrderStatus.OPEN, OrderStatus.PARTIALLY_FILLED)
                        ]
                        if len(live) > settings.execution.max_open_orders:
                            circuit_breaker.force_trip(
                                f"{len(live)} bot-owned open orders exceeds limit "
                                f"{settings.execution.max_open_orders}"
                            )
                        actual = await execution_client.get_positions()
                        await _release_finalized_positions(execution_client, actual, believed_positions, risk_state)
                        tracked_tickers = set(risk_state.ticker_to_event) | set(believed_positions)
                        mismatches = {
                            ticker: (believed_positions.get(ticker, Decimal(0)), actual.get(ticker, Decimal(0)))
                            for ticker in tracked_tickers
                            if believed_positions.get(ticker, Decimal(0)) != actual.get(ticker, Decimal(0))
                        }
                        risk_state.persist(settings.execution.risk_state_path)
                    except ExchangeError as exc:
                        mismatches = {}
                        circuit_breaker.force_trip(f"position reconciliation unavailable: {exc}")
                    last_reconciliation_mismatches = mismatches
                    if mismatches:
                        logger.error(
                            "position_reconciliation_mismatch",
                            mismatches={t: {"believed": str(b), "actual": str(a)} for t, (b, a) in mismatches.items()},
                        )
                        circuit_breaker.force_trip(
                            f"position reconciliation found {len(mismatches)} ticker(s) where our records "
                            "disagree with Kalshi's — our bookkeeping can no longer be trusted. The trip is "
                            "DURABLE: restarting reloads it. Investigate immediately, then clear the "
                            "breaker in data/risk_state.json to re-arm."
                        )

            _write_scanner_status(
                settings.logging.status_path,
                **(status | {
                    "ws_connected": True,
                    "open_opportunities": len(open_opportunities),
                    "pending_execution": len(open_opportunities) - len(executed_opportunities),
                    "open_no_opportunities": len(open_no_opportunities),
                    "pending_no_execution": len(open_no_opportunities) - len(executed_no_opportunities),
                    "circuit_breaker_tripped": circuit_breaker.tripped,
                    "circuit_breaker_reason": circuit_breaker.tripped_reason,
                    "position_reconciliation_mismatches": {
                        t: {"believed": str(b), "actual": str(a)} for t, (b, a) in last_reconciliation_mismatches.items()
                    },
                    "detection_diagnostics": {
                        **{k: v for k, v in detection_diagnostics.items() if k != "closest_baskets"},
                        "closest_baskets": sorted(
                            detection_diagnostics["closest_baskets"].values(),
                            key=lambda row: Decimal(row["basket_cost_usd"]),
                        ),
                    },
                    "risk": {
                        "gross_exposure_usd": str(risk_state.gross_exposure_usd),
                        "daily_notional_usd": str(risk_state.daily_notional_usd),
                        "daily_realized_loss_usd": str(risk_state.daily_realized_loss_usd),
                        "daily_basket_attempts": risk_state.daily_basket_attempts,
                        "session_basket_attempts": risk_state.session_basket_attempts,
                        "max_baskets_per_day": settings.execution.max_baskets_per_day,
                        "max_baskets_per_session": settings.execution.max_baskets_per_session,
                        "max_unwind_loss_usd": str(settings.execution.max_unwind_loss_usd),
                    },
                    "phase": "streaming",
                }),
            )


async def run_stage3(
    client: KalshiClient,
    ws_client: KalshiWSClient,
    settings: Settings,
    execution_client: Optional[KalshiClient] = None,
    store: Optional[OrderStore] = None,
) -> None:
    """Runs forever: refreshes Stage 1 + the volume ranking every
    settings.scanner.rescan_interval_s and streams Stage 3 continuously
    over whatever the top settings.scanner.max_ws_legs by volume are in
    between (see module docstring for why this replaced the old
    Stage-2-gated shortlist). execution_client, when provided (see
    scripts/run_scanner.py — only constructed when settings.execution.enabled),
    is where a freshly detected opportunity's basket gets placed — see
    _execute_and_log. store, if given (execution/store.py's OrderStore), also
    records every detected opportunity and every rescan cycle's relationship
    set — additive alongside data/opportunities.jsonl, defaults to None
    (no persistence), the exact behavior every existing caller exercises."""
    try:
        risk_state = RiskState.load(settings.execution.risk_state_path)
    except RiskStateError as exc:
        # A corrupt/missingly parseable risk ledger makes ownership and limits
        # unknowable. Refuse to start rather than silently resetting it.
        raise RuntimeError(f"execution cannot start safely: {exc}") from exc
    circuit_breaker = _CircuitBreaker(
        settings.execution.circuit_breaker_max_mismatches,
        risk_state=risk_state,
        risk_state_path=settings.execution.risk_state_path,
    )
    believed_positions: dict[str, Decimal] = dict(risk_state.believed_positions)

    # Recovery is completed before the first market-data message can trigger
    # execution. Cancel any bot-owned order left by a prior crash, then verify
    # persisted positions against the exchange. Unknown state trips durably.
    if settings.execution.enabled and execution_client is not None:
        try:
            owned_orders = await execution_client.get_orders(client_order_id_prefix=STRATEGY_CLIENT_ORDER_ID_PREFIX)
            live_ids = [
                order.order_id for order in owned_orders
                if order.order_id and order.status in (OrderStatus.OPEN, OrderStatus.PARTIALLY_FILLED)
            ]
            if len(live_ids) > settings.execution.max_open_orders:
                circuit_breaker.force_trip(
                    f"startup found {len(live_ids)} bot-owned open orders, above limit "
                    f"{settings.execution.max_open_orders}"
                )
            if live_ids:
                cancel_errors = await execution_client.cancel_orders(live_ids)
                failures = {oid: err for oid, err in cancel_errors.items() if err is not None}
                if failures:
                    circuit_breaker.force_trip(f"startup could not cancel {len(failures)} bot-owned open order(s)")
            actual = await execution_client.get_positions()
            await _release_finalized_positions(execution_client, actual, believed_positions, risk_state)
            tracked_tickers = set(risk_state.ticker_to_event) | set(believed_positions)
            mismatches = {
                ticker: (believed_positions.get(ticker, Decimal(0)), actual.get(ticker, Decimal(0)))
                for ticker in tracked_tickers
                if believed_positions.get(ticker, Decimal(0)) != actual.get(ticker, Decimal(0))
            }
            if mismatches:
                circuit_breaker.force_trip(
                    f"startup position recovery found {len(mismatches)} mismatch(es); execution remains disabled"
                )
        except ExchangeError as exc:
            circuit_breaker.force_trip(f"startup account recovery failed: {exc}")
        if settings.execution.target == "prod":
            alert_ok = await _emit_incident(
                settings, "execution_startup_check",
                circuit_breaker_tripped=circuit_breaker.tripped,
                circuit_breaker_reason=circuit_breaker.tripped_reason,
            )
            if not alert_ok:
                circuit_breaker.force_trip("production incident webhook failed startup delivery check")
        risk_state.persist(settings.execution.risk_state_path)

    stage1_backoff_s = _STAGE1_MIN_BACKOFF_S
    while True:
        # Stage 1 is a plain public REST pass, and an unguarded failure here
        # used to end the process: data/scanner.log records a run dying on
        # `RateLimitError: Kalshi rate limit hit on GET /events`. A detection
        # loop must outlive a transient exchange blip, so back off and retry
        # instead — quickly at first, escalating to the normal rescan cadence,
        # so a brief 502 costs seconds rather than a full rescan interval.
        try:
            raw_events = await client.get_all_events(status="open", with_nested_markets=True)
        except (ExchangeError, httpx.HTTPError) as exc:
            # httpx.ConnectError (DNS failure after a network blip — verified live
            # 2026-11-10 when the sleeping laptop's wake-up left this call in the
            # middle of resolving external-api.kalshi.com and the next stage1 pass
            # crashed the whole scanner) is an httpx.TransportError subclass. The
            # transport-layer retry inside KalshiClient covers short blips; longer
            # ones (sleep/standby/wifi drop) exhaust tenacity's 4 attempts and
            # reraise. ExchangeError alone didn't catch that, so the loop died.
            logger.error("stage1_fetch_failed", error=str(exc), retry_in_s=stage1_backoff_s)
            _write_scanner_status(
                settings.logging.status_path,
                phase="idle_stage1_error", error=str(exc), retry_in_s=stage1_backoff_s,
                execution_enabled=settings.execution.enabled,
                execution_target=settings.execution.target,
                ws_connected=False,
                circuit_breaker_tripped=circuit_breaker.tripped,
                circuit_breaker_reason=circuit_breaker.tripped_reason,
            )
            await asyncio.sleep(stage1_backoff_s)
            stage1_backoff_s = min(stage1_backoff_s * 2, settings.scanner.rescan_interval_s)
            continue
        stage1_backoff_s = _STAGE1_MIN_BACKOFF_S

        candidates = filter_events(
            raw_events,
            min_outcome_legs=settings.scanner.min_outcome_legs,
            max_outcome_legs=settings.scanner.max_outcome_legs,
            blacklisted_series=set(settings.scanner.blacklisted_series),
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
            "active_events_omitted_from_ws": len(active_candidates) - len(shortlist),
            "active_legs_total": sum(len(c.leg_tickers) for c in active_candidates),
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
            "execution_target": settings.execution.target,
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
                _stream_shortlist(
                    ws_client, shortlist, settings, execution_client, client, status,
                    circuit_breaker, believed_positions, risk_state, store,
                ),
                timeout=settings.scanner.rescan_interval_s,
            )
        except asyncio.TimeoutError:
            pass
        except ExchangeError as exc:
            # Narrow on purpose: only exchange/transport faults are absorbed
            # here, and only to survive to the next cycle. Anything execution
            # actually depends on has already tripped the durable breaker at
            # the point of failure, and risk state is persisted before every
            # submission — so continuing cannot re-arm around a real fault.
            logger.error("stage3_stream_failed", error=str(exc), next_rescan_s=settings.scanner.rescan_interval_s)
        logger.info("stage3_rescan", reason="rescan_interval_elapsed")
