"""Stage 2 — order book evaluation over Stage 1's structural/time
candidates, by:

  1. Ghost-spread filter: skip events with no recent trading activity on
     every leg (scanner.filters.has_recent_activity — see that module's
     docstring). Kalshi's nested market objects (from Stage 1's
     with_nested_markets=True pull) carry `volume_24h`, not a precise
     last-trade timestamp, so this uses `volume_24h > 0` as a coarser proxy
     for the spec's "traded in the last 1-2h". Tightening this to a true
     1-2h window would need GET /markets/{ticker}/trades with a min_ts
     cutoff, not implemented here to avoid an extra REST call per leg on
     every rescan.
  2. Fetches top-of-book for every remaining leg and runs scanner.basket's
     fee-adjusted edge calc.
  3. Depth floor (scanner.basket.compute_opportunity already rejects below
     scanner.min_depth_contracts).

NOT used by stream/monitor.py's live run_stage3 loop anymore — that now
selects its WS subscription set via scanner.filters.select_by_volume
directly off Stage 1's free volume data, so it no longer needs this
module's REST book-fetch pass just to decide what to watch (see that
module's docstring for why: this stage's own "already at the edge right
now" gate meant Stage 3 could only ever watch opportunities that happened
to already be open at the exact moment of a 5-minute REST poll, never one
that opened and closed between polls). Still used by scripts/run_scanner.py's
`--once` mode as a REST-only sanity-check summary.

Runs sequentially per event (not concurrently) on purpose — this is a REST
pass over at most ~80 events every 15-30 min, well within Kalshi's rate
limits, and sequential keeps the implementation simple and the logs
readable in order.
"""

from __future__ import annotations

import structlog

from kalshi.client import ExchangeError, KalshiClient, Side
from scanner.basket import BasketOpportunity, LegQuote, compute_opportunity
from scanner.filters import EventCandidate, has_recent_activity

logger = structlog.get_logger(__name__)


async def scan_event(
    client: KalshiClient,
    candidate: EventCandidate,
    orderbook_depth: int,
    buy_basket_max_cost_usd,
    min_depth_contracts,
) -> BasketOpportunity | None:
    """Fetches top-of-book for every leg in `candidate` and returns a
    BasketOpportunity if the fee-adjusted edge and depth floor both clear,
    else None. Any per-leg fetch failure drops the whole event for this
    scan pass (logged, not raised) rather than pricing a partial basket."""
    legs: list[LegQuote] = []
    for ticker in candidate.leg_tickers:
        try:
            book = await client.get_orderbook(ticker, side=Side.YES, depth=orderbook_depth)
        except ExchangeError as exc:
            logger.warning("orderbook_fetch_failed", event_ticker=candidate.event_ticker, ticker=ticker, error=str(exc))
            return None

        if book.best_ask is None or book.best_bid is None:
            logger.debug("leg_missing_quote", event_ticker=candidate.event_ticker, ticker=ticker)
            return None

        legs.append(
            LegQuote(
                ticker=ticker,
                best_ask=book.best_ask.price,
                ask_size=book.best_ask.size,
                best_bid=book.best_bid.price,
                bid_size=book.best_bid.size,
            )
        )

    return compute_opportunity(
        event_ticker=candidate.event_ticker,
        legs=legs,
        buy_basket_max_cost_usd=buy_basket_max_cost_usd,
        min_depth_contracts=min_depth_contracts,
    )


async def run_stage2(
    client: KalshiClient,
    candidates: list[EventCandidate],
    orderbook_depth: int,
    buy_basket_max_cost_usd,
    min_depth_contracts,
) -> list[EventCandidate]:
    """Returns the subset of `candidates` that currently clear the
    fee-adjusted edge + depth floor — the "active subset" the spec hands
    off to Stage 3's WS subscription. Everything else is dropped for this
    rescan cycle (re-checked next cycle, not tracked in between) — this is
    what keeps the WS subscription set to the target <20 order books
    instead of everything that merely passed Stage 1's structural filter."""
    shortlist: list[EventCandidate] = []

    for candidate in candidates:
        if not has_recent_activity(candidate):
            logger.debug("stage2_dropped_inactive", event_ticker=candidate.event_ticker)
            continue

        opportunity = await scan_event(
            client,
            candidate,
            orderbook_depth=orderbook_depth,
            buy_basket_max_cost_usd=buy_basket_max_cost_usd,
            min_depth_contracts=min_depth_contracts,
        )
        if opportunity is None:
            continue

        logger.info(
            "opportunity_detected",
            stage="stage2",
            event_ticker=opportunity.event_ticker,
            direction=opportunity.direction,
            legs=opportunity.legs,
            basket_price_usd=str(opportunity.basket_price_usd),
            edge_usd=str(opportunity.edge_usd),
            max_size=str(opportunity.max_size),
        )
        shortlist.append(candidate)

    return shortlist
