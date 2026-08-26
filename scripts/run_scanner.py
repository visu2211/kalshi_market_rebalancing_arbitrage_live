"""Entry point — same-day ("_live") variant of the sibling
kalshi_market_rebalancing_arbitrage scanner: Stage 1 only keeps events
settling by the end of the current UTC calendar day (scanner/filters.py),
and a detected opportunity is filled with a short-expiration (IOC-style)
order sized to exactly the observed book depth, so it either fills at the
priced-in price or doesn't fill at all — see execution/executor.py.

    python scripts/run_scanner.py [--once]

Default: runs forever — Stage 1/2 refresh on a timer, Stage 3 streaming
continuously against the current shortlist. Ctrl+C to stop. If
config/settings.yaml's execution.enabled is true, a fresh opportunity also
triggers demo-account order placement (execution/executor.py) — see
KALSHI_DEMO_API_KEY_ID/KALSHI_DEMO_PRIVATE_KEY_PATH in .env.example.

--once: runs Stage 1 -> Stage 2 exactly once, prints the shortlist, and
exits without opening a WS connection. Useful for sanity-checking the
filters/fee math against live data without needing KALSHI_API_KEY_ID /
KALSHI_PRIVATE_KEY_PATH configured (Stage 3's WS handshake needs those;
Stages 1-2 are public REST only).
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import sys
from pathlib import Path

import structlog

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.loader import Settings, load_settings
from kalshi.client import KalshiClient
from kalshi.ws_client import KalshiWSClient
from scanner.filters import filter_events
from scanner.orderbook_scan import run_stage2
from stream.monitor import run_stage3

logger = structlog.get_logger(__name__)


def _configure_logging(level: str) -> None:
    logging.basicConfig(format="%(message)s", level=level)
    # httpx logs one INFO line per request — at thousands of Stage 2 requests
    # per pass, that drowns out the opportunity_detected lines it's actually
    # useful to see. Keep it at WARNING regardless of our own log level.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(logging.getLevelName(level)),
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.add_log_level,
            structlog.processors.KeyValueRenderer(sort_keys=True),
        ],
    )


async def run_once(settings: Settings) -> None:
    async with KalshiClient(
        base_url=settings.kalshi.base_url,
        request_timeout_s=settings.kalshi.request_timeout_s,
        max_retries=settings.kalshi.max_retries,
        api_key_id=settings.kalshi.api_key_id,
        private_key_path=settings.kalshi.private_key_path,
    ) as client:
        raw_events = await client.get_all_events(status="open", with_nested_markets=True)
        candidates = filter_events(
            raw_events,
            min_outcome_legs=settings.scanner.min_outcome_legs,
            max_outcome_legs=settings.scanner.max_outcome_legs,
        )
        print(f"Stage 1: {len(candidates)} of {len(raw_events)} open events passed structural filters "
              f"(mutually_exclusive, {settings.scanner.min_outcome_legs}-{settings.scanner.max_outcome_legs} legs, "
              f"settling by midnight UTC tonight).")

        shortlist = await run_stage2(
            client,
            candidates,
            orderbook_depth=settings.scanner.orderbook_depth,
            buy_basket_max_cost_usd=settings.arb.buy_basket_max_cost_usd,
            min_depth_contracts=settings.scanner.min_depth_contracts,
        )
        print(f"Stage 2: {len(shortlist)} events currently clear the fee-adjusted edge + depth floor.")
        for candidate in shortlist:
            print(f"  {candidate.event_ticker}: {candidate.title!r} ({len(candidate.leg_tickers)} legs)")


async def run_forever(settings: Settings) -> None:
    async with contextlib.AsyncExitStack() as stack:
        client = await stack.enter_async_context(KalshiClient(
            base_url=settings.kalshi.base_url,
            request_timeout_s=settings.kalshi.request_timeout_s,
            max_retries=settings.kalshi.max_retries,
            api_key_id=settings.kalshi.api_key_id,
            private_key_path=settings.kalshi.private_key_path,
        ))

        demo_client = None
        if settings.execution.enabled:
            # Separate KalshiClient/account — execution/executor.py places
            # orders here, never against `client` above (prod). Only opened
            # when execution.enabled, so a detect-only run never needs demo
            # credentials at all.
            demo_client = await stack.enter_async_context(KalshiClient(
                base_url=settings.execution.demo_base_url,
                request_timeout_s=settings.kalshi.request_timeout_s,
                max_retries=settings.kalshi.max_retries,
                api_key_id=settings.execution.demo_api_key_id,
                private_key_path=settings.execution.demo_private_key_path,
            ))

        ws_client = KalshiWSClient(settings.kalshi.ws_url, client)
        await run_stage3(client, ws_client, settings, demo_client=demo_client)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--once", action="store_true", help="run Stage 1+2 once and exit, no WS connection")
    args = parser.parse_args()

    settings = load_settings()
    _configure_logging(settings.logging.level)

    if args.once:
        asyncio.run(run_once(settings))
    else:
        asyncio.run(run_forever(settings))


if __name__ == "__main__":
    main()
