"""Paper-trading validation for this strategy, using paper_trading_engine
against REAL prod order book data — no Kalshi orders placed anywhere, demo
or prod. Built after discovering that execution/executor.py's demo-account
path can't answer "is this strategy actually profitable": most legs have
no counterparty at all on demo's disconnected paper book (verified live —
see execution/executor.py's module docstring and stream/monitor.py's git
history around 2026-08-26), so nearly every detected opportunity either
gets zero fills (free, but proves nothing) or fills unevenly and eats fees
on the unwind. This script instead answers the profitability question
directly: run the exact same Stage 1 + Stage 2 detection against real prod
data, and for every opportunity actually found, simulate the fill against
the real observed depth (which — unlike demo — is what's genuinely there).

Modeling choice — settle immediately, not per-leg: a real basket trade
holds N separate leg positions and only learns its outcome when the event
actually resolves, hours or days later. But for THIS strategy specifically,
that outcome is irrelevant to total P&L by construction — buying one full
matched set of every leg in a mutually-exclusive event pays out exactly
$1.00 regardless of which leg wins (see scanner/basket.py's docstring).
So rather than model N individual leg positions and arbitrarily pick a
"winner," this treats "buy the whole basket" as ONE simulated instrument
per event (instrument_id = event_ticker), bought at basket_price_usd (fees
already included — see scanner/basket.py) for max_size contracts, and
settled immediately at $1.00. This is an honest simplification specific to
this strategy's guaranteed-payout structure, not a general assumption of
paper_trading_engine itself — a different strategy using that engine would
need to actually wait for and observe its real settlement value.

Run:
    python scripts/run_paper_backtest.py                       # loop forever at settings.scanner.rescan_interval_s
    python scripts/run_paper_backtest.py --once                 # single pass, then exit
    python scripts/run_paper_backtest.py --iterations 5          # 5 passes, then exit
    python scripts/run_paper_backtest.py --interval-s 60          # override the pacing between passes

State persists to data/paper_state.json (cash/positions/realized P&L —
survives a restart) and every simulated trade is appended to
data/paper_trades.jsonl.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import structlog
from paper_trading_engine import PaperBroker, Side

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.loader import Settings, load_settings
from kalshi.client import KalshiClient
from scanner.basket import BasketOpportunity
from scanner.filters import filter_events, has_recent_activity
from scanner.orderbook_scan import scan_event

logger = structlog.get_logger(__name__)

_SETTLEMENT_PRICE = Decimal("1.00")   # guaranteed — see module docstring


def _append_trade_jsonl(opportunity: BasketOpportunity, path: Path) -> None:
    record = {
        "traded_at": datetime.now(timezone.utc).isoformat(),
        "event_ticker": opportunity.event_ticker,
        "legs": opportunity.legs,
        "basket_price_usd": str(opportunity.basket_price_usd),
        "edge_usd": str(opportunity.edge_usd),
        "size": str(opportunity.max_size),
        "simulated_profit_usd": str(opportunity.edge_usd * opportunity.max_size),
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a") as f:
        f.write(json.dumps(record) + "\n")


async def run_pass(client: KalshiClient, settings: Settings, broker: PaperBroker, trades_log_path: Path) -> list[BasketOpportunity]:
    """One Stage 1 + Stage 2 pass over real prod data — identical
    detection logic to the live scanner (stream/monitor.py). Every
    opportunity found gets an immediate simulated buy + settlement."""
    raw_events = await client.get_all_events(status="open", with_nested_markets=True)
    candidates = filter_events(
        raw_events,
        min_outcome_legs=settings.scanner.min_outcome_legs,
        max_outcome_legs=settings.scanner.max_outcome_legs,
    )
    active_candidates = [c for c in candidates if has_recent_activity(c)]

    executed: list[BasketOpportunity] = []
    for candidate in active_candidates:
        opportunity = await scan_event(
            client,
            candidate,
            orderbook_depth=settings.scanner.orderbook_depth,
            buy_basket_max_cost_usd=settings.arb.buy_basket_max_cost_usd,
            min_depth_contracts=settings.scanner.min_depth_contracts,
        )
        if opportunity is None:
            continue

        # available_size == max_size always here: compute_opportunity
        # already capped max_size to the real observed depth (the
        # binding constraint across every leg), so this fill is
        # guaranteed complete — never partial, unlike execution against
        # demo's disconnected book.
        broker.place_order(
            instrument=opportunity.event_ticker,
            side=Side.BUY,
            price=opportunity.basket_price_usd,
            size=opportunity.max_size,
            available_size=opportunity.max_size,
        )
        broker.settle_position(opportunity.event_ticker, settlement_price=_SETTLEMENT_PRICE)
        _append_trade_jsonl(opportunity, trades_log_path)
        executed.append(opportunity)

        logger.info(
            "paper_trade_settled",
            event_ticker=opportunity.event_ticker,
            basket_price_usd=str(opportunity.basket_price_usd),
            edge_usd=str(opportunity.edge_usd),
            size=str(opportunity.max_size),
            simulated_profit_usd=str(opportunity.edge_usd * opportunity.max_size),
        )

    logger.info(
        "pass_complete",
        stage1_candidates=len(candidates),
        active_candidates=len(active_candidates),
        trades_this_pass=len(executed),
        cash=str(broker.get_cash()),
        realized_pnl=str(broker.get_realized_pnl()),
    )
    return executed


async def main_async(args: argparse.Namespace) -> None:
    settings = load_settings()
    state_path = settings.logging.opportunities_log_path.parent / "paper_state.json"
    trades_log_path = settings.logging.opportunities_log_path.parent / "paper_trades.jsonl"

    broker = PaperBroker(starting_cash=Decimal(str(args.starting_cash)), state_path=state_path)
    logger.info(
        "paper_backtest_starting",
        starting_cash=str(args.starting_cash),
        resuming_from=str(state_path) if state_path.exists() else None,
        current_cash=str(broker.get_cash()),
        current_realized_pnl=str(broker.get_realized_pnl()),
    )

    interval_s = args.interval_s if args.interval_s is not None else settings.scanner.rescan_interval_s

    async with KalshiClient(
        base_url=settings.kalshi.base_url,
        request_timeout_s=settings.kalshi.request_timeout_s,
        max_retries=settings.kalshi.max_retries,
    ) as client:
        iteration = 0
        while True:
            iteration += 1
            await run_pass(client, settings, broker, trades_log_path)

            if args.once or (args.iterations is not None and iteration >= args.iterations):
                break
            await asyncio.sleep(interval_s)

    print()
    print(f"Final: cash=${broker.get_cash():.2f}  realized_pnl=${broker.get_realized_pnl():.2f}  fees_paid=${broker.get_total_fees_paid():.2f}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--once", action="store_true", help="run a single Stage 1+2 pass and exit")
    parser.add_argument("--iterations", type=int, default=None, help="run this many passes, then exit (default: loop forever)")
    parser.add_argument("--interval-s", type=int, default=None, help="seconds between passes (default: scanner.rescan_interval_s)")
    parser.add_argument("--starting-cash", type=str, default="1000", help="starting paper cash, only used if data/paper_state.json doesn't already exist")
    args = parser.parse_args()

    logging.basicConfig(format="%(message)s", level="INFO")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.add_log_level,
            structlog.processors.KeyValueRenderer(sort_keys=True),
        ],
    )

    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
