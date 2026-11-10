"""Paper-feasibility analysis for the maker strategy, run against the
scanner's own opportunities.jsonl (which captures every leg's bid/ask at
the moment the taker detected an edge).

What this DOES: given a candidate maker config
(quote_offset_ticks, target_margin_per_basket), replays every historical
opportunity through execution.maker.quoter.quote_basket and reports how
many would have CLEARED the margin gate, plus the margin distribution.
That tells us whether the config is even in the right ballpark to ever
fire.

What this does NOT do: simulate fills. The .jsonl is a sequence of single
snapshots, not an orderbook time-series — so the question "did the book
ever drift through our quote" has no answer here. Fill rate is a
forward-measurement problem. A later tool can tap the live WS stream for
that; this one is for CONFIG tuning before we spend bandwidth on live
observation.

Usage:
    .venv/bin/python scripts/run_maker_paper.py \\
        --offset 1 \\
        --target-margin 0.03 \\
        --direction buy_basket \\
        --log data/opportunities.jsonl

Or grid search:
    .venv/bin/python scripts/run_maker_paper.py --grid
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Iterator

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from execution.maker.quoter import MakerQuote, quote_basket   # noqa: E402
from scanner.basket import BasketOpportunity, LegQuote         # noqa: E402


@dataclass
class SimResult:
    """Outcome for one historical opportunity run through the maker
    quoter at a given config."""

    event_ticker: str
    direction: str
    detected_at: str
    would_quote: bool
    expected_margin_usd: Decimal | None   # None when quote_basket refused
    basket_cost_usd: Decimal | None


def _parse_opportunity_record(raw: dict) -> BasketOpportunity:
    """Rebuilds a BasketOpportunity from one row of opportunities.jsonl.
    Bid-only leg quotes become LegQuote exactly the way scanner.basket
    and stream/monitor.py already pair them."""
    legs = [
        LegQuote(
            ticker=lq["ticker"],
            best_ask=Decimal(lq["ask"]),
            ask_size=Decimal(lq["ask_size"]),
            best_bid=Decimal(lq["bid"]),
            bid_size=Decimal(lq["bid_size"]),
        )
        for lq in raw["leg_quotes"]
    ]
    return BasketOpportunity(
        event_ticker=raw["event_ticker"],
        direction=raw["direction"],
        legs=[lq.ticker for lq in legs],
        leg_quotes=legs,
        basket_price_usd=Decimal(raw["basket_price_usd"]),
        edge_usd=Decimal(raw["edge_usd"]),
        max_size=Decimal(raw["max_size"]),
        fee_multiplier=Decimal(str(raw.get("fee_multiplier", 1))),
    )


def _load(path: Path, direction_filter: str | None) -> Iterator[BasketOpportunity]:
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            raw = json.loads(line)
        except json.JSONDecodeError:
            continue
        if direction_filter and raw.get("direction") != direction_filter:
            continue
        try:
            yield _parse_opportunity_record(raw)
        except (KeyError, ValueError):
            continue


def simulate(
    opportunities: Iterator[BasketOpportunity],
    *,
    quote_offset_ticks: int,
    target_margin_per_basket: Decimal,
    size: Decimal = Decimal(1),
) -> list[SimResult]:
    """Run each opportunity through quote_basket and summarize per-row."""
    results: list[SimResult] = []
    for opp in opportunities:
        quote = quote_basket(
            opp,
            quote_offset_ticks=quote_offset_ticks,
            target_margin_per_basket=target_margin_per_basket,
            size=size,
        )
        results.append(SimResult(
            event_ticker=opp.event_ticker,
            direction=opp.direction,
            detected_at="",   # retained from source if ever threaded; not needed for summary math
            would_quote=quote is not None,
            expected_margin_usd=(quote.expected_margin_usd if quote else None),
            basket_cost_usd=(quote.expected_basket_cost_usd if quote else None),
        ))
    return results


def _summary_line(label: str, results: list[SimResult]) -> str:
    n = len(results)
    if n == 0:
        return f"{label:<40} (no opportunities)"
    quoted = [r for r in results if r.would_quote]
    quote_rate = 100.0 * len(quoted) / n
    if quoted:
        margins = sorted(r.expected_margin_usd for r in quoted)
        median = margins[len(margins) // 2]
        gross_if_all_fill = sum((m for m in margins), Decimal(0))
        return (
            f"{label:<40} n={n:>4}  quoted={len(quoted):>4} ({quote_rate:5.1f}%)  "
            f"median_margin=${median:.4f}  gross_if_all_fill=${gross_if_all_fill:.2f}"
        )
    return f"{label:<40} n={n:>4}  quoted={0:>4} (  0.0%)"


def _run_single(args, log_path: Path) -> int:
    opps = list(_load(log_path, args.direction))
    results = simulate(
        iter(opps),
        quote_offset_ticks=args.offset,
        target_margin_per_basket=Decimal(str(args.target_margin)),
        size=Decimal(str(args.size)),
    )
    label = f"offset={args.offset} target={args.target_margin}"
    print(_summary_line(label, results))
    if args.verbose:
        for r in results[:20]:
            marker = "Q" if r.would_quote else "."
            margin = f"${r.expected_margin_usd:.4f}" if r.expected_margin_usd else "—"
            print(f"  [{marker}] {r.event_ticker:<40} {r.direction:<12} margin={margin}")
    return 0


def _run_grid(args, log_path: Path) -> int:
    opps = list(_load(log_path, args.direction))
    offsets = [1, 2, 3, 5, 10]
    targets = [Decimal("0.01"), Decimal("0.02"), Decimal("0.03"),
               Decimal("0.05"), Decimal("0.10")]
    print(f"grid search over {len(offsets)}x{len(targets)} configs, {len(opps)} opportunities")
    print("-" * 110)
    for offset in offsets:
        for target in targets:
            results = simulate(
                iter(opps),
                quote_offset_ticks=offset,
                target_margin_per_basket=target,
                size=Decimal(str(args.size)),
            )
            print(_summary_line(f"offset={offset:<3} target=${target}", results))
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--log", default="data/opportunities.jsonl",
                   help="path to opportunities.jsonl (default: data/opportunities.jsonl)")
    p.add_argument("--offset", type=int, default=1, help="quote_offset_ticks")
    p.add_argument("--target-margin", type=str, default="0.03")
    p.add_argument("--size", type=str, default="1")
    p.add_argument("--direction", choices=["buy_basket", "buy_all_no"],
                   default=None, help="filter to one direction (default: both)")
    p.add_argument("--grid", action="store_true",
                   help="sweep (offset, target_margin) combinations instead of single config")
    p.add_argument("-v", "--verbose", action="store_true",
                   help="print first 20 rows of per-opportunity detail")
    args = p.parse_args(argv)

    log_path = Path(args.log)
    if not log_path.is_file():
        print(f"error: {log_path} not found", file=sys.stderr)
        return 2

    if args.grid:
        return _run_grid(args, log_path)
    return _run_single(args, log_path)


if __name__ == "__main__":
    raise SystemExit(main())
