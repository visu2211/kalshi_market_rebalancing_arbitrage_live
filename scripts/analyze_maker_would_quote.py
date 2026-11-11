"""Measure realized fill rate from observe_only data.

Each row in data/orders_maker.jsonl is a "if we were armed, we'd have
posted these resting orders now" event, captured with the mid/ask/bid of
every leg at that instant. The maker monitor logs one row per book tick
that clears the margin gate, so subsequent rows for the same event are
the forward evolution of its book — exactly what we need to simulate
"did our resting quote fill before we'd have canceled it?".

Fill condition per leg:
  side=yes, BUY at P_yes: fill iff a later row's yes_ask <= P_yes
                          (someone's ask crossed our bid)
  side=no,  BUY at P_no:  fill iff a later row's yes_bid >= 1 - P_no
                          (synthetic NO ask = 1 - yes_bid, dropped to P_no)

Cancel-replace condition per leg:
  |current_mid - mid_at_quote| >= requote_threshold_ticks * $0.01

Stale-cancel condition per leg:
  elapsed since quote >= max_resting_order_age_s

Outcomes per LEG (one per leg per would-quote row):
  FILLED            — quote price crossed within age window
  CANCELED_DRIFT    — mid drifted past the requote threshold first
  CANCELED_STALE    — age window expired with neither fill nor drift
  OPEN_AT_END       — no further snapshots for this event (data ran out)

Per-BASKET classification: a basket's outcome is the worst (safest) of
its legs. A basket is "FULL_FILL" iff EVERY leg filled; "PARTIAL_FILL"
iff at least one but not all filled (the flatten-loss scenario); "NO_FILL"
iff no leg filled.

Usage:
    .venv/bin/python scripts/analyze_maker_would_quote.py \\
        [--log data/orders_maker.jsonl] \\
        [--max-age-s 300] \\
        [--requote-threshold-ticks 2]
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Iterator

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

TICK = Decimal("0.01")


@dataclass
class LegQuoteRecord:
    ticker: str
    side: str                 # "yes" or "no"
    price: Decimal            # our resting BUY limit
    mid_at_quote: Decimal


@dataclass
class EventSnapshot:
    """One row = one book tick with every leg's mid/ask/bid."""

    at: datetime
    event_ticker: str
    direction: str
    expected_margin_usd: Decimal
    size: Decimal
    legs: list[LegQuoteRecord]
    # Per-leg current ask/bid at this snapshot time, for forward checks.
    ask: dict[str, Decimal] = field(default_factory=dict)
    bid: dict[str, Decimal] = field(default_factory=dict)
    mid: dict[str, Decimal] = field(default_factory=dict)


def _load(path: Path) -> list[EventSnapshot]:
    snapshots: list[EventSnapshot] = []
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            raw = json.loads(line)
        except json.JSONDecodeError:
            continue
        if raw.get("type") != "maker_would_quote":
            continue
        try:
            legs = [
                LegQuoteRecord(
                    ticker=l["ticker"], side=l["side"],
                    price=Decimal(l["price"]), mid_at_quote=Decimal(l["mid"]),
                )
                for l in raw["legs"]
            ]
            snap = EventSnapshot(
                at=datetime.fromisoformat(raw["at"]),
                event_ticker=raw["event_ticker"],
                direction=raw["direction"],
                expected_margin_usd=Decimal(raw["expected_margin_usd"]),
                size=Decimal(raw["size"]),
                legs=legs,
            )
            for l in raw["legs"]:
                snap.ask[l["ticker"]] = Decimal(l["ask_at_quote"])
                snap.bid[l["ticker"]] = Decimal(l["bid_at_quote"])
                snap.mid[l["ticker"]] = Decimal(l["mid"])
            snapshots.append(snap)
        except (KeyError, ValueError):
            continue
    return snapshots


def _fill_condition(leg: LegQuoteRecord, later_ask: Decimal, later_bid: Decimal) -> bool:
    """Would a resting BUY on this leg have filled given the later book?"""
    if leg.side == "yes":
        # BUY YES: fills when ask crosses down to our price
        return later_ask <= leg.price
    # BUY NO: synthetic NO ask = 1 - yes_bid; fills when that drops to our price
    return (Decimal(1) - later_bid) <= leg.price


def _drift_cancel(leg: LegQuoteRecord, later_mid: Decimal, requote_threshold_ticks: int) -> bool:
    diff = abs(later_mid - leg.mid_at_quote)
    threshold = Decimal(requote_threshold_ticks) * TICK
    return diff >= threshold


@dataclass
class LegOutcome:
    kind: str                            # FILLED | CANCELED_DRIFT | CANCELED_STALE | OPEN_AT_END
    elapsed_s: float                     # seconds between quote and resolution (or end of walk)


def classify_leg(
    quote_snap: EventSnapshot,
    leg: LegQuoteRecord,
    later_snaps: list[EventSnapshot],
    *,
    max_age_s: int,
    requote_threshold_ticks: int,
) -> LegOutcome:
    for later in later_snaps:
        elapsed = (later.at - quote_snap.at).total_seconds()
        if elapsed > max_age_s:
            return LegOutcome("CANCELED_STALE", max_age_s)
        if leg.ticker not in later.ask:
            # This later snapshot's event didn't carry this leg — ignore and
            # keep walking. Shouldn't happen for same-event groupings, but
            # skip defensively.
            continue
        later_ask = later.ask[leg.ticker]
        later_bid = later.bid[leg.ticker]
        later_mid = later.mid[leg.ticker]
        if _fill_condition(leg, later_ask, later_bid):
            return LegOutcome("FILLED", elapsed)
        if _drift_cancel(leg, later_mid, requote_threshold_ticks):
            return LegOutcome("CANCELED_DRIFT", elapsed)
    return LegOutcome("OPEN_AT_END", (later_snaps[-1].at - quote_snap.at).total_seconds() if later_snaps else 0.0)


@dataclass
class BasketOutcome:
    kind: str                            # FULL_FILL | PARTIAL_FILL | NO_FILL | OPEN_AT_END
    legs_filled: int
    legs_total: int
    expected_margin_usd: Decimal         # from the quote row — what we'd have earned if FULL_FILL


def classify_basket(leg_outcomes: list[LegOutcome], expected_margin_usd: Decimal) -> BasketOutcome:
    filled = sum(1 for lo in leg_outcomes if lo.kind == "FILLED")
    total = len(leg_outcomes)
    if filled == total and total > 0:
        return BasketOutcome("FULL_FILL", filled, total, expected_margin_usd)
    if filled == 0:
        # If any leg is OPEN_AT_END (data ran out), we can't say NO_FILL yet.
        if any(lo.kind == "OPEN_AT_END" for lo in leg_outcomes):
            return BasketOutcome("OPEN_AT_END", filled, total, expected_margin_usd)
        return BasketOutcome("NO_FILL", filled, total, expected_margin_usd)
    return BasketOutcome("PARTIAL_FILL", filled, total, expected_margin_usd)


def analyze(
    snapshots: list[EventSnapshot],
    *,
    max_age_s: int,
    requote_threshold_ticks: int,
) -> tuple[list[BasketOutcome], dict[str, int]]:
    by_event: dict[str, list[EventSnapshot]] = defaultdict(list)
    for s in snapshots:
        by_event[s.event_ticker].append(s)
    for group in by_event.values():
        group.sort(key=lambda s: s.at)

    basket_outcomes: list[BasketOutcome] = []
    leg_counts: dict[str, int] = defaultdict(int)

    for event_ticker, group in by_event.items():
        for i, quote_snap in enumerate(group):
            later = group[i + 1:]
            leg_outcomes = [
                classify_leg(
                    quote_snap, leg, later,
                    max_age_s=max_age_s,
                    requote_threshold_ticks=requote_threshold_ticks,
                )
                for leg in quote_snap.legs
            ]
            for lo in leg_outcomes:
                leg_counts[lo.kind] += 1
            basket_outcomes.append(classify_basket(leg_outcomes, quote_snap.expected_margin_usd))
    return basket_outcomes, dict(leg_counts)


def _report(basket_outcomes: list[BasketOutcome], leg_counts: dict[str, int], unflat_loss: Decimal) -> None:
    n = len(basket_outcomes)
    if n == 0:
        print("no would-quote rows to analyze")
        return

    by_kind: dict[str, int] = defaultdict(int)
    gross_full = Decimal(0)
    gross_partial_loss = Decimal(0)
    for bo in basket_outcomes:
        by_kind[bo.kind] += 1
        if bo.kind == "FULL_FILL":
            gross_full += bo.expected_margin_usd
        elif bo.kind == "PARTIAL_FILL":
            gross_partial_loss += unflat_loss

    conclusive = n - by_kind.get("OPEN_AT_END", 0)
    full_rate = (100.0 * by_kind["FULL_FILL"] / conclusive) if conclusive else 0.0
    partial_rate = (100.0 * by_kind["PARTIAL_FILL"] / conclusive) if conclusive else 0.0
    no_fill_rate = (100.0 * by_kind["NO_FILL"] / conclusive) if conclusive else 0.0
    net = gross_full - gross_partial_loss

    print(f"Total would-quote baskets:      {n}")
    print(f"Conclusive (not OPEN_AT_END):   {conclusive}")
    print(f"  FULL_FILL:    {by_kind['FULL_FILL']:>4} ({full_rate:5.1f}%)  gross=${gross_full:>8.2f}")
    print(f"  PARTIAL_FILL: {by_kind['PARTIAL_FILL']:>4} ({partial_rate:5.1f}%)  assumed_loss=${gross_partial_loss:>8.2f}  (at ${unflat_loss} per)")
    print(f"  NO_FILL:      {by_kind['NO_FILL']:>4} ({no_fill_rate:5.1f}%)")
    print(f"  OPEN_AT_END:  {by_kind['OPEN_AT_END']:>4}  (data ran out before resolution)")
    print()
    print(f"Net observed P&L:               ${net:>+8.2f}  (gross_full - partial_losses)")
    print()
    print("Per-leg outcomes (all legs across all would-quote rows):")
    total_legs = sum(leg_counts.values())
    for kind in ("FILLED", "CANCELED_DRIFT", "CANCELED_STALE", "OPEN_AT_END"):
        c = leg_counts.get(kind, 0)
        rate = (100.0 * c / total_legs) if total_legs else 0.0
        print(f"  {kind:<16} {c:>5}  ({rate:5.1f}%)")


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--log", default="data/orders_maker.jsonl")
    p.add_argument("--max-age-s", type=int, default=300)
    p.add_argument("--requote-threshold-ticks", type=int, default=2)
    p.add_argument("--unflat-loss-usd", type=str, default="0.35",
                   help="assumed loss per PARTIAL_FILL basket (flatten cost). "
                        "Decimal; 0.35 matches settings.yaml max_unwind_loss_usd default, "
                        "0.01 approximates holding-to-settlement at ~fair value.")
    args = p.parse_args(argv)

    path = Path(args.log)
    if not path.is_file():
        print(f"error: {path} not found (observe_only mode hasn't written anything yet)", file=sys.stderr)
        return 2
    snapshots = _load(path)
    if not snapshots:
        print(f"{path}: no maker_would_quote rows")
        return 0

    outcomes, leg_counts = analyze(
        snapshots,
        max_age_s=args.max_age_s,
        requote_threshold_ticks=args.requote_threshold_ticks,
    )
    _report(outcomes, leg_counts, Decimal(args.unflat_loss_usd))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
