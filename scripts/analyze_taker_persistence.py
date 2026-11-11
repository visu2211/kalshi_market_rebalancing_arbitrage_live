"""Measure taker edge persistence from data/opportunities.jsonl.

Every row is one detected edge at one WS tick. Consecutive rows for the
same event_ticker within `burst_gap_s` seconds are a single "burst" —
one underlying opportunity the scanner re-detected on subsequent ticks.
A burst's LENGTH (number of ticks) tells us whether the edge was a
one-tick flicker or a persistent opportunity the market left on the
table for seconds.

Key questions this answers:
  - Fraction of detections that are 1-tick flickers (bogus, FOK would
    fail regardless because the edge was gone by submission)
  - Median burst duration (how many ticks of persistence is realistic)
  - Current min_edge_stability_ticks setting (3) — is it close to the
    median burst, well below, or well above?
  - For bursts where the edge SHRANK across the burst: what fraction of
    burst-time was shrinking vs. stable? Shrinking-first bursts are
    where we'd fire and the edge dies by the time our order lands.

Usage:
    .venv/bin/python scripts/analyze_taker_persistence.py \\
        [--log data/opportunities.jsonl] \\
        [--burst-gap-s 2]
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from statistics import median

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


@dataclass
class Detection:
    at: datetime
    event_ticker: str
    direction: str
    edge_usd: Decimal
    basket_price_usd: Decimal


def _load(path: Path) -> list[Detection]:
    out: list[Detection] = []
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            raw = json.loads(line)
            out.append(Detection(
                at=datetime.fromisoformat(raw["detected_at"]),
                event_ticker=raw["event_ticker"],
                direction=raw["direction"],
                edge_usd=Decimal(str(raw["edge_usd"])),
                basket_price_usd=Decimal(str(raw["basket_price_usd"])),
            ))
        except (KeyError, ValueError, json.JSONDecodeError):
            continue
    return out


@dataclass
class Burst:
    event_ticker: str
    direction: str
    detections: list[Detection]        # time-ordered

    @property
    def length(self) -> int:
        return len(self.detections)

    @property
    def duration_s(self) -> float:
        if self.length < 2:
            return 0.0
        return (self.detections[-1].at - self.detections[0].at).total_seconds()

    @property
    def edge_first(self) -> Decimal:
        return self.detections[0].edge_usd

    @property
    def edge_last(self) -> Decimal:
        return self.detections[-1].edge_usd

    @property
    def edge_change(self) -> Decimal:
        return self.edge_last - self.edge_first

    @property
    def edge_median(self) -> Decimal:
        edges = sorted(d.edge_usd for d in self.detections)
        return edges[len(edges) // 2]


def _group_bursts(detections: list[Detection], burst_gap_s: float) -> list[Burst]:
    # Sort globally by event + direction + time so consecutive = same burst
    by_key: dict[tuple[str, str], list[Detection]] = defaultdict(list)
    for d in detections:
        by_key[(d.event_ticker, d.direction)].append(d)
    for lst in by_key.values():
        lst.sort(key=lambda d: d.at)

    bursts: list[Burst] = []
    for (ev, direction), lst in by_key.items():
        current: list[Detection] = []
        for d in lst:
            if current and (d.at - current[-1].at).total_seconds() > burst_gap_s:
                bursts.append(Burst(ev, direction, current))
                current = []
            current.append(d)
        if current:
            bursts.append(Burst(ev, direction, current))
    return bursts


def _report(bursts: list[Burst]) -> None:
    if not bursts:
        print("no bursts")
        return
    lengths = sorted(b.length for b in bursts)
    durations = sorted(b.duration_s for b in bursts if b.length > 1)
    one_tick = sum(1 for b in bursts if b.length == 1)
    n = len(bursts)

    print(f"Total bursts:                   {n}")
    print(f"Total detections:               {sum(lengths)}")
    print()
    print(f"Burst length distribution (ticks per burst):")
    for threshold in (1, 2, 3, 5, 10, 20, 50):
        at_or_above = sum(1 for L in lengths if L >= threshold)
        print(f"  >= {threshold:>3} ticks:  {at_or_above:>5} ({100.0 * at_or_above / n:5.1f}%)")
    print()
    print(f"One-tick flickers (bogus edges): {one_tick} ({100.0 * one_tick / n:5.1f}%)")
    print(f"Median burst length:             {lengths[n // 2]} ticks")
    if durations:
        print(f"Median multi-tick burst duration: {durations[len(durations) // 2]:.2f}s")
        print(f"P90 multi-tick burst duration:    {durations[int(len(durations) * 0.9)]:.2f}s")
    print()

    # Edge drift within a burst — does the edge persist, grow, or shrink?
    multi = [b for b in bursts if b.length >= 2]
    if multi:
        shrinking = sum(1 for b in multi if b.edge_change < 0)
        growing = sum(1 for b in multi if b.edge_change > 0)
        stable = len(multi) - shrinking - growing
        avg_change = sum((b.edge_change for b in multi), Decimal(0)) / len(multi)
        print(f"Edge drift across burst (over {len(multi)} multi-tick bursts):")
        print(f"  Shrinking (edge_last < edge_first): {shrinking} ({100.0 * shrinking / len(multi):5.1f}%)")
        print(f"  Growing (edge_last > edge_first):   {growing} ({100.0 * growing / len(multi):5.1f}%)")
        print(f"  Stable:                              {stable} ({100.0 * stable / len(multi):5.1f}%)")
        print(f"  Mean edge change first->last:       ${avg_change:+.4f}")
    print()

    # Stability-window retrospective: for each burst, would min_edge_stability_ticks
    # have passed? i.e. did the burst sustain enough ticks for the filter to clear?
    print(f"Stability-window retrospective — fraction of bursts whose length clears each threshold:")
    for n_ticks in (1, 2, 3, 5, 10):
        clears = sum(1 for b in bursts if b.length >= n_ticks)
        print(f"  min_edge_stability_ticks={n_ticks}: {clears} ({100.0 * clears / n:5.1f}%) bursts would have cleared")


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--log", default="data/opportunities.jsonl")
    p.add_argument("--burst-gap-s", type=float, default=2.0,
                   help="max seconds between consecutive detections to still call them one burst")
    args = p.parse_args(argv)

    path = Path(args.log)
    if not path.is_file():
        print(f"error: {path} not found", file=sys.stderr)
        return 2
    detections = _load(path)
    if not detections:
        print("no detections to analyze")
        return 0
    bursts = _group_bursts(detections, args.burst_gap_s)
    _report(bursts)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
