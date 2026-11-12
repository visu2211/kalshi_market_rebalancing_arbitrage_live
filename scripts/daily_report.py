"""One-shot daily report for the taker strategy.

Reads the standard log files and prints:
  - Detections in the last N hours, grouped by direction
  - Burst-persistence summary (reuses analyze_taker_persistence logic
    but scoped to the recent window)
  - Order submissions: filed, fully filled, partially filled, rejected
  - Realized P&L from basket orders (entry cost minus unwind credit),
    strictly from what's in data/orders.jsonl — not a projection
  - Risk state: breaker status, daily counters, open orders

Usage:
    .venv/bin/python scripts/daily_report.py [--hours 24]

No side effects, no network, no writes. Safe to run anytime.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


def _load_jsonl(path: Path) -> list[dict]:
    if not path.is_file():
        return []
    rows: list[dict] = []
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return rows


def _parse_ts(raw: str) -> datetime | None:
    try:
        return datetime.fromisoformat(raw)
    except (ValueError, TypeError):
        return None


def _within(ts: datetime | None, cutoff: datetime) -> bool:
    return ts is not None and ts >= cutoff


def _report_detections(rows: list[dict], cutoff: datetime) -> None:
    recent = [r for r in rows if _within(_parse_ts(r.get("detected_at", "")), cutoff)]
    print(f"Detections ({len(recent)} in window, {len(rows)} total):")
    if not recent:
        return
    by_dir = Counter(r.get("direction", "?") for r in recent)
    for d, n in by_dir.most_common():
        print(f"  {d:<20} {n}")
    edges = sorted(Decimal(str(r["edge_usd"])) for r in recent if "edge_usd" in r)
    if edges:
        print(f"  edge_usd range:  ${edges[0]:.4f} - ${edges[-1]:.4f}, median ${edges[len(edges)//2]:.4f}")


def _report_orders(rows: list[dict], cutoff: datetime) -> None:
    recent = [r for r in rows if _within(_parse_ts(r.get("placed_at", "")), cutoff)]
    basket_rows = [r for r in recent if r.get("kind") == "basket"]
    unwind_rows = [r for r in recent if r.get("kind") == "unwind"]
    print(f"\nOrders ({len(recent)} in window, {len(rows)} total):")
    print(f"  basket attempts: {len(basket_rows)}")
    print(f"  unwind attempts: {len(unwind_rows)}")

    if basket_rows:
        leg_statuses: Counter[str] = Counter()
        rejections: Counter[str] = Counter()
        for r in basket_rows:
            for leg in r.get("legs", []):
                status = leg.get("status", "?")
                leg_statuses[status] += 1
                if status == "rejected":
                    err = (leg.get("error") or "").split(":", 1)[0] or "unspecified"
                    rejections[err] += 1
        print("  leg outcomes:")
        for s, n in leg_statuses.most_common():
            print(f"    {s:<12} {n}")
        if rejections:
            print("  rejection reasons:")
            for reason, n in rejections.most_common():
                print(f"    {n:>3}  {reason}")


def _report_pnl(rows: list[dict], cutoff: datetime) -> None:
    recent = [r for r in rows if _within(_parse_ts(r.get("placed_at", "")), cutoff)]
    basket_rows = [r for r in recent if r.get("kind") == "basket"]
    if not basket_rows:
        print("\nRealized P&L (window): no basket attempts")
        return

    total_entry = Decimal(0)
    total_payout = Decimal(0)
    for r in basket_rows:
        try:
            total_entry += Decimal(str(r.get("actual_matched_entry_cost_usd", "0")))
            total_payout += Decimal(str(r.get("actual_locked_payout_usd", "0")))
        except (ValueError, TypeError):
            continue
    net = total_payout - total_entry
    print(f"\nRealized P&L (window, from orders.jsonl basket rows):")
    print(f"  entry cost:    ${total_entry:.4f}")
    print(f"  locked payout: ${total_payout:.4f}")
    print(f"  net:           ${net:+.4f}")
    print("  (does not account for unwind P&L — those are separate kind=unwind rows)")


def _report_alerts(rows: list[dict], cutoff: datetime) -> None:
    recent = [r for r in rows if _within(_parse_ts(r.get("occurred_at", "")), cutoff)]
    if not recent:
        print("\nAlerts: none in window")
        return
    by_type = Counter(r.get("type", "?") for r in recent)
    print(f"\nAlerts ({len(recent)} in window):")
    for t, n in by_type.most_common():
        print(f"  {n:>3}  {t}")


def _report_risk_state(path: Path) -> None:
    if not path.is_file():
        print("\nRisk state: file missing")
        return
    try:
        state = json.loads(path.read_text())
    except json.JSONDecodeError:
        print("\nRisk state: unreadable")
        return
    print("\nRisk state:")
    tripped = state.get("tripped")
    print(f"  breaker tripped: {tripped}")
    if tripped:
        print(f"  reason: {state.get('tripped_reason')}")
    print(f"  utc_date:            {state.get('utc_date')}")
    print(f"  daily_basket_attempts: {state.get('daily_basket_attempts')}")
    print(f"  daily_notional_usd:  {state.get('daily_notional_usd')}")
    print(f"  daily_realized_loss_usd: {state.get('daily_realized_loss_usd')}")
    open_orders = state.get("owned_open_order_ids") or []
    print(f"  open owned orders:   {len(open_orders)}")
    positions = state.get("believed_positions") or {}
    if positions:
        print(f"  believed positions:  {len(positions)} tickers held")


def _report_persistence(path: Path, cutoff: datetime) -> None:
    rows = _load_jsonl(path)
    if not rows:
        print("\nPersistence: no detections")
        return
    recent = [r for r in rows if _within(_parse_ts(r.get("detected_at", "")), cutoff)]
    if not recent:
        print("\nPersistence: no detections in window")
        return

    from collections import defaultdict
    by_key: dict[tuple[str, str], list[datetime]] = defaultdict(list)
    for r in recent:
        ts = _parse_ts(r.get("detected_at", ""))
        if ts is None:
            continue
        by_key[(r.get("event_ticker", ""), r.get("direction", ""))].append(ts)
    for lst in by_key.values():
        lst.sort()

    burst_lengths: list[int] = []
    gap = timedelta(seconds=2)
    for lst in by_key.values():
        if not lst:
            continue
        current = 1
        for i in range(1, len(lst)):
            if lst[i] - lst[i - 1] <= gap:
                current += 1
            else:
                burst_lengths.append(current)
                current = 1
        burst_lengths.append(current)

    if not burst_lengths:
        print("\nPersistence: no bursts")
        return
    n = len(burst_lengths)
    one_tick = sum(1 for L in burst_lengths if L == 1)
    cleared_3 = sum(1 for L in burst_lengths if L >= 3)
    print(f"\nPersistence (window, {n} bursts):")
    print(f"  1-tick flickers:            {one_tick} ({100.0 * one_tick / n:4.1f}%)")
    print(f"  bursts clearing stability=3: {cleared_3} ({100.0 * cleared_3 / n:4.1f}%)")


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--hours", type=int, default=24,
                   help="window to report on, counted back from now (default 24)")
    args = p.parse_args(argv)

    cutoff = datetime.now(timezone.utc) - timedelta(hours=args.hours)
    data = ROOT / "data"

    print(f"=== Daily taker report ===")
    print(f"window: last {args.hours}h (since {cutoff.isoformat()})")
    print()

    _report_detections(_load_jsonl(data / "opportunities.jsonl"), cutoff)
    _report_persistence(data / "opportunities.jsonl", cutoff)
    _report_orders(_load_jsonl(data / "orders.jsonl"), cutoff)
    _report_pnl(_load_jsonl(data / "orders.jsonl"), cutoff)
    _report_alerts(_load_jsonl(data / "alerts.jsonl"), cutoff)
    _report_risk_state(data / "risk_state.json")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
