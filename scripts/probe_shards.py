"""Probe which Kalshi series the configured execution account can place orders on.

A $0.01 non-marketable FOK buy on one leg of each currently-watched event
either gets `fill_or_kill_insufficient_resting_volume` (normal rejection —
account is onboarded on the shard, the order just couldn't fill) or
`insufficient_shard_balance` ("Exchange user not found" — account is NOT
onboarded on that shard, so every real order will fail the same way).

Usage: python scripts/probe_shards.py

Prints a YAML-ready list of blacklisted series. Does not modify settings.
"""
from __future__ import annotations

import asyncio
import json
import sys
import uuid
from collections import Counter
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from config.loader import load_settings  # noqa: E402
from kalshi.client import (  # noqa: E402
    KalshiClient,
    OrderAction,
    OrderRequest,
    OrderType,
    Side,
    TimeInForce,
)


async def _probe(client: KalshiClient, leg_ticker: str) -> str:
    req = OrderRequest(
        market_id=leg_ticker,
        side=Side.YES,
        action=OrderAction.BUY,
        order_type=OrderType.LIMIT,
        size=Decimal(1),
        limit_price=Decimal("0.01"),
        client_order_id=f"kmra-probe-{uuid.uuid4()}",
        time_in_force=TimeInForce.FILL_OR_KILL,
    )
    try:
        results = await client.place_orders_batch([req])
        raw = getattr(results[0], "raw", {}) or {}
        err = raw.get("error") or {}
        return err.get("code", "unknown")
    except Exception as exc:
        return f"exc:{type(exc).__name__}"


async def _one_leg(client: KalshiClient, event_ticker: str) -> str | None:
    try:
        raw = await client._signed_request("GET", f"/events/{event_ticker}")
    except Exception:
        return None
    markets = raw.get("markets", [])
    if not markets:
        return None
    return markets[0]["ticker"]


async def main() -> None:
    settings = load_settings()
    client = KalshiClient(
        base_url=settings.execution.demo_base_url,
        api_key_id=settings.execution.demo_api_key_id,
        private_key_path=settings.execution.demo_private_key_path,
    )
    await client.connect()
    try:
        status = json.loads((ROOT / "data" / "scanner_status.json").read_text())
        watched = status.get("watched_events", [])
        # One sample event per series (collapses redundant probes)
        by_series: dict[str, str] = {}
        for item in watched:
            event_ticker = item["event_ticker"]
            series = event_ticker.split("-")[0]
            by_series.setdefault(series, event_ticker)
        print(f"probing {len(by_series)} series...", file=sys.stderr)

        results: dict[str, str] = {}
        for series, sample_event in by_series.items():
            leg = await _one_leg(client, sample_event)
            if leg is None:
                results[series] = "no_markets"
                continue
            results[series] = await _probe(client, leg)
            print(f"  {series}: {results[series]}", file=sys.stderr)

        counter = Counter(results.values())
        print("\n--- summary ---", file=sys.stderr)
        for code, count in counter.most_common():
            print(f"  {code}: {count}", file=sys.stderr)

        blocked = sorted(s for s, code in results.items() if code == "insufficient_shard_balance")
        print("\nblacklisted_series:")
        for s in blocked:
            print(f"  - {s}")
    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())
