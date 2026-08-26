"""Streamlit dashboard over the _live scanner — Stage 1's same-day
mutually-exclusive event universe, Stage 2's live per-leg order book
pricing for every individual market inside it, and a history tail of what
the actual production loop (scripts/run_scanner.py) has detected/executed.

Read-only for the first two sections: only calls public GET /events and
GET /markets/{ticker}/orderbook (the same calls scanner/filters.py and
scanner/orderbook_scan.py make). Never places orders itself —
execution/executor.py is untouched by this file.

Run:
    streamlit run ui/app.py

Data is fetched once per "Refresh" click (not on every widget interaction —
selecting a different event in the filter below re-renders from the same
snapshot in st.session_state, no extra API calls) since a full Stage 1+2
pass fetches an order book per leg and can take several seconds.

This dashboard is a separate process from scripts/run_scanner.py — it has
no live connection to that process's WS stream, so the two top sections
only ever show a REST snapshot as of your last Refresh click, not what a
running production scanner is seeing tick-by-tick. The History section
below closes that gap: it tails data/opportunities.jsonl and
data/orders.jsonl, which is where stream/monitor.py's WS loop actually
persists every opportunity it detects and every order it places — so it's
the authoritative record of the production process, once one has run.
"""

from __future__ import annotations

import asyncio
import json
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Optional

import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.loader import Settings, load_settings
from kalshi.client import ExchangeError, KalshiClient, Side
from scanner.basket import LegQuote, compute_opportunity
from scanner.fees import taker_fee
from scanner.filters import EventCandidate, filter_events, has_recent_activity

st.set_page_config(page_title="Kalshi Same-Day Rebalancing Arb", layout="wide")

# Bounds how many order books are in flight at once during a refresh.
# scanner/orderbook_scan.py fetches these fully sequentially by design (its
# docstring: "well within Kalshi's rate limits") because these are
# unauthenticated public calls, which get a tighter per-IP limit than
# signed requests. 20-way concurrency tripped that limit on some legs — 4
# is a compromise that's still noticeably faster than fully sequential
# without provoking it.
_MAX_CONCURRENT_ORDERBOOK_FETCHES = 4


@dataclass
class LegSnapshot:
    ticker: str
    event_ticker: str
    best_bid: Optional[Decimal]
    bid_size: Optional[Decimal]
    best_ask: Optional[Decimal]
    ask_size: Optional[Decimal]
    error: Optional[str] = None


async def _fetch_leg(client: KalshiClient, sem: asyncio.Semaphore, event_ticker: str, ticker: str, depth: int) -> LegSnapshot:
    async with sem:
        try:
            book = await client.get_orderbook(ticker, side=Side.YES, depth=depth)
        except ExchangeError as exc:
            return LegSnapshot(ticker, event_ticker, None, None, None, None, error=str(exc))
        return LegSnapshot(
            ticker=ticker,
            event_ticker=event_ticker,
            best_bid=book.best_bid.price if book.best_bid else None,
            bid_size=book.best_bid.size if book.best_bid else None,
            best_ask=book.best_ask.price if book.best_ask else None,
            ask_size=book.best_ask.size if book.best_ask else None,
        )


async def _run_scan(settings: Settings) -> tuple[int, list[EventCandidate], list[EventCandidate], list[LegSnapshot]]:
    async with KalshiClient(
        base_url=settings.kalshi.base_url,
        request_timeout_s=settings.kalshi.request_timeout_s,
        max_retries=settings.kalshi.max_retries,
    ) as client:
        raw_events = await client.get_all_events(status="open", with_nested_markets=True)
        candidates = filter_events(
            raw_events,
            min_outcome_legs=settings.scanner.min_outcome_legs,
            max_outcome_legs=settings.scanner.max_outcome_legs,
        )
        active_candidates = [c for c in candidates if has_recent_activity(c)]

        sem = asyncio.Semaphore(_MAX_CONCURRENT_ORDERBOOK_FETCHES)
        legs = await asyncio.gather(*(
            _fetch_leg(client, sem, c.event_ticker, ticker, settings.scanner.orderbook_depth)
            for c in active_candidates for ticker in c.leg_tickers
        ))
        return len(raw_events), candidates, active_candidates, list(legs)


def _safe_fee(price: Decimal) -> Decimal:
    """taker_fee() only accepts 0 < price < 1 — a leg quoted at exactly the
    boundary (a near-certain outcome) is priced with $0 fee here rather
    than crashing the whole dashboard render."""
    return taker_fee(price) if Decimal(0) < price < Decimal(1) else Decimal(0)


def _event_rows(candidates: list[EventCandidate], legs_by_event: dict[str, list[LegSnapshot]], settings: Settings) -> list[dict]:
    rows = []
    for c in candidates:
        leg_snaps = legs_by_event.get(c.event_ticker, [])
        quotes = [
            LegQuote(ticker=l.ticker, best_ask=l.best_ask, ask_size=l.ask_size, best_bid=l.best_bid, bid_size=l.bid_size)
            for l in leg_snaps if l.best_ask is not None and l.best_bid is not None
        ]
        missing = len(leg_snaps) - len(quotes)

        buy_cost = sum((q.best_ask + _safe_fee(q.best_ask) for q in quotes), start=Decimal(0)) if quotes else None
        opportunity = (
            compute_opportunity(
                c.event_ticker, quotes,
                settings.arb.buy_basket_max_cost_usd,
                settings.scanner.min_depth_contracts,
            )
            if quotes and missing == 0 else None
        )

        rows.append({
            "event_ticker": c.event_ticker,
            "title": c.title,
            "legs": len(c.leg_tickers),
            "missing_quotes": missing,
            "settles_utc": c.expected_settlement_time.strftime("%H:%M:%S"),
            "buy_cost_usd": float(buy_cost) if buy_cost is not None else None,
            "opportunity": opportunity.direction if opportunity else "-",
            "edge_usd": float(opportunity.edge_usd) if opportunity else None,
        })
    return rows


def _load_jsonl(path: Path) -> list[dict]:
    """Tolerant JSONL reader for data/opportunities.jsonl and
    data/orders.jsonl — an empty/missing file (production loop never run
    yet) returns [] rather than erroring, and a malformed trailing line
    (e.g. the writer was killed mid-line) is skipped rather than failing
    the whole read."""
    if not path.exists():
        return []
    records = []
    with path.open() as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return records


def _load_status(path: Path) -> Optional[dict]:
    """Tolerant reader for data/scanner_status.json — None if
    scripts/run_scanner.py has never run (file missing) or the file is
    unreadable (shouldn't happen: stream/monitor.py's _write_scanner_status
    writes atomically via temp-file-then-rename, but a reader is defensive
    here the same way _load_jsonl is)."""
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text())
    except json.JSONDecodeError:
        return None


def _opportunity_history_rows(records: list[dict]) -> list[dict]:
    return [
        {
            "detected_at": r.get("detected_at"),
            "event_ticker": r.get("event_ticker"),
            "direction": r.get("direction"),
            "legs": ", ".join(r.get("legs", [])),
            "basket_price_usd": r.get("basket_price_usd"),
            "edge_usd": r.get("edge_usd"),
            "max_size": r.get("max_size"),
        }
        for r in records
    ]


def _order_history_rows(records: list[dict]) -> list[dict]:
    return [
        {
            "placed_at": r.get("placed_at"),
            "kind": r.get("kind", "basket"),   # "basket" (original attempt) or "unwind" (offsetting flatten after a mismatch)
            "event_ticker": r.get("event_ticker"),
            "direction": r.get("direction"),
            "ticker": leg.get("ticker"),
            "action": leg.get("action"),
            "limit_price": leg.get("limit_price"),
            "size": leg.get("size"),
            "order_id": leg.get("order_id"),
            "status": leg.get("status"),
            "filled_size": leg.get("filled_size"),
            "error": leg.get("error"),
        }
        for r in records
        for leg in r.get("legs", [])
    ]


def main() -> None:
    settings = load_settings()

    st.title("Kalshi Same-Day Rebalancing Arb — Live View")
    st.caption(
        "Stage 1 (same-day mutually-exclusive events) + Stage 2 (live per-leg order books) "
        "from the kalshi_market_rebalancing_arbitrage_live scanner. Read-only — never places orders."
    )

    st.subheader("Production scanner status")
    status = _load_status(settings.logging.status_path)
    if status is None:
        st.warning(
            "scripts/run_scanner.py hasn't written a status file yet — either it's never been run, "
            f"or {settings.logging.status_path} doesn't exist. Nothing below reflects a live process."
        )
    else:
        updated_at = datetime.fromisoformat(status["updated_at"])
        age_s = (datetime.now(timezone.utc) - updated_at).total_seconds()
        rescan_interval_s = status.get("rescan_interval_s", settings.scanner.rescan_interval_s)
        # "Live" within a few heartbeats (_HEARTBEAT_MIN_INTERVAL_S=5 in
        # stream/monitor.py); "stale" up to one missed rescan cycle — beyond
        # that the process is either idle-with-no-candidates (which still
        # writes fresh status) or actually dead.
        if age_s < 20:
            badge = "🟢 Live"
        elif age_s < rescan_interval_s * 2:
            badge = "🟡 Stale"
        else:
            badge = "🔴 Not responding"

        if status.get("circuit_breaker_tripped"):
            st.error(
                f"🛑 Circuit breaker tripped — execution auto-disabled. {status.get('circuit_breaker_reason', '')}",
                icon="🛑",
            )

        position_mismatches = status.get("position_reconciliation_mismatches") or {}
        if position_mismatches:
            st.error(
                "⚠️ Position reconciliation mismatch — our records disagree with Kalshi's actual positions: "
                + ", ".join(f"{t} (believed {m['believed']}, actual {m['actual']})" for t, m in position_mismatches.items()),
                icon="⚠️",
            )

        sc1, sc2, sc3, sc4, sc5 = st.columns(5)
        sc1.metric("Status", badge)
        sc2.metric("Stage 1 candidates", status.get("stage1_candidates", "—"))
        sc3.metric("...with recent activity", status.get("active_candidates", "—"))
        sc4.metric("Watching via WS", f"{status.get('ws_shortlist_events', '—')} events / {status.get('ws_shortlist_legs', '—')} legs")
        sc5.metric("Open opportunities", status.get("open_opportunities", 0))

        st.caption(
            f"Phase: {status.get('phase', 'unknown')} · WS connected: {status.get('ws_connected')} · "
            f"Execution (demo orders) enabled: {status.get('execution_enabled')} · "
            f"Pending capital allocation: {status.get('pending_execution', 0)} "
            "(open opportunities not yet executed — awaiting capital or a ranking re-check, see execution/capital.py) · "
            f"Updated {age_s:.0f}s ago ({updated_at.strftime('%Y-%m-%d %H:%M:%S')} UTC)"
        )

        watched = status.get("watched_events", [])
        if watched:
            with st.expander(f"Events currently subscribed via WebSocket ({len(watched)})"):
                watched_df = pd.DataFrame(watched).sort_values("volume_24h", ascending=False)
                st.dataframe(
                    watched_df,
                    width="stretch",
                    hide_index=True,
                    column_config={
                        "event_ticker": "Event",
                        "title": "Title",
                        "legs": st.column_config.NumberColumn("Legs"),
                        "volume_24h": st.column_config.NumberColumn("24h volume"),
                    },
                )

    auto_refresh_live_views = st.checkbox(
        "Auto-refresh scanner status & history every 5s", value=False,
        help="Both read local files only (no Kalshi API calls), so this is safe to leave on.",
    )

    st.divider()
    st.subheader("On-demand REST snapshot")
    col_refresh, col_status = st.columns([1, 5])
    with col_refresh:
        refresh = st.button("🔄 Refresh", type="primary")

    if refresh or "last_scan" not in st.session_state:
        with st.spinner("Fetching open events + order books from Kalshi..."):
            try:
                total_open, candidates, active_candidates, legs = asyncio.run(_run_scan(settings))
            except ExchangeError as exc:
                st.error(f"Kalshi request failed: {exc}")
                st.stop()
            st.session_state.last_scan = (total_open, candidates, active_candidates, legs)
            st.session_state.last_refreshed = datetime.now(timezone.utc)

    with col_status:
        st.caption(f"Last refreshed: {st.session_state.last_refreshed.strftime('%Y-%m-%d %H:%M:%S')} UTC")

    total_open, candidates, active_candidates, legs = st.session_state.last_scan
    legs_by_event: dict[str, list[LegSnapshot]] = {}
    for leg in legs:
        legs_by_event.setdefault(leg.event_ticker, []).append(leg)

    rows = _event_rows(active_candidates, legs_by_event, settings)
    opportunities = [r for r in rows if r["opportunity"] != "-"]

    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("Open events (Kalshi)", total_open)
    m2.metric("Stage 1 — same-day mutually exclusive", len(candidates))
    m3.metric("...with recent trading activity", len(active_candidates))
    m4.metric("Individual markets watched", sum(len(c.leg_tickers) for c in active_candidates))
    m5.metric("Arb opportunities right now", len(opportunities))

    st.subheader("Stage 1 — events (with recent trading activity)")
    st.caption(
        f"{len(candidates)} events settle by midnight UTC tonight; {len(candidates) - len(active_candidates)} "
        "of those are skipped here (no order book fetched) for having zero 24h volume on at least one leg — "
        "the same ghost-spread gate scanner/orderbook_scan.py's Stage 2 applies before ever pricing a basket."
    )
    if rows:
        df = pd.DataFrame(rows).sort_values("buy_cost_usd", na_position="last")
        st.dataframe(
            df,
            width="stretch",
            hide_index=True,
            column_config={
                "event_ticker": "Event",
                "title": "Title",
                "legs": st.column_config.NumberColumn("Legs"),
                "missing_quotes": st.column_config.NumberColumn("Missing quotes"),
                "settles_utc": "Settles (UTC)",
                "buy_cost_usd": st.column_config.ProgressColumn(
                    "Buy basket cost", min_value=0.0, max_value=1.10, format="$%.3f",
                ),
                "opportunity": "Opportunity",
                "edge_usd": st.column_config.NumberColumn("Edge", format="$%.3f"),
            },
        )
        st.caption(
            f"Flags below ${settings.arb.buy_basket_max_cost_usd} · "
            f"needs ≥{settings.scanner.min_depth_contracts} contracts on every leg's thinnest side. "
            "Buy-basket only — see README.md."
        )
    else:
        st.info("No open events currently settle by midnight UTC tonight with a mutually-exclusive, 2-6 leg structure.")

    st.subheader("Individual markets (legs)")
    if active_candidates:
        event_options = {f"{c.event_ticker} — {c.title}": c.event_ticker for c in active_candidates}
        chosen_label = st.selectbox("Filter by event", ["All events"] + list(event_options.keys()))
        chosen_ticker = event_options.get(chosen_label)

        leg_rows = []
        for c in active_candidates:
            if chosen_ticker and c.event_ticker != chosen_ticker:
                continue
            for leg in legs_by_event.get(c.event_ticker, []):
                leg_rows.append({
                    "event_ticker": leg.event_ticker,
                    "ticker": leg.ticker,
                    "best_bid": float(leg.best_bid) if leg.best_bid is not None else None,
                    "bid_size": float(leg.bid_size) if leg.bid_size is not None else None,
                    "best_ask": float(leg.best_ask) if leg.best_ask is not None else None,
                    "ask_size": float(leg.ask_size) if leg.ask_size is not None else None,
                    "error": leg.error or "",
                })

        st.dataframe(
            pd.DataFrame(leg_rows),
            width="stretch",
            hide_index=True,
            column_config={
                "event_ticker": "Event",
                "ticker": "Market",
                "best_bid": st.column_config.NumberColumn("Best bid", format="$%.2f"),
                "bid_size": st.column_config.NumberColumn("Bid size"),
                "best_ask": st.column_config.NumberColumn("Best ask", format="$%.2f"),
                "ask_size": st.column_config.NumberColumn("Ask size"),
                "error": "Error",
            },
        )
    else:
        st.info("No individual markets to show — no events passed Stage 1.")

    st.divider()
    st.subheader("History — from the running production scanner")
    st.caption(
        "Tailed from data/opportunities.jsonl and data/orders.jsonl, written by "
        "stream/monitor.py's WS loop (scripts/run_scanner.py) — independent of the "
        "Refresh button above, and empty until that process has run at least once."
    )

    opp_records = _load_jsonl(settings.logging.opportunities_log_path)
    order_records = _load_jsonl(settings.execution.orders_log_path)
    total_order_legs = sum(len(r.get("legs", [])) for r in order_records)

    hist_col1, hist_col2 = st.columns(2)
    with hist_col1:
        st.markdown(f"**Opportunities detected** ({len(opp_records)})")
        if opp_records:
            df = pd.DataFrame(_opportunity_history_rows(opp_records)).sort_values("detected_at", ascending=False)
            st.dataframe(
                df,
                width="stretch",
                hide_index=True,
                column_config={
                    "detected_at": "Detected (UTC)",
                    "event_ticker": "Event",
                    "direction": "Direction",
                    "legs": "Legs",
                    "basket_price_usd": st.column_config.NumberColumn("Basket price", format="$%.3f"),
                    "edge_usd": st.column_config.NumberColumn("Edge", format="$%.3f"),
                    "max_size": st.column_config.NumberColumn("Max size"),
                },
            )
        else:
            st.info(f"No opportunities recorded yet at {settings.logging.opportunities_log_path}.")

    with hist_col2:
        st.markdown(f"**Orders placed (demo)** ({len(order_records)} baskets / {total_order_legs} legs)")
        if order_records:
            df = pd.DataFrame(_order_history_rows(order_records)).sort_values("placed_at", ascending=False)
            st.dataframe(
                df,
                width="stretch",
                hide_index=True,
                column_config={
                    "placed_at": "Placed (UTC)",
                    "kind": "Kind",
                    "event_ticker": "Event",
                    "direction": "Direction",
                    "ticker": "Market",
                    "action": "Action",
                    "limit_price": st.column_config.NumberColumn("Limit price", format="$%.2f"),
                    "size": st.column_config.NumberColumn("Size"),
                    "order_id": "Order ID",
                    "status": "Status",
                    "filled_size": st.column_config.NumberColumn("Filled"),
                    "error": "Error",
                },
            )
        else:
            st.info(f"No orders recorded yet at {settings.execution.orders_log_path}.")

    if auto_refresh_live_views:
        time.sleep(5)
        st.rerun()


if __name__ == "__main__":
    main()
