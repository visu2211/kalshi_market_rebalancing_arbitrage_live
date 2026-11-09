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
            blacklisted_series=set(settings.scanner.blacklisted_series),
        )
        active_candidates = [c for c in candidates if has_recent_activity(c)]

        sem = asyncio.Semaphore(_MAX_CONCURRENT_ORDERBOOK_FETCHES)
        legs = await asyncio.gather(*(
            _fetch_leg(client, sem, c.event_ticker, ticker, settings.scanner.orderbook_depth)
            for c in active_candidates for ticker in c.leg_tickers
        ))
        return len(raw_events), candidates, active_candidates, list(legs)


def _event_rows(candidates: list[EventCandidate], legs_by_event: dict[str, list[LegSnapshot]], settings: Settings) -> list[dict]:
    rows = []
    for c in candidates:
        leg_snaps = legs_by_event.get(c.event_ticker, [])
        quotes = [
            LegQuote(ticker=l.ticker, best_ask=l.best_ask, ask_size=l.ask_size, best_bid=l.best_bid, bid_size=l.bid_size)
            for l in leg_snaps if l.best_ask is not None and l.best_bid is not None
        ]
        missing = len(leg_snaps) - len(quotes)

        configured_size = getattr(settings.execution, "max_order_size_contracts", Decimal(1))
        execution_size = min(configured_size, min((q.ask_size for q in quotes), default=Decimal(1)))
        buy_cost = (
            sum(
                (q.best_ask + taker_fee(q.best_ask, execution_size, c.fee_multiplier) / execution_size for q in quotes),
                start=Decimal(0),
            )
            if quotes else None
        )
        opportunity = (
            compute_opportunity(
                c.event_ticker, quotes,
                settings.arb.buy_basket_max_cost_usd,
                settings.scanner.min_depth_contracts,
                contract_count=execution_size,
                fee_multiplier=c.fee_multiplier,
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
    rows = []
    for r in records:
        quotes = r.get("leg_quotes") or []
        # Thinnest leg binds the whole basket — it is the size actually
        # obtainable, and the one most likely to be a stale or token quote.
        thinnest = min((Decimal(q["ask_size"]) for q in quotes), default=None)
        widest = max((Decimal(q["spread"]) for q in quotes), default=None)
        rows.append({
            "detected_at": r.get("detected_at"),
            "event_ticker": r.get("event_ticker"),
            "direction": r.get("direction"),
            "leg_count": len(r.get("legs", [])),
            "sum_ask_usd": r.get("sum_ask_usd"),
            "fees_usd": r.get("fees_usd"),
            "basket_price_usd": r.get("basket_price_usd"),
            "edge_usd": r.get("edge_usd"),
            "max_size": r.get("max_size"),
            "thinnest_leg_size": float(thinnest) if thinnest is not None else None,
            "widest_leg_spread": float(widest) if widest is not None else None,
        })
    return rows


def _opportunity_leg_rows(record: dict) -> list[dict]:
    """Per-leg quotes for one detected basket, in submission order.

    A basket price alone cannot show which leg carried the edge, whether a leg
    was barely two-sided, or how thin the binding leg was — and those decide
    whether the basket was genuinely executable.
    """
    return [
        {
            "ticker": q.get("ticker"),
            "ask": float(q["ask"]) if q.get("ask") is not None else None,
            "ask_size": float(q["ask_size"]) if q.get("ask_size") is not None else None,
            "bid": float(q["bid"]) if q.get("bid") is not None else None,
            "bid_size": float(q["bid_size"]) if q.get("bid_size") is not None else None,
            "spread": float(q["spread"]) if q.get("spread") is not None else None,
            "market_url": _kalshi_market_url(q.get("ticker")),
        }
        for q in (record.get("leg_quotes") or [])
    ]


def _order_history_rows(records: list[dict]) -> list[dict]:
    return [
        {
            "placed_at": r.get("placed_at"),
            "kind": r.get("kind", "basket"),   # "basket" (original attempt) or "unwind" (offsetting flatten after a mismatch)
            "execution_stage": leg.get("execution_stage", "entry" if r.get("kind", "basket") == "basket" else "unwind"),
            "target": r.get("target", "demo"),   # "demo" or "prod" (REAL MONEY) — absent on rows logged before this field existed
            "event_ticker": r.get("event_ticker"),
            "direction": r.get("direction"),
            "ticker": leg.get("ticker"),
            "action": leg.get("action"),
            "limit_price": leg.get("limit_price"),
            "size": leg.get("size"),
            "order_id": leg.get("order_id"),
            "status": leg.get("status"),
            "filled_size": leg.get("filled_size"),
            "average_fill_price": leg.get("average_fill_price"),
            "average_fee_paid": leg.get("average_fee_paid"),
            "market_url": _kalshi_market_url(leg.get("ticker")),
            "error": leg.get("error"),
        }
        for r in records
        for leg in r.get("legs", [])
    ]


def _kalshi_market_url(ticker: Optional[str]) -> Optional[str]:
    """Stable Kalshi web redirect for a market ticker."""
    return f"https://kalshi.com/markets_by_ticker/{ticker.lower()}" if ticker else None


def _filled_trade_rows(records: list[dict]) -> list[dict]:
    """One row per basket order attempt (kind == "basket") that actually got
    at least one contract filled on Kalshi — i.e. a real trade, not just a
    placement attempt. Unwind legs (kind == "unwind") are excluded: they're
    the safety net flattening an uneven fill, not the original arbitrage
    trade, and their P&L is already netted into what the basket legs above
    them actually captured.

    filled_size is the MINIMUM fill across the basket's legs, not the sum
    or any single leg's fill — a basket only locks in its edge on the
    contracts where every leg filled; if legs filled unevenly the excess on
    the better-filled leg(s) is what triggers the unwind logged separately.
    expected_profit_usd is that filled_size times the per-contract edge
    recorded at placement time (edge_usd_per_contract), i.e. what this
    trade is expected to net before Kalshi's settlement, not counting any
    unwind cost if the fill was uneven.
    """
    rows = []
    for r in records:
        if r.get("kind", "basket") != "basket":
            continue
        legs = r.get("legs", [])
        if not legs:
            continue
        filled_sizes = [Decimal(leg["filled_size"]) for leg in legs if leg.get("filled_size") is not None]
        if not filled_sizes:
            continue
        filled_size = min(filled_sizes)
        if filled_size <= 0:
            continue
        edge_per_contract = Decimal(r["edge_usd_per_contract"]) if r.get("edge_usd_per_contract") is not None else None
        actual_expected = Decimal(r["actual_expected_profit_usd"]) if r.get("actual_expected_profit_usd") is not None else None
        rows.append({
            "placed_at": r.get("placed_at"),
            "target": r.get("target", "demo"),
            "event_ticker": r.get("event_ticker"),
            "direction": r.get("direction"),
            "legs": len(legs),
            "filled_size": float(filled_size),
            "edge_usd_per_contract": float(edge_per_contract) if edge_per_contract is not None else None,
            "expected_profit_usd": float(actual_expected) if actual_expected is not None else (float(edge_per_contract * filled_size) if edge_per_contract is not None else None),
            "economics_source": "Actual fills + fees" if actual_expected is not None else "Detected estimate",
            "fill_quality": "Matched" if len(set(filled_sizes)) == 1 else "Partial / unwound",
            "order_ids": ", ".join(str(leg["order_id"]) for leg in legs if leg.get("order_id")),
            "kalshi_market": _kalshi_market_url(legs[0].get("ticker")),
        })
    return rows


def main() -> None:
    settings = load_settings()

    st.title("Kalshi Arbitrage Control Room")
    st.caption("Read-only · execution happens exclusively in `scripts/run_scanner.py`")

    header_left, header_mid, header_right = st.columns([2, 1, 1])
    with header_left:
        auto_refresh_live_views = st.toggle(
            "Live refresh (5s)", value=False,
            help="Refreshes local status and history files only; it does not call Kalshi or place orders.",
        )
    with header_mid:
        st.link_button("Open Kalshi portfolio ↗", "https://kalshi.com/portfolio", width="stretch")
    with header_right:
        if st.button("Refresh dashboard", width="stretch"):
            st.rerun()

    status = _load_status(settings.logging.status_path)
    with st.container(border=True):
        if status is None:
            st.markdown("#### 🔴 Offline")
            st.caption("Scanner status is unavailable — execution should be treated as offline.")
            age_s = None
        else:
            updated_at = datetime.fromisoformat(status["updated_at"])
            age_s = (datetime.now(timezone.utc) - updated_at).total_seconds()
            rescan_interval_s = status.get("rescan_interval_s", settings.scanner.rescan_interval_s)
            if age_s < 20:
                badge = "🟢 Live"
            elif age_s < rescan_interval_s * 2:
                badge = "🟡 Stale"
            else:
                badge = "🔴 Offline"

            risk = status.get("risk") or {}
            # getattr keeps Streamlit hot reload compatible with a process that
            # imported the older ExecutionSettings shape before `target` existed.
            execution_target = status.get("execution_target") or getattr(settings.execution, "target", "demo")
            target_label = "PROD · REAL MONEY" if execution_target == "prod" else "Demo"

            sc1, sc2, sc3, sc4, sc5, sc6 = st.columns(6)
            sc1.metric("Status", badge, help=f"Updated {age_s:.0f}s ago")
            sc2.metric("Account", target_label)
            sc3.metric("WS events", status.get("ws_shortlist_events", "—"))
            sc4.metric("Open signals", status.get("open_opportunities", 0))
            sc5.metric("Gross exposure", f"${Decimal(str(risk.get('gross_exposure_usd', 0))):.2f}")
            sc6.metric("Daily loss", f"${Decimal(str(risk.get('daily_realized_loss_usd', 0))):.2f}")

            st.caption(
                f"{status.get('phase', 'unknown')} · "
                f"WS {'connected' if status.get('ws_connected') else 'disconnected'} · "
                f"execution {'armed' if status.get('execution_enabled') else 'disabled'} · "
                f"attempts {risk.get('daily_basket_attempts', 0)}/{risk.get('max_baskets_per_day', '—')} today"
            )

            if status.get("circuit_breaker_tripped"):
                st.error(f"Circuit breaker tripped: {status.get('circuit_breaker_reason', 'No reason recorded')}", icon="🛑")

            position_mismatches = status.get("position_reconciliation_mismatches") or {}
            if position_mismatches:
                st.error(
                    "Position reconciliation mismatch: "
                    + ", ".join(f"{t} (believed {m['believed']}, actual {m['actual']})" for t, m in position_mismatches.items()),
                    icon="⚠️",
                )

            with st.expander("Risk budget detail"):
                st.caption(
                    f"Daily notional reserve ${Decimal(str(risk.get('daily_notional_usd', 0))):.2f} · "
                    f"attempts {risk.get('daily_basket_attempts', 0)}/{risk.get('max_baskets_per_day', '—')} daily, "
                    f"{risk.get('session_basket_attempts', 0)}/{risk.get('max_baskets_per_session', '—')} session"
                )

    opp_records = _load_jsonl(settings.logging.opportunities_log_path)
    order_records = _load_jsonl(settings.execution.orders_log_path)
    total_order_legs = sum(len(r.get("legs", [])) for r in order_records)
    filled_rows = _filled_trade_rows(order_records)

    st.write("")
    trades_tab, signals_tab, snapshot_tab, diagnostics_tab = st.tabs(
        ["📈  Executed trades", "🔍  Detected opportunities", "🧪  Market snapshot", "🛠️  Diagnostics"]
    )

    with trades_tab:
        st.subheader("Trades sent to Kalshi")
        st.caption(
            "Expected profit applies only to the quantity matched across every leg — "
            "partial/unwound rows can still realize an unwind loss.",
            help=(
                "Click Market to open the contract on Kalshi; use the order ID to locate the "
                "exact private order in your portfolio history."
            ),
        )
        if filled_rows:
            prod_rows = [row for row in filled_rows if row["target"] == "prod"]
            with st.container(border=True):
                c1, c2, c3, c4 = st.columns(4)
                c1.metric("Completed baskets", len(filled_rows))
                c2.metric("Real-money baskets", len(prod_rows))
                c3.metric("Matched contracts", f"{sum(row['filled_size'] for row in filled_rows):.2f}")
                c4.metric("Expected settlement profit", f"${sum(row['expected_profit_usd'] or 0 for row in filled_rows):.2f}")
            st.dataframe(
                pd.DataFrame(filled_rows).sort_values("placed_at", ascending=False),
                width="stretch", hide_index=True,
                column_config={
                    "placed_at": "Placed (UTC)", "target": "Account", "event_ticker": "Event",
                    "direction": None, "legs": st.column_config.NumberColumn("Legs"),
                    "filled_size": st.column_config.NumberColumn("Matched size", format="%.2f"),
                    "edge_usd_per_contract": st.column_config.NumberColumn("Expected edge / contract", format="$%.4f"),
                    "expected_profit_usd": st.column_config.NumberColumn("Expected profit", format="$%.4f"),
                    "economics_source": "Profit basis",
                    "fill_quality": "Execution", "order_ids": "Kalshi order IDs",
                    "kalshi_market": st.column_config.LinkColumn("Market ↗", display_text="Open on Kalshi"),
                },
            )
        else:
            st.info("No complete basket has filled on every leg yet.")

        with st.expander(f"All order attempts ({len(order_records)} batches / {total_order_legs} legs)"):
            if order_records:
                st.dataframe(
                    pd.DataFrame(_order_history_rows(order_records)).sort_values("placed_at", ascending=False),
                    width="stretch", hide_index=True,
                    column_config={
                        "placed_at": "Placed (UTC)", "kind": "Type", "execution_stage": "Stage", "target": "Account",
                        "event_ticker": "Event", "direction": None, "ticker": "Market ticker",
                        "action": "Action", "limit_price": st.column_config.NumberColumn("Limit", format="$%.4f"),
                        "size": st.column_config.NumberColumn("Requested"), "order_id": "Order ID",
                        "status": "Status", "filled_size": st.column_config.NumberColumn("Filled"),
                        "average_fill_price": st.column_config.NumberColumn("Avg fill", format="$%.4f"),
                        "average_fee_paid": st.column_config.NumberColumn("Fee / contract", format="$%.4f"),
                        "market_url": st.column_config.LinkColumn("Market ↗", display_text="Open"), "error": "Error",
                    },
                )
            else:
                st.info("No orders have been submitted.")

    with signals_tab:
        st.subheader(f"Detected opportunities ({len(opp_records)})")
        st.caption("Signals are not trades. They may disappear before execution or be rejected by risk controls.")
        if opp_records:
            df = pd.DataFrame(_opportunity_history_rows(opp_records)).sort_values("detected_at", ascending=False)
            st.dataframe(
                df, width="stretch", hide_index=True,
                column_config={
                    "detected_at": "Detected (UTC)", "event_ticker": "Event", "direction": None,
                    "leg_count": st.column_config.NumberColumn("Legs", format="%d"),
                    "sum_ask_usd": st.column_config.NumberColumn("Sum of asks", format="$%.3f"),
                    "fees_usd": st.column_config.NumberColumn("Fees", format="$%.3f"),
                    "basket_price_usd": st.column_config.NumberColumn("Basket price", format="$%.3f"),
                    "edge_usd": st.column_config.NumberColumn("Expected edge", format="$%.3f"),
                    "max_size": st.column_config.NumberColumn("Displayed max size"),
                    "thinnest_leg_size": st.column_config.NumberColumn("Thinnest leg", format="%.2f"),
                    "widest_leg_spread": st.column_config.NumberColumn("Widest spread", format="$%.3f"),
                },
            )

            st.write("")
            st.markdown("##### Price per leg")
            labelled = sorted(
                enumerate(opp_records),
                key=lambda pair: pair[1].get("detected_at") or "",
                reverse=True,
            )
            options = [
                f"{r.get('detected_at', '?')[11:19]} · {r.get('event_ticker', '?')} · "
                f"edge ${Decimal(str(r.get('edge_usd', 0))):.3f}"
                for _, r in labelled
            ]
            chosen = st.selectbox(
                "Detection", options, index=0, key="opp_leg_pick",
                help="The basket price is the sum of every leg's ask plus Kalshi's fee on each.",
            )
            selected = labelled[options.index(chosen)][1]
            leg_rows = _opportunity_leg_rows(selected)
            with st.container(border=True):
                if leg_rows:
                    st.dataframe(
                        pd.DataFrame(leg_rows), width="stretch", hide_index=True,
                        column_config={
                            "ticker": "Leg",
                            "ask": st.column_config.NumberColumn("Ask (you pay)", format="$%.4f"),
                            "ask_size": st.column_config.NumberColumn("Ask size", format="%.2f"),
                            "bid": st.column_config.NumberColumn("Bid (exit)", format="$%.4f"),
                            "bid_size": st.column_config.NumberColumn("Bid size", format="%.2f"),
                            "spread": st.column_config.NumberColumn("Spread", format="$%.4f"),
                            "market_url": st.column_config.LinkColumn("Market", display_text="open"),
                        },
                    )
                    sum_ask = sum(Decimal(str(q["ask"])) for q in leg_rows)
                    fees = Decimal(str(selected.get("fees_usd", 0)))
                    st.caption(
                        f"sum of asks ${sum_ask:.4f} + fees ${fees:.4f} = "
                        f"${sum_ask + fees:.4f} basket cost, against a $1.00 payout "
                        f"→ edge ${Decimal(1) - sum_ask - fees:.4f} per contract"
                    )
                else:
                    st.info(
                        "This detection predates per-leg quote logging. Newer detections show "
                        "each leg's ask, bid and depth here."
                    )
        else:
            st.info("No opportunities recorded.")

    with snapshot_tab:
        st.subheader("On-demand market snapshot")
        st.caption("This performs public REST calls and is independent of the scanner's live WebSocket state.")
        refresh_snapshot = st.button("Fetch fresh snapshot", type="primary")
        if refresh_snapshot:
            with st.spinner("Fetching events and order books..."):
                try:
                    st.session_state.last_scan = asyncio.run(_run_scan(settings))
                    st.session_state.last_refreshed = datetime.now(timezone.utc)
                except ExchangeError as exc:
                    st.error(f"Kalshi request failed: {exc}")
        if "last_scan" in st.session_state:
            total_open, candidates, active_candidates, legs = st.session_state.last_scan
            legs_by_event: dict[str, list[LegSnapshot]] = {}
            for leg in legs:
                legs_by_event.setdefault(leg.event_ticker, []).append(leg)
            rows = _event_rows(active_candidates, legs_by_event, settings)
            opportunities = [row for row in rows if row["opportunity"] != "-"]
            with st.container(border=True):
                c1, c2, c3 = st.columns(3)
                c1.metric("Open events", total_open)
                c2.metric("Eligible active events", len(active_candidates))
                c3.metric("Snapshot opportunities", len(opportunities))
                st.caption(f"Snapshot taken {st.session_state.last_refreshed.strftime('%Y-%m-%d %H:%M:%S')} UTC")
            if rows:
                st.dataframe(
                    pd.DataFrame(rows).sort_values("buy_cost_usd", na_position="last"),
                    width="stretch", hide_index=True,
                    column_config={
                        "event_ticker": "Event", "title": "Title", "legs": "Legs",
                        "missing_quotes": "Missing", "settles_utc": "Settles UTC",
                        "buy_cost_usd": st.column_config.NumberColumn("Basket cost", format="$%.4f"),
                        "opportunity": "Signal", "edge_usd": st.column_config.NumberColumn("Edge", format="$%.4f"),
                    },
                )
        else:
            st.info("Click Fetch fresh snapshot when you need a manual market check.")

    with diagnostics_tab:
        st.subheader("Scanner diagnostics")
        if status is not None:
            diagnostics = status.get("detection_diagnostics") or {}
            with st.container(border=True):
                d1, d2, d3, d4 = st.columns(4)
                d1.metric("Fully priced evaluations", diagnostics.get("fully_priced_evaluations", 0))
                d2.metric("Rejected on price", diagnostics.get("price_threshold_rejections", 0))
                d3.metric("Rejected as stale", diagnostics.get("stale_book_rejections", 0))
                d4.metric("Active events omitted", status.get("active_events_omitted_from_ws", 0))
                st.caption(
                    f"Incomplete books: {diagnostics.get('incomplete_book_rejections', 0)} · "
                    f"insufficient depth: {diagnostics.get('insufficient_depth_rejections', 0)} · "
                    f"incoherent books: {diagnostics.get('incoherent_book_rejections', 0)} · "
                    f"active legs {status.get('ws_shortlist_legs', 0)}/{status.get('active_legs_total', '—')} subscribed"
                )
            closest = diagnostics.get("closest_baskets") or []
            if closest:
                st.write("")
                st.markdown("##### Closest baskets this connection")
                st.caption("Fees included — sorted by however the scanner last wrote them.")
                st.dataframe(
                    pd.DataFrame(closest), width="stretch", hide_index=True,
                    column_config={
                        "event_ticker": "Event",
                        "basket_cost_usd": st.column_config.NumberColumn("Basket cost", format="$%.4f"),
                        "distance_to_threshold_usd": st.column_config.NumberColumn("Above/below trigger", format="$%.4f"),
                        "minimum_ask_depth": st.column_config.NumberColumn("Minimum depth"),
                        "legs": st.column_config.NumberColumn("Legs"),
                    },
                )
            watched = status.get("watched_events", [])
            if watched:
                st.write("")
                st.markdown(f"##### WebSocket subscriptions ({len(watched)} events)")
                st.dataframe(pd.DataFrame(watched).sort_values("volume_24h", ascending=False), width="stretch", hide_index=True)
            st.write("")
            with st.expander("Raw scanner status"):
                st.json(status)
        st.caption(f"Opportunity log: `{settings.logging.opportunities_log_path}`  ·  Order log: `{settings.execution.orders_log_path}`")

    if auto_refresh_live_views:
        time.sleep(5)
        st.rerun()


if __name__ == "__main__":
    main()
