"""execution.store.OrderStore — no network, no mocking library (matches
this project's convention: real sqlite against a tmp_path file). Templated
on tests/test_risk.py's shape: round-trip persist/load, fail-closed on a
corrupt/unreadable existing file, and the update-in-place behavior that is
this store's actual reason to exist over the append-only JSONL logs.

Also covers the opportunities/relationships tables (schema v2) — both
append-only histories, unlike orders' update-in-place rows."""

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from execution.store import OrderStore, OrderStoreError, PendingOrder
from scanner.basket import LegQuote, compute_opportunity
from scanner.relationships import Relationship

NOW = datetime(2026, 8, 29, tzinfo=timezone.utc)


def _pending(client_order_id="kmra-entry-abc", ticker="CUT50", limit_price="0.04", size="45") -> PendingOrder:
    return PendingOrder(
        client_order_id=client_order_id, ticker=ticker, side="yes", action="buy",
        execution_stage="entry", limit_price=Decimal(limit_price), size=Decimal(size),
    )


def test_order_store_round_trip(tmp_path):
    path = tmp_path / "orders.db"
    store = OrderStore.open(path)
    store.record_submitted([_pending()], now=NOW)
    store.record_result(
        "kmra-entry-abc", order_id="order-1", status="filled",
        filled_size=Decimal("45"), remaining_size=Decimal("0"),
        average_fill_price=Decimal("0.04"), average_fee_paid=Decimal("0.02"),
        terminal_verified=True, now=NOW,
    )
    store.close()

    reopened = OrderStore.open(path)
    record = reopened.get("kmra-entry-abc")
    assert record is not None
    assert record.order_id == "order-1"
    assert record.status == "filled"
    assert record.filled_size == Decimal("45")
    assert record.remaining_size == Decimal("0")
    assert record.average_fill_price == Decimal("0.04")
    assert record.average_fee_paid == Decimal("0.02")
    assert record.terminal_verified is True
    assert record.ticker == "CUT50"
    assert record.limit_price == Decimal("0.04")
    assert record.size == Decimal("45")


def test_missing_file_creates_fresh_schema_not_an_error(tmp_path):
    """A missing file is the first-run case (mirrors RiskState.empty), not
    corruption — must not raise."""
    path = tmp_path / "nested" / "orders.db"
    store = OrderStore.open(path)
    assert store.get("kmra-entry-nonexistent") is None
    store.close()
    assert path.exists()


def test_corrupt_database_file_fails_closed(tmp_path):
    path = tmp_path / "orders.db"
    path.write_bytes(b"this is not a sqlite database")
    with pytest.raises(OrderStoreError):
        OrderStore.open(path)


def test_unrecognized_schema_version_fails_closed(tmp_path):
    """A store from a future/incompatible schema must refuse to open
    rather than silently reading (or worse, writing) it as if it matched
    today's schema."""
    import sqlite3

    path = tmp_path / "orders.db"
    conn = sqlite3.connect(str(path))
    conn.execute("CREATE TABLE schema_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
    conn.execute("INSERT INTO schema_meta (key, value) VALUES ('schema_version', '99')")
    conn.commit()
    conn.close()

    with pytest.raises(OrderStoreError, match="schema_version"):
        OrderStore.open(path)


def test_record_submitted_is_queryable_and_unconfirmed_before_any_result(tmp_path):
    """The property this store exists for: a row written before submission
    is immediately queryable, in a clearly-not-terminal state — proving a
    crash between record_submitted and record_result leaves a durable,
    non-silent partial-submission record rather than nothing at all."""
    path = tmp_path / "orders.db"
    store = OrderStore.open(path)
    store.record_submitted([_pending()], now=NOW)

    record = store.get("kmra-entry-abc")
    assert record is not None
    assert record.status == "submitted"
    assert record.terminal_verified is False
    assert record.order_id is None
    assert record.filled_size == Decimal("0")


def test_record_result_updates_the_same_row_in_place(tmp_path):
    """Update-in-place, not a second row — the property JSONL's append-only
    model can't provide (see module docstring)."""
    path = tmp_path / "orders.db"
    store = OrderStore.open(path)
    store.record_submitted([_pending()], now=NOW)
    store.record_result(
        "kmra-entry-abc", order_id="order-1", status="filled",
        filled_size=Decimal("45"), remaining_size=Decimal("0"), now=NOW,
    )

    row_count = store._conn.execute("SELECT COUNT(*) AS n FROM orders").fetchone()["n"]
    assert row_count == 1
    assert store.get("kmra-entry-abc").status == "filled"


def test_record_submitted_batch_writes_every_order(tmp_path):
    path = tmp_path / "orders.db"
    store = OrderStore.open(path)
    store.record_submitted(
        [_pending("kmra-entry-1", "A"), _pending("kmra-entry-2", "B"), _pending("kmra-entry-3", "C")],
        now=NOW,
    )
    assert {store.get(cid).ticker for cid in ("kmra-entry-1", "kmra-entry-2", "kmra-entry-3")} == {"A", "B", "C"}


def test_record_submitted_with_no_orders_is_a_noop(tmp_path):
    path = tmp_path / "orders.db"
    store = OrderStore.open(path)
    store.record_submitted([], now=NOW)   # must not raise
    assert store.get("anything") is None


# --- opportunities / relationships tables (schema v2) -------------------------


def test_record_opportunity_round_trip(tmp_path):
    legs = [
        LegQuote(ticker="A", best_ask=Decimal("0.30"), ask_size=Decimal("100"), best_bid=Decimal("0.29"), bid_size=Decimal("100")),
        LegQuote(ticker="B", best_ask=Decimal("0.60"), ask_size=Decimal("100"), best_bid=Decimal("0.59"), bid_size=Decimal("100")),
    ]
    opportunity = compute_opportunity("KXFED-1", legs, Decimal("0.98"), Decimal("1"))
    assert opportunity is not None

    store = OrderStore.open(tmp_path / "orders.db")
    store.record_opportunity(opportunity, now=NOW)

    records = store.get_opportunities()
    assert len(records) == 1
    record = records[0]
    assert record.event_ticker == "KXFED-1"
    assert record.direction == "buy_basket"
    assert record.legs == ["A", "B"]
    assert record.basket_price_usd == opportunity.basket_price_usd
    assert record.edge_usd == opportunity.edge_usd
    assert record.max_size == opportunity.max_size
    assert record.detected_at == NOW.isoformat()
    assert {q["ticker"] for q in record.leg_quotes} == {"A", "B"}


def test_record_opportunity_is_append_only_not_updated_in_place(tmp_path):
    legs = [
        LegQuote(ticker="A", best_ask=Decimal("0.30"), ask_size=Decimal("100"), best_bid=Decimal("0.29"), bid_size=Decimal("100")),
        LegQuote(ticker="B", best_ask=Decimal("0.60"), ask_size=Decimal("100"), best_bid=Decimal("0.59"), bid_size=Decimal("100")),
    ]
    opportunity = compute_opportunity("KXFED-1", legs, Decimal("0.98"), Decimal("1"))
    store = OrderStore.open(tmp_path / "orders.db")

    store.record_opportunity(opportunity, now=NOW)
    store.record_opportunity(opportunity, now=NOW)   # a second detection of the same event

    assert len(store.get_opportunities("KXFED-1")) == 2
    assert len(store.get_opportunities()) == 2


def test_get_opportunities_filters_by_event_ticker(tmp_path):
    legs = [
        LegQuote(ticker="A", best_ask=Decimal("0.30"), ask_size=Decimal("100"), best_bid=Decimal("0.29"), bid_size=Decimal("100")),
        LegQuote(ticker="B", best_ask=Decimal("0.60"), ask_size=Decimal("100"), best_bid=Decimal("0.59"), bid_size=Decimal("100")),
    ]
    opp_a = compute_opportunity("KXFED-A", legs, Decimal("0.98"), Decimal("1"))
    opp_b = compute_opportunity("KXFED-B", legs, Decimal("0.98"), Decimal("1"))
    store = OrderStore.open(tmp_path / "orders.db")
    store.record_opportunity(opp_a, now=NOW)
    store.record_opportunity(opp_b, now=NOW)

    assert [r.event_ticker for r in store.get_opportunities("KXFED-A")] == ["KXFED-A"]
    assert len(store.get_opportunities()) == 2


def _relationship(event_ticker="KXFED-1", verified=False, source="auto_kalshi_metadata"):
    return Relationship(
        event_ticker=event_ticker, title="Fed decision", leg_tickers=["A", "B"],
        leg_volume_24h={"A": Decimal("10"), "B": Decimal("20")},
        expected_settlement_time=NOW, fee_multiplier=Decimal(1),
        verified=verified, source=source,
    )


def test_record_relationships_round_trip(tmp_path):
    store = OrderStore.open(tmp_path / "orders.db")
    store.record_relationships([_relationship(verified=True, source="manual")], now=NOW)

    records = store.get_relationships()
    assert len(records) == 1
    record = records[0]
    assert record.event_ticker == "KXFED-1"
    assert record.leg_tickers == ["A", "B"]
    assert record.relationship_type == "exhaustive_partition"
    assert record.verified is True
    assert record.source == "manual"
    assert record.recorded_at == NOW.isoformat()


def test_record_relationships_is_append_only_history(tmp_path):
    """A relationship's trust status is a fact about a point in time —
    recording it again (e.g. next rescan cycle) must not overwrite the
    earlier row, so later analysis can see when auto-trust and manual
    verification agreed or disagreed over time."""
    store = OrderStore.open(tmp_path / "orders.db")
    store.record_relationships([_relationship(verified=False)], now=NOW)
    store.record_relationships([_relationship(verified=True, source="manual")], now=NOW)

    records = store.get_relationships("KXFED-1")
    assert len(records) == 2
    assert [r.verified for r in records] == [False, True]


def test_record_relationships_with_empty_list_is_a_noop(tmp_path):
    store = OrderStore.open(tmp_path / "orders.db")
    store.record_relationships([], now=NOW)   # must not raise
    assert store.get_relationships() == []


def test_schema_v2_still_fails_closed_on_v1_only_database(tmp_path):
    """A database created by the pre-opportunities/relationships schema
    must be refused, not silently treated as if it had the new tables —
    the same fail-closed discipline as any other unrecognized version."""
    import sqlite3

    path = tmp_path / "orders.db"
    conn = sqlite3.connect(str(path))
    conn.execute("CREATE TABLE schema_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
    conn.execute("INSERT INTO schema_meta (key, value) VALUES ('schema_version', '1')")
    conn.execute("CREATE TABLE orders (client_order_id TEXT PRIMARY KEY)")
    conn.commit()
    conn.close()

    with pytest.raises(OrderStoreError, match="schema_version"):
        OrderStore.open(path)
