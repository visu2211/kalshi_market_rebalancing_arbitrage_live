"""Durable, queryable per-order state store (SQLite, stdlib — no new
dependency). Additive alongside data/orders.jsonl and data/risk_state.json,
never a replacement for either: the JSONL append-only log stays the
simple, human-greppable audit trail it already is; this store answers
"what's the current state of order X" without replaying it.

Rows are written in two passes, mirroring execution/risk.py's RiskState
persist-before-submit discipline: record_submitted() runs before
place_orders_batch() is even called, so a crash between submission and
response leaves a durable, queryable "submitted but unconfirmed" row
rather than silence; record_result() then updates that same row in place
once the order's terminal (or best-known) state is resolved.

Fails closed on a corrupt/unreadable existing database file — mirrors
RiskState.load's RiskStateError: refuse to start armed execution rather
than silently opening a fresh, empty store over one that couldn't be
read. A missing file is not corruption; it's just the first run, same as
RiskState.empty.

Money/size columns are stored as TEXT (str(Decimal(...))), never REAL —
the same discipline every other persistence layer in this project
follows, since REAL is binary floating point and would silently lose
precision on exactly the values correctness depends on.

opportunities / relationships tables (schema v2): mirror what
stream/monitor.py already writes to data/opportunities.jsonl and what
scanner/relationships.py's Relationship objects carry, so that history
becomes queryable (frequency of detection, average edge, how often
auto-trust vs. manual verification disagreed) without replaying JSONL.
Both are append-only logs, not update-in-place like orders — a
relationship's verified/source status is a fact about a point in time,
not a single current value to overwrite.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from scanner.basket import BasketOpportunity
    from scanner.relationships import Relationship

_SCHEMA_VERSION = "2"

_ORDERS_TABLE_SQL = """
CREATE TABLE orders (
    client_order_id TEXT PRIMARY KEY,
    order_id TEXT,
    ticker TEXT NOT NULL,
    side TEXT NOT NULL,
    action TEXT NOT NULL,
    execution_stage TEXT NOT NULL,
    limit_price TEXT NOT NULL,
    size TEXT NOT NULL,
    status TEXT NOT NULL,
    filled_size TEXT NOT NULL,
    remaining_size TEXT NOT NULL,
    average_fill_price TEXT,
    average_fee_paid TEXT,
    terminal_verified INTEGER NOT NULL,
    submitted_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
)
"""

_OPPORTUNITIES_TABLE_SQL = """
CREATE TABLE opportunities (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_ticker TEXT NOT NULL,
    direction TEXT NOT NULL,
    detected_at TEXT NOT NULL,
    legs TEXT NOT NULL,
    basket_price_usd TEXT NOT NULL,
    edge_usd TEXT NOT NULL,
    max_size TEXT NOT NULL,
    fee_multiplier TEXT NOT NULL,
    leg_quotes TEXT NOT NULL
)
"""

_RELATIONSHIPS_TABLE_SQL = """
CREATE TABLE relationships (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_ticker TEXT NOT NULL,
    title TEXT NOT NULL,
    leg_tickers TEXT NOT NULL,
    relationship_type TEXT NOT NULL,
    verified INTEGER NOT NULL,
    source TEXT NOT NULL,
    fee_multiplier TEXT NOT NULL,
    expected_settlement_time TEXT NOT NULL,
    recorded_at TEXT NOT NULL
)
"""


class OrderStoreError(RuntimeError):
    """Raised when the store's backing file exists but cannot be safely
    read (corrupt SQLite file, or a schema_version this code doesn't
    recognize). Never raised for a simply-missing file — that's the
    first-run case, handled by creating a fresh schema."""


@dataclass(frozen=True)
class OrderRecord:
    client_order_id: str
    order_id: Optional[str]
    ticker: str
    side: str
    action: str
    execution_stage: str
    limit_price: Decimal
    size: Decimal
    status: str
    filled_size: Decimal
    remaining_size: Decimal
    average_fill_price: Optional[Decimal]
    average_fee_paid: Optional[Decimal]
    terminal_verified: bool
    submitted_at: str
    updated_at: str


def _row_to_record(row: sqlite3.Row) -> OrderRecord:
    def _opt_decimal(value: Optional[str]) -> Optional[Decimal]:
        return Decimal(value) if value is not None else None

    return OrderRecord(
        client_order_id=row["client_order_id"],
        order_id=row["order_id"],
        ticker=row["ticker"],
        side=row["side"],
        action=row["action"],
        execution_stage=row["execution_stage"],
        limit_price=Decimal(row["limit_price"]),
        size=Decimal(row["size"]),
        status=row["status"],
        filled_size=Decimal(row["filled_size"]),
        remaining_size=Decimal(row["remaining_size"]),
        average_fill_price=_opt_decimal(row["average_fill_price"]),
        average_fee_paid=_opt_decimal(row["average_fee_paid"]),
        terminal_verified=bool(row["terminal_verified"]),
        submitted_at=row["submitted_at"],
        updated_at=row["updated_at"],
    )


@dataclass(frozen=True)
class PendingOrder:
    """One order about to be submitted — everything record_submitted needs,
    already known before the exchange is ever called."""

    client_order_id: str
    ticker: str
    side: str
    action: str
    execution_stage: str
    limit_price: Decimal
    size: Decimal


@dataclass(frozen=True)
class OpportunityRecord:
    id: int
    event_ticker: str
    direction: str
    detected_at: str
    legs: list[str]
    basket_price_usd: Decimal
    edge_usd: Decimal
    max_size: Decimal
    fee_multiplier: Decimal
    leg_quotes: list[dict]


def _row_to_opportunity_record(row: sqlite3.Row) -> OpportunityRecord:
    return OpportunityRecord(
        id=row["id"],
        event_ticker=row["event_ticker"],
        direction=row["direction"],
        detected_at=row["detected_at"],
        legs=json.loads(row["legs"]),
        basket_price_usd=Decimal(row["basket_price_usd"]),
        edge_usd=Decimal(row["edge_usd"]),
        max_size=Decimal(row["max_size"]),
        fee_multiplier=Decimal(row["fee_multiplier"]),
        leg_quotes=json.loads(row["leg_quotes"]),
    )


@dataclass(frozen=True)
class RelationshipRecord:
    id: int
    event_ticker: str
    title: str
    leg_tickers: list[str]
    relationship_type: str
    verified: bool
    source: str
    fee_multiplier: Decimal
    expected_settlement_time: str
    recorded_at: str


def _row_to_relationship_record(row: sqlite3.Row) -> RelationshipRecord:
    return RelationshipRecord(
        id=row["id"],
        event_ticker=row["event_ticker"],
        title=row["title"],
        leg_tickers=json.loads(row["leg_tickers"]),
        relationship_type=row["relationship_type"],
        verified=bool(row["verified"]),
        source=row["source"],
        fee_multiplier=Decimal(row["fee_multiplier"]),
        expected_settlement_time=row["expected_settlement_time"],
        recorded_at=row["recorded_at"],
    )


class OrderStore:
    def __init__(self, connection: sqlite3.Connection) -> None:
        self._conn = connection

    @classmethod
    def open(cls, path: Path) -> "OrderStore":
        path.parent.mkdir(parents=True, exist_ok=True)
        is_new = not path.exists()
        try:
            conn = sqlite3.connect(str(path))
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            if is_new:
                cls._init_schema(conn)
            else:
                cls._verify_schema(conn, path)
        except sqlite3.DatabaseError as exc:
            raise OrderStoreError(f"cannot safely open order store {path}: {exc}") from exc
        return cls(conn)

    @staticmethod
    def _init_schema(conn: sqlite3.Connection) -> None:
        conn.execute("CREATE TABLE schema_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
        conn.execute("INSERT INTO schema_meta (key, value) VALUES ('schema_version', ?)", (_SCHEMA_VERSION,))
        conn.execute(_ORDERS_TABLE_SQL)
        conn.execute(_OPPORTUNITIES_TABLE_SQL)
        conn.execute(_RELATIONSHIPS_TABLE_SQL)
        conn.commit()

    @staticmethod
    def _verify_schema(conn: sqlite3.Connection, path: Path) -> None:
        try:
            row = conn.execute("SELECT value FROM schema_meta WHERE key = 'schema_version'").fetchone()
        except sqlite3.OperationalError as exc:
            raise OrderStoreError(f"order store {path} is missing expected tables: {exc}") from exc
        if row is None or row["value"] != _SCHEMA_VERSION:
            found = row["value"] if row is not None else None
            raise OrderStoreError(
                f"order store {path} has schema_version {found!r}, expected {_SCHEMA_VERSION!r}"
            )

    def record_submitted(self, orders: list[PendingOrder], now: Optional[datetime] = None) -> None:
        """Inserts one row per order in 'submitted, unconfirmed' state —
        called before the batch ever reaches the exchange, so a crash
        before the response comes back still leaves a durable, queryable
        record rather than silence."""
        if not orders:
            return
        ts = (now or datetime.now(timezone.utc)).isoformat()
        self._conn.executemany(
            "INSERT INTO orders (client_order_id, order_id, ticker, side, action, execution_stage, "
            "limit_price, size, status, filled_size, remaining_size, average_fill_price, "
            "average_fee_paid, terminal_verified, submitted_at, updated_at) "
            "VALUES (?, NULL, ?, ?, ?, ?, ?, ?, 'submitted', '0', ?, NULL, NULL, 0, ?, ?)",
            [
                (o.client_order_id, o.ticker, o.side, o.action, o.execution_stage,
                 str(o.limit_price), str(o.size), str(o.size), ts, ts)
                for o in orders
            ],
        )
        self._conn.commit()

    def record_result(
        self,
        client_order_id: str,
        *,
        order_id: Optional[str],
        status: str,
        filled_size: Decimal,
        remaining_size: Decimal,
        average_fill_price: Optional[Decimal] = None,
        average_fee_paid: Optional[Decimal] = None,
        terminal_verified: bool = True,
        now: Optional[datetime] = None,
    ) -> None:
        """Updates the row in place with the best-known state — the
        terminal state on the happy path, or whatever was actually
        resolved (possibly still non-terminal) when execution halted."""
        ts = (now or datetime.now(timezone.utc)).isoformat()
        self._conn.execute(
            "UPDATE orders SET order_id = ?, status = ?, filled_size = ?, remaining_size = ?, "
            "average_fill_price = ?, average_fee_paid = ?, terminal_verified = ?, updated_at = ? "
            "WHERE client_order_id = ?",
            (
                order_id, status, str(filled_size), str(remaining_size),
                str(average_fill_price) if average_fill_price is not None else None,
                str(average_fee_paid) if average_fee_paid is not None else None,
                int(terminal_verified), ts, client_order_id,
            ),
        )
        self._conn.commit()

    def get(self, client_order_id: str) -> Optional[OrderRecord]:
        row = self._conn.execute(
            "SELECT * FROM orders WHERE client_order_id = ?", (client_order_id,)
        ).fetchone()
        return _row_to_record(row) if row is not None else None

    def record_opportunity(self, opportunity: "BasketOpportunity", now: Optional[datetime] = None) -> None:
        """Append-only: mirrors stream/monitor.py's _append_opportunity_jsonl
        call site (same trigger — a fresh close->open transition), so
        detection frequency/edge/depth become queryable without replaying
        data/opportunities.jsonl. Never updated in place — a later
        transition for the same event_ticker is a new row, not a
        correction of this one."""
        ts = (now or datetime.now(timezone.utc)).isoformat()
        self._conn.execute(
            "INSERT INTO opportunities (event_ticker, direction, detected_at, legs, basket_price_usd, "
            "edge_usd, max_size, fee_multiplier, leg_quotes) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                opportunity.event_ticker, opportunity.direction, ts, json.dumps(opportunity.legs),
                str(opportunity.basket_price_usd), str(opportunity.edge_usd), str(opportunity.max_size),
                str(opportunity.fee_multiplier),
                json.dumps([
                    {
                        "ticker": q.ticker, "ask": str(q.best_ask), "ask_size": str(q.ask_size),
                        "bid": str(q.best_bid), "bid_size": str(q.bid_size),
                    }
                    for q in opportunity.leg_quotes
                ]),
            ),
        )
        self._conn.commit()

    def record_relationships(self, relationships: list["Relationship"], now: Optional[datetime] = None) -> None:
        """Append-only history of Relationship objects (see
        scanner/relationships.py) as built each rescan cycle — a queryable
        record of what was auto-detected vs. manually verified over time,
        not just the current state. A no-op for an empty list."""
        if not relationships:
            return
        ts = (now or datetime.now(timezone.utc)).isoformat()
        self._conn.executemany(
            "INSERT INTO relationships (event_ticker, title, leg_tickers, relationship_type, verified, "
            "source, fee_multiplier, expected_settlement_time, recorded_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    r.event_ticker, r.title, json.dumps(r.leg_tickers), r.relationship_type,
                    int(r.verified), r.source, str(r.fee_multiplier),
                    r.expected_settlement_time.isoformat(), ts,
                )
                for r in relationships
            ],
        )
        self._conn.commit()

    def get_opportunities(self, event_ticker: Optional[str] = None) -> list[OpportunityRecord]:
        if event_ticker is None:
            rows = self._conn.execute("SELECT * FROM opportunities ORDER BY id").fetchall()
        else:
            rows = self._conn.execute(
                "SELECT * FROM opportunities WHERE event_ticker = ? ORDER BY id", (event_ticker,)
            ).fetchall()
        return [_row_to_opportunity_record(row) for row in rows]

    def get_relationships(self, event_ticker: Optional[str] = None) -> list[RelationshipRecord]:
        if event_ticker is None:
            rows = self._conn.execute("SELECT * FROM relationships ORDER BY id").fetchall()
        else:
            rows = self._conn.execute(
                "SELECT * FROM relationships WHERE event_ticker = ? ORDER BY id", (event_ticker,)
            ).fetchall()
        return [_row_to_relationship_record(row) for row in rows]

    def close(self) -> None:
        self._conn.close()
