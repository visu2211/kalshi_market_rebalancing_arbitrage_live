"""Order store for the maker strategy — thin wrapper over
execution.store.OrderStore pointed at a MAKER-OWNED SQLite file.

The underlying schema (orders table with the same columns the taker
uses) is identical: a resting GTC maker order and a submitted FOK taker
order share the same lifecycle events the row needs to represent
(submitted, filled_size + remaining_size, terminal_verified, timestamps).
Reusing the schema is a deliberate choice — the alternative (a separate
schema module for maker) would be ~200 lines of duplicated code that
would drift against the taker's over time. The ONLY difference between
the two strategies at the store layer is the file path, which is what
the plan's isolation decision is really about.

That reuse is READ-ONLY: this module imports OrderStore and doesn't
modify it. If the taker ever changes its schema, the maker picks up the
change automatically on next open (via OrderStore._verify_schema's
version check — a mismatch raises OrderStoreError, same as the taker).

maker_order_store_open(settings) is the only public symbol; the returned
value is a plain OrderStore instance the executor uses the same way the
taker does (record_submitted / record_result / query helpers), just
backed by maker_execution.order_store_path.
"""

from __future__ import annotations

from execution.store import OrderStore
from config.loader import MakerExecutionSettings


def maker_order_store_open(settings: MakerExecutionSettings) -> OrderStore:
    """Opens (or creates) the maker-strategy order store at
    `settings.order_store_path`. Mirrors execution.store.OrderStore.open's
    fail-closed semantics — a corrupt file raises OrderStoreError, a
    missing one creates a fresh schema."""
    return OrderStore.open(settings.order_store_path)
