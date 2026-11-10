"""stream.maker_monitor — in-process state only. The live WS loops are
not unit tested here (they need a real exchange); the RestingOrderTracker
is tested directly because it does non-trivial bookkeeping that the
monitor's fill-dispatch correctness depends on."""

from decimal import Decimal

from stream.maker_monitor import RestingOrderTracker


def test_record_and_lookup():
    tracker = RestingOrderTracker()
    tracker.record("EVT", "A", "order-A-1")
    tracker.record("EVT", "B", "order-B-1")
    assert tracker.event_order_ids("EVT") == {"A": "order-A-1", "B": "order-B-1"}
    assert tracker.event_for_order("order-A-1") == ("EVT", "A")
    assert tracker.event_for_order("order-unknown") is None


def test_record_empty_order_id_is_noop():
    tracker = RestingOrderTracker()
    tracker.record("EVT", "A", "")
    assert tracker.event_order_ids("EVT") == {}


def test_forget_clears_event_entirely():
    tracker = RestingOrderTracker()
    tracker.record("E1", "A", "order-A-1")
    tracker.record("E1", "B", "order-B-1")
    tracker.record("E2", "C", "order-C-1")
    tracker.forget("E1")
    assert tracker.event_order_ids("E1") == {}
    assert tracker.event_for_order("order-A-1") is None
    assert tracker.event_for_order("order-B-1") is None
    # E2 untouched
    assert tracker.event_order_ids("E2") == {"C": "order-C-1"}


def test_record_cumulative_returns_delta():
    tracker = RestingOrderTracker()
    tracker.record("EVT", "A", "order-A-1")
    # First fill: cumulative 2 -> delta 2
    assert tracker.record_cumulative("order-A-1", Decimal(2)) == Decimal(2)
    # Second fill: cumulative 5 -> delta 3
    assert tracker.record_cumulative("order-A-1", Decimal(5)) == Decimal(3)
    # Replay of same cumulative -> delta 0
    assert tracker.record_cumulative("order-A-1", Decimal(5)) == Decimal(0)
    # A lower value (shouldn't happen but harmless) clamps to 0
    assert tracker.record_cumulative("order-A-1", Decimal(3)) == Decimal(0)
