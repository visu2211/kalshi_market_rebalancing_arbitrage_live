"""execution.maker.inventory — pure state math, no network."""

from decimal import Decimal

from execution.maker.inventory import (
    EventInventory,
    InventoryActionKind,
    MakerInventoryBook,
)


def _book(flatten_on_leg_fill: bool = True) -> MakerInventoryBook:
    return MakerInventoryBook(
        flatten_on_leg_fill=flatten_on_leg_fill,
        max_inventory_per_event_usd=Decimal("5"),
    )


def test_open_event_idempotent():
    book = _book()
    a = book.open_event("EVT", ["A", "B"], Decimal(1))
    b = book.open_event("EVT", ["A", "B"], Decimal(1))
    assert a is b
    assert a.event_ticker == "EVT"
    assert a.target_size == Decimal(1)


def test_no_action_when_event_not_tracked():
    book = _book()
    action = book.record_fill(
        "UNKNOWN", "A",
        filled_size_delta=Decimal(1), fill_price=Decimal("0.5"),
        remaining_size=Decimal(0),
    )
    assert action.kind == InventoryActionKind.NO_ACTION


def test_basket_complete_when_every_leg_hits_target():
    book = _book(flatten_on_leg_fill=True)
    book.open_event("EVT", ["A", "B"], Decimal(1))

    # Leg A fully fills first — flatten_on_leg_fill would normally flatten,
    # but B is still resting with 0 fill so A is above the floor. The fill
    # triggers the flatten action because the basket isn't balanced yet.
    action1 = book.record_fill(
        "EVT", "A",
        filled_size_delta=Decimal(1), fill_price=Decimal("0.5"),
        remaining_size=Decimal(0),
    )
    assert action1.kind == InventoryActionKind.CANCEL_REMAINING_AND_FLATTEN

    # The CANCEL_REMAINING_AND_FLATTEN action already closed the event in
    # the executor. Reopen for the explicit "basket completes in one tick"
    # case: flatten_on_leg_fill=False keeps the event tracked.
    book = _book(flatten_on_leg_fill=False)
    book.open_event("EVT", ["A", "B"], Decimal(1))
    _ = book.record_fill(
        "EVT", "A",
        filled_size_delta=Decimal(1), fill_price=Decimal("0.5"),
        remaining_size=Decimal(0),
    )
    action = book.record_fill(
        "EVT", "B",
        filled_size_delta=Decimal(1), fill_price=Decimal("0.3"),
        remaining_size=Decimal(0),
    )
    assert action.kind == InventoryActionKind.BASKET_COMPLETE
    assert action.floor_size == Decimal(1)


def test_cancel_and_flatten_when_one_leg_fills_first():
    book = _book(flatten_on_leg_fill=True)
    book.open_event("EVT", ["A", "B", "C"], Decimal(1))

    action = book.record_fill(
        "EVT", "A",
        filled_size_delta=Decimal(1), fill_price=Decimal("0.4"),
        remaining_size=Decimal(0),
    )
    assert action.kind == InventoryActionKind.CANCEL_REMAINING_AND_FLATTEN
    # A came off the resting set; B and C are still resting -> both to cancel
    assert sorted(action.tickers_to_cancel) == ["B", "C"]
    # A has 1 excess over the 0 floor
    assert action.unwind_excess == {"A": Decimal(1)}
    assert action.floor_size == Decimal(0)


def test_no_flatten_when_flag_off():
    book = _book(flatten_on_leg_fill=False)
    book.open_event("EVT", ["A", "B"], Decimal(1))

    action = book.record_fill(
        "EVT", "A",
        filled_size_delta=Decimal(1), fill_price=Decimal("0.4"),
        remaining_size=Decimal(0),
    )
    assert action.kind == InventoryActionKind.NO_ACTION


def test_would_exceed_inventory_cap_absent_event_uses_zero_base():
    book = _book()
    assert book.would_exceed_inventory_cap("EVT", Decimal("4")) is False
    assert book.would_exceed_inventory_cap("EVT", Decimal("6")) is True   # cap is 5


def test_would_exceed_inventory_cap_adds_to_existing_notional():
    # inventory.record_fill never auto-closes; the EXECUTOR does (after
    # it's finished flattening). So within a single lifecycle, notional
    # accumulates as fills arrive and would_exceed sees it.
    book = _book(flatten_on_leg_fill=False)
    book.open_event("EVT", ["A", "B"], Decimal(1))
    book.record_fill(
        "EVT", "A",
        filled_size_delta=Decimal(1), fill_price=Decimal(3),
        remaining_size=Decimal(0),
    )
    # $3 already on the event; cap is $5. Another $1 fits; another $3 doesn't.
    assert book.would_exceed_inventory_cap("EVT", Decimal(1)) is False
    assert book.would_exceed_inventory_cap("EVT", Decimal(3)) is True


def test_excess_by_ticker_computes_from_floor():
    inv = EventInventory(
        event_ticker="EVT",
        target_size=Decimal(5),
        leg_tickers=["A", "B", "C"],
        filled={"A": Decimal(5), "B": Decimal(3), "C": Decimal(0)},
    )
    assert inv.floor == Decimal(0)
    assert inv.excess_by_ticker == {"A": Decimal(5), "B": Decimal(3)}
