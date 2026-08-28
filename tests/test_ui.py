from ui.app import _filled_trade_rows, _kalshi_market_url, _order_history_rows, _opportunity_history_rows, _opportunity_leg_rows


def test_market_link_uses_stable_ticker_redirect():
    assert _kalshi_market_url("KXABC-26-A") == "https://kalshi.com/markets_by_ticker/kxabc-26-a"


def test_filled_trade_row_has_expected_profit_and_order_link():
    records = [{
        "placed_at": "2026-08-26T12:00:00+00:00",
        "kind": "basket",
        "target": "prod",
        "event_ticker": "EVT",
        "edge_usd_per_contract": "0.04",
        "legs": [
            {"ticker": "EVT-A", "filled_size": "2", "order_id": "order-a"},
            {"ticker": "EVT-B", "filled_size": "2", "order_id": "order-b"},
        ],
    }]
    row = _filled_trade_rows(records)[0]
    assert row["expected_profit_usd"] == 0.08
    assert row["fill_quality"] == "Matched"
    assert row["order_ids"] == "order-a, order-b"
    assert row["kalshi_market"].endswith("/evt-a")


def test_raw_order_rows_expose_actual_fill_economics():
    rows = _order_history_rows([{
        "legs": [{
            "ticker": "EVT-A", "filled_size": "1", "average_fill_price": "0.42",
            "average_fee_paid": "0.01",
        }],
    }])
    assert rows[0]["average_fill_price"] == "0.42"
    assert rows[0]["average_fee_paid"] == "0.01"
    assert rows[0]["market_url"].endswith("/evt-a")


def test_opportunity_rows_expose_per_leg_economics():
    """A basket price alone can't show which leg carried the edge or how thin
    the binding leg was — both decide whether it was really executable."""
    record = {
        "detected_at": "2026-08-28T16:03:26+00:00",
        "event_ticker": "KXTEST",
        "legs": ["KXTEST-A", "KXTEST-B"],
        "basket_price_usd": "0.95", "edge_usd": "0.05",
        "sum_ask_usd": "0.93", "fees_usd": "0.02", "max_size": "11",
        "leg_quotes": [
            {"ticker": "KXTEST-A", "ask": "0.0100", "ask_size": "30280", "bid": "0.0050", "bid_size": "10", "spread": "0.0050"},
            {"ticker": "KXTEST-B", "ask": "0.9200", "ask_size": "11", "bid": "0.9000", "bid_size": "500", "spread": "0.0200"},
        ],
    }
    row = _opportunity_history_rows([record])[0]
    assert row["sum_ask_usd"] == "0.93"
    assert row["fees_usd"] == "0.02"
    assert row["thinnest_leg_size"] == 11.0      # the binding leg, not the deep one
    assert row["widest_leg_spread"] == 0.02

    legs = _opportunity_leg_rows(record)
    assert [l["ticker"] for l in legs] == ["KXTEST-A", "KXTEST-B"]
    assert legs[0]["ask"] == 0.01 and legs[0]["ask_size"] == 30280.0
    assert legs[1]["ask"] == 0.92 and legs[1]["bid"] == 0.90
    assert all(l["market_url"].startswith("https://kalshi.com/") for l in legs)


def test_opportunity_leg_rows_tolerate_records_without_quotes():
    """Detections logged before per-leg quotes existed must not break the tab."""
    assert _opportunity_leg_rows({"event_ticker": "KXOLD", "legs": ["A", "B"]}) == []
    row = _opportunity_history_rows([{"event_ticker": "KXOLD", "legs": ["A", "B"], "edge_usd": "0.03"}])[0]
    assert row["thinnest_leg_size"] is None
    assert row["leg_count"] == 2
