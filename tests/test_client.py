"""Live smoke tests against Kalshi's public prod REST API (no credentials
required — market data endpoints are unauthenticated). Mirrors the sibling
arb-bot project's tests/test_kalshi_client.py.

Run: pytest tests/test_client.py -v
"""

from decimal import Decimal

import httpx
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa

from kalshi.client import (
    AmbiguousSubmissionError,
    ExchangeError,
    KalshiClient,
    OrderAction,
    OrderRequest,
    OrderStatus,
    OrderType,
    ServerError,
    Side,
    TimeInForce,
)

PROD_BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"


@pytest.fixture
async def client():
    async with KalshiClient(base_url=PROD_BASE_URL) as c:
        yield c


def test_has_signing_credentials_false_when_unconfigured():
    assert KalshiClient(base_url=PROD_BASE_URL).has_signing_credentials is False


def test_has_signing_credentials_true_when_both_set():
    client = KalshiClient(base_url=PROD_BASE_URL, api_key_id="abc", private_key_path="/tmp/does-not-need-to-exist.pem")
    assert client.has_signing_credentials is True


@pytest.mark.asyncio
@pytest.mark.integration
async def test_get_events_returns_open_events_with_expected_fields(client):
    events = await client.get_events(status="open", limit=5, with_nested_markets=True)
    assert "events" in events
    assert len(events["events"]) > 0

    event = events["events"][0]
    assert "event_ticker" in event
    assert "mutually_exclusive" in event   # scanner/filters.py's Stage 1 field
    assert "markets" in event


@pytest.mark.asyncio
@pytest.mark.integration
async def test_get_all_events_pages_through_cursor(client):
    # A small page size forces at least one cursor hop within a short list.
    events = await client.get_all_events(status="open", with_nested_markets=False)
    assert len(events) > 5  # sanity: there's always more than one page's worth of open events


@pytest.mark.asyncio
@pytest.mark.integration
async def test_get_markets_returns_markets(client):
    markets = await client.get_markets(status="open", limit=1)
    assert "markets" in markets
    assert len(markets["markets"]) > 0
    assert "ticker" in markets["markets"][0]


@pytest.mark.asyncio
@pytest.mark.integration
async def test_get_orderbook_normalizes_both_sides(client):
    markets = await client.get_markets(status="open", limit=1)
    ticker = markets["markets"][0]["ticker"]

    yes_book = await client.get_orderbook(ticker, side=Side.YES)
    no_book = await client.get_orderbook(ticker, side=Side.NO)

    assert yes_book.side == Side.YES
    assert no_book.side == Side.NO

    # bids sorted best (highest) first, asks sorted best (lowest) first
    assert all(yes_book.bids[i].price >= yes_book.bids[i + 1].price for i in range(len(yes_book.bids) - 1))
    assert all(yes_book.asks[i].price <= yes_book.asks[i + 1].price for i in range(len(yes_book.asks) - 1))

    # YES ask should equal 1 - NO bid at the same depth (complementary book)
    if yes_book.asks and no_book.bids:
        assert yes_book.asks[0].price == pytest.approx(1 - no_book.bids[0].price, abs=1e-6)


# ---- _derive_order_status (pure, no network) ----
# Regression coverage for 2026-08-26: a canceled-with-zero-fill order
# (filled=0, remaining=0) was being mislabeled OPEN, indistinguishable from
# a genuinely still-resting order (filled=0, remaining>0) — see
# kalshi/client.py's OrderStatus/_derive_order_status docstrings.

def test_derive_order_status_open_when_unfilled_and_still_resting():
    assert KalshiClient._derive_order_status(Decimal("0"), Decimal("45")) == OrderStatus.OPEN


def test_derive_order_status_canceled_when_unfilled_and_nothing_remaining():
    assert KalshiClient._derive_order_status(Decimal("0"), Decimal("0")) == OrderStatus.CANCELED


def test_derive_order_status_partially_filled():
    assert KalshiClient._derive_order_status(Decimal("20"), Decimal("25")) == OrderStatus.PARTIALLY_FILLED


def test_derive_order_status_filled_when_fully_filled():
    assert KalshiClient._derive_order_status(Decimal("45"), Decimal("0")) == OrderStatus.FILLED


def _request(**overrides):
    values = dict(
        market_id="TEST", side=Side.YES, action=OrderAction.BUY,
        order_type=OrderType.LIMIT, size=Decimal("2"),
        limit_price=Decimal("0.1234"),
        time_in_force=TimeInForce.IMMEDIATE_OR_CANCEL,
    )
    values.update(overrides)
    return OrderRequest(**values)


def test_v2_order_body_preserves_subcent_precision_and_native_ioc():
    body = KalshiClient._build_order_body(_request(), "cid")
    assert body["price"] == "0.1234"
    assert body["time_in_force"] == "immediate_or_cancel"
    assert body["cancel_order_on_pause"] is True
    assert "expiration_time" not in body


def test_v2_order_body_uses_documented_expiration_field():
    body = KalshiClient._build_order_body(
        _request(time_in_force=TimeInForce.GOOD_TILL_CANCELED, expiration_ts=123), "cid"
    )
    assert body["expiration_time"] == 123
    assert "expiration_ts" not in body


def test_v2_order_body_rejects_price_off_market_tick():
    with pytest.raises(ExchangeError, match="tick size"):
        KalshiClient._build_order_body(_request(tick_size=Decimal("0.01")), "cid")


def test_portfolio_order_parser_derives_actual_average_price_and_fee():
    result = KalshiClient._parse_portfolio_order({
        "order_id": "oid", "fill_count_fp": "4", "remaining_count_fp": "0",
        "taker_fill_cost_dollars": "1.20", "maker_fill_cost_dollars": "0.40",
        "taker_fees_dollars": "0.08", "maker_fees_dollars": "0.04",
    })
    assert result.average_fill_price == Decimal("0.40")
    assert result.average_fee_paid == Decimal("0.03")


# --- HTTP error taxonomy (added 2026-08-27) ---------------------------------
# Every caller in this project handles ExchangeError and nothing else, so
# raise_for_status() leaking httpx.HTTPStatusError meant a routine Kalshi 5xx
# bypassed all of them and killed the scanner loop.


def _mock_client(status_code, body=None, text=None):
    def handler(request):
        if text is not None:
            return httpx.Response(status_code, text=text)
        return httpx.Response(status_code, json=body if body is not None else {"error": "boom"})
    client = KalshiClient(base_url="https://x.test/trade-api/v2", api_key_id="k", private_key_path="p")
    # Pre-seed the parsed key so signed-path tests reach the HTTP status
    # handling instead of failing on a missing key file first.
    client._private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    client._http = httpx.AsyncClient(base_url="https://x.test/trade-api/v2",
                                     transport=httpx.MockTransport(handler))
    return client


@pytest.mark.parametrize("status_code", [500, 502, 503])
@pytest.mark.asyncio
async def test_public_get_raises_retryable_server_error_on_5xx(status_code):
    client = _mock_client(status_code)
    with pytest.raises(ServerError):
        await client.get_orderbook("T")
    assert issubclass(ServerError, ExchangeError)


@pytest.mark.parametrize("status_code", [400, 403, 404])
@pytest.mark.asyncio
async def test_public_get_raises_exchange_error_on_4xx(status_code):
    client = _mock_client(status_code)
    with pytest.raises(ExchangeError) as excinfo:
        await client.get_orderbook("T")
    assert not isinstance(excinfo.value, ServerError)   # 4xx must not be retried


@pytest.mark.asyncio
async def test_public_get_raises_exchange_error_on_non_json_body():
    """A CDN/proxy error page can arrive as a 200 with HTML."""
    client = _mock_client(200, text="<html>502 Bad Gateway</html>")
    with pytest.raises(ExchangeError):
        await client.get_orderbook("T")


@pytest.mark.asyncio
async def test_signed_get_5xx_is_retryable_server_error():
    client = _mock_client(503)
    with pytest.raises(ServerError):
        await client.get_positions()


@pytest.mark.asyncio
async def test_signed_mutation_5xx_is_not_retryable():
    """A 502 on an order POST may mean the order was accepted and only the
    response was lost — retrying is a duplicate-submission risk, so it must
    not come back as the retryable ServerError."""
    client = _mock_client(502)
    order = OrderRequest(
        market_id="T", side=Side.YES, action=OrderAction.BUY, order_type=OrderType.LIMIT,
        size=Decimal(1), limit_price=Decimal("0.40"), time_in_force=TimeInForce.FILL_OR_KILL,
    )
    with pytest.raises(ExchangeError) as excinfo:
        await client.place_order(order)
    assert not isinstance(excinfo.value, ServerError)
    assert "not retried" in str(excinfo.value)      # reached the 5xx branch, not AuthError
    assert "502" in str(excinfo.value)


# --- read vs mutation retry policy (added 2026-08-28) ------------------------
# A POST timeout is the ambiguous case: the exchange may have applied it and
# only lost the answer. Retrying that blindly risks a duplicate basket.


@pytest.mark.asyncio
async def test_transport_failure_on_mutation_is_ambiguous_and_not_retried():
    attempts = []

    def handler(request):
        attempts.append(request)
        raise httpx.ConnectTimeout("timed out")

    client = KalshiClient(base_url="https://x.test/trade-api/v2", api_key_id="k", private_key_path="p")
    client._private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    client._http = httpx.AsyncClient(base_url="https://x.test/trade-api/v2",
                                     transport=httpx.MockTransport(handler))
    order = OrderRequest(
        market_id="T", side=Side.YES, action=OrderAction.BUY, order_type=OrderType.LIMIT,
        size=Decimal(1), limit_price=Decimal("0.40"), client_order_id="kmra-entry-abc",
        time_in_force=TimeInForce.FILL_OR_KILL,
    )
    with pytest.raises(AmbiguousSubmissionError) as excinfo:
        await client.place_order(order)

    assert len(attempts) == 1                                   # NOT retried
    assert excinfo.value.client_order_ids == ["kmra-entry-abc"]  # names what to look up


@pytest.mark.asyncio
async def test_transport_failure_on_signed_read_is_still_retried():
    attempts = []

    def handler(request):
        attempts.append(request)
        raise httpx.ConnectTimeout("timed out")

    client = KalshiClient(base_url="https://x.test/trade-api/v2", api_key_id="k", private_key_path="p")
    client._private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    client._http = httpx.AsyncClient(base_url="https://x.test/trade-api/v2",
                                     transport=httpx.MockTransport(handler))
    with pytest.raises(httpx.TransportError):
        await client.get_positions()
    # 4 attempts: the backoff is sized to Kalshi's token buckets (~50ms to
    # afford a 10-token GET at 200 tokens/s), so more, faster retries fit
    # inside the same wall-clock budget the old 3x500ms schedule used.
    assert len(attempts) == 4     # idempotent read, safe to retry


@pytest.mark.asyncio
async def test_ambiguous_batch_error_carries_every_client_order_id():
    def handler(request):
        raise httpx.ReadTimeout("timed out")

    client = KalshiClient(base_url="https://x.test/trade-api/v2", api_key_id="k", private_key_path="p")
    client._private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    client._http = httpx.AsyncClient(base_url="https://x.test/trade-api/v2",
                                     transport=httpx.MockTransport(handler))
    orders = [
        OrderRequest(market_id=t, side=Side.YES, action=OrderAction.BUY, order_type=OrderType.LIMIT,
                     size=Decimal(1), limit_price=Decimal("0.40"), client_order_id=f"kmra-entry-{t}",
                     time_in_force=TimeInForce.FILL_OR_KILL)
        for t in ("A", "B", "C")
    ]
    with pytest.raises(AmbiguousSubmissionError) as excinfo:
        await client.place_orders_batch(orders)
    assert excinfo.value.client_order_ids == ["kmra-entry-A", "kmra-entry-B", "kmra-entry-C"]
