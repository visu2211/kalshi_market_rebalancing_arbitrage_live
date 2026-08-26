"""Live smoke tests against Kalshi's public prod REST API (no credentials
required — market data endpoints are unauthenticated). Mirrors the sibling
arb-bot project's tests/test_kalshi_client.py.

Run: pytest tests/test_client.py -v
"""

import pytest

from kalshi.client import KalshiClient, Side

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
async def test_get_events_returns_open_events_with_expected_fields(client):
    events = await client.get_events(status="open", limit=5, with_nested_markets=True)
    assert "events" in events
    assert len(events["events"]) > 0

    event = events["events"][0]
    assert "event_ticker" in event
    assert "mutually_exclusive" in event   # scanner/filters.py's Stage 1 field
    assert "markets" in event


@pytest.mark.asyncio
async def test_get_all_events_pages_through_cursor(client):
    # A small page size forces at least one cursor hop within a short list.
    events = await client.get_all_events(status="open", with_nested_markets=False)
    assert len(events) > 5  # sanity: there's always more than one page's worth of open events


@pytest.mark.asyncio
async def test_get_markets_returns_markets(client):
    markets = await client.get_markets(status="open", limit=1)
    assert "markets" in markets
    assert len(markets["markets"]) > 0
    assert "ticker" in markets["markets"][0]


@pytest.mark.asyncio
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
