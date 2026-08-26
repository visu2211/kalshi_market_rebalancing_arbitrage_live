"""Kalshi REST client — public market data, plus signed order placement for
execution/executor.py (demo-account only — see that module's docstring for
why prod is detection-only in this project).

Adapted from the sibling arb-bot project's exchanges/kalshi_client.py.
Market data (GET /markets, GET /markets/{ticker}/orderbook, GET /events) is
public — no auth required. Order placement (POST /portfolio/events/orders)
is signed the same way the WS handshake already is (sign_kalshi_request,
shared with kalshi/ws_client.py).

Orderbook shape verified live against arb-bot's development (2026-08-25):
Kalshi's book is bid-only per side — the response gives `yes` bids and `no`
bids, and because Kalshi is a binary complementary market the YES ask is
implied by the NO bid (ask = 1 - no_bid) and vice versa. We synthesize asks
from the opposite side's bids here so scanner/basket.py can treat this like
any other two-sided book.

Order placement CONFIRMED LIVE 2026-08-25 in arb-bot against the v2 create
endpoint (POST /portfolio/events/orders, not the docs' deprecated
/portfolio/orders) up through `insufficient_balance` — that demo account
had a $0 paper balance, so an actual resting order/fill hasn't been
observed even there. Fund the demo account via Kalshi's demo UI to get
past that.
"""

from __future__ import annotations

import base64
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlparse

import httpx
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential


class ExchangeError(Exception):
    """Base class for transport/auth-level errors talking to Kalshi."""


class AuthError(ExchangeError):
    """Signing or credential failure."""


class RateLimitError(ExchangeError):
    """Kalshi rate limit hit; caller should back off."""


class OrderRejectedError(ExchangeError):
    """Exchange rejected the order at submission time (e.g. insufficient_balance)."""


class Side(str, Enum):
    YES = "yes"
    NO = "no"


class OrderAction(str, Enum):
    BUY = "buy"
    SELL = "sell"


class OrderType(str, Enum):
    LIMIT = "limit"   # only mode implemented — see place_order


class OrderStatus(str, Enum):
    OPEN = "open"                          # resting on the book
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    REJECTED = "rejected"


@dataclass(frozen=True)
class OrderRequest:
    market_id: str                              # Kalshi ticker
    side: Side
    action: OrderAction
    order_type: OrderType
    size: Decimal                               # contracts
    limit_price: Decimal                        # required for LIMIT — the only order_type implemented
    client_order_id: Optional[str] = None       # idempotency key; auto-generated if omitted
    expiration_ts: Optional[int] = None         # unix seconds; see place_order's IOC note


@dataclass
class OrderResult:
    order_id: str
    client_order_id: Optional[str]
    status: OrderStatus
    filled_size: Decimal
    remaining_size: Decimal
    average_fill_price: Optional[Decimal]
    raw: dict = field(default_factory=dict)     # untouched exchange response, for debugging


@dataclass(frozen=True)
class OrderbookLevel:
    price: Decimal   # USD probability, e.g. Decimal("0.63")
    size: Decimal    # contracts available at this price


@dataclass(frozen=True)
class Orderbook:
    market_id: str                 # Kalshi ticker
    side: Side
    bids: list[OrderbookLevel]     # best (highest price) bid first
    asks: list[OrderbookLevel]     # best (lowest price) ask first
    timestamp: datetime

    @property
    def best_bid(self) -> Optional[OrderbookLevel]:
        return self.bids[0] if self.bids else None

    @property
    def best_ask(self) -> Optional[OrderbookLevel]:
        return self.asks[0] if self.asks else None


# Only retry transient/network-level failures. Auth misconfiguration is
# terminal — retrying won't fix it, and reraise=True means the ORIGINAL
# exception surfaces after retries are exhausted rather than tenacity's
# RetryError wrapper.
_transient_retry = retry(
    retry=retry_if_exception_type((httpx.TransportError, RateLimitError)),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=0.5, min=0.5, max=5),
    reraise=True,
)


def sign_kalshi_request(private_key: rsa.RSAPrivateKey, timestamp_ms: str, method: str, full_path: str) -> str:
    """RSA-PSS-SHA256 sign a Kalshi request, base64-encoded. Split out as a
    free function so it can be unit-tested (verify with the matching public
    key) without needing network access or real API credentials. Used for
    the WS handshake in kalshi/ws_client.py — this client never places
    orders, so no other request needs signing."""
    message = f"{timestamp_ms}{method}{full_path}".encode("utf-8")
    signature = private_key.sign(
        message,
        padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH),
        hashes.SHA256(),
    )
    return base64.b64encode(signature).decode("utf-8")


class KalshiClient:
    def __init__(
        self,
        base_url: str,
        request_timeout_s: float = 10.0,
        max_retries: int = 3,
        api_key_id: Optional[str] = None,
        private_key_path: Optional[str] = None,
    ) -> None:
        """api_key_id / private_key_path are only needed by kalshi/ws_client.py
        for the WS handshake — every method on this class works without them."""
        self._base_url = base_url.rstrip("/")
        self._base_path = urlparse(self._base_url).path.rstrip("/")  # e.g. "/trade-api/v2"
        self._timeout_s = request_timeout_s
        self._max_retries = max_retries
        self._api_key_id = api_key_id
        self._private_key_path = private_key_path
        self._private_key: Optional[rsa.RSAPrivateKey] = None
        self._http: Optional[httpx.AsyncClient] = None

    async def connect(self) -> None:
        if self._http is None:
            self._http = httpx.AsyncClient(base_url=self._base_url, timeout=self._timeout_s)

    async def close(self) -> None:
        if self._http is not None:
            await self._http.aclose()
            self._http = None

    async def __aenter__(self) -> "KalshiClient":
        await self.connect()
        return self

    async def __aexit__(self, *exc_info) -> None:
        await self.close()

    def _client(self) -> httpx.AsyncClient:
        if self._http is None:
            raise ExchangeError("KalshiClient used before connect() — use `async with` or call connect() first")
        return self._http

    @_transient_retry
    async def _get(self, path: str, params: Optional[dict] = None) -> dict:
        resp = await self._client().get(path, params=params)
        if resp.status_code == 429:
            raise RateLimitError(f"Kalshi rate limit hit on GET {path}")
        resp.raise_for_status()
        return resp.json()

    @_transient_retry
    async def _signed_request(self, method: str, path: str, json_body: Optional[dict] = None) -> dict:
        private_key = self.load_private_key()
        timestamp_ms = str(int(datetime.now(timezone.utc).timestamp() * 1000))
        full_path = f"{self._base_path}{path}"
        signature = sign_kalshi_request(private_key, timestamp_ms, method, full_path)
        headers = {
            "KALSHI-ACCESS-KEY": self._api_key_id,
            "KALSHI-ACCESS-SIGNATURE": signature,
            "KALSHI-ACCESS-TIMESTAMP": timestamp_ms,
        }
        resp = await self._client().request(method, path, json=json_body, headers=headers)
        if resp.status_code == 401:
            raise AuthError(f"Kalshi rejected signed {method} {path}: {resp.status_code} {resp.text}")
        if resp.status_code == 429:
            raise RateLimitError(f"Kalshi rate limit hit on {method} {path}")
        if resp.status_code >= 400:
            raise OrderRejectedError(f"Kalshi rejected {method} {path}: {resp.status_code} {resp.text}")
        if resp.status_code == 204 or not resp.content:
            return {}
        return resp.json()

    def load_private_key(self) -> rsa.RSAPrivateKey:
        """Loads and caches the RSA private key used to sign both the WS
        handshake and REST order placement. Raises AuthError if credentials
        aren't configured — callers that only need public market data never
        call this."""
        if self._private_key is not None:
            return self._private_key
        if not self._api_key_id or not self._private_key_path:
            raise AuthError(
                "KalshiClient: api_key_id and private_key_path are required for signed "
                "requests — set the appropriate KALSHI_*_API_KEY_ID / KALSHI_*_PRIVATE_KEY_PATH."
            )
        key_path = Path(self._private_key_path)
        if not key_path.exists():
            raise AuthError(f"KalshiClient: private key not found at {key_path}")
        key = serialization.load_pem_private_key(key_path.read_bytes(), password=None)
        if not isinstance(key, rsa.RSAPrivateKey):
            raise AuthError(f"KalshiClient: {key_path} is not an RSA private key")
        self._private_key = key
        return key

    @property
    def api_key_id(self) -> Optional[str]:
        return self._api_key_id

    @property
    def has_signing_credentials(self) -> bool:
        """Cheap presence check (no key-loading/parsing) so stream/monitor.py
        can skip Stage 3's WS streaming, or execution/executor.py's order
        placement, rather than crash, when credentials aren't configured.
        Gates both — the WS handshake and REST order signing share the same
        api_key_id/private_key_path pair."""
        return bool(self._api_key_id and self._private_key_path)

    @property
    def base_path(self) -> str:
        return self._base_path

    # ---- public market data (no auth) ----

    async def get_events(
        self,
        status: Optional[str] = None,
        limit: int = 200,
        cursor: Optional[str] = None,
        with_nested_markets: bool = False,
    ) -> dict:
        """GET /events."""
        params: dict[str, Any] = {"limit": limit, "with_nested_markets": with_nested_markets}
        if status:
            params["status"] = status
        if cursor:
            params["cursor"] = cursor
        return await self._get("/events", params=params)

    async def get_all_events(self, status: Optional[str] = None, with_nested_markets: bool = False) -> list[dict]:
        """Pages through GET /events via cursor until exhausted."""
        events: list[dict] = []
        cursor: Optional[str] = None
        while True:
            page = await self.get_events(status=status, cursor=cursor, with_nested_markets=with_nested_markets)
            page_events = page.get("events", [])
            events.extend(page_events)
            cursor = page.get("cursor")
            if not cursor or not page_events:
                break
        return events

    async def get_markets(
        self,
        event_ticker: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 200,
        cursor: Optional[str] = None,
    ) -> dict:
        """GET /markets — one page of markets."""
        params: dict[str, Any] = {"limit": limit}
        if event_ticker:
            params["event_ticker"] = event_ticker
        if status:
            params["status"] = status
        if cursor:
            params["cursor"] = cursor
        return await self._get("/markets", params=params)

    async def get_orderbook(self, market_id: str, side: Side = Side.YES, depth: int = 25) -> Orderbook:
        """GET /markets/{ticker}/orderbook — see module docstring for the
        bid-only-per-side -> synthesized-ask normalization.

        Live response shape (verified live in arb-bot 2026-08-25; the
        published API docs describe an older integer-cents format that the
        live API no longer returns):

            {"orderbook_fp": {"yes_dollars": [["0.4600", "30909.54"], ...],
                               "no_dollars":  [["0.2800", "4036.92"], ...]}}

        where each pair is [price_as_dollar_string, size_as_decimal_string].
        Falls back to the legacy `orderbook.yes`/`orderbook.no` integer-cents
        shape if that's ever what a given environment returns.
        """
        raw = await self._get(f"/markets/{market_id}/orderbook", params={"depth": depth})

        if "orderbook_fp" in raw:
            book = raw["orderbook_fp"] or {}
            yes_bids_raw = book.get("yes_dollars") or []
            no_bids_raw = book.get("no_dollars") or []
            price_parser = lambda p: Decimal(str(p))  # already dollars, e.g. "0.4600"
        else:
            book = raw.get("orderbook", {}) or {}
            yes_bids_raw = book.get("yes") or []
            no_bids_raw = book.get("no") or []
            price_parser = lambda p: Decimal(p) / Decimal(100)  # legacy integer cents

        def to_levels(raw_levels: list, descending: bool) -> list[OrderbookLevel]:
            levels = [
                OrderbookLevel(price=price_parser(price), size=Decimal(str(size)))
                for price, size in raw_levels
            ]
            return sorted(levels, key=lambda l: l.price, reverse=descending)

        yes_bids = to_levels(yes_bids_raw, descending=True)
        no_bids = to_levels(no_bids_raw, descending=True)

        # asks on one side = 1 - bids on the other side, re-sorted ascending (best/lowest ask first)
        yes_asks = sorted(
            (OrderbookLevel(price=Decimal(1) - lvl.price, size=lvl.size) for lvl in no_bids),
            key=lambda l: l.price,
        )
        no_asks = sorted(
            (OrderbookLevel(price=Decimal(1) - lvl.price, size=lvl.size) for lvl in yes_bids),
            key=lambda l: l.price,
        )

        if side == Side.YES:
            bids, asks = yes_bids, yes_asks
        else:
            bids, asks = no_bids, no_asks

        return Orderbook(
            market_id=market_id,
            side=side,
            bids=bids,
            asks=asks,
            timestamp=datetime.now(timezone.utc),
        )

    # ---- signed account state ----

    async def get_balance(self) -> Decimal:
        """GET /portfolio/balance — signed. VERIFIED LIVE 2026-08-26
        against the demo account (both unfunded, returning $0.00, and
        after funding, returning the funded amount). Used by
        execution/capital.py to check available capital live before
        committing to a basket, rather than only discovering a shortfall
        via an insufficient_balance rejection after the order's already
        been sent."""
        raw = await self._signed_request("GET", "/portfolio/balance")
        return Decimal(str(raw.get("balance_dollars", "0")))

    # ---- signed order placement (execution/executor.py) ----

    @staticmethod
    def _build_order_body(order: OrderRequest, client_order_id: str) -> dict[str, Any]:
        """Kalshi's v2 API only knows a single two-sided book quoted from
        the YES contract: side="bid" buys YES, side="ask" sells YES. Maps
        our Side(YES/NO) x OrderAction(BUY/SELL) onto that single bid/ask
        axis and flips the price to the YES-quote when the caller means
        the NO contract (execution/executor.py only ever trades the YES
        side, but this mapping is kept general rather than assuming that
        here). Shared by place_order (single) and place_orders_batch."""
        buying_yes = (order.side == Side.YES) == (order.action == OrderAction.BUY)
        kalshi_side = "bid" if buying_yes else "ask"
        price = order.limit_price if order.side == Side.YES else (Decimal(1) - order.limit_price)

        body: dict[str, Any] = {
            "ticker": order.market_id,
            "client_order_id": client_order_id,
            "side": kalshi_side,
            "count": str(int(order.size)),
            "price": f"{price:.2f}",
            "time_in_force": "good_till_canceled",
            "self_trade_prevention_type": "taker_at_cross",
        }
        if order.expiration_ts is not None:
            body["expiration_ts"] = order.expiration_ts
        return body

    @staticmethod
    def _parse_order_response(raw: dict, order: OrderRequest, client_order_id: str) -> OrderResult:
        """Shared response parsing for a single order's result, whether it
        came back from place_order's own response body or as one entry of
        place_orders_batch's per-order list — both carry the same
        fill_count/remaining_count/order_id/client_order_id fields."""
        filled = Decimal(str(raw.get("fill_count", 0) or 0))
        remaining = Decimal(str(raw.get("remaining_count", order.size) or order.size))
        if filled <= 0:
            status = OrderStatus.OPEN
        elif remaining > 0:
            status = OrderStatus.PARTIALLY_FILLED
        else:
            status = OrderStatus.FILLED
        avg_price_raw = raw.get("average_fill_price")

        return OrderResult(
            order_id=str(raw.get("order_id", "")),
            client_order_id=raw.get("client_order_id") or client_order_id,
            status=status,
            filled_size=filled,
            remaining_size=remaining,
            average_fill_price=Decimal(str(avg_price_raw)) if avg_price_raw is not None else None,
            raw=raw,
        )

    async def place_order(self, order: OrderRequest) -> OrderResult:
        """POST /portfolio/events/orders. See module docstring — field
        mapping and response shape confirmed live in the sibling arb-bot
        project 2026-08-25, up through insufficient_balance.

        Raises OrderRejectedError (not returned as a status) on an
        exchange-level reject like insufficient_balance — caller decides
        whether that should stop the rest of a multi-leg basket.

        order.expiration_ts (unix seconds), when set, is passed through as
        Kalshi's documented order-expiration field: any portion unfilled by
        that instant is auto-canceled instead of resting on the book. This
        project's IOC substitute (Kalshi's v2 API has no distinct
        immediate_or_cancel time_in_force) — execution/executor.py's
        no-slippage fill sets this a couple seconds out from "now" so a
        basket leg either matches immediately against the depth it was
        priced against, or cancels, rather than resting and filling later
        at a stale price. CONFIRMED LIVE 2026-08-26 against the demo
        account, including real fills (see execution/executor.py's module
        docstring for what that surfaced).
        """
        if order.order_type != OrderType.LIMIT:
            raise ExchangeError("KalshiClient.place_order: only LIMIT orders are implemented")

        client_order_id = order.client_order_id or str(uuid.uuid4())
        body = self._build_order_body(order, client_order_id)
        raw = await self._signed_request("POST", "/portfolio/events/orders", json_body=body)
        return self._parse_order_response(raw, order, client_order_id)

    async def place_orders_batch(self, orders: list[OrderRequest]) -> list[OrderResult]:
        """POST /portfolio/events/orders/batched — places every order in
        `orders` in a single request instead of one sequential round-trip
        per order, and returns results in the same order as `orders`
        (matched back via client_order_id, not response array position —
        not verified live whether Kalshi preserves request order).

        VERIFIED LIVE 2026-08-26: the plain /portfolio/orders/batched path
        (no "events") is a deprecated v1 endpoint (410,
        deprecated_v1_order_endpoint) — this one is the live v2 replacement.
        Response shape is {"orders": [{client_order_id, order_id,
        fill_count, remaining_count, ts_ms} | {client_order_id, error:
        {code, message, details}, ts_ms: null}]} — a per-order failure
        (e.g. insufficient_balance on just that leg) comes back as a
        structured entry alongside successes in one 200 response, not as
        an exception for the whole batch. Raises only on a request-level
        failure (e.g. the whole batch rejected before per-order
        processing) — execution/executor.py's caller is built to expect a
        per-leg OrderRejectedError-equivalent instead, so a failed order
        here surfaces as an OrderResult with status=REJECTED and the error
        detail in .raw, not a raised exception, letting the rest of the
        basket's results still come through.
        """
        if not orders:
            return []

        client_order_ids = [order.client_order_id or str(uuid.uuid4()) for order in orders]
        body = {
            "orders": [
                self._build_order_body(order, coid)
                for order, coid in zip(orders, client_order_ids)
            ]
        }
        raw = await self._signed_request("POST", "/portfolio/events/orders/batched", json_body=body)
        raw_by_client_order_id = {entry.get("client_order_id"): entry for entry in raw.get("orders", [])}

        results: list[OrderResult] = []
        for order, coid in zip(orders, client_order_ids):
            entry = raw_by_client_order_id.get(coid)
            if entry is None:
                # Shouldn't happen (every submitted order should get a
                # matching response entry) — degrade to REJECTED with a
                # synthetic error rather than raising, so the rest of the
                # batch's real results still come through to the caller.
                results.append(OrderResult(
                    order_id="", client_order_id=coid, status=OrderStatus.REJECTED,
                    filled_size=Decimal(0), remaining_size=order.size, average_fill_price=None,
                    raw={"error": {"code": "missing_response_entry"}},
                ))
                continue
            if "error" in entry:
                results.append(OrderResult(
                    order_id="", client_order_id=coid, status=OrderStatus.REJECTED,
                    filled_size=Decimal(0), remaining_size=order.size, average_fill_price=None,
                    raw=entry,
                ))
                continue
            results.append(self._parse_order_response(entry, order, coid))
        return results

    async def cancel_order(self, order_id: str) -> None:
        """DELETE /portfolio/events/orders/{order_id} — VERIFIED LIVE
        2026-08-26 (the plain /portfolio/orders/{order_id} path is the
        same deprecated v1 endpoint as place_orders_batch's). Used to
        immediately cancel a resting unfilled remainder rather than
        waiting out an order's expiration_ts — see
        execution/executor.py's unwind logic. Raises OrderRejectedError
        if the order can't be canceled (e.g. it already fully filled or
        was already canceled) — the caller treats that as informational,
        not fatal, since "nothing left to cancel" isn't a failure."""
        await self._signed_request("DELETE", f"/portfolio/events/orders/{order_id}")

    async def get_order(self, order_id: str) -> OrderResult:
        """GET /portfolio/orders/{order_id} — signed. VERIFIED LIVE
        2026-08-26. Note the path has no "/events/" segment, unlike
        place_order/place_orders_batch/cancel_order — this one lives
        under the older /portfolio/orders/ prefix and works fine as a GET
        (only the v1 POST/DELETE order-mutation endpoints under that
        prefix are deprecated). Used to poll an order's actual current
        status — e.g. confirming an unwind order eventually filled —
        rather than only trusting the snapshot returned at placement
        time. Response field names differ from place_order's own
        response (fill_count_fp/remaining_count_fp with an "_fp" suffix,
        vs fill_count/remaining_count without one) — this method
        normalizes both into the same OrderResult shape."""
        raw = await self._signed_request("GET", f"/portfolio/orders/{order_id}")
        order = raw.get("order", {})
        filled = Decimal(str(order.get("fill_count_fp", 0) or 0))
        remaining = Decimal(str(order.get("remaining_count_fp", 0) or 0))
        if filled <= 0:
            status = OrderStatus.OPEN
        elif remaining > 0:
            status = OrderStatus.PARTIALLY_FILLED
        else:
            status = OrderStatus.FILLED
        return OrderResult(
            order_id=str(order.get("order_id", order_id)),
            client_order_id=order.get("client_order_id"),
            status=status,
            filled_size=filled,
            remaining_size=remaining,
            average_fill_price=None,   # not present on this endpoint's response — only needed here for fill/remaining size, not price
            raw=order,
        )

    async def get_positions(self) -> dict[str, Decimal]:
        """GET /portfolio/positions — signed. VERIFIED LIVE 2026-08-26.
        Returns {ticker: net position} for every market with a nonzero
        position (flat/closed positions aren't included in Kalshi's
        response, so there's nothing to filter). Used by
        stream/monitor.py's position reconciliation to check our
        internally-tracked believed position against what Kalshi's own
        records actually show, catching any drift (a delayed unwind fill,
        a cancel that raced a fill, anything else) that fill-response
        bookkeeping alone wouldn't."""
        raw = await self._signed_request("GET", "/portfolio/positions")
        return {
            p["ticker"]: Decimal(str(p.get("position_fp", 0)))
            for p in raw.get("market_positions", [])
        }
