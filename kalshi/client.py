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


class ServerError(ExchangeError):
    """Kalshi returned 5xx — transient on the exchange's side, so safe to
    retry for reads. Deliberately NOT raised for a mutating signed request:
    a 502 on an order POST may mean the order was accepted and only the
    response was lost, so retrying it is a duplicate-submission risk that
    client_order_id idempotency should not be relied on alone to absorb."""


class AmbiguousSubmissionError(ExchangeError):
    """A mutating request (order placement or cancellation) failed in a way
    that does NOT establish whether the exchange applied it.

    This is the distinction the client previously could not express, and
    collapsing it into "the request failed, so nothing happened" is the most
    dangerous default available: a lost entry response looks like a clean
    no-fill while real positions sit open, and a lost unwind response invites
    a second unwind that oversells.

    Raised for a mutation that hits a transport timeout/reset (the request may
    have reached the matching engine) or a 5xx (the engine may have processed
    it before failing to answer). NEVER retried automatically — the caller
    must resolve it by querying the submitted client_order_id values and
    reconciling against what the exchange actually holds. A clean rejection
    (4xx, 429) is not ambiguous and does not use this type.
    """

    def __init__(self, message: str, client_order_ids: list[str] | None = None) -> None:
        super().__init__(message)
        # The exact ids to look up when resolving what actually happened.
        self.client_order_ids = list(client_order_ids or [])


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


class TimeInForce(str, Enum):
    GOOD_TILL_CANCELED = "good_till_canceled"
    IMMEDIATE_OR_CANCEL = "immediate_or_cancel"
    FILL_OR_KILL = "fill_or_kill"


class OrderStatus(str, Enum):
    OPEN = "open"                          # still resting on the book, remaining_size > 0
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    CANCELED = "canceled"                  # zero fill AND nothing left resting — distinct from OPEN, added 2026-08-26 after CANCELED-with-zero-remaining was being mislabeled OPEN
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
    time_in_force: TimeInForce = TimeInForce.GOOD_TILL_CANCELED
    expiration_ts: Optional[int] = None         # compatibility name; serialized as V2 `expiration_time`
    tick_size: Decimal = Decimal("0.0001")      # V2 fixed-point precision; override from market price_ranges


@dataclass
class OrderResult:
    order_id: str
    client_order_id: Optional[str]
    status: OrderStatus
    filled_size: Decimal
    remaining_size: Decimal
    average_fill_price: Optional[Decimal]
    average_fee_paid: Optional[Decimal] = None
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
# Backoff sized to Kalshi's token buckets, not guessed. A 429 carries no
# penalty or cooldown: the bucket keeps refilling and the next request succeeds
# as soon as the balance covers the cost. On the Basic tier (read 200 tokens/s,
# default cost 10) a GET is affordable again ~50ms after a 429 — the previous
# 500ms floor was ~10x longer than needed, and long enough to push a REST
# confirmation past its 1s freshness bound (observed live: a 1.585s
# confirmation that was rejected as stale). Transport errors reuse the same
# schedule; they are rarer and the extra attempts still cap out quickly.
_transient_retry = retry(
    retry=retry_if_exception_type((httpx.TransportError, RateLimitError, ServerError)),
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=0.05, min=0.05, max=1.0),
    reraise=True,
)


def _client_order_ids_in(json_body: Optional[dict]) -> list[str]:
    """Pulls every client_order_id out of a single-order or batched body, so an
    ambiguous submission can name exactly what needs looking up."""
    if not json_body:
        return []
    if "orders" in json_body and isinstance(json_body["orders"], list):
        return [o["client_order_id"] for o in json_body["orders"] if o.get("client_order_id")]
    coid = json_body.get("client_order_id")
    return [coid] if coid else []


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
        # Every caller in this project handles ExchangeError and nothing else.
        # raise_for_status() used to leak httpx.HTTPStatusError straight past
        # those handlers, so a routine Kalshi 5xx killed the whole scanner loop
        # rather than being retried or skipped.
        if resp.status_code >= 500:
            raise ServerError(f"Kalshi server error on GET {path}: {resp.status_code}")
        if resp.status_code >= 400:
            raise ExchangeError(f"Kalshi rejected GET {path}: {resp.status_code} {resp.text[:200]}")
        try:
            return resp.json()
        except ValueError as exc:
            # A proxy/CDN error page in front of the API is still a 200 sometimes.
            raise ExchangeError(f"Kalshi returned a non-JSON body for GET {path}") from exc

    async def _signed_request(
        self,
        method: str,
        path: str,
        json_body: Optional[dict] = None,
        params: Optional[dict] = None,
    ) -> dict:
        """Reads and mutations get different retry policies.

        A signed GET is idempotent, so transport blips, 429s and 5xx are all
        retried. A POST/DELETE is not: a timeout or 5xx may mean the exchange
        already applied the order and only the answer was lost, so retrying is
        a duplicate-submission risk. Those surface as AmbiguousSubmissionError
        for the caller to resolve by client_order_id. client_order_id
        idempotency may well absorb a duplicate entry, but it is unverified
        here and cancel_order carries no such key at all, so it is not
        something to lean on.
        """
        if method == "GET":
            return await self._signed_request_retrying(method, path, json_body, params)
        try:
            return await self._signed_request_once(method, path, json_body, params)
        except httpx.TransportError as exc:
            raise AmbiguousSubmissionError(
                f"transport failure on {method} {path}: {exc} — the exchange may or may not "
                "have applied this request; resolve by client_order_id",
                client_order_ids=_client_order_ids_in(json_body),
            ) from exc

    @_transient_retry
    async def _signed_request_retrying(
        self,
        method: str,
        path: str,
        json_body: Optional[dict] = None,
        params: Optional[dict] = None,
    ) -> dict:
        return await self._signed_request_once(method, path, json_body, params)

    async def _signed_request_once(
        self,
        method: str,
        path: str,
        json_body: Optional[dict] = None,
        params: Optional[dict] = None,
    ) -> dict:
        private_key = self.load_private_key()
        timestamp_ms = str(int(datetime.now(timezone.utc).timestamp() * 1000))
        full_path = f"{self._base_path}{path}"
        signature = sign_kalshi_request(private_key, timestamp_ms, method, full_path)
        headers = {
            "KALSHI-ACCESS-KEY": self._api_key_id,
            "KALSHI-ACCESS-SIGNATURE": signature,
            "KALSHI-ACCESS-TIMESTAMP": timestamp_ms,
        }
        resp = await self._client().request(method, path, json=json_body, params=params, headers=headers)
        if resp.status_code == 401:
            raise AuthError(f"Kalshi rejected signed {method} {path}: {resp.status_code} {resp.text}")
        if resp.status_code == 429:
            raise RateLimitError(f"Kalshi rate limit hit on {method} {path}")
        if resp.status_code >= 500:
            # Retryable only for reads. A 5xx on a POST/DELETE may mean the
            # exchange already applied the mutation and merely lost the
            # response, so it surfaces as a plain (non-retried) ExchangeError
            # and the caller's own reconciliation decides what actually
            # happened — see ServerError's docstring.
            if method == "GET":
                raise ServerError(f"Kalshi server error on GET {path}: {resp.status_code}")
            raise AmbiguousSubmissionError(
                f"Kalshi server error on {method} {path}: {resp.status_code} — not retried; "
                "the request may or may not have been applied",
                client_order_ids=_client_order_ids_in(json_body),
            )
        if resp.status_code >= 400:
            raise OrderRejectedError(f"Kalshi rejected {method} {path}: {resp.status_code} {resp.text}")
        if resp.status_code == 204 or not resp.content:
            return {}
        try:
            return resp.json()
        except ValueError as exc:
            raise ExchangeError(f"Kalshi returned a non-JSON body for {method} {path}") from exc

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

    async def get_market(self, market_id: str) -> dict:
        """GET one market; used by recovery to distinguish settlement from drift."""
        raw = await self._get(f"/markets/{market_id}")
        return raw.get("market", raw)

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

        if order.size <= 0 or order.size != order.size.to_integral_value():
            raise ExchangeError("order size must be a positive whole number of contracts")
        if price <= 0 or price >= 1:
            raise ExchangeError("order price must be strictly between 0 and 1")
        if order.tick_size <= 0 or price % order.tick_size != 0:
            raise ExchangeError(f"order price {price} is not aligned to tick size {order.tick_size}")
        if order.time_in_force != TimeInForce.GOOD_TILL_CANCELED and order.expiration_ts is not None:
            raise ExchangeError("expiration_time is only valid with good_till_canceled")

        body: dict[str, Any] = {
            "ticker": order.market_id,
            "client_order_id": client_order_id,
            "side": kalshi_side,
            "count": str(int(order.size)),
            "price": format(price, "f"),
            "time_in_force": order.time_in_force.value,
            "self_trade_prevention_type": "taker_at_cross",
            "cancel_order_on_pause": True,
        }
        if order.expiration_ts is not None:
            body["expiration_time"] = order.expiration_ts
        return body

    @staticmethod
    def _derive_order_status(filled: Decimal, remaining: Decimal) -> OrderStatus:
        """Shared status derivation for place_order/place_orders_batch's
        response and get_order's polled response — both only give raw
        fill/remaining counts, not a status label Kalshi computes for us.
        filled<=0 alone isn't enough to mean "still resting" (OPEN): a
        canceled-with-zero-fill order also has filled<=0, but remaining is
        also 0 — that combination is CANCELED, not OPEN. Conflating the two
        (as an earlier version of this did) mislabels a genuinely closed
        order as still-live in the logs and dashboard."""
        if filled <= 0:
            return OrderStatus.CANCELED if remaining <= 0 else OrderStatus.OPEN
        if remaining > 0:
            return OrderStatus.PARTIALLY_FILLED
        return OrderStatus.FILLED

    @classmethod
    def _parse_order_response(cls, raw: dict, order: OrderRequest, client_order_id: str) -> OrderResult:
        """Shared response parsing for a single order's result, whether it
        came back from place_order's own response body or as one entry of
        place_orders_batch's per-order list — both carry the same
        fill_count/remaining_count/order_id/client_order_id fields."""
        filled = Decimal(str(raw.get("fill_count", 0) or 0))
        remaining = Decimal(str(raw.get("remaining_count", order.size) or order.size))
        status = cls._derive_order_status(filled, remaining)
        avg_price_raw = raw.get("average_fill_price")
        avg_fee_raw = raw.get("average_fee_paid")

        return OrderResult(
            order_id=str(raw.get("order_id", "")),
            client_order_id=raw.get("client_order_id") or client_order_id,
            status=status,
            filled_size=filled,
            remaining_size=remaining,
            average_fill_price=Decimal(str(avg_price_raw)) if avg_price_raw is not None else None,
            average_fee_paid=Decimal(str(avg_fee_raw)) if avg_fee_raw is not None else None,
            raw=raw,
        )

    async def place_order(self, order: OrderRequest) -> OrderResult:
        """POST /portfolio/events/orders. See module docstring — field
        mapping and response shape confirmed live in the sibling arb-bot
        project 2026-08-25, up through insufficient_balance.

        Raises OrderRejectedError (not returned as a status) on an
        exchange-level reject like insufficient_balance — caller decides
        whether that should stop the rest of a multi-leg basket.

        `time_in_force` supports native IOC/FOK/GTC. `expiration_ts`, when
        supplied for a GTC order, is serialized using V2's documented
        `expiration_time` field. IOC/FOK cannot be combined with an
        expiration. Prices retain fixed-point precision and must align to
        the request's market tick size.
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
                    average_fee_paid=None,
                    raw={"error": {"code": "missing_response_entry"}},
                ))
                continue
            if "error" in entry:
                results.append(OrderResult(
                    order_id="", client_order_id=coid, status=OrderStatus.REJECTED,
                    filled_size=Decimal(0), remaining_size=order.size, average_fill_price=None,
                    average_fee_paid=None,
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
        return self._parse_portfolio_order(order, order_id)

    @classmethod
    def _parse_portfolio_order(cls, order: dict, fallback_order_id: str = "") -> OrderResult:
        """Normalize Get Order/Get Orders' portfolio-order representation."""
        filled = Decimal(str(order.get("fill_count_fp", order.get("fill_count", 0)) or 0))
        remaining = Decimal(str(order.get("remaining_count_fp", order.get("remaining_count", 0)) or 0))
        status = cls._derive_order_status(filled, remaining)
        avg_price_raw = order.get("average_fill_price")
        avg_fee_raw = order.get("average_fee_paid")
        if filled > 0 and avg_price_raw is None:
            fill_cost = Decimal(str(order.get("taker_fill_cost_dollars", 0) or 0)) + Decimal(
                str(order.get("maker_fill_cost_dollars", 0) or 0)
            )
            if fill_cost:
                avg_price_raw = fill_cost / filled
        if filled > 0 and avg_fee_raw is None:
            fees = Decimal(str(order.get("taker_fees_dollars", 0) or 0)) + Decimal(
                str(order.get("maker_fees_dollars", 0) or 0)
            )
            avg_fee_raw = fees / filled
        return OrderResult(
            order_id=str(order.get("order_id", fallback_order_id)),
            client_order_id=order.get("client_order_id"),
            status=status,
            filled_size=filled,
            remaining_size=remaining,
            average_fill_price=Decimal(str(avg_price_raw)) if avg_price_raw is not None else None,
            average_fee_paid=Decimal(str(avg_fee_raw)) if avg_fee_raw is not None else None,
            raw=order,
        )

    async def get_orders(
        self,
        status: Optional[str] = None,
        client_order_id_prefix: Optional[str] = None,
    ) -> list[OrderResult]:
        """Return all current orders, paginating and optionally filtering ownership.

        Kalshi supports server-side status filtering but not a client-order-id
        prefix, so the latter is deliberately applied locally after all pages
        have been retrieved.
        """
        results: list[OrderResult] = []
        cursor: Optional[str] = None
        while True:
            params: dict[str, Any] = {"limit": 1000}
            if status is not None:
                params["status"] = status
            if cursor:
                params["cursor"] = cursor
            raw = await self._signed_request("GET", "/portfolio/orders", params=params)
            for order in raw.get("orders", []):
                client_id = str(order.get("client_order_id", ""))
                if client_order_id_prefix is None or client_id.startswith(client_order_id_prefix):
                    results.append(self._parse_portfolio_order(order))
            cursor = raw.get("cursor") or None
            if not cursor:
                return results

    async def resolve_client_order_ids(self, client_order_ids: list[str]) -> dict[str, OrderResult]:
        """Look up what the exchange actually holds for specific submitted ids.

        This is how an AmbiguousSubmissionError gets settled: after a lost
        response, the client_order_id is the only handle we have on an order
        whose exchange-assigned order_id we never received. Returns only the
        ids Kalshi knows about — an id absent from the result was genuinely
        never accepted, which is the only case where treating it as "nothing
        happened" is safe.

        Reads every status, not just resting orders: an order that filled and
        closed between submission and this lookup must still be found, or the
        caller would wrongly conclude nothing happened.
        """
        wanted = set(client_order_ids)
        if not wanted:
            return {}
        found: dict[str, OrderResult] = {}
        for order in await self.get_orders():
            coid = order.client_order_id
            if coid in wanted:
                found[coid] = order
        return found

    async def cancel_orders(self, order_ids: list[str]) -> dict[str, Optional[str]]:
        """Best-effort cancel exact known IDs; returns ID -> error or None.

        Exact IDs keep emergency cancellation strategy-scoped. The sequential
        V2 endpoint is used intentionally so one malformed/already-terminal ID
        cannot reject cancellation of every other order in a batch.
        """
        outcomes: dict[str, Optional[str]] = {}
        for order_id in dict.fromkeys(order_ids):
            try:
                await self.cancel_order(order_id)
            except ExchangeError as exc:
                outcomes[order_id] = str(exc)
            else:
                outcomes[order_id] = None
        return outcomes

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
        positions: dict[str, Decimal] = {}
        cursor: str | None = None
        while True:
            params: dict[str, Any] = {"limit": 1000, "count_filter": "position"}
            if cursor:
                params["cursor"] = cursor
            raw = await self._signed_request("GET", "/portfolio/positions", params=params)
            for position in raw.get("market_positions", []):
                positions[position["ticker"]] = Decimal(str(position.get("position_fp", 0)))
            cursor = raw.get("cursor") or None
            if not cursor:
                return positions
