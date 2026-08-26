"""Kalshi WebSocket client — subscribes to the orderbook_delta channel for
a set of tickers and yields every message.

CONFIRMED LIVE 2026-08-25: connecting to wss://api.elections.kalshi.com/
trade-api/ws/v2 with no auth headers gets an immediate HTTP 401 — Kalshi's
WS upgrade requires the same signed KALSHI-ACCESS-KEY/SIGNATURE/TIMESTAMP
headers as authenticated REST even for market-data-only channels, exactly
as arb-bot's exchanges/kalshi_client.py docstring claimed.

CONFIRMED LIVE 2026-08-26: the signed handshake works with a correctly
registered API key (an earlier key was rejected 401/NOT_FOUND — a Kalshi-
side registration problem, not a signing bug; isolated by confirming a
signed REST call, e.g. GET /portfolio/balance, failed the same way).
Subscribing 668 tickers on one connection worked with zero errors — no
observed cap anywhere near what was originally guessed. The message schema
did NOT match this module's original assumptions, since those were
untested: real orderbook_snapshot levels arrive under `yes_dollars_fp`/
`no_dollars_fp` (not `yes`/`no`), and orderbook_delta carries `price_dollars`/
`delta_fp` (not `price`/`delta`) where `delta_fp` is a *signed relative
change* to the existing level (observed a live -999.00), not an absolute
size — see stream/monitor.py's LocalBook docstring for the fix and what
the original code would have done instead (raised on the first real delta).
"""

from __future__ import annotations

import asyncio
import json
import time
from typing import AsyncIterator, Iterable
from urllib.parse import urlparse

import structlog
import websockets

from kalshi.client import AuthError, KalshiClient, sign_kalshi_request

logger = structlog.get_logger(__name__)

_INITIAL_BACKOFF_S = 1.0
_MAX_BACKOFF_S = 30.0


class KalshiWSClient:
    def __init__(self, ws_url: str, client: KalshiClient) -> None:
        self._ws_url = ws_url
        self._ws_path = urlparse(ws_url).path
        self._client = client

    def _auth_headers(self) -> dict[str, str]:
        private_key = self._client.load_private_key()
        timestamp_ms = str(int(time.time() * 1000))
        signature = sign_kalshi_request(private_key, timestamp_ms, "GET", self._ws_path)
        return {
            "KALSHI-ACCESS-KEY": self._client.api_key_id,
            "KALSHI-ACCESS-SIGNATURE": signature,
            "KALSHI-ACCESS-TIMESTAMP": timestamp_ms,
        }

    async def stream(self, market_tickers: Iterable[str], channels: Iterable[str] = ("orderbook_delta",)) -> AsyncIterator[dict]:
        """Long-lived generator: connects, subscribes to `market_tickers`,
        yields every parsed JSON message, and transparently reconnects +
        re-subscribes with exponential backoff on any connection drop.
        Runs until the caller stops iterating (e.g. via a timeout wrapper
        in stream/monitor.py) — never returns on its own."""
        market_tickers = list(market_tickers)
        backoff = _INITIAL_BACKOFF_S
        while True:
            try:
                async with websockets.connect(self._ws_url, additional_headers=self._auth_headers()) as ws:
                    backoff = _INITIAL_BACKOFF_S
                    await ws.send(json.dumps({
                        "id": 1,
                        "cmd": "subscribe",
                        "params": {"channels": list(channels), "market_tickers": market_tickers},
                    }))
                    async for raw in ws:
                        try:
                            yield json.loads(raw)
                        except json.JSONDecodeError:
                            logger.warning("ws_message_not_json", raw=raw[:200])
            except (websockets.exceptions.WebSocketException, OSError) as exc:
                logger.warning("ws_disconnected", error=str(exc), retry_in_s=backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, _MAX_BACKOFF_S)
            except AuthError as exc:
                # Distinct from a transient disconnect — most likely a bad/
                # revoked/misconfigured key. Backs off the same way rather
                # than crashing the process (stream/monitor.py's caller
                # checks has_signing_credentials up front for the "not configured
                # at all" case; this covers "configured but wrong").
                logger.error("ws_auth_failed", error=str(exc), retry_in_s=backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, _MAX_BACKOFF_S)
