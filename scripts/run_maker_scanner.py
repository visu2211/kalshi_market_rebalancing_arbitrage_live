"""Entrypoint for the maker-side strategy — parallel to run_scanner.py.

Reads config, acquires the maker-specific process lock (separate from
the taker's lock, so both strategies can run against the same account
simultaneously), opens the Kalshi clients, loads risk state + inventory,
and hands control to stream.maker_monitor.MakerMonitor.

    python scripts/run_maker_scanner.py [--confirm-live-trading]

Refuses to start if maker_execution.enabled is false. For prod, the
flag is required the same way run_scanner.py requires it for the taker
— one more human-in-the-loop gate before any real-money order is
placed.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import fcntl
import logging
import os
import sys
from pathlib import Path

import structlog

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from config.loader import Settings, load_settings           # noqa: E402
from execution.maker.inventory import MakerInventoryBook     # noqa: E402
from execution.maker.risk import MakerRiskState              # noqa: E402
from kalshi.client import KalshiClient                        # noqa: E402
from kalshi.ws_client import KalshiWSClient                   # noqa: E402
from stream.maker_monitor import MakerMonitor, install_shutdown_handlers  # noqa: E402

logger = structlog.get_logger(__name__)


@contextlib.contextmanager
def _maker_process_lock(path: Path):
    """Non-blocking flock on a maker-owned file. Separate from the
    taker's lock so both can run — this is one of the plan's three
    isolation legs."""
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = path.open("a+")
    try:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError(f"another maker scanner already holds {path}") from exc
        handle.seek(0)
        handle.truncate()
        handle.write(str(os.getpid()))
        handle.flush()
        yield
    finally:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        finally:
            handle.close()


def _configure_logging(level: str) -> None:
    logging.basicConfig(format="%(message)s", level=level)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(logging.getLevelName(level)),
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.add_log_level,
            structlog.processors.KeyValueRenderer(sort_keys=True),
        ],
    )


async def run(settings: Settings) -> None:
    mk = settings.maker_execution
    if not mk.enabled and not mk.observe_only:
        logger.warning("maker_disabled — nothing to do", strategy_id=mk.strategy_id)
        return

    async with contextlib.AsyncExitStack() as stack:
        public_client = await stack.enter_async_context(KalshiClient(
            base_url=settings.kalshi.base_url,
            request_timeout_s=settings.kalshi.request_timeout_s,
            max_retries=settings.kalshi.max_retries,
            api_key_id=settings.kalshi.api_key_id,
            private_key_path=settings.kalshi.private_key_path,
        ))

        # observe_only never submits or reads private channels, so it doesn't
        # need an exec client at all — reuse public_client to satisfy the
        # MakerMonitor constructor. This also means observe_only can run on
        # prod without the maker ever holding prod execution credentials.
        if mk.observe_only and not mk.enabled:
            exec_client = public_client
        elif mk.target == "prod":
            exec_client = await stack.enter_async_context(KalshiClient(
                base_url=settings.kalshi.base_url,
                request_timeout_s=settings.kalshi.request_timeout_s,
                max_retries=settings.kalshi.max_retries,
                api_key_id=settings.kalshi.api_key_id,
                private_key_path=settings.kalshi.private_key_path,
            ))
        else:
            if not settings.execution.demo_api_key_id or not settings.execution.demo_private_key_path:
                raise RuntimeError("maker demo requires KALSHI_DEMO_API_KEY_ID / KALSHI_DEMO_PRIVATE_KEY_PATH")
            exec_client = await stack.enter_async_context(KalshiClient(
                base_url=settings.execution.demo_base_url,
                request_timeout_s=settings.kalshi.request_timeout_s,
                max_retries=settings.kalshi.max_retries,
                api_key_id=settings.execution.demo_api_key_id,
                private_key_path=settings.execution.demo_private_key_path,
            ))

        public_ws = KalshiWSClient(settings.kalshi.ws_url, public_client)
        private_ws = KalshiWSClient(settings.kalshi.ws_url, exec_client)

        risk = MakerRiskState.load(mk.risk_state_path, mk.strategy_id)
        inventory = MakerInventoryBook(
            flatten_on_leg_fill=mk.flatten_on_leg_fill,
            max_inventory_per_event_usd=mk.max_inventory_per_event_usd,
        )

        monitor = MakerMonitor(
            settings=settings,
            public_client=public_client,
            exec_client=exec_client,
            public_ws=public_ws,
            private_ws=private_ws,
            risk=risk,
            inventory=inventory,
        )
        install_shutdown_handlers(monitor)
        await monitor.run()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--confirm-live-trading",
        action="store_true",
        help="required process-level confirmation when maker_execution.target is prod (REAL MONEY)",
    )
    args = parser.parse_args()

    settings = load_settings()
    mk = settings.maker_execution
    if mk.enabled and mk.target == "prod" and not args.confirm_live_trading:
        parser.error("production maker execution requires --confirm-live-trading")
    _configure_logging(settings.logging.level)

    lock_ctx = (
        _maker_process_lock(mk.process_lock_path)
        if (mk.enabled or mk.observe_only) else contextlib.nullcontext()
    )
    with lock_ctx:
        asyncio.run(run(settings))


if __name__ == "__main__":
    main()
