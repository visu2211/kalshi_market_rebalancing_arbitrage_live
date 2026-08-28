"""Safely clear a tripped circuit breaker and re-sync the risk ledger.

The breaker is deliberately durable: once tripped it survives restarts, so a
process bounce can never silently re-arm execution into the same failure. That
left no supported way back, and the only route was hand-editing
data/risk_state.json — error-prone precisely when you are least calm.

This does the minimum, and refuses to guess:

  * Reads the exchange's ACTUAL positions and writes them into
    believed_positions, so reconciliation starts from truth rather than from
    whatever the ledger held when execution halted. A halt can leave the ledger
    reporting flat while a real position is open (observed live 2026-08-28).
  * Clears `tripped` / `tripped_reason` only.

What it deliberately does NOT reset:

  * daily_basket_attempts — a spent attempt is spent; resetting it would turn a
    canary limit into a suggestion.
  * attempted_events_at — the per-event cooldown exists so a bad event is not
    retried immediately, which is exactly the situation after a trip.
  * daily_realized_loss_usd / daily_notional_usd — real money already moved.

    uv run python scripts/reset_breaker.py            # dry run, shows the diff
    uv run python scripts/reset_breaker.py --confirm  # apply
"""

from __future__ import annotations

import argparse
import asyncio
import fcntl
import sys
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.loader import load_settings
from execution.risk import RiskState
from kalshi.client import ExchangeError, KalshiClient, OrderStatus
from execution.executor import STRATEGY_CLIENT_ORDER_ID_PREFIX


def _scanner_is_running(lock_path: Path) -> bool:
    """True if an armed scanner still holds the execution lock.

    Resetting underneath a live process would let it keep running on the state
    it loaded at startup, then overwrite this file from memory.
    """
    if not lock_path.exists():
        return False
    handle = lock_path.open("a+")
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        return True
    else:
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        return False
    finally:
        handle.close()


async def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--confirm", action="store_true", help="apply the reset (default is a dry run)")
    args = parser.parse_args()

    settings = load_settings()
    state = RiskState.load(settings.execution.risk_state_path)

    if _scanner_is_running(settings.execution.process_lock_path):
        print("REFUSED: an armed scanner still holds "
              f"{settings.execution.process_lock_path}. Stop it first, or it will "
              "overwrite this reset from its in-memory state.")
        return 1

    if settings.execution.target == "prod":
        client = KalshiClient(
            base_url=settings.kalshi.base_url,
            request_timeout_s=settings.kalshi.request_timeout_s,
            api_key_id=settings.kalshi.api_key_id,
            private_key_path=settings.kalshi.private_key_path,
        )
    else:
        client = KalshiClient(
            base_url=settings.execution.demo_base_url,
            request_timeout_s=settings.kalshi.request_timeout_s,
            api_key_id=settings.execution.demo_api_key_id,
            private_key_path=settings.execution.demo_private_key_path,
        )

    async with client:
        try:
            actual = await client.get_positions()
            owned = await client.get_orders(client_order_id_prefix=STRATEGY_CLIENT_ORDER_ID_PREFIX)
        except ExchangeError as exc:
            print(f"REFUSED: cannot read account state, so the ledger cannot be trusted after a reset: {exc}")
            return 1

    live = [o for o in owned if o.order_id and o.status in (OrderStatus.OPEN, OrderStatus.PARTIALLY_FILLED)]
    if live:
        print(f"REFUSED: {len(live)} bot-owned order(s) are still live at the exchange. "
              "Cancel them before resetting — otherwise the next run inherits unknown exposure.")
        for o in live:
            print(f"   {o.order_id}  filled={o.filled_size}  remaining={o.remaining_size}")
        return 1

    believed = {k: v for k, v in state.believed_positions.items() if v != 0}
    nonzero_actual = {k: v for k, v in actual.items() if v != 0}

    print(f"target                 : {settings.execution.target}")
    print(f"breaker tripped        : {state.tripped}")
    if state.tripped_reason:
        print(f"reason                 : {state.tripped_reason[:160]}")
    print(f"believed positions     : {believed or 'flat'}")
    print(f"actual positions       : {nonzero_actual or 'flat'}")
    print(f"attempts today         : {state.daily_basket_attempts}/{settings.execution.max_baskets_per_day}  (NOT reset)")
    print(f"realized loss today    : ${state.daily_realized_loss_usd}  (NOT reset)")
    print(f"event cooldowns held   : {sorted(state.attempted_events_at)}  (NOT reset)")

    drift = {t for t in set(believed) | set(nonzero_actual)
             if believed.get(t, Decimal(0)) != nonzero_actual.get(t, Decimal(0))}
    print(f"\nwill sync {len(drift)} position(s) to the exchange's view: {sorted(drift) or 'none'}")
    print("will clear: tripped, tripped_reason")

    if not args.confirm:
        print("\nDRY RUN — nothing written. Re-run with --confirm to apply.")
        return 0

    state.believed_positions = dict(nonzero_actual)
    state.tripped = False
    state.tripped_reason = None
    state.persist(settings.execution.risk_state_path)
    print(f"\nAPPLIED. Breaker cleared, positions synced to the exchange. "
          f"{settings.execution.max_baskets_per_day - state.daily_basket_attempts} attempt(s) remain today.")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
