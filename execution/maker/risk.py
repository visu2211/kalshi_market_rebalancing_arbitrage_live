"""Durable risk state for the maker strategy.

Mirrors execution/risk.py's persist-before-submit discipline but is a
*separate file* at maker_execution.risk_state_path, holding only maker
state. The two strategies never touch each other's risk files — it's the
"total isolation" leg of the plan's three isolation legs (state files,
order store, process lock). If both strategies ever need a shared
cross-strategy exposure read, that's a composition at a higher layer, not
a merge of these two files.

The maker's risk shape is narrower than the taker's because the taker
lives and dies by atomic batch math (need to know every open order, every
in-flight unwind, down to the cent), while the maker's position is
decomposable into per-event inventory the in-memory MakerInventoryBook
already carries. So this file persists only what must survive a restart:

- strategy_id header (sanity check — refusing to load a file written by a
  different strategy_id prevents accidentally pointing two strategies at
  the same path)
- utc_date (daily counter resets on date change)
- daily_basket_attempts (per-day cap)
- attempted_events_at (durable event_cooldown across restarts)
- tripped / tripped_reason (breaker flag)

NOT persisted (and deliberately so): the live inventory picture. That
lives in-memory in MakerInventoryBook and is rebuilt on startup from
MakerOrderStore — the sibling layer whose job is exactly "what resting
orders exist and how much has each one filled." Trying to persist
inventory here too would create two sources of truth for the same fact.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Optional


class MakerRiskStateError(RuntimeError):
    """Raised when the risk-state file exists but cannot be safely loaded
    — bad JSON, wrong strategy_id, unrecognized schema. Never raised for
    a simply-missing file (that's first-run)."""


@dataclass
class MakerRiskState:
    strategy_id: str
    utc_date: str                                         # ISO date; rolled over in record_basket_attempt
    daily_basket_attempts: int = 0
    attempted_events_at: dict[str, float] = field(default_factory=dict)   # event_ticker -> monotonic/epoch timestamp of last attempt
    tripped: bool = False
    tripped_reason: Optional[str] = None
    version: int = 1                                     # bump if the on-disk shape ever changes incompatibly

    @classmethod
    def empty(cls, strategy_id: str) -> "MakerRiskState":
        return cls(strategy_id=strategy_id, utc_date=datetime.now(timezone.utc).date().isoformat())

    @classmethod
    def load(cls, path: Path, strategy_id: str) -> "MakerRiskState":
        """Load from `path`; a missing file returns an empty state. A file
        with a different strategy_id is a configuration error — refuse,
        don't silently take it over."""
        if not path.exists():
            return cls.empty(strategy_id)
        try:
            raw = json.loads(path.read_text())
        except json.JSONDecodeError as exc:
            raise MakerRiskStateError(f"maker risk state at {path} is not valid JSON: {exc}") from exc
        file_strategy = raw.get("strategy_id")
        if file_strategy != strategy_id:
            raise MakerRiskStateError(
                f"maker risk state at {path} belongs to strategy_id {file_strategy!r}, "
                f"but config has {strategy_id!r} — refusing to load. If this is intentional, "
                f"move the old file aside; we never auto-overwrite."
            )
        return cls(
            strategy_id=file_strategy,
            utc_date=raw.get("utc_date", datetime.now(timezone.utc).date().isoformat()),
            daily_basket_attempts=int(raw.get("daily_basket_attempts", 0)),
            attempted_events_at={
                str(k): float(v) for k, v in (raw.get("attempted_events_at") or {}).items()
            },
            tripped=bool(raw.get("tripped", False)),
            tripped_reason=raw.get("tripped_reason"),
            version=int(raw.get("version", 1)),
        )

    def save(self, path: Path) -> None:
        """Atomic write: tmp + rename, same discipline as execution.risk.
        A crash mid-write never produces a half-written file."""
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps({
            "strategy_id": self.strategy_id,
            "utc_date": self.utc_date,
            "daily_basket_attempts": self.daily_basket_attempts,
            "attempted_events_at": dict(self.attempted_events_at),
            "tripped": self.tripped,
            "tripped_reason": self.tripped_reason,
            "version": self.version,
        }))
        tmp.replace(path)

    def _roll_date_if_needed(self, now: Optional[datetime] = None) -> None:
        now = now or datetime.now(timezone.utc)
        today = now.date().isoformat()
        if self.utc_date != today:
            self.utc_date = today
            self.daily_basket_attempts = 0

    def record_basket_attempt(self, event_ticker: str, now: Optional[datetime] = None) -> None:
        """Called BEFORE submitting a quoted basket. Rolls the daily
        counter, bumps the attempt count, and timestamps the event for
        cooldown tracking. Caller is expected to save() after returning."""
        self._roll_date_if_needed(now)
        self.daily_basket_attempts += 1
        self.attempted_events_at[event_ticker] = time.time()

    def in_event_cooldown(self, event_ticker: str, cooldown_s: int, now: Optional[float] = None) -> bool:
        """Returns True iff this event was attempted within the cooldown
        window. Used by the executor to refuse re-quoting an event too
        soon after a prior maker basket resolved (filled, flattened, or
        canceled)."""
        last = self.attempted_events_at.get(event_ticker)
        if last is None:
            return False
        current = now if now is not None else time.time()
        return (current - last) < cooldown_s

    def trip(self, reason: str) -> None:
        """Set the breaker. Reason is written verbatim into the file so
        the next startup can print exactly why. Idempotent — tripping a
        tripped breaker preserves the earlier reason."""
        if not self.tripped:
            self.tripped = True
            self.tripped_reason = reason

    def reset_breaker(self) -> None:
        """Explicit clear — called only by scripts/reset_breaker_maker.py
        (or equivalent); the loop never clears itself. Mirrors the
        taker's manual-only reset discipline."""
        self.tripped = False
        self.tripped_reason = None
