"""Durable, fail-closed strategy risk ledger.

This is intentionally independent of the scanner loop so it can be loaded
and validated before any order is submitted.  It records conservative cost
exposure (not mark-to-market value), daily submitted notional, realized
losses reported by execution, and recent order timestamps.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path


class RiskStateError(RuntimeError):
    pass


@dataclass
class RiskState:
    utc_date: str
    tripped: bool = False
    tripped_reason: str | None = None
    daily_notional_usd: Decimal = Decimal(0)
    daily_realized_loss_usd: Decimal = Decimal(0)
    daily_basket_attempts: int = 0
    event_exposure_usd: dict[str, Decimal] = field(default_factory=dict)
    believed_positions: dict[str, Decimal] = field(default_factory=dict)
    ticker_to_event: dict[str, str] = field(default_factory=dict)
    owned_open_order_ids: set[str] = field(default_factory=set)
    recent_order_timestamps: list[float] = field(default_factory=list)
    attempted_events_at: dict[str, float] = field(default_factory=dict)
    session_basket_attempts: int = 0  # deliberately not persisted; scoped to this process

    @classmethod
    def empty(cls, now: datetime | None = None) -> "RiskState":
        now = now or datetime.now(timezone.utc)
        return cls(utc_date=now.date().isoformat())

    @classmethod
    def load(cls, path: Path, now: datetime | None = None) -> "RiskState":
        if not path.exists():
            return cls.empty(now)
        try:
            raw = json.loads(path.read_text())
            state = cls(
                utc_date=str(raw["utc_date"]),
                tripped=bool(raw.get("tripped", False)),
                tripped_reason=raw.get("tripped_reason"),
                daily_notional_usd=Decimal(str(raw.get("daily_notional_usd", 0))),
                daily_realized_loss_usd=Decimal(str(raw.get("daily_realized_loss_usd", 0))),
                daily_basket_attempts=int(raw.get("daily_basket_attempts", 0)),
                event_exposure_usd={k: Decimal(str(v)) for k, v in raw.get("event_exposure_usd", {}).items()},
                believed_positions={k: Decimal(str(v)) for k, v in raw.get("believed_positions", {}).items()},
                ticker_to_event={str(k): str(v) for k, v in raw.get("ticker_to_event", {}).items()},
                owned_open_order_ids=set(raw.get("owned_open_order_ids", [])),
                recent_order_timestamps=[float(v) for v in raw.get("recent_order_timestamps", [])],
                attempted_events_at={str(k): float(v) for k, v in raw.get("attempted_events_at", {}).items()},
            )
        except (OSError, ValueError, TypeError, KeyError, json.JSONDecodeError) as exc:
            raise RiskStateError(f"cannot safely load risk state {path}: {exc}") from exc
        state.roll_day(now or datetime.now(timezone.utc))
        return state

    def roll_day(self, now: datetime) -> None:
        today = now.astimezone(timezone.utc).date().isoformat()
        if self.utc_date != today:
            self.utc_date = today
            self.daily_notional_usd = Decimal(0)
            self.daily_realized_loss_usd = Decimal(0)
            self.daily_basket_attempts = 0
            self.recent_order_timestamps.clear()

    @property
    def gross_exposure_usd(self) -> Decimal:
        return sum(self.event_exposure_usd.values(), start=Decimal(0))

    def persist(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "version": 1,
            "utc_date": self.utc_date,
            "tripped": self.tripped,
            "tripped_reason": self.tripped_reason,
            "daily_notional_usd": str(self.daily_notional_usd),
            "daily_realized_loss_usd": str(self.daily_realized_loss_usd),
            "daily_basket_attempts": self.daily_basket_attempts,
            "event_exposure_usd": {k: str(v) for k, v in self.event_exposure_usd.items()},
            "believed_positions": {k: str(v) for k, v in self.believed_positions.items()},
            "ticker_to_event": self.ticker_to_event,
            "owned_open_order_ids": sorted(self.owned_open_order_ids),
            "recent_order_timestamps": self.recent_order_timestamps,
            "attempted_events_at": self.attempted_events_at,
        }
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(payload, sort_keys=True))
        tmp.replace(path)

    def trip(self, reason: str) -> None:
        self.tripped = True
        self.tripped_reason = self.tripped_reason or reason
