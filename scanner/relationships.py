"""Relationship model — generalizes scanner.filters.EventCandidate with
explicit trust provenance (auto-detected from Kalshi's own event metadata
vs. manually verified by a human) without changing today's trading
behavior by default.

config.loader.RelationshipsSettings.trust_auto_detected defaults to True
with an empty verified set, so build_relationships() with no manual
verification reproduces exactly what Stage 1 (scanner.filters.filter_events)
already selects, just wrapped in Relationship instead of EventCandidate —
this module is additive to scanner/filters.py, not a replacement for it.
scanner/filters.py's own auto-detection (mutually_exclusive + MECNET) is
unmodified; see its module docstring for the residual risk that flag alone
doesn't prove collective exhaustiveness, which manual verification exists
to reduce.

Only relationship_type="exhaustive_partition" (Kalshi's mutually_exclusive
+ MECNET structure) is supported today. The field exists so a future
relationship type has somewhere to go, not to speculatively build out a
type hierarchy nothing consumes yet.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Optional

from scanner.filters import EventCandidate

AUTO_SOURCE = "auto_kalshi_metadata"
MANUAL_SOURCE = "manual"


@dataclass(frozen=True)
class Relationship:
    event_ticker: str
    title: str
    leg_tickers: list[str]
    leg_volume_24h: dict[str, Optional[Decimal]]
    expected_settlement_time: datetime
    fee_multiplier: Decimal = Decimal(1)
    relationship_type: str = "exhaustive_partition"
    verified: bool = False
    source: str = AUTO_SOURCE

    @classmethod
    def from_event_candidate(
        cls, candidate: EventCandidate, *, verified: bool = False, source: str = AUTO_SOURCE
    ) -> "Relationship":
        return cls(
            event_ticker=candidate.event_ticker,
            title=candidate.title,
            leg_tickers=candidate.leg_tickers,
            leg_volume_24h=candidate.leg_volume_24h,
            expected_settlement_time=candidate.expected_settlement_time,
            fee_multiplier=candidate.fee_multiplier,
            verified=verified,
            source=source,
        )


def build_relationships(
    auto_candidates: list[EventCandidate],
    verified_event_tickers: frozenset[str] = frozenset(),
    *,
    trust_auto_detected: bool = True,
) -> list[Relationship]:
    """Wraps Stage 1's auto-detected candidates as Relationships, tagging
    provenance and upgrading trust for any event_ticker a human has
    manually verified (config/relationships.yaml, loaded into
    config.loader.RelationshipsSettings.verified_event_tickers).

    trust_auto_detected=False drops every candidate that isn't also in
    verified_event_tickers, so only explicitly human-verified relationships
    are returned at all — the eventual "replace auto-trust" migration path,
    not the default.

    Default arguments (empty verified set, trust_auto_detected=True)
    reproduce today's behavior exactly: every auto-detected candidate comes
    through unverified/auto-sourced, same set as scanner.filters.filter_events
    always produced.
    """
    relationships: list[Relationship] = []
    for candidate in auto_candidates:
        verified = candidate.event_ticker in verified_event_tickers
        if not trust_auto_detected and not verified:
            continue
        relationships.append(
            Relationship.from_event_candidate(
                candidate,
                verified=verified,
                source=MANUAL_SOURCE if verified else AUTO_SOURCE,
            )
        )
    return relationships
