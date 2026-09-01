"""scanner.relationships.build_relationships — pure, no network. Verifies
the two properties the merge plan requires: (1) default arguments (empty
verified set, trust_auto_detected=True) reproduce scanner.filters'
auto-detected candidates exactly, just wrapped as Relationship, so today's
trading behavior is unchanged; (2) manual verification upgrades trust
without dropping anything, while trust_auto_detected=False flips to
"only explicitly verified relationships trade" — the eventual
replace-auto-trust migration path, not the default."""

from datetime import datetime, timezone
from decimal import Decimal

from scanner.filters import EventCandidate
from scanner.relationships import AUTO_SOURCE, MANUAL_SOURCE, Relationship, build_relationships

NOW = datetime(2026, 8, 25, 12, 0, 0, tzinfo=timezone.utc)


def _candidate(event_ticker, leg_tickers=("A", "B"), fee_multiplier=Decimal(1)):
    return EventCandidate(
        event_ticker=event_ticker,
        title=f"Event {event_ticker}",
        leg_tickers=list(leg_tickers),
        leg_volume_24h={t: Decimal("10") for t in leg_tickers},
        expected_settlement_time=NOW,
        fee_multiplier=fee_multiplier,
    )


def test_default_arguments_reproduce_auto_detected_candidates_unverified():
    candidates = [_candidate("KXFED-1"), _candidate("KXNBA-1", leg_tickers=("X", "Y", "Z"))]

    relationships = build_relationships(candidates)

    assert len(relationships) == 2
    by_ticker = {r.event_ticker: r for r in relationships}
    assert by_ticker["KXFED-1"].leg_tickers == ["A", "B"]
    assert by_ticker["KXNBA-1"].leg_tickers == ["X", "Y", "Z"]
    assert all(not r.verified for r in relationships)
    assert all(r.source == AUTO_SOURCE for r in relationships)
    assert all(r.relationship_type == "exhaustive_partition" for r in relationships)


def test_manual_verification_upgrades_trust_without_dropping_auto_detected():
    candidates = [_candidate("KXFED-1"), _candidate("KXNBA-1")]

    relationships = build_relationships(candidates, verified_event_tickers=frozenset({"KXFED-1"}))

    by_ticker = {r.event_ticker: r for r in relationships}
    assert len(relationships) == 2   # nothing dropped — parallel with auto-trust, not a replacement
    assert by_ticker["KXFED-1"].verified is True
    assert by_ticker["KXFED-1"].source == MANUAL_SOURCE
    assert by_ticker["KXNBA-1"].verified is False
    assert by_ticker["KXNBA-1"].source == AUTO_SOURCE


def test_untrusted_auto_detected_candidate_dropped_when_trust_auto_detected_false():
    candidates = [_candidate("KXFED-1"), _candidate("KXNBA-1")]

    relationships = build_relationships(
        candidates, verified_event_tickers=frozenset({"KXFED-1"}), trust_auto_detected=False
    )

    assert [r.event_ticker for r in relationships] == ["KXFED-1"]
    assert relationships[0].verified is True
    assert relationships[0].source == MANUAL_SOURCE


def test_trust_auto_detected_false_with_no_verified_tickers_yields_nothing():
    candidates = [_candidate("KXFED-1"), _candidate("KXNBA-1")]

    relationships = build_relationships(candidates, trust_auto_detected=False)

    assert relationships == []


def test_from_event_candidate_preserves_all_fields():
    candidate = _candidate("KXFED-1", fee_multiplier=Decimal("1.5"))

    relationship = Relationship.from_event_candidate(candidate, verified=True, source=MANUAL_SOURCE)

    assert relationship.event_ticker == candidate.event_ticker
    assert relationship.title == candidate.title
    assert relationship.leg_tickers == candidate.leg_tickers
    assert relationship.leg_volume_24h == candidate.leg_volume_24h
    assert relationship.expected_settlement_time == candidate.expected_settlement_time
    assert relationship.fee_multiplier == candidate.fee_multiplier
    assert relationship.verified is True
    assert relationship.source == MANUAL_SOURCE
