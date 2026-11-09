"""Stage 1 — structural + time filters on events, run via REST every
scanner.rescan_interval_s (15-30 min per the strategy spec). This is the
cheap pass: no order books are fetched here, just the /events payload
itself, so it's safe to run against the full open-event universe.

_live variant: instead of a rolling N-day horizon, only keeps events whose
legs are all expected to settle by the end of the current UTC calendar day
— same-day markets only. `end_of_day_utc` below is recomputed from `now`
on every call rather than cached, so a long-running process
(stream/monitor.py's run_stage3 loop) naturally rolls the cutoff forward
at each UTC midnight without needing a restart.

VERIFIED LIVE 2026-08-25 against GET /events?status=open&with_nested_markets=true
(prod): `mutually_exclusive` is a top-level boolean on the event object as
the strategy spec assumed (33/200 sampled open events had it true with >=2
legs, e.g. KXNEXTNATOSECGEN-99 with 8 legs). Still read via
`.get("mutually_exclusive", False)` rather than assumed present, so a
future field rename degrades to "drop the event" rather than a KeyError.

VERIFIED LIVE 2026-08-26: the time bound reads `expected_expiration_time`,
not `close_time` (falling back to close_time on any leg missing it — some
market types may not always populate it). Confirmed live against
KXATPCHALLENGERMATCH-26AUG26MENWEH (an ATP match actually playing today):
its legs' close_time/expiration_time/latest_expiration_time were all
2026-09-09 — Kalshi's outer administrative deadline in case of postponement
(the market's own rules_secondary: "If this match is postponed or delayed,
the market will remain open... within two weeks") — while
expected_expiration_time correctly read 2026-08-26, today. Using close_time
was silently dropping every sports match whose real settlement is same-day,
since close_time only reflects "last time trading is allowed to remain
open under a worst-case postponement," not "when this actually resolves."
Checked expected_expiration_time is also present and today-valued on the
already-working weather markets (e.g. KXHIGHTSFO-26AUG25-T68), so this
isn't a narrower signal for the events that previously worked correctly —
only a fix for the ones that didn't.

has_recent_activity() and select_by_volume() below are the ghost-spread and
WS-subscription-budget selection Stage 3 (stream/monitor.py) uses to decide
what to watch — both pure/synchronous, since they only read the volume
data filter_events() already extracted, no I/O. Promoted here (rather than
living only in scanner/orderbook_scan.py, which used a private async
version of the same activity check) so stream/monitor.py's live WS
subscription set no longer needs Stage 2's REST order-book pass just to
decide what to subscribe to — see stream/monitor.py's module docstring for
why that mattered.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time, timezone
from decimal import Decimal, InvalidOperation
from typing import Optional


@dataclass(frozen=True)
class EventCandidate:
    event_ticker: str
    title: str
    leg_tickers: list[str]                          # open nested market tickers
    leg_volume_24h: dict[str, Optional[Decimal]]     # ticker -> volume_24h, for Stage 2's activity filter
    expected_settlement_time: datetime               # max across legs of expected_expiration_time (falls back to close_time) — see module docstring
    fee_multiplier: Decimal = Decimal(1)              # multiplier on the standard quadratic taker fee


def _parse_iso(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _end_of_day_utc(now: datetime) -> datetime:
    return datetime.combine(now.date(), time.max, tzinfo=timezone.utc)


def _is_supported_market(market: dict) -> bool:
    try:
        notional = Decimal(str(market.get("notional_value_dollars", "1")))
    except (InvalidOperation, ValueError, TypeError):
        return False
    return (
        market.get("status") == "active"
        and market.get("market_type") in (None, "binary")
        and not market.get("is_provisional", False)
        and notional.is_finite()
        and notional == Decimal(1)
    )


def filter_events(
    raw_events: list[dict],
    min_outcome_legs: int,
    max_outcome_legs: int,
    now: Optional[datetime] = None,
    blacklisted_series: Optional[set[str]] = None,
) -> list[EventCandidate]:
    """raw_events: the `events` list from GET /events?with_nested_markets=true.
    Keeps only events that are mutually exclusive, have leg count in
    [min_outcome_legs, max_outcome_legs], and settle by the end of the
    current UTC calendar day (see module docstring).

    blacklisted_series: optional set of Kalshi series_ticker values (e.g.
    "KXWTAMATCH") the execution account is NOT onboarded on — rejecting here
    avoids spending WS bandwidth, Stage 2 book reads, and worse, real
    execution attempts, on events whose orders will come back with the
    Kalshi `insufficient_shard_balance` ("Exchange user not found") error.
    None means no filter, matching prior behavior."""
    now = now or datetime.now(timezone.utc)
    end_of_day = _end_of_day_utc(now)
    blacklist = blacklisted_series or set()

    candidates: list[EventCandidate] = []
    for event in raw_events:
        if event.get("series_ticker") in blacklist:
            continue
        # MECNET is the only supported mutually-exclusive collateral
        # structure. It does not by itself prove collective exhaustiveness;
        # live execution intentionally accepts this residual semantic risk
        # after full-rule review.
        if not event.get("mutually_exclusive", False) or event.get("collateral_return_type") != "MECNET":
            continue

        fee_type = event.get("fee_type_override")
        if fee_type not in (None, "", "quadratic"):
            continue
        try:
            fee_multiplier = Decimal(str(event.get("fee_multiplier_override", 1)))
        except (InvalidOperation, ValueError, TypeError):
            continue
        if not fee_multiplier.is_finite() or fee_multiplier < 0:
            continue

        # Kalshi's nested market `status` field reads "active" for a
        # tradeable market, not "open" (verified live 2026-08-25 against
        # GET /events?status=open&with_nested_markets=true — the `status=open`
        # query param filters at the *event* level; individual legs can
        # still show up "finalized" inside an otherwise-open multi-leg
        # event, e.g. one candidate has already dropped out).
        markets = event.get("markets", [])
        # Trading only active legs from a partially settled event is not an
        # exhaustive basket. Unknown market types, provisional contracts,
        # and non-$1 notionals likewise invalidate the fixed payout proof.
        if not markets or not all(_is_supported_market(m) for m in markets):
            continue
        open_legs = markets
        if not (min_outcome_legs <= len(open_legs) <= max_outcome_legs):
            continue

        # Every leg must carry a parseable settlement time. Accepting an event
        # because *some* leg parsed meant the horizon was decided from a subset,
        # so a leg settling next week could ride along inside a basket the
        # same-day filter had already approved.
        leg_settlement_times = [
            _parse_iso(m.get("expected_expiration_time") or m.get("close_time")) for m in open_legs
        ]
        if any(t is None for t in leg_settlement_times):
            continue
        expected_settlement_time = max(leg_settlement_times)
        if expected_settlement_time > end_of_day:
            continue

        event_ticker = event.get("event_ticker")
        if not event_ticker:
            continue

        # `volume_24h_fp` (verified live 2026-08-25) — Kalshi's fixed-point
        # ("_fp") 24h volume, not the "1-2h" window the strategy spec
        # describes; see scanner/orderbook_scan.py's docstring for why this
        # coarser proxy is used instead of a per-leg trades-endpoint call.
        leg_volume_24h = {
            m["ticker"]: (Decimal(str(m["volume_24h_fp"])) if m.get("volume_24h_fp") is not None else None)
            for m in open_legs
        }

        candidates.append(
            EventCandidate(
                event_ticker=event_ticker,
                title=event.get("title", ""),
                leg_tickers=[m["ticker"] for m in open_legs],
                leg_volume_24h=leg_volume_24h,
                expected_settlement_time=expected_settlement_time,
                fee_multiplier=fee_multiplier,
            )
        )

    return candidates


def has_recent_activity(candidate: EventCandidate) -> bool:
    """Ghost-spread gate: every leg needs nonzero 24h volume, else the
    event is dropped whole (a basket needs every leg tradeable, so one dead
    leg poisons the rest). Deliberately conservative — missing/zero volume
    data drops the leg's event rather than assuming activity it can't
    confirm."""
    return all((candidate.leg_volume_24h.get(t) or 0) > 0 for t in candidate.leg_tickers)


def _total_volume(candidate: EventCandidate) -> Decimal:
    return sum((candidate.leg_volume_24h.get(t) or Decimal(0)) for t in candidate.leg_tickers)


def select_by_volume(candidates: list[EventCandidate], max_legs: int) -> list[EventCandidate]:
    """Ranks candidates by total 24h volume (highest first) and greedily
    keeps whole events until the next one would push the total leg count
    over max_legs — this is the WS subscription budget (Kalshi's per-
    connection ticker limit), and a basket needs every leg subscribed
    together or none of them, so this never includes a partial event.
    Ties broken by event_ticker for a deterministic, testable order."""
    ranked = sorted(candidates, key=lambda c: (-_total_volume(c), c.event_ticker))
    selected: list[EventCandidate] = []
    leg_count = 0
    for candidate in ranked:
        n = len(candidate.leg_tickers)
        if leg_count + n > max_legs:
            continue
        selected.append(candidate)
        leg_count += n
    return selected
