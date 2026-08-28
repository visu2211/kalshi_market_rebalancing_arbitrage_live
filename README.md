# kalshi_market_rebalancing_arbitrage_live

A same-day variant of the sibling `kalshi_market_rebalancing_arbitrage` project: it watches structurally supported net-collateralized multi-outcome events that settle by midnight UTC tonight and can attempt to acquire complete baskets below their fixed payout. A completed equal-size basket has fixed positive economics only when exactly one selected leg pays $1; acquiring it is **not risk-free** because Kalshi does not provide atomic execution across separate markets. Execution targets whichever account `execution.target` configures: `"demo"` (paper money) or `"prod"` (real money — see §6).

This document has three parts: what the strategy actually is (high level), how the code implements it (low level), and — just as important — what's deliberately **not** implemented and every assumption baked into the numbers.

---

## 1. The strategy, high level

Kalshi lists **mutually exclusive events**: a group of separate markets where exactly one outcome will happen. "Who wins Team A vs Team B" is really three separate markets — *Team A*, *Team B*, *Tie* — each trading its own YES/NO contract independently, with its own price.

Because exactly one of those three will resolve YES, buying one YES contract on **every single leg** guarantees exactly $1.00 back, no matter which one wins. If you can buy all three YES contracts for a total of less than $1.00, that gap is risk-free profit — the outcome of the actual game doesn't matter, only that you bought the full set.

That's the whole strategy:

```
buy_cost = sum(ask price of every leg) + sum(Kalshi's fee on each)
if buy_cost < $0.98 → flag it, buy every leg
```

The $0.02 buffer below $1.00 exists so only edges comfortably clearing fees and noise get flagged, not marginal ones.

Because each leg trades on its own separate order book, this mispricing is usually small and short-lived — a moment where the legs haven't been priced consistently with each other. Catching it requires watching every leg continuously, not checking in periodically.

---

## 2. Architecture, low level

The pipeline has three stages, run continuously by `scripts/run_scanner.py` (via `stream/monitor.py`'s `run_stage3`):

### Stage 1 — same-day structural filter (`scanner/filters.py`)

Pulls every open event from Kalshi's public `GET /events` and keeps only ones that are:
- `mutually_exclusive: true`
- 2–6 legs (`scanner.min_outcome_legs` / `max_outcome_legs`)
- expected to settle by midnight UTC tonight (`expected_expiration_time`, not `close_time` — see §4)

This is a cheap REST pass with no order-book calls, re-run every `scanner.rescan_interval_s` (5 min).

### Activity gate (`scanner/filters.py::has_recent_activity`)

Drops any event where at least one leg has zero 24-hour trading volume — a "ghost spread" filter. A basket needs every leg to actually be tradeable; one dead leg poisons the whole basket.

### WS subscription selection (`scanner/filters.py::select_by_volume`)

Of the events that pass the activity gate, ranks by total 24-hour volume and fills the WebSocket subscription budget (`scanner.max_ws_legs`, currently 1,000 individual markets) with the highest-volume ones first, never splitting an event across the budget boundary. This normally covers the full active same-day universe; status and the UI explicitly report any events omitted when it does not. This computation uses only data already in Stage 1's free payload — no extra REST calls needed to decide what to watch.

*(Earlier versions of this project selected the WS subscription set differently — via a periodic REST snapshot that only kept events already clearing the threshold at that instant. That meant it could only ever catch an opportunity that happened to still be open when a 5-minute poll landed on it. Volume-based selection instead watches a fixed, continuously-updated set in real time, so an edge is caught the moment it opens.)*

### Stage 3 — live WebSocket detection (`stream/monitor.py`)

Opens one authenticated WebSocket connection to Kalshi over the selected tickers, maintains a local order book per ticker from `orderbook_snapshot`/`orderbook_delta` messages, and on every tick re-runs the buy-basket calculation (`scanner/basket.py::compute_opportunity`). A detected opportunity is logged to `data/opportunities.jsonl` the instant it opens and again when it closes.

### Execution (`execution/executor.py`, when `execution.enabled: true`)

If armed, a detected opportunity triggers `_reconcile_execution`, which:

0. **Re-confirms the edge against Kalshi's own REST book** (`_confirm_opportunity_via_rest`) immediately before submission, and places orders off *those* quotes rather than the WS ones. The WS book is a locally maintained reconstruction; this is an independent read of the same book, so the two agreeing is corroboration that the edge exists rather than that local state drifted. An edge that cannot survive one round-trip per leg could not have survived the time to fill either. A failed confirmation marks the event consumed for that open episode rather than retrying, since `_reconcile_execution` runs on every tick and retrying would issue a REST burst per tick against exactly the corrupt-book case this exists to catch.
1. **Ranks by return per hour of capital lock-up** (`execution/capital.py`), not raw dollar edge — a basket that returns 2% in 1 hour is a better use of limited capital than one that returns 3% but ties up the money for 10 hours, since the faster one frees capital to redeploy sooner.
2. **Checks live demo-account balance** (cached 3s to avoid a REST round-trip on every tick — see the reservation-ledger note in §4) and only executes what currently fits.
3. **Places every leg of the basket in one batched order** (`KalshiClient.place_orders_batch`) rather than one-at-a-time, minimizing the window during which the book can move between legs.
4. **Checks whether every leg actually filled to the same size.** If not — even if only one leg got zero fill — every leg that filled *above* the common floor gets an immediate, aggressively-priced offsetting sell to bring the position back to matched (or fully flat). This is the standard "legging risk" fallback used whenever an exchange has no native atomic multi-leg order type, which Kalshi doesn't.
5. **Polls to confirm the unwind actually completed**, rather than firing it and trusting the response.
6. **Reconciles our own bookkeeping against Kalshi's actual account positions** every 30 seconds, catching any drift a fill-response-only view could miss.
7. **Trips a durable circuit breaker** on the first mismatch, a failed unwind, unavailable reconciliation, or position drift. Restarting does not re-arm it. Risk state, positions, attempts, cooldowns, exposure, daily notional/loss, and order-rate accounting persist in `data/risk_state.json`.

### Local book integrity (`stream/monitor.py`, `scanner/basket.py`)

Stage 3's prices come from a locally maintained order book rebuilt from incremental WS deltas, so a detected "opportunity" is only as trustworthy as that local state. Two checks exist because it was previously not trustworthy at all:

- **Reconnect invalidation.** `LocalBook.snapshot_received` used to stay `True` across a WS reconnect, and `_stream_shortlist` never reset its books — so a delta arriving before that ticker's replacement snapshot was applied to the *previous* connection's levels, and because `apply_delta` refreshes `updated_at_monotonic`, the resulting garbage quote also passed the `is_fresh` staleness gate. `kalshi/ws_client.py` now emits a `WS_RECONNECTED_MESSAGE_TYPE` sentinel after every subscribe, and every book is invalidated on it.
- **Coherence rejection.** `scanner/basket.py::incoherent_legs` drops any leg whose book is locked or crossed (`best_bid >= best_ask`), or priced outside `(0, 1)`. Those are free instant round-trips that no live exchange shows, so they indicate corrupt local state, not an edge. This also protects the unwind guard: a crossed book makes `estimate_worst_case_unwind_loss` read a *negative* spread and therefore approve exactly the basket it should reject. Rejections are logged as `incoherent_book_rejected` — a steady trickle is normal mid-update churn, a burst across many unrelated events is the reconnect signature.

**What this was fixed in response to (2026-08-27):** `data/opportunities.jsonl` recorded 105 "opportunities" with 3–34¢ edges, arriving in simultaneous bursts across unrelated markets — ten distinct ATP, WTA, ITF and MLB events all opening a 4–34¢ risk-free basket within the same two seconds. None were real. Over the same period a REST pass across the whole same-day universe found **zero** baskets below `$1.00` (mode `$1.01`), and 67,032 live WS evaluations found nothing better than a `$0.98` gross basket that netted to exactly `$1.00` after fees. 81% of the universe is 2-leg events whose two markets are the same binary (`ask(A) == 1 - bid(B)`), where `sum(ask)` is mechanically `1 + spread` and no arb can exist at all.

### Dashboard (`ui/app.py`)

A separate Streamlit process (`streamlit run ui/app.py`) that shows the same-day event universe on demand (its own REST snapshot, not connected to the live scanner), plus a live "Production Scanner Status" panel and opportunity/order history that both read files the scanner writes (`data/scanner_status.json`, `data/opportunities.jsonl`, `data/orders.jsonl`).

---

## 3. What's deliberately not implemented

**`sell_basket` (the reverse direction).** The opposite arbitrage — selling YES on every leg when the sum of bids is *overpriced* above ~$1.02 — was implemented at one point and then removed. Every real opportunity observed live was `buy_basket`; the sell-side path had never been exercised against a real Kalshi response, and keeping it would have doubled the surface area of the execution and unwind logic for a path with zero live validation. `scanner/basket.py::compute_opportunity` only ever returns a buy-basket opportunity now; `BasketOpportunity.direction` is kept as a field (used throughout logging/JSONL/the dashboard) but will only ever read `"buy_basket"`.

**True atomic multi-leg execution.** Kalshi has no order type that fills a whole basket as one atomic unit. Every fix in this project (batching, unwind-on-mismatch, confirmation, reconciliation) is the standard fallback tier for that — reduce the risk window, then detect and correct a mismatch fast — not a substitute for actual atomicity, which would require exchange support this API doesn't offer.

**Colocation / sub-millisecond execution.** Real HFT firms shrink the leg-risk window by running servers physically next to the exchange's matching engine. That's not available here — this runs over normal internet API calls, so the batching/unwind/circuit-breaker layers are close to the practical ceiling of what's achievable without exchange-side infrastructure access.

---

## 4. Assumptions worth knowing about

**Demo execution cannot validate profitability, only wiring.** This is the most important one. Detection prices come from Kalshi's real **prod** order books — that's the only place with real liquidity to find genuine mispricing in. Execution happens against Kalshi's separate **demo** (paper-money) account, which has its own, largely disconnected order book. Even with every safety fix in place, most executions against demo will likely still mismatch, because the liquidity that was priced (prod) isn't the liquidity being traded against (demo). Every mismatch costs a real (paper) round-trip of taker fees with no captured edge. **Running this against demo can prove the pipeline correctly detects, executes, and protects itself — it structurally cannot prove the strategy would be profitable**, since that requires trading against the same book you priced against.

**FOK entry and staged IOC unwind are native.** Entry legs use Kalshi V2 `fill_or_kill`, submitted in one batch. Each leg fills completely or not at all, but different legs can still disagree. Before entry, every leg must have enough displayed bid depth to liquidate the proposed size within `execution.max_unwind_loss_usd`. A mismatch first sells at the observed bid, then refreshes up to 100 live bid levels and crosses only as far as needed for the residual size, then makes a final IOC attempt at $0.01. If that final attempt cannot fill, execution halts with an alert and records the residual exposure.

**24-hour volume is a liquidity proxy, not a precise measure.** The activity gate (`has_recent_activity`) and the WS-selection ranking (`select_by_volume`) both use Kalshi's 24h volume field, not a true "traded in the last 1-2 hours" window (which would need a separate trades-endpoint call per leg on every rescan). A market with real volume yesterday but nothing today would still pass.

**Timing constants are starting heuristics, not empirically tuned:**
- Balance cache TTL: 3 seconds (`_BALANCE_CACHE_TTL_S`)
- Status/execution-retry heartbeat: 5 seconds (`_HEARTBEAT_MIN_INTERVAL_S`)
- Unwind confirmation poll interval / timeout: 0.5s / 5s (`_UNWIND_CONFIRM_POLL_INTERVAL_S` / `_UNWIND_CONFIRM_TIMEOUT_S`)
- Position reconciliation interval: 30 seconds (`_POSITION_RECONCILE_INTERVAL_S`)
- Circuit breaker threshold: first mismatch (`execution.circuit_breaker_max_mismatches: 1`)

**The balance cache has a reservation ledger, not perfect real-time accuracy.** Checking live balance on every WS tick would add latency and block the message loop, so balance is cached for a few seconds. To stop two nearby executions from both thinking they can afford the same capital (since neither sees the other's spending until the cache actually refreshes), each executed candidate's estimated cost is subtracted from the cached figure for the rest of that window (`_BalanceCache.reserve`). This closes the specific cross-call race that existed before, but the balance is still, by construction, up to a few seconds stale.

**Position reconciliation runs every 30 seconds, not continuously.** Up to 30 seconds of undetected drift between our internal bookkeeping and Kalshi's actual records is possible in principle before the next check catches it.

**An emergency unwind can lose substantially more than the pre-trade estimate.** Entry orders cannot pay above their limits. Before entry, the bot verifies displayed exit depth and estimates the round-trip loss at the observed bid, including fees. If that liquidity vanishes, the staged fallback prioritizes flattening and may sell as low as $0.01. Even that cannot guarantee a fill: naked exposure can remain when there is no buyer, in which case the durable breaker trips and alerts. There is no mechanism that can guarantee both flattening and a bounded loss without exchange-level atomic basket execution.

**Structural filtering is automated, not a proof of settlement exhaustiveness.** The scanner requires supported event/market structure, same-event membership, distinct outcomes, compatible settlement timing, and the configured quote-freshness limits. With no manual event allowlist, malformed or unusually worded rules that pass those metadata checks can still omit an outcome or overlap semantically. That can make an apparently complete basket fail to guarantee a $1 payout.

**Detection is aligned to the one-contract canary.** `min_depth_contracts` is 1, so a basket is not hidden merely because it lacks the old, unrelated 25-contract depth floor. `max_ws_legs` is 1,000; if the active universe exceeds it, lower-volume events outside the budget remain unwatched and the omission count is exposed in scanner status and the UI.

**WebSocket silence does not make a resting quote stale.** Once a full snapshot has arrived, ordered deltas keep the local book current even when nothing changes. A sequence gap or reconnect invalidates every affected book immediately, and the five-minute subscription rebuild supplies new snapshots. Final execution still requires an independent concurrent REST confirmation whose entire multi-leg fetch must complete within one second.

---

## 5. Exploratory paper simulation

`scripts/run_paper_backtest.py` runs detection against production snapshots and simulates fills at displayed depth. It is useful for finding structural/data bugs, but it does **not** validate profitability: it omits cross-leg latency, queue position, stale-book gaps, adverse selection, partial fills, unwind losses, and real settlement delay.

It uses [`paper_trading_engine`](../paper_trading_engine) — a small, strategy-agnostic paper trading library (sibling directory, not Kalshi-specific) that simulates fills against depth you hand it yourself, tracks positions/cash/P&L, and persists state to disk. Built specifically because a broker's own paper-trading account (Kalshi's demo included) often has a disconnected, unrealistic order book — the standard fix in systematic trading is to build your own simulator against real market data instead of trusting the broker's paper account.

**Modeling simplification:** a real basket holds N separate leg positions and only learns its outcome when the event resolves, hours or days later. Since this strategy's payout doesn't depend on which leg wins — buying one full matched set always pays exactly $1.00 — the backtest script settles each trade immediately at $1.00 rather than modeling per-leg resolution. This is valid specifically because of this strategy's guaranteed-payout structure, not a general assumption of the underlying engine.

```bash
pip install -e ../paper_trading_engine
python scripts/run_paper_backtest.py --once           # single pass
python scripts/run_paper_backtest.py                  # loop forever at scanner.rescan_interval_s
python scripts/run_paper_backtest.py --iterations 20   # fixed number of passes
```

State persists to `data/paper_state.json` (cash/positions/realized P&L, survives a restart); every simulated trade is appended to `data/paper_trades.jsonl`.

---

## 6. Real (prod) trading

`execution.target` in `config/settings.yaml` selects which account `execution/executor.py` actually places orders against:

- `"demo"` (default) — the paper-money account. Detection prices come from prod's real book regardless, so demo fills are frequently asymmetric or absent — see §4's "demo execution cannot validate profitability" note.
- `"prod"` — **the real Kalshi account, real money.** It requires all of: `execution.enabled: true`, `execution.target: prod`, `execution.prod_enabled: true`, `KALSHI_PROD_TRADING_ENABLED=true`, an exact `KALSHI_EXPECTED_PROD_API_KEY_ID` match, and the `--confirm-live-trading` CLI flag. The system also performs startup order cancellation and position recovery before consuming actionable market data.

Production additionally requires either native Telegram alerts (`TELEGRAM_BOT_TOKEN` plus `TELEGRAM_CHAT_ID`) or `KALSHI_ALERT_WEBHOOK_URL`, enforces a lifetime OS process lock, defaults to one basket attempt per process and per UTC day, and durably prevents retrying the same event for 24 hours. Incidents are always appended to `data/alerts.jsonl` and are also sent to the configured remote alert channel. Alert delivery is tested before production execution is allowed to proceed.

`max_daily_notional_usd` is a turnover guard, not a loss guarantee. Before each attempt the ledger conservatively reserves the estimated entry cost plus the value of a full emergency unwind for every leg. This can overstate actual turnover when no unwind occurs, intentionally: a crash after submission cannot erase the day's risk budget. Actual unwind loss is tracked separately by `max_daily_loss_usd`.

**Verified live 2026-08-26:** the same `KALSHI_API_KEY_ID`/`KALSHI_PRIVATE_KEY_PATH` already used for detection does have order-placement permission on prod — confirmed with a deliberately non-marketable $0.01 limit order on a real open market (accepted, then immediately canceled; account balance unaffected).

**`execution.max_order_size_contracts`** (default `1`) is the guardrail specific to real money: it caps every leg's requested size, applied *before* capital ranking (`stream/monitor.py::_cap_opportunity_size`), regardless of what the opportunity's actual `max_size` would otherwise support. The current `scanner.min_depth_contracts: 1` matches that canary size, and final REST confirmation verifies the quantity again before submission. Meant to be ramped up manually as confidence builds, not automatically.

Every other safety system (batching, unwind-on-mismatch, confirmation polling, position reconciliation, the circuit breaker — see §2) applies identically regardless of target; none of it is demo-specific. What changes with real money isn't the mechanism, it's the stakes — worth treating `circuit_breaker_max_mismatches` and the other timing constants in §4 as values to revisit deliberately for prod, not just carry over from demo by default.

---

## Running it

```bash
# detection loop (and demo execution if enabled)
python scripts/run_scanner.py

# production requires every config/env interlock plus this explicit flag
python scripts/run_scanner.py --confirm-live-trading

# one-shot Stage 1/2 REST check, no WebSocket, no execution
python scripts/run_scanner.py --once

# dashboard (separate process)
streamlit run ui/app.py

# optimistic paper simulation against production snapshots; this does not
# establish executable profitability
# (see scripts/run_paper_backtest.py's module docstring and section 5 below)
pip install -e ../paper_trading_engine   # local sibling package, one-time
python scripts/run_paper_backtest.py --once
```

Configuration lives in `config/settings.yaml` (strategy/risk parameters, safe to commit) and `.env` (credentials — never commit). See `.env.example`.
