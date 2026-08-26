# kalshi_market_rebalancing_arbitrage_live

A same-day variant of the sibling `kalshi_market_rebalancing_arbitrage` project: it watches Kalshi's mutually-exclusive multi-outcome markets that settle by midnight UTC tonight, detects a specific kind of risk-free arbitrage in real time over a live WebSocket feed, and — when armed — attempts to trade it against Kalshi's demo (paper-money) account.

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

Of the events that pass the activity gate, ranks by total 24-hour volume and fills a fixed WebSocket subscription budget (`scanner.max_ws_legs`, 200 individual markets) with the highest-volume ones first, never splitting an event across the budget boundary. This computation uses only data already in Stage 1's free payload — no extra REST calls needed to decide what to watch.

*(Earlier versions of this project selected the WS subscription set differently — via a periodic REST snapshot that only kept events already clearing the threshold at that instant. That meant it could only ever catch an opportunity that happened to still be open when a 5-minute poll landed on it. Volume-based selection instead watches a fixed, continuously-updated set in real time, so an edge is caught the moment it opens.)*

### Stage 3 — live WebSocket detection (`stream/monitor.py`)

Opens one authenticated WebSocket connection to Kalshi over the selected tickers, maintains a local order book per ticker from `orderbook_snapshot`/`orderbook_delta` messages, and on every tick re-runs the buy-basket calculation (`scanner/basket.py::compute_opportunity`). A detected opportunity is logged to `data/opportunities.jsonl` the instant it opens and again when it closes.

### Execution (`execution/executor.py`, when `execution.enabled: true`)

If armed, a detected opportunity triggers `_reconcile_execution`, which:

1. **Ranks by return per hour of capital lock-up** (`execution/capital.py`), not raw dollar edge — a basket that returns 2% in 1 hour is a better use of limited capital than one that returns 3% but ties up the money for 10 hours, since the faster one frees capital to redeploy sooner.
2. **Checks live demo-account balance** (cached 3s to avoid a REST round-trip on every tick — see the reservation-ledger note in §4) and only executes what currently fits.
3. **Places every leg of the basket in one batched order** (`KalshiClient.place_orders_batch`) rather than one-at-a-time, minimizing the window during which the book can move between legs.
4. **Checks whether every leg actually filled to the same size.** If not — even if only one leg got zero fill — every leg that filled *above* the common floor gets an immediate, aggressively-priced offsetting sell to bring the position back to matched (or fully flat). This is the standard "legging risk" fallback used whenever an exchange has no native atomic multi-leg order type, which Kalshi doesn't.
5. **Polls to confirm the unwind actually completed**, rather than firing it and trusting the response.
6. **Reconciles our own bookkeeping against Kalshi's actual account positions** every 30 seconds, catching any drift a fill-response-only view could miss.
7. **Trips a circuit breaker** — auto-disabling execution for the rest of the process — after repeated consecutive mismatches, a confirmed-failed unwind, or a position-reconciliation mismatch. Only a manual restart re-arms it.

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

**IOC is simulated, not native.** Kalshi's v2 API has no `immediate_or_cancel` order type. This project simulates it by setting a short `expiration_ts` (`execution/executor.py::_IOC_WINDOW_S`, 2 seconds) on the original basket order, plus an explicit `cancel_order` call immediately after the batch response comes back, rather than waiting out the expiration.

**24-hour volume is a liquidity proxy, not a precise measure.** The activity gate (`has_recent_activity`) and the WS-selection ranking (`select_by_volume`) both use Kalshi's 24h volume field, not a true "traded in the last 1-2 hours" window (which would need a separate trades-endpoint call per leg on every rescan). A market with real volume yesterday but nothing today would still pass.

**Timing constants are starting heuristics, not empirically tuned:**
- Balance cache TTL: 3 seconds (`_BALANCE_CACHE_TTL_S`)
- Status/execution-retry heartbeat: 5 seconds (`_HEARTBEAT_MIN_INTERVAL_S`)
- Unwind confirmation poll interval / timeout: 0.5s / 5s (`_UNWIND_CONFIRM_POLL_INTERVAL_S` / `_UNWIND_CONFIRM_TIMEOUT_S`)
- Position reconciliation interval: 30 seconds (`_POSITION_RECONCILE_INTERVAL_S`)
- Circuit breaker threshold: 2 consecutive mismatches (`execution.circuit_breaker_max_mismatches`, configurable)

**The balance cache has a reservation ledger, not perfect real-time accuracy.** Checking live balance on every WS tick would add latency and block the message loop, so balance is cached for a few seconds. To stop two nearby executions from both thinking they can afford the same capital (since neither sees the other's spending until the cache actually refreshes), each executed candidate's estimated cost is subtracted from the cached figure for the rest of that window (`_BalanceCache.reserve`). This closes the specific cross-call race that existed before, but the balance is still, by construction, up to a few seconds stale.

**Position reconciliation runs every 30 seconds, not continuously.** Up to 30 seconds of undetected drift between our internal bookkeeping and Kalshi's actual records is possible in principle before the next check catches it.

**The no-slippage guarantee applies to the original basket fill, not the unwind.** A basket's initial order is priced exactly at the depth observed at detection time (`scanner/basket.py`'s `max_size` is capped to what's actually resting at that price). An unwind order, by contrast, is priced aggressively ($0.01) specifically to guarantee it crosses whatever's there — accepting a worse price on purpose, as the cost of eliminating naked exposure fast, not an oversight.

**`min_depth_contracts` and the WS subscription budget (`max_ws_legs`) are configured, not derived.** Events outside the top-200-by-volume WS subscription set are never watched by Stage 3 at all — they only appear in the dashboard's on-demand REST snapshot, not the live detection loop.

---

## Running it

```bash
# detection loop (and, if execution.enabled: true in config/settings.yaml, execution against demo)
python scripts/run_scanner.py

# one-shot Stage 1/2 REST check, no WebSocket, no execution
python scripts/run_scanner.py --once

# dashboard (separate process)
streamlit run ui/app.py
```

Configuration lives in `config/settings.yaml` (strategy/risk parameters, safe to commit) and `.env` (credentials — never commit). See `.env.example`.
