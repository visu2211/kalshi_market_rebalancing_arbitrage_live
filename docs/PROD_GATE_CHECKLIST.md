# Prod gate checklist

This checklist is the ordered set of things that must be true before
`execution.target: prod` can be flipped on. Every item is enforced
somewhere — either in `config/loader.py`, `execution/executor.py`, or
by human judgment that cannot be skipped by a running agent.

**No agent (including Claude) may flip `target: prod` autonomously.**
A human has to walk this list and make each decision.

---

## 0. Preconditions (do not proceed if any are false)

- [ ] Circuit breaker is NOT tripped
  - Check: `jq .tripped data/risk_state.json` → must be `null` or `false`
  - If tripped: investigate the alert in `data/alerts.jsonl`, reconcile
    any orphan positions in `believed_positions`, then clear the
    breaker explicitly (no automatic reset path exists — this is
    deliberate).
- [ ] No orphan positions
  - Check: `jq '.believed_positions | length' data/risk_state.json` → 0
  - If >0: these are leftover legs from past baskets whose unwind
    didn't confirm. Each must be either closed on the exchange or
    reconciled out of `believed_positions` after confirming the
    exchange shows no such position.
- [ ] No open owned orders
  - Check: `jq '.owned_open_order_ids | length' data/risk_state.json` → 0
- [ ] `data/execution.lock` is NOT held by a live process
  - Check: `lsof data/execution.lock` → empty
  - If another scanner is running, you'll double-trade.

## 1. Account / credentials

- [ ] `.env` has real-money `KALSHI_API_KEY_ID` and `KALSHI_PRIVATE_KEY_PATH`
  - These are DIFFERENT credentials from demo. Do not reuse.
- [ ] `KALSHI_PROD_TRADING_ENABLED=true` in the environment
  - `config/loader.py` refuses to load with `target: prod` otherwise.
- [ ] `KALSHI_EXPECTED_PROD_API_KEY_ID` matches `KALSHI_API_KEY_ID`
  - This is the belt-and-suspenders check: someone has to type the
    key id into env twice. If they don't match, loader refuses.

## 2. Alerting

At least one of these must be configured:
- [ ] Telegram: `KALSHI_TELEGRAM_BOT_TOKEN` + `KALSHI_TELEGRAM_CHAT_ID`
- [ ] Generic webhook: `KALSHI_ALERT_WEBHOOK_URL`

Without one of these, no out-of-band alert fires when the breaker
trips mid-run. Loader refuses prod otherwise.

## 3. Config safety gates

In `config/settings.yaml`:
- [ ] `execution.target: "prod"`
- [ ] `execution.prod_enabled: true`
- [ ] `execution.require_rest_confirmation: true`
  - OR `execution.prod_unconfirmed_orders_acknowledged: true` AND you
    have deliberately decided to trade on WS-only pricing. The
    demo-era experiment found REST confirm refused 13/13 detections
    because edges died in the RTT, so a WS-only policy is defensible
    for prod *at size 1*. Revisit before raising size.
- [ ] `execution.max_order_size_contracts: 1`
  - Start here regardless of account size. Ramp manually only after
    N successful round-trips.
- [ ] `execution.max_daily_loss_usd` and `max_gross_exposure_usd` sized
  to what you can lose today without regret
- [ ] `execution.min_edge_stability_ticks: >= 3`
- [ ] `execution.require_non_shrinking_edge: true`
  - Demo retrospective: of 418 bursts, 78% shrank by mean $0.04.
    This filter is the only measured non-noise signal we have.

## 4. Proof we actually have an edge (not just a plausible one)

- [ ] At least 24h of forward demo data since the non-shrinking filter
      was enabled (flipped 2026-11-11, commit `d8a090a`)
- [ ] `scripts/daily_report.py --hours 24` shows:
  - [ ] Detections passing BOTH filters > 0 (i.e. the filter isn't
        so tight it never fires)
  - [ ] Detections that pass are NOT dominated by stale-book
        artifacts (sanity check: do the event tickers look like real
        current events?)

**Demo will not fill** (no counterparty flow). So "the strategy made
money on demo" cannot be a gate. The actual gates are: the filter
fires, the detections look real, and the account has alerting.

## 5. First real-money session (post-gate)

- [ ] Start with `execution.max_baskets_per_session: 1`
- [ ] Watch the terminal live. Do not let it run unattended on the
      first prod session.
- [ ] After the first attempt succeeds OR rejects with a clear reason,
      STOP THE SCANNER and inspect `data/orders.jsonl`,
      `data/alerts.jsonl`, and the exchange UI.
- [ ] Only then raise `max_baskets_per_session` and leave it running.

## 6. Rollback

If anything looks wrong at any step:
1. `kill` the scanner process (SIGTERM, not SIGKILL — lets it drop
   the process lock cleanly)
2. Verify `jq '.owned_open_order_ids | length' data/risk_state.json`
   is 0 and no positions were opened you didn't expect
3. If positions remain: close them manually on the exchange UI, then
   reconcile `believed_positions` by hand
4. Flip `execution.target` back to `"demo"` and commit — do not leave
   `prod` in a half-rolled-back state

---

## Why this checklist exists

The account is small ($23 at last check). The strategy's realistic
ceiling at 1-contract size is single-digit dollars per week even in
the best case. So the asymmetric risk is a BUG, not a loss — a code
path that submits more than intended, or doesn't unwind correctly,
can burn the whole account on one basket. Every gate above exists
because of a specific past incident or a measured failure mode, not
out of paranoia.
