"""Maker-side basket rebalancing strategy — sibling to execution/executor.py.

The taker strategy (execution/executor.py) submits FOK limit orders when a
visible edge appears, racing other taker bots for the thin cross. This
strategy posts resting GTC limit orders on each leg at a price below the
current mid, and earns the rebalancing spread when the book drifts through
our quote. We no longer race for the edge — we wait for it. The cost is
inventory risk (partially filled baskets) and adverse selection.

Design note: ENTIRELY SEPARATE from the taker strategy. Every piece of
mutable state (risk file, order store, process lock) lives at a
maker-specific path. The maker never reads or writes the taker's files and
vice versa. This matches the plan's isolation decision — strategy-keying
the shared state would have required editing taker-visible modules, which
is exactly what this branch is meant NOT to do.

Modules:
- quoter.py        :: given a BasketOpportunity's leg mid-prices, decide
                      the target buy price on each leg, or reject if the
                      aggregate margin doesn't clear the threshold
- inventory.py     :: per-event long/short tracking; decide what to do
                      when a leg fills (complete the basket, hold, unwind)
- risk.py          :: MakerRiskState (persist-before-submit discipline,
                      same shape as execution/risk.py's RiskState but
                      pointed at its own file)
- store.py         :: MakerOrderStore (re-uses execution.store.OrderStore's
                      schema — same table — just backed by a separate file)
- executor.py      :: top-level execute_opportunity_maker() — the entry
                      point stream/maker_monitor.py calls
"""
