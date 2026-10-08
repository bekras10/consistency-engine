# Mathematical model

This document defines exactly what the engine computes in milestone 1: relationships and their
admissible scenario sets, portfolios and their worst-case payoff, depth walking, the fee model,
quantity optimisation, execution thresholds, the classification precedence, and the proof
certificate. Every number in the golden fixtures (§10) is derived by hand here and asserted by
the tests in `tests/golden/`.

All arithmetic is exact `Decimal` (or `Fraction` for settlement preimages). Floats are refused
at every boundary. Rounding happens only where this document says it does.

## 1. Notation

- A binary contract pays $1 on YES and $0 on NO. Prices `P` are dollars in (0, 1).
- `C` is a contract quantity (fractional quantities are allowed where the market's
  `quantity_increment` allows them).
- A relationship has ordered members `m_1 … m_n`. A **state** is a vector
  `s ∈ {0,1}^n` (`s_i = 1` ⇔ `m_i` settles YES).
- `ceil_6dp(x)` rounds up to $0.000001; `floor_p(x)` rounds toward −∞ to a multiple of `p`.

## 2. Relationships and admissible scenario sets

A relationship is only usable for pricing when its `verification_status` is `VERIFIED`. The
verification policy is: any `FAIL` evidence → `REJECTED`; otherwise any `UNKNOWN` →
`CANDIDATE_REVIEW`; only all-`PASS` → `VERIFIED`. A manual review may resolve `UNKNOWN`, never
`FAIL`, and is pinned to the members' `rules_hash` (a rules change demotes the relationship).

| Type | Meaning | Admissible states `S` | Scenario kind |
|---|---|---|---|
| `IMPLICATION` (A ⇒ B) | A YES forces B YES | (0,0), (0,1), (1,1) — **(1,0) excluded** | CHAIN |
| `NESTED_THRESHOLDS` (m_1 ⇒ m_2 ⇒ … ⇒ m_n, narrowest first) | chain of implications | `0…0`, `0…01`, …, `1…1` (n + 1 states, monotone) | CHAIN |
| `EQUIVALENT` | A ⇔ B | (0,0), (1,1) | EXPLICIT |
| `MUTUALLY_EXCLUSIVE` | at most one YES | `Σ s_i ≤ 1` | CARDINALITY 0..1 |
| `EXHAUSTIVE_PARTITION` | exactly one YES | `Σ s_i = 1` | CARDINALITY 1..1 |
| `DISJOINT_INTERVALS` | bins of one numeric value | `Σ s_i ≤ 1`, or `= 1` if coverage of the value domain is proven | CARDINALITY |

A manual review may replace the default with an explicit truth table (e.g. the Owls
champion ⇒ finalist review lists `[[0,0],[0,1],[1,1]]`).

**Exhaustiveness is never assumed.** A categorical group is exhaustive only if the event
declares `outcome_set_complete` *and* the listed outcomes equal the declared universe; interval
bins only if `covers` proves the union of their exact raw preimages covers the value domain.
Otherwise the group is merely exclusive and "all NO" is admissible (golden C).

**Numeric contracts** are compared on exact raw-value preimages. A threshold "report ≥ 0.3"
with half-up rounding to 0.1 settles YES iff the raw value `x ≥ 0.25`, i.e. on `[1/4, +∞)`.
Two contracts are `EQUIVALENT` iff the preimages are equal and `IMPLICATION` iff one is a strict
subset. Containment of the *reported* thresholds that fails on raw values is `REJECTED` with a
concrete counterexample (golden G).

Scenario minimisation (`ScenarioSpace.min_linear`) is exact: CHAIN and EXPLICIT spaces are
enumerated; CARDINALITY spaces take, for each admissible count `k`, the `k` smallest weights,
so large groups are never enumerated. A property test checks it against brute force.

## 3. Portfolios and payoff

A portfolio is a list of legs `(market, side, ratio)` with positive integer ratios. One
**basket unit** buys `ratio` contracts of each leg. Its payoff in state `s` is linear:

```
payoff(s) = Σ_{YES legs} r_i · s_i + Σ_{NO legs} r_i · (1 − s_i)
          = constant + Σ w_i s_i,   constant = Σ_{NO legs} r_i,
                                   w_i = r_i (YES leg) or −r_i (NO leg)
```

The **guaranteed payoff per unit** is `min_{s ∈ S} payoff(s)`, computed over *every
admissible state* of the relationship (not assumed from the template). The certificate lists
the worst state and, when `|S| ≤ 256`, every state's payoff.

Canonical constructions (`canonical_portfolios`):

| Relationship | Portfolio | Guaranteed payoff per unit |
|---|---|---|
| A ⇒ B | NO(A) + YES(B) | 1 (state (1,0) is excluded) |
| nested chain | NO(m_i) + YES(m_j) for every i < j | 1 |
| A ⇔ B | YES(A) + NO(B), and NO(A) + YES(B) | 1 |
| exclusive group (n members) | NO basket | n − 1 |
| proven exhaustive group | YES basket (offered **only** if exhaustive) | 1 |
| | NO basket | n − 1 |

## 4. Depth walking

The ask curve for buying `side` is derived only from the displayed *opposite* bids:
`ask price = 1 − bid price`, same quantity, cheapest first. Buying `q` contracts walks the
curve level by level:

```
fill_k = min(remaining, level_k.quantity);  premium = Σ fill_k · price_k
```

Each consumed level is a separate fill (§5.2). If depth runs out, `unfilled = q − filled > 0`
and the walk is reported as not fully filled; the engine never invents liquidity. The VWAP is
informational only (floored to 1e-8); all decisions use exact premiums.

For a basket of `Q` units the leg needs `Q · ratio` contracts. The **maximum depth-supported
basket** is `qmax = floor_step(min_legs(available_leg / ratio_leg))`.

## 5. Fee model

Two separate layers: a per-fill **fee model** (what the schedule says the fee is) and the
**rounding layer** (how the exchange charges it against balances).

### 5.1 Fee model (published schedule, effective 2026-07-07)

```
taker fee = M_taker · 0.07   · C · P · (1 − P)
maker fee = M_maker · 0.0175 · C · P · (1 − P)
```

Default `M_taker = 1`, `M_maker = 0`. There is no settlement fee. Perpetual-futures fees are
outside the engine. Series exceptions are matched on the **exact** series ticker (never by
prefix): KXCPI, KXFED, KXMLBGAME, KXNFLGAME, KXBALLONDOR 1/1; KXBTCY, KXETHY, KXDOED 0/0;
KXMVE (specified combos) maker 2 / taker 1. The stored table is **PARTIAL**.

### 5.2 Rounding layer (official fee-rounding algorithm)

For each fill with signed revenue `R` (negative for a buyer: `R = −P·C`) and exact model fee
`F`:

```
trade_fee      = ceil_6dp(F)                         # NOT rounded to cents first
aligned_change = floor_p(R − trade_fee)              # p = member balance precision
rounding_fee   = (R − trade_fee) − aligned_change    # 0 <= rounding_fee < p
accumulator   += rounding_fee
rebate         = floor_p( min(accumulator, trade_fee + rounding_fee) )
accumulator   -= rebate
net_fee        = trade_fee + rounding_fee − rebate   # always >= 0
balance_change = aligned_change + rebate             # on the p grid
```

Member precision: DIRECT `p = $0.0001`, NON_DIRECT `p = $0.01`. **Default NON_DIRECT** when
the member class is unknown: the coarser grid produces rounding fees at least as large on a
single fill, so the estimate is conservative — and the unknown class still makes a real-venue
fee `FEE_UNVERIFIED` (§5.5).

The accumulator is **per order** and persists across that order's taker and maker fills. Every
walked depth level is a separate fill of the same order sharing one accumulator; the
certificate reports each fill's trade fee, rounding fee and rebate plus order totals. Because
rebates only return whole increments of previously charged rounding,

```
order net fee = Σ trade_fee + accumulator_remaining  >=  Σ ceil_6dp(model fee)
```

(asserted by a property test).

### 5.3 Rebate-cap interpretation (documented ambiguity)

The page says rebates are made "in increments of the user's target balance precision, capped so
the fill's net fee cannot be negative". It does not say whether the cap is applied before or
after aligning to the increment. We apply the cap **first, then floor to `p`**:
`rebate = floor_p(min(acc, trade_fee + rounding_fee))`. The literal alternative
`min(floor_p(acc), cap)` produces an off-grid rebate whenever the cap binds (e.g. NON_DIRECT,
cap $0.005 → rebate $0.005), which would leave the balance off the member's precision grid,
contradicting the page's own statement that balances are aligned. Our reading keeps balances
on-grid and never produces a negative net fee. The collapsed "Accumulator across fills" worked
example was not captured, so this remains an **interpretation** (tests:
`test_rebate_cap_binds`).

### 5.4 Synthetic (fictional) schedule

The synthetic exchange uses `synthetic-fictional-v1` (taker coefficient 0.05, maker 0.0125,
M 1/0, NON_DIRECT), clearly labelled `FICTIONAL`. It routes through the **same** rounding
layer. For integer `C` and cent prices `P·C` is whole cents, so one fill's net fee is simply
`ceil_cent(0.05·C·P·(1 − P))` — the shortcut used in the golden arithmetic below.

### 5.5 Live-fee verification gate

A real-venue fee is `verified` only if **all** are known: the exact series and its resolved
override row; the schedule effective at the evaluation timestamp; the member classification;
intermediary/FCM fees (accounted for or explicitly excluded); and confirmation that the
schedule landing page was checked for later revisions. Any gap yields `FEE_UNVERIFIED` with a
`FEE:<reason>` code (`NO_SCHEDULE_EFFECTIVE`, `SERIES_NOT_IN_PARTIAL_TABLE`,
`COMBO_CLASSIFICATION_UNKNOWN`, `COMBO_CLASSIFICATION_NOT_LISTED`, `MEMBER_CLASS_UNKNOWN`,
`INTERMEDIARY_FEES_UNKNOWN`, `SCHEDULE_REVISIONS_NOT_CHECKED`, …). Timestamps before
2026-07-07 have no stored schedule → `FEE_UNVERIFIED`. KXMVE requires a supplied combo
classification. An estimate may still be shown when coefficients are known, but such a result
can never be `FEE_ADJUSTED_CANDIDATE`.

## 6. Quantity search and execution thresholds

### 6.1 Domain

`step = lcm(leg quantity increments)` (leg ratios are positive integers). The domain is the
multiples of `step` from

```
Q_lo = target_quantity                                         (if configured), else
Q_lo = ceil_step(max(minimum_available_quantity, step))
```

up to `qmax` (§4). `qmax < Q_lo` → `DEPTH_BELOW_MINIMUM`; a target off the step grid →
`TARGET_OFF_QUANTITY_GRID`.

### 6.2 Objective at quantity Q

```
gross(Q)   = min_payoff · Q − Σ_legs premium_leg(Q · ratio)
net(Q)     = gross(Q) − Σ_legs order_net_fee_leg(Q)
exec(Q)    = net(Q) − Σ_legs Q · ratio · assumed_extra_slippage_per_leg
                    − Σ_legs fee_buffer_leg            # default: one p increment per leg
```

Domains of at most `exhaustive_search_limit` (2000) points are searched exhaustively. Larger
domains are evaluated at every depth breakpoint (where a leg moves to its next level) ± 3
steps plus the endpoints: between breakpoints the pre-rounding objective is linear in `Q`, so
its maximum is at a breakpoint; only sub-cent rounding can differ. The method is recorded in
the certificate. Ties always resolve to the **smaller** quantity.

### 6.3 Thresholds (`EvaluationConfig`, defaults)

| Threshold | Default | Applied as |
|---|---|---|
| `max_book_age_ms` | 2000 | `now − observed_ts ≤ limit` for every leg book |
| `max_cross_market_skew_ms` | 500 | `max(observed) − min(observed) ≤ limit` |
| `minimum_net_edge` | $0.01 per unit | `exec(Q) ≥ minimum_net_edge · Q` (multiplication, no division) |
| `minimum_available_quantity` | 10 | lower end of the quantity domain |
| `assumed_extra_slippage_per_leg` | $0.005 per contract | subtracted in `exec(Q)` |
| `minimum_candidate_duration_ms` | 1000 | the condition must have persisted this long |

`observed_ts` is the connection's confirmed-through time when known, else the exchange
timestamp, else the receive time. The reported quantity is the **best edge-qualifying
quantity**: maximum `exec(Q)` among the `Q` satisfying the edge gate (over all `Q` if none
does, in which case `EDGE_BELOW_MINIMUM` is reported). Maximising unconstrained `exec` first and
testing the gate afterwards would wrongly reject an opportunity that is profitable at small size
but thins out at depth (found by golden I / S8; regression test
`test_edge_gate_picks_best_qualifying_quantity`).

### 6.4 Scanner execution assumption: TAKER on every leg

Scanner evaluations assume each leg **takes** displayed liquidity (`execution_role = TAKER`).
Resting maker orders would need the counterparty to arrive and provide no simultaneity
guarantee across markets, so maker fees are not a valid basis for a scanner opportunity.
Evaluating with `execution_role = MAKER` is allowed only as a what-if: it adds
`NON_DEFAULT_MAKER_ASSUMPTION` and can **never** produce `FEE_ADJUSTED_CANDIDATE`.

## 7. Classification precedence

Exactly one primary classification. Steps run in order; the **first failing step decides**
and later steps are recorded as `not_reached` in the trace.

| Step | Check | Fails as | Reason codes |
|---|---|---|---|
| 1 | relationship verified, rules unchanged, portfolio drawn from members, ratios valid, scenarios modelable | `INVALID_RELATIONSHIP` | `RELATIONSHIP_NOT_VERIFIED`, `RULES_CHANGED`, `MARKET_UNKNOWN`, `PORTFOLIO_NOT_IN_RELATIONSHIP`, `UNSUPPORTED_LEG_RATIO`, `DUPLICATE_LEG`, `SCENARIOS_NOT_MODELABLE` |
| 2 | every leg book present and SYNCHRONIZED | `UNSYNCHRONIZED_DATA` | `BOOK_MISSING`, `BOOK_UNSYNCHRONIZED` |
| 3 | book age and cross-market skew | `STALE_DATA` | `BOOK_TOO_OLD`, `CROSS_MARKET_SKEW` |
| 4 | leg markets OPEN, every leg has asks | `INSUFFICIENT_LIQUIDITY` | `MARKET_NOT_OPEN`, `NO_ASKS` |
| 5 | top-of-book worst-case edge `min_payoff − Σ best asks > 0` | `NO_OPPORTUNITY` | `NO_PRE_FEE_EDGE` (+ `NO_GUARANTEED_PAYOFF` if min payoff ≤ 0) |
| 6 | depth supports the required size | `INSUFFICIENT_LIQUIDITY` | `DEPTH_BELOW_MINIMUM`, `TARGET_OFF_QUANTITY_GRID` |
| 7 | best `gross(Q) > 0` over the domain | `INSUFFICIENT_LIQUIDITY` | `EDGE_EXHAUSTED_BY_DEPTH` |
| 8 | every leg's fee resolution verified | `FEE_UNVERIFIED` | `FEE_UNVERIFIED`, `FEE:<reason>` |
| 9 | best `net(Q) > 0` | `THEORETICAL_ONLY` | `FEES_EXCEED_EDGE` |
| 10 | edge gate, duration known and ≥ minimum, TAKER role | `DEPTH_SUPPORTED` | `EDGE_BELOW_MINIMUM`, `DURATION_UNKNOWN`, `DURATION_BELOW_MINIMUM`, `NON_DEFAULT_MAKER_ASSUMPTION` |
| — | everything passed | `FEE_ADJUSTED_CANDIDATE` | (none) |

Order rationale: an unverified relationship or untrusted data makes every later number
meaningless, so those come first; liquidity before fees because fees depend on the walked
fills; fee verification before profitability so a real-venue result is never presented as
fee-adjusted on guessed fees.

## 8. Proof certificate

`ProofCertificate` (`proof-certificate/1`) is a frozen Pydantic model serialised as canonical
JSON (sorted keys, no floats, every Decimal a fixed-point string). Its SHA-256
(`sha256:<hex>`) identifies the evaluation; processing latency is reported beside it and
excluded from the hash, so identical inputs give byte-identical certificates. It contains:
relationship (id, type, members, status, rules hashes, scenario spec, constraints); portfolio
(strategy id, legs); per-leg book inputs (sync, sequence, timestamps, age, book hash, asks
used); timing; payoff analysis (method, admissible state count, worst state, per-state payoffs
when ≤ 256); top-of-book edge; quantity search (step, domain, `qmax`, method, points, best
gross/net/execution quantities, max profitable quantity, edge-qualifying points); the full
evaluation at the reported quantity (per-leg walk, per-fill fee components, order totals,
slippage and fee buffers); fee resolutions and schedule ids (with a `fictional` flag); the
step-by-step trace; the configuration used; and a disclaimer that independently observed books
do not establish an atomic multi-market execution.

## 9. Execution caveat

The engine reasons about displayed books observed independently. It never places orders and
cannot guarantee that every leg would fill at the observed prices simultaneously. Slippage and
fee buffers, the age/skew limits, and the duration gate reduce, but do not remove, that risk.

## 10. Golden fixtures (data in `fixtures/golden/`, tests in `tests/golden/`)

Fees use the fictional synthetic schedule (taker coefficient 0.05, NON_DIRECT) unless stated.
Defaults: slippage $0.005 per contract per leg, fee buffer $0.01 per leg, minimum size 10.

**A — correct implication.** GE3 ⇒ GE2 (VERIFIED). NO(A) $0.30 + YES(B) $0.35 = $0.65; payoff
≥ 1 in (0,0), (0,1), (1,1). Depth 100 each, Q* = 100:
premium 65.00, gross 35.00; NO(A) fee 0.05·100·0.30·0.70 = 1.05 (rounding 0);
YES(B) 1.1375 → aligned −36.14, rounding 0.0025, net 1.14; fees 2.19; net 32.81;
exec 32.81 − 1.00 − 0.02 = **31.79** → `FEE_ADJUSTED_CANDIDATE`.
At Q = 1: trade fees 0.0105 / 0.011375, rounding 0.0095 / 0.008625, net 0.02 each; net 0.31;
exec 0.28.

**B — exhaustive basket.** YES 0.25 + 0.30 + 0.35 = 0.90; Q = 100: gross 10.00; fees
ceil_cent(0.9375) + 1.05 + ceil_cent(1.1375) = 0.94 + 1.05 + 1.14 = 3.13; net 6.87;
exec 6.87 − 1.50 − 0.03 = **5.34** → `FEE_ADJUSTED_CANDIDATE`. The NO basket has no YES bids:
`INSUFFICIENT_LIQUIDITY [NO_ASKS]`.

**C — non-exhaustive group.** Three exclusive outcomes of a four-outcome universe. No YES
basket is offered; forcing one: "all NO" is admissible → min payoff 0, edge −0.90 →
`NO_OPPORTUNITY [NO_PRE_FEE_EDGE, NO_GUARANTEED_PAYOFF]` (a naive exhaustive reading would show
+$0.10). NO basket: payoff ≥ 2, cost 0.80 + 0.75 + 0.70 = 2.25, edge −0.25 → `NO_OPPORTUNITY`.

**D — insufficient depth.** NO(A) 100 deep, YES(B) 3 deep → qmax = 3. A 10-basket walk fills
10 / 3 (7 unfilled) → `INSUFFICIENT_LIQUIDITY [DEPTH_BELOW_MINIMUM]`. With minimum size 1,
Q* = 3: premium 1.95, gross 1.05, fees 0.04 + 0.04, net 0.97, exec 0.97 − 0.03 − 0.02 = 0.92 →
`FEE_ADJUSTED_CANDIDATE`.

**E — fees remove profitability.** 0.49 + 0.49 = 0.98 (gross $0.02/unit); fees per unit
2·0.05·0.49·0.51 = 0.02499 > 0.02. Best net at Q = 10: gross 0.20, fees 0.13 + 0.13, net
−0.06 (ties with Q = 11, 12; smallest wins) → `THEORETICAL_ONLY [FEES_EXCEED_EDGE]`.

**F — stale data.** B's basket with books 5000 ms old → `STALE_DATA [BOOK_TOO_OLD]`; ages
100 / 800 / 300 ms → skew 700 > 500 → `STALE_DATA [CROSS_MARKET_SKEW]`.

**G — boundary mismatch.** "≥ 0.3, half-up to 0.1" ⇒ raw `[1/4, ∞)`; "> 0.27, half-up to 0.01"
⇒ raw `[11/40, ∞)`. Not a subset: x = 0.25 reports 0.3 (YES) vs 0.25 (NO) → `REJECTED` with
that witness; the converse holds on raw values but conventions differ → `CANDIDATE_REVIEW`.

**H — recovery.** Snapshot/delta script: a lost seq 4 desynchronises both books on sid 1; the
seq-6 delta is ignored; every evaluation is `UNSYNCHRONIZED_DATA` until both legs are
re-snapshotted on sid 2, after which the candidate returns with the new depth (qmax 40).

**I — replay consistency.** Replaying the bundled `inconsistent` dataset twice gives identical
certificate hashes, and every injected scenario receives its ground-truth label:
S1, S2, S6 `FEE_ADJUSTED_CANDIDATE`; S3 `STALE_DATA [BOOK_TOO_OLD]`; S4
`THEORETICAL_ONLY [FEES_EXCEED_EDGE]`; S5 `INSUFFICIENT_LIQUIDITY [EDGE_EXHAUSTED_BY_DEPTH]`;
S7 `INVALID_RELATIONSHIP [RELATIONSHIP_NOT_VERIFIED]`; S8 (~600 ms) `DEPTH_SUPPORTED
[DURATION_BELOW_MINIMUM]`. Duration is the local-clock time a strategy has continuously passed
every gate except the duration gate, sampled on leg updates and at least every 100 ms.

**J — fractional precision.** 3.33 baskets, 4-dp prices:
premium 0.3333·3.33 + 0.4444·3.33 = 1.109889 + 1.479852 = 2.589741; gross 0.740259.
NO(A): model 0.036998149815 → trade 0.036999; −1.146888 → aligned −1.15, rounding 0.003112.
YES(B): model 0.04111028856 → trade 0.041111; −1.520963 → −1.53, rounding 0.009037.
Fees 0.090259, total cost 2.68, net **0.65**, exec 0.65 − 0.0333 − 0.02 = 0.5967. JSON round
trip has no floats.

**Official fee examples** (published coefficients, NON_DIRECT): 1 @ $0.055: model 0.00363825 →
trade 0.003639, aligned −0.06, rounding 0.001361, net 0.005. P = 0.50: C = 1 → raw 0.0175, net
0.02; C = 100 → 1.75. P = 0.10, C = 100 → 0.63. Zero multiplier, 3.33 @ 0.3333 → rounding fee
0.000111.
