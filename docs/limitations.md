# Limitations

These are properties of the engine as it runs today. A detection is a calculation on
recorded or synthetic books. It is not a fill, and it is not a profit.

## Settlement-rule ambiguity

A relationship is priced only when `verification_status` is `VERIFIED`. Any `FAIL`
evidence rejects it. Any remaining `UNKNOWN` leaves it in `CANDIDATE_REVIEW`. Exhaustiveness
is not assumed: a categorical group is exhaustive only when the event declares a complete
outcome set that matches the listed outcomes, and interval bins only when their raw
preimages cover the value domain. Otherwise "all NO" stays admissible. A review that
removes derived-admissible states without a separate verified justifier does not become
`VERIFIED`. Numeric contracts are compared on exact raw-value preimages, not on the
rounded threshold printed in the title. Ambiguous rules stay unverified, and an
unverified relationship cannot confirm a detection.

## Stale quotes

Each leg must be synchronized, and its age and the skew across legs must sit inside
`EvaluationConfig` (`max_book_age_ms` default 2000, `max_cross_market_skew_ms` default
500). A book that is stale, skewed, or unsynchronized fails before a payoff is treated
as a candidate. Confirmed-through time advances only on accepted messages. A timestamp
in the future beyond `max_future_ms` is quarantined. Those gates suppress a quote; they
do not make two venues simultaneous.

## Non-atomic multi-market execution

The scanner prices every leg as a taker of displayed liquidity. The legs are separate
books. Nothing in this system submits orders, and nothing can fill several markets as
one transaction. A maker what-if is allowed and is marked
`NON_DEFAULT_MAKER_ASSUMPTION`. It cannot be classified `FEE_ADJUSTED_CANDIDATE`.

## Liquidity disappearance

Depth is the displayed size on the opposite bid. The walk stops when that size runs
out and does not invent the rest. The size can be gone before any order would reach
the book. `INSUFFICIENT_LIQUIDITY` covers a book with no asks, a domain below the
minimum quantity, and a gross edge that depth exhausts.

## Incomplete relationships

Discovery emits candidates. Only a verified scenario set is priced. A group that is
exclusive but not proven exhaustive does not get the YES basket. A rules change demotes
the pinned review. Missing markets, duplicate legs, and a scenario override that cannot
be checked fail closed.

## Fee assumptions

The published formula used here is, per fill,

```
taker fee = M_taker · 0.07   · C · P · (1 − P)
maker fee = M_maker · 0.0175 · C · P · (1 − P)
```

with the schedule dated 2026-07-07. The model fee is ceiled to $0.000001, then the
balance change is floored to account precision. Rebates return accumulated rounding
and are not allowed to make the net fee negative. Direct accounts use $0.0001 and
non-direct accounts use $0.01. Unknown member class defaults to the coarser grid and
still marks a live fee `FEE_UNVERIFIED`.

A live fee is verified only when the exact series override, the schedule in force at
the evaluation time, the member class, and intermediary or FCM fees are all known, and
the schedule page has been checked for a later revision. The stored series table is
partial. The synthetic schedule is `synthetic-fictional-v1` and is labelled fictional.
It uses the same rounding layer. An unverified fee cannot become
`FEE_ADJUSTED_CANDIDATE`.

## Market halts

A leg that is not `OPEN` fails as `MARKET_NOT_OPEN` and an open detection expires for
that reason. A halt, a close, or a missing ask removes the candidate. It does not
carry a theoretical edge forward.

## Capital constraints

The quantity domain starts at `minimum_available_quantity` (default 10 contracts, on
the step grid) and stops at displayed depth. There is no account balance, position
limit, or margin check. A larger theoretical size is not a size the engine can fund.

## Theoretical opportunities are not realized profits

`FEE_ADJUSTED_CANDIDATE` means the worst admissible payoff, minus walked premium, minus
modeled fees, minus the configured slippage and fee buffer, is positive at a reported
quantity, and that the duration gate passed. No order was sent. No fill was observed.
The certificate is the proof of that calculation. Classifications below that step
(`THEORETICAL_ONLY`, `DEPTH_SUPPORTED`, `FEE_UNVERIFIED`, and the earlier failures)
are not candidates.

## Synthetic results are not empirical Kalshi findings

`DATA_SOURCE` defaults to `synthetic`. The inconsistent preset is a generated recording.
Replay reads that recording back. Neither is a sample of live Kalshi trading. The Kalshi
adapter exists and stays off unless `ENABLE_KALSHI_API` and
`KALSHI_AUTHORIZATION_CONFIRMED` are both set. This repository does not turn those on.

## In-memory viewer cursors

Historical replay forks one viewer per client. The cursor, pause state, and playback
task live in the gateway process. They are not written to the canonical journal. Idle
viewers are dropped after `REPLAY_SESSION_TTL_S` (default 1800 seconds). A gateway
restart drops them. Two viewers of the same recording do not share a cursor.

## npm dev advisories

`npm audit` in `apps/web` still reports two development advisories:
`GHSA-vfj7-8cjw-p6xm` (`braces`, through Tailwind 3) and `GHSA-rj75-hqrm-r3gf`
(`postcss-selector-parser`). Production audit count is zero. The check fails if a
production finding appears, if a finding has no GHSA id, or if an allowlisted id
disappears from the report.

## Detection latency

Book-update and `pricing.evaluate` times are not the time from a book update until
the detection row commits. On the inconsistency workload in
[benchmarks.md](benchmarks.md), certificate serialization was 0.6041 ms at p50, the
detection commit was 18.3789 ms at p50, and end-to-end was 2281.125 ms at p50
(n=144). The end-to-end figure includes outbox queue wait. The 1000 msg/s goal was
hit only on the earlier single-process 54- and 270-market runs. Later steady-state
medians, and the 1026- and 5022-market runs, missed it. Those measurements were not
repeated for this document.
