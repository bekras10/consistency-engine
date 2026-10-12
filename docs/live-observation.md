# Live observation methodology

This note is for an engineer checking what a Kalshi run of Consistency Engine can
and cannot establish. It is not a claim that a multi-week study has been run.

## What is connected

`DATA_SOURCE=kalshi_authorized` uses the read-only adapter in
`packages/connectors/src/consistency_connectors/kalshi/`. Synthetic and replay
sessions are unchanged and are labeled separately in PostgreSQL and in the
dashboard.

The adapter is constructed only when `ENABLE_KALSHI_API=true` and
`KALSHI_AUTHORIZATION_CONFIRMED=true`. It still refuses to open a socket until
`KALSHI_API_KEY_ID` and `KALSHI_PRIVATE_KEY_PATH` are set. Those flags are not
turned on by the software. `ENABLE_LIVE_TRADING=true` is rejected at startup.
The HTTP allowlist is GET-only and excludes portfolio, order, and balance paths.
The connectivity check is `GET /account/limits` (usage tier, not a balance).

## Official references consulted on 2026-10-11

- https://docs.kalshi.com/getting_started/api_keys
- https://docs.kalshi.com/getting_started/quick_start_market_data
- https://docs.kalshi.com/getting_started/quick_start_websockets
- https://docs.kalshi.com/getting_started/orderbook_responses
- https://docs.kalshi.com/websockets/orderbook-updates
- https://docs.kalshi.com/getting_started/fixed_point_migration
- https://docs.kalshi.com/getting_started/rate_limits
- https://docs.kalshi.com/getting_started/fee_rounding
- https://docs.kalshi.com/api-reference/market/get-markets
- https://docs.kalshi.com/api-reference/market/get-multiple-market-orderbooks

Production REST used by default: `https://external-api.kalshi.com/trade-api/v2`.
Production WebSocket: `wss://external-api-ws.kalshi.com/trade-api/ws/v2`.
Demo hosts are selected only with `KALSHI_ENVIRONMENT=demo`. A production
setting cannot be pointed at a demo host.

Signing covers `timestamp_ms + METHOD + path` without the query string.
Ed25519 signs the raw UTF-8 string. RSA uses PSS with SHA-256, MGF1 SHA-256,
and salt length equal to the digest length. The private key is read from the
configured file and is not written to PostgreSQL, logs, API responses, or the
browser.

## Books

Kalshi order books are bids. The YES ask is one minus the NO bid, and the NO
ask is one minus the YES bid, with the opposite quantity carried through.
Prices are `Decimal` dollars (at most 4 decimal places). Quantities are
`Decimal` contracts (at most 2 decimal places). `price_ranges` is the tick
grid. A market whose notional is not exactly $1, or whose settlement bounds
are not the default $1 payoff, is skipped.

WebSocket sequence numbers belong to the subscription id (`sid`), not to a
market. The first sequence on a sid is accepted. A later sequence must be
exactly one higher. A gap, a disconnect, or a malformed frame marks every
market on that sid unsynchronized. An unsynchronized book is not evaluated as
a positive opportunity. A REST snapshot is a per-market comparison. It is not
applied as a stream resynchronization, and sequential REST snapshots of
different markets are not one atomic exchange snapshot.

## Relationships

Discovery uses the existing engine. A relationship is `VERIFIED` only when
every required settlement check passes. Title similarity, an LLM guess, or a
missing rule cannot raise that status. Live Kalshi payloads do not include
the engine's full identity (underlying, measurement, unit, observation window,
methodology, exceptional resolution). Those markets stay candidates.

Bounded discovery keeps whole events. It does not take the first N markets
and split a family. `KALSHI_SERIES_TICKERS` limits the scan to those series.
One review bundle is written to `reports/kalshi-review-bundle.json` (override
with `KALSHI_REVIEW_BUNDLE`). The bundle holds relationship proposals, rule
excerpts, rules hashes, proposed logic, and edge cases. It does not grant
`VERIFIED`.

Approved settlement fields enter the strict verifier through
`apply_settlement_overlay`, then `discover` and, where a human must resolve
an unknown, the existing manual-review YAML and `apply_reviews`. A later
rules-hash change demotes the relationship until it is reviewed again.
`mutually_exclusive` on a Kalshi event means at most one YES (MECNET). It is
not treated as an exhaustive outcome set.

Periodic discovery (`KALSHI_DISCOVERY_INTERVAL_S`, default 3600) reloads the
catalog, records enter and exit intervals, and updates the WebSocket
subscription. No live verified relationship set is claimed by this repository
state.

## Fees

The Kalshi schedule file is `fixtures/fees/kalshi-2026-07-07.yaml`. It is a
partial published schedule. The fictional synthetic schedule is not applied to
Kalshi provenance. Default assumptions leave member class, intermediary fees,
and schedule-revision confirmation unknown, so the result is `FEE_UNVERIFIED`.
An event `fee_type_override` or `fee_multiplier_override` adds
`EVENT_FEE_OVERRIDE` and also stays unverified. An unverified fee result is
not a fee-adjusted positive candidate.

## What is retained

Raw third-party order books are stored only when
`THIRD_PARTY_RAW_PERSISTENCE_AUTHORIZED=true`. The default is false. In that
default the journal, checkpoints, and order-book tables do not keep provider
books. Replay of that session is unavailable and is not described as bit-exact.

A stored proof certificate is still derived from the live book. When raw
retention is off, the writer removes `books.asks` and
`evaluation.legs.walk.levels` before the row and the outbox payload are
committed, and it stores the hash of those redacted bytes. The certificate
keeps these third-party quote fields, and the note in the certificate says so:

- `top_of_book.best_asks` (best ask price, no size)
- `books.book_hash` and book timestamps
- walk aggregates: premium, vwap, marginal price, filled quantity

Those fields are quote data. They are not "no raw book data." Reports omit
the ladders. Mismatch rows record timestamps and a verdict
(`match`, `plausible_update`, `inconsistency`) and do not store the book.

A REST book that differs from an older WebSocket book by more than
`KALSHI_COMPARE_SKEW_MS` (default 2000) is a plausible update, not corruption.
A disagreement inside that bound invalidates the book until a snapshot.

## Observation window

`make kalshi-study-report` reads `STUDY_ID` from PostgreSQL. It keeps only
eligible `kalshi_authorized` sessions. Synthetic sessions are not mixed in.

An opportunity episode is one detection id from open through close. Book
updates inside that id are the same discrepancy. A new detection id is a new
episode. Evaluation rows include negative results. Idempotency keys make a
restart replay a no-op.

The evaluation funnel is a partition of all recorded evaluations:

- rule-verification rejection
- market-data quality exclusion (unsynchronized, stale, or insufficient depth)
- no theoretical deviation
- theoretical deviation
- depth-supported
- fee-unverified
- fee-verified

Overlapping views, such as "theoretical inconsistency," name their population
and are not empirical false-positive rates.

Observed duration is the union of recorded coverage and downtime intervals.
Synchronized monitoring is the union of synchronized coverage only. A planned
end date produces `COMPLETE` only when that synchronized coverage reaches both
ends of the planned window. A past end date without that evidence is
`INCOMPLETE`. An open window is `IN_PROGRESS`. No eligible session and no
observations is `NOT_STARTED`. Zero fee-adjusted candidates is a valid result.
A candidate is not an executed trade and not a guaranteed fill.

## Unresolved restrictions

Public documentation distinguishes unauthenticated market-data reads from
permission to collect continuously, retain raw books, publish screenshots, or
redistribute data. The Kalshi Developer Agreement was not treated as granting
those rights. The operator must confirm authorization before enabling the
flags. This repository does not publish raw books and does not start a
21-day study by itself.

## Reproduce a derived report

From `consistency-engine`, with no live credentials:

```bash
make kalshi-doctor
make kalshi-study-report
```

`kalshi-doctor` exits 2 until flags and credentials are already in the
environment. `kalshi-study-report` writes `reports/kalshi-study.json`,
`reports/kalshi-study.md`, and an unpublished draft post. The draft is not
sent anywhere.
