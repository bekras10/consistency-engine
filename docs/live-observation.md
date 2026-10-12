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
methodology, exceptional resolution). Those markets stay candidates until an
operator review pins the rules hashes. A later change to the rules hash
requires review again. `mutually_exclusive` on a Kalshi event means at most
one YES (MECNET). It is not treated as an exhaustive outcome set.

No live verified relationship set is claimed by this repository state. Fixture
tests cover the mapping and the refusal to verify from titles.

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
default, the journal does not keep provider books, mismatch records omit
price levels, and reports omit raw books. Replay of a Kalshi session is then
unavailable. Derived detection rows can still be stored. That is not
bit-exact historical replay.

## Observation window

An opportunity episode is one detection id from open through close. Book
updates inside that id are not extra opportunities. A gap closes the trusted
window; a later snapshot can start a new episode. Calendar time is not
synchronized monitoring time. A report whose end timestamp is in the future,
or whose end is unset, is `IN_PROGRESS`. A report with no start and no rows
is `NOT_STARTED`. Zero fee-adjusted candidates is a valid result. A candidate
is not an executed trade and not a guaranteed fill.

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
