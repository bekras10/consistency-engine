# Compliance and data-use boundaries

Consistency Engine is an analysis tool. It observes order books, reasons about logical
relationships between contracts, and reports evidence. It does **not** trade.

## Data-source modes

| `DATA_SOURCE` | What it does | Network |
|---|---|---|
| `synthetic` (default) | Deterministic synthetic exchange (`consistency_simulation`) | none |
| `replay` | Plays a recorded dataset directory (`REPLAY_DATASET_PATH`), sha-verified | none |
| `kalshi_authorized` | Reserved for an authorized Kalshi connector (milestone 4) | none in milestone 1 |

Every bundled dataset is **synthetic** and says so in its metadata (`"synthetic": true` plus a
disclaimer). Markets carry a `Provenance` (`SYNTHETIC`, `REPLAY`, `KALSHI_AUTHORIZED`) that the
fee calculator uses to choose the fictional or the real schedule.

## Flags and guards

- `ENABLE_LIVE_TRADING=true` is **refused** at configuration time. There is no order-placement
  code anywhere in the repository.
- `DATA_SOURCE=kalshi_authorized` is refused unless **both** `ENABLE_KALSHI_API=true` and
  `KALSHI_AUTHORIZATION_CONFIRMED=true`. The operator setting the second flag asserts that they
  hold the necessary authorization.
- Even with both flags, the milestone-1 `KalshiDataSource` is a skeleton: every data method
  raises `NotImplementedError` and the module imports no network client (tested).
- No Kalshi API endpoint (REST or WebSocket, production or demo) was contacted while building
  this milestone. Only public documentation pages were consulted, and most of those were not
  fetched at all (see `docs/data-contracts.md`, documentation access log).

## Kalshi Developer Agreement

Use of Kalshi's APIs and market data is governed by Kalshi's Developer Agreement and related
terms, which may restrict how data is accessed, stored, displayed, redistributed, or used to
build derived products, and may require disclosures. This repository has **not** reviewed those
terms against any particular deployment. Before enabling `kalshi_authorized` mode, the operator
must confirm that their intended use — including recording books for replay, displaying
detections, and publishing any results — is permitted, and must make any disclosure the terms
require. Nothing here is legal advice.

The fee schedule PDF is not committed (redistribution permission unknown); only an owner
transcription of the specific rows used is stored under `docs/sources/`.

## Fees

Real-venue fee results are `FEE_UNVERIFIED` unless the member classification, intermediary/FCM
fees, the exact series row and the effective schedule are all known and schedule revisions have
been checked (see `docs/mathematical-model.md` §5.5). The stored exception table is PARTIAL.
The fee-schedule landing page must be checked for revisions after 2026-07-07 before any
authorized live integration.

## What results can and cannot support

- Results on synthetic data demonstrate that the engine's logic, arithmetic and determinism
  behave as specified. They **cannot** support any empirical claim about real markets — for
  example, how often inconsistencies occur on Kalshi, how large they are, or how long they
  last. The synthetic generator injects inconsistencies deliberately and uses a fictional fee
  schedule.
- Even on real data, a `FEE_ADJUSTED_CANDIDATE` is evidence about independently observed
  displayed books, not proof that an atomic multi-market execution was available. Every
  certificate carries this disclaimer.

## Raw data retention

Non-synthetic sources persist **no raw order-book data by default**. `Settings.persist_market_data`
is true only for `synthetic` and `replay`. A third-party source persists raw rows only when
`THIRD_PARTY_RAW_PERSISTENCE_AUTHORIZED=true`, which this repository never sets. Catalog rows
(markets, relationships, reviews, fee schedules) and detection certificates are not raw books;
they are stored so a result can be audited. Checkpoints embed books and follow the same raw-data
gate.

The cleanup job (`RETENTION_INTERVAL_S`, default one hour) applies `RetentionPolicy`:

- Sessions labeled `synthetic:inconsistent` or `replay:inconsistent` are pinned. Their snapshots,
  deltas, and checkpoints are kept so the bundled demo can be reconstructed exactly.
- Any other synthetic or replay session loses raw rows once it is older than
  `RETENTION_MAX_AGE_HOURS` (default 168). Among the sessions still inside that window, only the
  newest `RETENTION_MAX_SESSIONS` (default 20) keep raw rows.
- Third-party raw rows are deleted once they are older than `RETENTION_THIRD_PARTY_HOURS`
  (default 0, meaning immediately on the next cleanup).

Deleting raw data removes `orderbook_snapshots`, `orderbook_updates`, and `session_checkpoints`
and sets `raw_persisted` false. Detections, relationships, and market metadata stay.
