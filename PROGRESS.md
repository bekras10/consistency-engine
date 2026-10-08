# PROGRESS

Source of truth: [docs/SPECIFICATION.md](docs/SPECIFICATION.md). This file holds the
implementation plan, per-phase log, decisions, and known limitations.

## Environment inspection (Phase 1, step 1.1) — 2026-10-08

- Repo contained only `docs/SPECIFICATION.md`; not a git repo. Nothing to preserve.
- macOS (darwin 25.6), zsh. System `python3` is 3.11.9 (too old); used Homebrew
  `python@3.12` (3.12.11) through `uv` 0.7.12. Node 22.14 + npm, Docker 28.3.
- Local Postgres 14 exists but is not used; `docker-compose.yml` pins `postgres:16`.
- No credentials found or needed. No Kalshi API endpoint was contacted at any time.

## Implementation plan (all phases)

| Milestone | Spec phases | Deliverable |
|---|---|---|
| **1 (this)** | 1–6 | bootstrap; Decimal domain models + normalization; synthetic exchange + datasets; ingestion core (sources, book manager, queues); relationship engine; pricing/portfolio/fee engine + certificates; golden fixtures A–J; property invariants 1–10 |
| 2 | 7–9 | detection pipeline (incremental scans, lifecycle, dedupe), PostgreSQL schema + Alembic, recording + replay processor |
| 3 | 10–12 | REST/SSE APIs with contract tests, Next.js dashboard, visual replay, Playwright |
| 4 | 13–17 | benchmarks, reliability/security hardening, Docker deployment, README/demo, authorized Kalshi connector with offline contract tests |

Module boundaries for later milestones:

- Detection pipeline (M2) consumes `consistency_connectors.ingestion.BookUpdate` from the
  bounded queue, looks up relationships through a market→relationship index, and calls
  `consistency_core.pricing.evaluator.evaluate_relationship(...)`, which is pure and takes an
  explicit `as_of_ms`. Certificates are already JSON-ready (`ProofCertificate.to_json()`).
- Persistence (M2) stores `Market`, `Relationship`, `Detection`, `ProofCertificate` models as
  they are; all money fields serialise to fixed-point strings.
- The API (M3) only reads persisted state and certificates; no business logic in `apps/`.

## Phase log

### Phase 1 — bootstrap ✅

- uv workspace: `packages/{core,simulation,connectors}`, `apps/{api,worker}`; `apps/web` is a
  placeholder directory (frontend is milestone 3).
- Ruff (lint + format), mypy `--strict` on **all** Python packages (spec requires core only),
  pytest + pytest-asyncio + Hypothesis (derandomized profile for reproducibility).
- `Settings` (pydantic-settings) validates env and enforces the guard: live trading refused;
  `kalshi_authorized` mode refused unless both flags true.
- FastAPI `GET /api/v1/health/live`, `GET /api/v1/health/ready` (503 on invalid config) + tests.
- `docker-compose.yml`: `postgres:16` with healthcheck (only service in M1).
- CI: ruff, ruff format check, mypy, unit/golden/replay tests, property tests, fixture
  reproducibility check.
- Makefile: `setup test lint typecheck seed datasets api` work; `dev build replay benchmark`
  print "not yet implemented — milestone N" and **exit 2** (no faked success).
- Decision: starlette 1.x emits a deprecation warning for the httpx-backed `TestClient`; it is
  filtered by message in pytest config (all other warnings are errors).
- Environment quirk: macOS flags uv's editable `.pth` files as hidden and Python 3.12.11 skips
  hidden `.pth` files. pytest therefore uses explicit `pythonpath`; `make setup` clears the flag.

### Phase 2 — domain models + fixed-point normalization ✅

- **Model technology decision: Pydantic v2 frozen models** (`extra="forbid"`). Reason: one
  declaration gives validation, immutability, and JSON schema for the later API; the custom
  `Dec` annotated type refuses floats/bools on input and serialises fixed-point strings.
- `consistency_core.money`: `dec()` (no floats), exact `ceil_to`/`floor_to`/`is_multiple`,
  `dec_str` (never exponent notation). Default decimal context is sufficient (documented why).
- `consistency_core.ticks`: `PriceGrid` of `TickRange(start, end, step)`; parsing from
  `price_ranges`, `tick_size_dollars`, legacy `tick_size`; off-grid prices are errors, never
  snapped.
- Models: `Series`, `Event` (with `mutually_exclusive` / `outcome_set_complete` evidence),
  `Market` (all spec fields + structured `SettlementSpec` + rules hash), `MarketRules`,
  `PriceLevel`, `AskLevel`, `OrderBook` (sync/health metadata, ms timestamps), `Relationship`
  (+ `ScenarioSpec`, `EvidenceCheck`, `ReviewRecord`), `Detection`, `Classification`.
- Binary book: ask curves derived only from opposing bids (high→low → complement →
  cheapest-first), zero-quantity levels dropped, crossed/locked books rejected.
- Normalization (`consistency_core.normalization`): strict literal parsers, REST (fixed-point
  and legacy-cent shapes) and WS snapshot/delta parsing via a single `FieldMap`.
- Docs: Kalshi docs pages could not be fetched (fee_rounding hung for ~15 min and was
  abandoned; others skipped per coordinator instruction). Field names are flagged
  **unverified** in docs/data-contracts.md. The owner later supplied the fee-rounding page and a
  fee-schedule transcription (saved under `docs/sources/`).

### Phase 3 — synthetic exchange ✅

- **Latent state first** (`consistency_simulation.latent`): categorical weights (elections,
  matches), exact lattice distributions of a raw numeric value (CPI, temperature), Bernoulli,
  and a chained model (finals ⇒ champion). Integer/`Fraction` arithmetic only — no
  transcendental floats — so output is bit-identical across platforms for a seed.
- **Contract probabilities are derived** from the latent state using the *same* exact settlement
  semantics as the relationship engine (`consistency_core.relationships.numeric`), so e.g. a
  "CPI ≥ 0.3, reported to 1 dp" contract is priced from P(raw ≥ 0.25).
- **Quoting** (`quoting.py`): bids at grid prices ≤ p − h (YES) and ≤ (1 − p) − h (NO) ⇒
  YES ask ≥ p + h; books never cross; baseline has no book-implied arbitrage (tested every tick).
  Deltas are emitted decreases-first so no intermediate state crosses.
- **Six families** (`families.py`): election (exhaustive with catch-all + an ambiguous primary),
  CPI thresholds (1-dp chain + a 2-dp decoy for Test G), temperature bins (exhaustive
  intervals), soccer (2-way listing = exclusive only; 3-way = exhaustive), equivalent Fed pair
  (+ false-equivalence decoy with a different settlement source), champion ⇒ finals. Tick
  grids: cent, tapered `price_ranges`, and 0.001 with fractional quantities.
- **Eight injected scenarios** (`scenarios.py`) with expected classifications: overpriced narrow
  threshold, underpriced basket, stale feed, fees eliminate, depth eliminates, static genuine,
  ambiguous settlement, short-lived.
- **Event stream** (`exchange.py`): snapshots, deltas (seq per subscription `sid`), status
  changes, market creation/removal, heartbeats, duplicates, gaps, reordering, malformed raw wire
  messages, disconnects, lagging feeds; per-connection monotone delivery. After a detectable
  fault, the recording contains the client's resubscription (fresh snapshots on a new `sid`).
- **Datasets** (`datasets.py`, `scripts/generate_datasets.py`): `smoke` and `inconsistent` are
  bundled (≈220 KB total, messages gzipped with mtime=0); `normal`, `high_vol`, `corruption`,
  `perf_large` (~1,000 markets) are generated on demand into `fixtures/datasets/generated/`
  (git-ignored). CI regenerates the bundled sets and compares the *decompressed* text.
- Playback speeds 0.5×/1×/2×/5×/10× (or unpaced) via `stream.playback`; pacing never changes
  content or order.
- Decision: injected scenarios' expected classifications assume the default `EvaluationConfig`
  and the fictional fee schedule; they are validated end-to-end by the Phase 6 ground-truth
  replay test.

### Phase 4 — ingestion core ✅

- **`MarketDataSource` ABC** (`consistency_connectors.base`): `get_series`, `get_events`,
  `get_markets`, `get_market_rules`, `get_market_orderbook` (REST-style, *not* sequence-aligned),
  `subscribe_orderbooks` (async iterator), `unsubscribe_orderbooks`, `request_recovery`,
  `get_connection_health`.
- **Sources**: `SyntheticDataSource` (runs the synthetic exchange, records exchange truth per
  tick for REST-style queries, plays at a speed), `ReplayDataSource` (dataset directory, sha
  verified), `KalshiDataSource` skeleton — construction refused unless `ENABLE_KALSHI_API` and
  `KALSHI_AUTHORIZATION_CONFIRMED` are both true; even then every data method raises
  `NotImplementedError`; the module imports no network client (tested).
- **`BookManager`**: snapshot/delta application; sequence tracking per subscription; duplicates
  dropped; a gap desynchronizes every market on that `sid`; malformed wire messages desync the
  `sid` (or the whole connection if the `sid` is unreadable); negative quantity, off-grid price,
  crossed book → UNSYNCHRONIZED + recovery request; deltas ignored while untrusted; only a fresh
  validated snapshot restores trust; late deltas on a superseded `sid` are ignored. Freshness via
  per-connection `confirmed_through_ms`. Timing metadata (exchange/received ms, processing ns)
  is excluded from the deterministic `state_digest`.
- **Queues**: bounded `asyncio.Queue` inbound (backpressure, never drops — dropping would corrupt
  sequence tracking); `CoalescingQueue` (latest update per market, bounded) toward detection.
- **`IngestionRunner`**: bounded exponential backoff with seeded jitter; attempts reset only after
  `reset_after_messages` messages (a peer that goes silent right after subscribing cannot loop
  forever — found by the heartbeat test); auth errors never retried; heartbeat timeout →
  `mark_connection_lost(HEARTBEAT_TIMEOUT)`; recovery requests forwarded to the source.
- **Recovery in recorded sources is in-stream**: recordings already contain the resubscription
  snapshots a correct client received, so `request_recovery` is recorded but needs no action.
  A REST snapshot is never used to resync a stream book (it is not sequence-aligned).
- **Tests** (`tests/unit/test_ingestion.py`, 33): Test H (gap → UNSYNCHRONIZED, deltas ignored,
  recovery request, resnapshot restores), duplicates, per-sid sequencing, negative/crossed/
  off-grid, malformed raw, disconnect, lifecycle, freshness, timing; replay of `smoke` and
  `corruption` reconstructs exchange truth exactly with all markets SYNCHRONIZED; digest
  determinism; queue bounds; backoff; runner reconnect/auth/heartbeat; Kalshi guard.
- Limitation: `SyntheticDataSource` simulates the session up front, then plays it back; live
  interactive recovery (re-snapshot on demand) is not modelled in milestone 1.

### Phase 5 — relationship engine ✅

- **Scenario spaces** (`relationships/scenarios.py`): EXPLICIT truth tables, CHAIN
  (`0..01..1`, n+1 states), CARDINALITY (`min_yes..max_yes`). All payoffs are linear in the
  state vector, so `min_linear` is an exact worst case; for large cardinality groups it is
  constraint-based (k smallest weights per admissible k) and never enumerates. Spaces ≤ 4096
  states are enumerable for certificates. Property test: equals brute force over {0,1}^n.
- **Verification policy** (`relationships/verification.py`): FAIL evidence → REJECTED; else any
  UNKNOWN → CANDIDATE_REVIEW; only all-PASS → VERIFIED. Identity fields (underlying, measurement,
  location, window, source, early-close policy; plus unit for numeric contracts) must match;
  unknown never matches. Rounding/methodology differences → UNKNOWN (review) unless a numeric
  counterexample exists → FAIL. Exceptional-resolution paths unknown or non-empty → UNKNOWN.
- **Deterministic discovery** (`relationships/discovery.py`): categorical events
  (exclusive; partition only if the listed outcomes equal the declared universe); numeric
  contracts via exact raw-value preimages — equal → EQUIVALENT, strict subset → IMPLICATION,
  "naive" reported-value containment that fails on raw values → REJECTED with a counterexample;
  ≥3 same-convention rays → one NESTED_THRESHOLDS chain (narrowest first); disjoint interval
  bins → DISJOINT_INTERVALS, exhaustive only if `covers` proves the value domain is covered;
  propositions with a shared id → EQUIVALENT candidate (never auto-verified); title-token match
  only when an underlying is missing → CANDIDATE_REVIEW at most. Output sorted by id; shuffling
  the catalog changes nothing (tested).
- **Manual review** (`relationships/review.py`, `fixtures/relationships/manual-reviews.yaml`):
  reviews pin members' `rules_hash`; stale → CANDIDATE_REVIEW with invalidation reason; a reviewer
  resolves UNKNOWN evidence but can never override FAIL; `revalidate` demotes relationships when
  rules change or members disappear. Reviews: Fed hike ⇔ upper bound (verified), Fed hike vs wire
  headline (rejected), Owls champion ⇒ finalist (verified, explicit truth table).
- **Ground truth**: discovery + reviews reproduce all 10 expected synthetic relationships with the
  expected status/exhaustiveness, and nothing outside them is VERIFIED (tested).
- **Test G**: `SYNCPI-26AUG-GE0.3` ⇒ `SYNCPI2D-26AUG-GT0.27` REJECTED with witness x = 0.25
  (reports 0.3 → YES vs 0.25 → NO). False equivalence (identical titles, different source)
  REJECTED.
- Data fix: synthetic election markets now declare `early_close_policy="none"` (it was left
  unknown, which correctly blocked verification). Bundled datasets regenerated (deterministic;
  message streams unchanged in count).
- Tests: `tests/unit/test_relationships.py` (26), `tests/property/test_scenarios_property.py`.
