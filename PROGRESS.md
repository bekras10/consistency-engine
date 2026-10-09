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
| 2 | 7–9 | detection pipeline, PostgreSQL + Alembic, recording and replay (done) |
| 3 | 10–12 | REST/SSE APIs with contract tests, Next.js dashboard, visual replay, Playwright |
| 4 | 13–17 | benchmarks, reliability/security hardening, Docker deployment, README/demo, authorized Kalshi connector with offline contract tests |

Module boundaries for later milestones:

- Detection pipeline (M2) consumes `consistency_connectors.ingestion.BookUpdate` from the
  bounded queue, looks up relationships through a market→relationship index, and calls
  `consistency_core.pricing.evaluator.evaluate(relationship, portfolio, markets=, books=,
  now_ms=, fees=, config=, observed_duration_ms=)`, which is pure (explicit clock, no I/O).
  It must track how long each strategy has continuously passed every gate except duration;
  `tests/golden/replay_support.py` is the reference behaviour (test-only scaffolding).
  Certificates are canonical JSON (`Evaluation.certificate_json()`, `certificate_hash`);
  `Evaluation.to_detection(detected_at)` builds the persisted `Detection`.
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

### Phase 6 — pricing, portfolio and fee engine ✅

- **Depth walking** (`pricing/depth.py`): walks the derived ask curve level by level; reports
  requested / available / filled / unfilled, exact premium, informational VWAP, marginal price
  and every consumed level (each level is one fill for fee purposes).
- **Canonical portfolios** (`pricing/portfolio.py`): implication, every ordered pair of a
  nested chain, both equivalence directions, NO basket, and the YES basket **only** for proven
  exhaustive groups. Stable `strategy_id` plus exact inverse `parse_strategy_id`.
- **Payoff** (`pricing/payoff.py`): linear payoff `constant + w·s`, minimised exactly over every
  admissible state of the relationship's scenario space; worst state and per-state payoffs in
  the certificate.
- **Fees** (`fees/`): two layers. Model `M·coef·C·P·(1−P)` (taker 0.07, maker 0.0175, defaults
  1/0, no settlement fee). Rounding layer exactly as the official page: `ceil_6dp` trade fee
  (never cents-first), `floor_precision(revenue − trade_fee)`, rounding fee remainder, per-order
  accumulator across taker and maker fills, rebate `floor_p(min(acc, trade + rounding))`.
  DIRECT $0.0001 / NON_DIRECT $0.01, default NON_DIRECT. Versioned schedules
  (`fixtures/fees/kalshi-2026-07-07.yaml`, `synthetic-fictional-v1.yaml`) selected by venue and
  timestamp; exact series match; KXMVE needs a combo classification; live-fee gate (series row,
  effective schedule, member class, intermediary fees, revisions checked) → otherwise
  `FEE_UNVERIFIED` with `FEE:<reason>` codes.
- **Quantity search + thresholds** (`pricing/evaluator.py`): step = lcm of increments; domain
  from the minimum size (or a target) to the depth-supported maximum; exhaustive up to 2000
  points, else breakpoints ± 3 steps; gross / net / execution-adjusted profit per quantity; all
  six spec thresholds plus fee buffer and execution role.
- **Classification**: exactly one of the nine statuses by a fixed 10-step precedence (first
  failing step decides), plus secondary reason codes and a pass / fail / not_reached trace.
  Documented in `docs/mathematical-model.md` §7.
- **Proof certificate** (`pricing/certificate.py`): deterministic canonical JSON, Decimals as
  strings, SHA-256 hash; latency excluded from the hash.
- **Bug found by golden I (S8), fixed**: the edge gate was tested only at the quantity that
  maximised absolute execution-adjusted profit. A strategy profitable per unit at small size
  but thin at depth was wrongly reported `EDGE_BELOW_MINIMUM`. The engine now maximises over the
  edge-qualifying quantities (`edge_qualifying_points` in the certificate); regression test
  `test_edge_gate_picks_best_qualifying_quantity`.
- **Test-side bugs found and fixed** (none weakened an assertion): unquoted YAML dates parsed as
  `date`; two of my hand-computed fee expectations were wrong and were re-derived (multi-level
  total cost 2.66, not 2.16; DIRECT fractional model fee 0.04294317825 → rounding 0.000006);
  the first replay duration tracker only sampled on leg updates, so a frozen book (S6) looked
  0 ms old — it now also samples every 100 ms of local time.
- **Tests**: golden A–J (`tests/golden/`), official fee goldens
  (`tests/golden/test_fee_golden.py`), evaluator edge cases (`tests/unit/test_evaluator.py`),
  invariants 1–10 (`tests/property/test_invariants.py`), fee properties
  (`tests/property/test_fee_property.py`). The invariant-3 multi-fill check was first written
  with a one-increment tolerance; a 3000-example strict probe found no counterexample, so the
  test is strict.
- Docs: `docs/mathematical-model.md`, `docs/compliance.md`, `docs/data-contracts.md` (fee config,
  strategy ids, certificate, fixture format).

## Hardening pass (phases 1–6, before Phase 7) — 2026-10-08

Process per issue: regression tests first, confirmed failing on the pre-fix code, then the fix.
No golden expectation (fixtures A–J, `tests/golden/*`, official fee examples) was changed:
`git diff bff0fd5 -- fixtures/golden tests/golden tests/golden/test_fee_golden.py` is empty
(the only fixture edit is a 3-line comment in `manual-reviews.yaml`).

| Issue | Tests added | Pre-fix failure | Commit |
|---|---|---|---|
| P1 scenario integrity | `test_scenario_integrity.py` (20) | review dropping state 01 from YES(A)+NO(B) stayed VERIFIED and evaluated `FEE_ADJUSTED_CANDIDATE`, min payoff 1 (true worst case 0) | f86e8bb |
| P1 connection-failure publication | `test_runner_desync.py` (11) | auth / exhausted / heartbeat / EOS: consumers only ever saw the stale `snapshot SYNCHRONIZED`; an unexpected source exception hung the runner (`TimeoutError`) | 826ca27 |
| P2 subscription identity | `test_subscription_identity.py` (8) | B's snapshot on c2/sid=1 dropped as a duplicate of c1/sid=1 (B stuck `AWAITING_SNAPSHOT`) | a587de4 |
| P2 trusted freshness | `test_trusted_freshness.py` (13) | duplicate / gapped / rejected / stale-sid messages advanced confirmed-through by 60 s; a book 1 ms in the future evaluated `FEE_ADJUSTED_CANDIDATE` with age −1 | dd02c45 |
| P2 evidence-bearing certificates | `test_certificate_evidence.py` (23, incl. re-derivation for all 12 golden cases) | certificate had no `verification` / `config_hash` / `fee_schedules` / `validation` / `payoff.scenario_specs` | aaa1352 |
| P2 approximate optimisation | `tests/property/test_breakpoint_search.py` (10, Hypothesis + seeded sweeps) | large domains labelled only `BREAKPOINT_APPROXIMATE`; the 3 recorded cases misclassified `DEPTH_SUPPORTED` (exhaustive: `FEE_ADJUSTED_CANDIDATE`) | 6914aad |
| P2 recovery failure fails closed (milestone 2, first commit) | `test_recovery_failure.py` (4) | `request_recovery` raising: the exception escaped `IngestionRunner.run()` and no `RECOVERY_FAILED` desync was published; a recovery that never returned hung `run()` forever (C was only desynced on cancellation, as `RUNNER_STOPPED`) | see git log |

Fix summaries:

- **Scenario integrity**: `scenario_provenance` (derived/overridden + exact diff) on every
  relationship; overrides that remove derived-admissible states need declared diff,
  justification, evidence and a separately verified justifier over the same members, else
  `CANDIDATE_REVIEW`; the evaluator re-checks independently (step 1) and prices the union with
  the derived set; an unproven `exhaustive` claim on an exclusive group is no longer silently
  ignored; `revalidate` demotes overrides whose justifier lapsed.
- **Connection failures**: the runner tracks every connection seen on the subscription and, on
  each loss path (`CONNECTION_LOST`, `HEARTBEAT_TIMEOUT`, `RECONNECT_EXHAUSTED`,
  `AUTHENTICATION_FAILED`, live `END_OF_STREAM`, `SOURCE_ERROR`, `RUNNER_STOPPED`), publishes
  `BookManager.connection_lost(...)` updates through non-blocking `put_urgent`; a coalesced
  resync over an unconsumed desync carries `interrupted=True`. Finite recordings
  (`stream_is_finite`) end without desync. Queue semantics documented in `queues.py` and
  data-contracts.
- **Subscription identity**: `_subs` keyed by `(connection_id, sid)` for sequence, gap,
  duplicate and malformed-recovery state; `state_view` keys are `conn/sid`.
- **Trusted freshness**: confirmed-through advances only on accepted messages (applied
  snapshot/delta, heartbeat — proof in `book_manager.py` and mathematical-model §6.3); messages
  dated > `max_future_ms` (default 1000, constructor parameter) after receipt are quarantined
  (`FUTURE_TIMESTAMP` desync for book messages, sequence still consumed); evaluator step 3 fails
  any `observed_ts > now` with `NEGATIVE_BOOK_AGE` and certificates never report negative ages.
- **Certificates** (`proof-certificate/2`): verification record (status, all evidence checks,
  reviewer + review ids/timestamps/justifications, recorded vs current rules hashes, integrity
  codes), scenario provenance, priced scenario specs, fee schedule id / content-hash version /
  effective date / verification status, `config_hash`, search method + exactness flags,
  validation metadata. Deterministic (hash equality tests).
- **Search**: new `BOUNDED_EXACT` refinement with a proven fee lower bound
  (`order_net_fee_lower_bound`); `BREAKPOINT_APPROXIMATE` kept as a labelled fallback beyond
  50 000 extra points with `optimal_quantity_is_exact=false` and
  `QUANTITY_SEARCH_APPROXIMATE` on negative findings. Figures at the reported quantity are exact
  in every method (`reported_quantity_evaluation_is_exact`).

Breakpoint-vs-exhaustive statistics (4000 seeded marginal books, `random_case` in
`tests/property/breakpoint_support.py`, domains 2–600 points, refinement disabled):
3197 comparable cases; 3 edge false negatives (0.09 %), 0 after-fee false negatives, 38
suboptimal reported quantities (1.2 %, max execution-adjusted shortfall $0.44), 0 cases where
breakpoint beat exhaustive, 3 classification differences (the false negatives). Every breakpoint
result re-evaluated exactly at its quantity. With refinement enabled (default), all 4000 cases
were identical to exhaustive search (classification, reasons, every evaluated figure).

Final verification: `make lint` clean (97 files formatted), `make typecheck` clean (55 source
files), `make test` 309 passed / 0 failed / 0 skipped; unit 252, golden 33, property 24; CI set
`tests/unit tests/golden tests/replay` 285 passed.

Remaining limitations: the `BREAKPOINT_APPROXIMATE` fallback can still miss optima on very large
domains (labelled); `max_future_ms` is a constructor parameter, not yet a `Settings` value; review
ids exist only on records created after this pass (older `ReviewRecord`s carry `review_id = null`,
the id remains in `source`). Live end-of-stream reconnect is implemented in Phase 7 (below), which
supersedes the "runner stops rather than reconnecting" note.

### Phase 7 — real-time detection pipeline ✅

Committed earlier as `d678122` ("startedp phase 7"). Not rewritten in the persistence pass.
In-memory journal and detections until Phase 8.

- `DetectionEngine` keeps a market→relationship index (verified legs, and every member for
  invalidation). A book update evaluates only the relationships that contain that market.
  Pricing and certificates stay in `consistency_core.pricing.evaluator.evaluate`.
- Lifecycle `OPEN` / `UPDATED` / `EXPIRED` / `RESOLVED` / `INVALIDATED`. Consecutive signals for
  the same relationship and portfolio direction merge. Tracked fields: first/last observed, max
  deviation, max capacity, max net edge, status, expiration reason. Duration matches
  `minimum_candidate_duration_ms` (continuous local-clock streak, swept at 100 ms). An unchanged
  re-evaluation writes no row and no event.
- A constituent book that becomes `UNSYNCHRONIZED`, stale, or interrupted invalidates open
  detections immediately, including desync notifications the runner already publishes. Withdrawing
  a review does the same.
- Timing (spec §4.5): source latency is `received_at − exchange_time`; internal latency is
  `detection_completed_ns − processing_started_ns`. Those two nanosecond fields are telemetry.
- `apps/worker` is a long-running asyncio service. `DATA_SOURCE=synthetic` by default; `replay`
  is supported; `kalshi_authorized` stays refused. Bounded queues, JSON logs, graceful shutdown.
  On a live end-of-stream the supervisor reconnects with backoff. Books stay fail-closed until a
  fresh snapshot; reconnect does not pretend recovery.
- Audit P2 (`request_recovery()` failures fail closed) was already committed at `679af2a`,
  before this continuation. Pre-fix, an exception from `request_recovery()` escaped
  `IngestionRunner.run()` and no `RECOVERY_FAILED` desync was published; a recovery that never
  returned hung `run()` until cancellation. The regression tests are
  `tests/unit/test_recovery_failure.py`.

### Phase 8 — PostgreSQL ✅

- SQLAlchemy 2.x + Alembic revision `0001_initial` in `migrations/`. Tables: `data_sources`,
  `series`, `events`, `markets`, `market_rules`, `market_price_ranges`, `relationships`,
  `relationship_members`, `relationship_reviews`, `fee_schedules`, `ingestion_sessions`,
  `orderbook_snapshots`, `orderbook_updates`, `detections`, `detection_legs`,
  `detection_scenarios`, `replay_sessions`, `system_health`, `configuration_versions`, plus
  `session_checkpoints` (spec §12.2; not in the §11 list).
- Indexes include `ix_markets_ticker`, `ix_events_ticker`, `ix_relationship_members_market`,
  `ix_detections_classification`, `ix_detections_first_observed`, `ix_detections_last_observed`,
  `ix_detections_session`, `ix_orderbook_snapshots_lookup`, unique
  `ix_orderbook_snapshots_order` and `ix_orderbook_updates_replay_order` on
  `(session_id, ordinal)`, `ix_orderbook_updates_session_sequence`,
  `ix_ingestion_sessions_fingerprint`, `ix_system_health_checked`.
- Money is `NUMERIC`. Timestamps are `timestamptz`. Certificate v2 JSON is stored with its hash.
  A detection, its legs, its scenarios, and its certificate commit in one transaction.
  High-frequency book rows batch (`JOURNAL_BATCH_SIZE`, default 500).
- Driver is asyncpg. `consistency_persistence` is the only repository. The worker adapts it in
  `apps/worker/.../store.py`; the API readiness check pings the same engine. SQLite URLs are
  refused.
- `make seed` loads the bundled inconsistent catalog, relationships, manual reviews, and fee
  schedules. Dataset regeneration moved to `make datasets` (CI still calls
  `scripts/generate_datasets.py --bundled --check` directly).
- Retention: pinned labels `synthetic:inconsistent` and `replay:inconsistent` keep raw rows.
  Other synthetic/replay sessions drop raw data after `RETENTION_MAX_AGE_HOURS` (168) and keep
  at most `RETENTION_MAX_SESSIONS` (20) newer sessions. Third-party raw data is off unless
  `THIRD_PARTY_RAW_PERSISTENCE_AUTHORIZED=true`, and `RETENTION_THIRD_PARTY_HOURS` defaults to 0.
  Documented in docs/compliance.md.
- Restart: a deterministic session resumes from the last checkpoint on the same id. Journal rows
  and detections past that checkpoint are reconciled. Open detections are not duplicated.
- CI runs `alembic upgrade head`, `downgrade base`, `upgrade head` against a `postgres:16`
  service, then `pytest tests/integration`. `DATABASE_URL` is set only on those steps.

### Phase 9 — recording and replay ✅

- The journal records snapshots, deltas, sequence metadata, exchange and receipt timestamps,
  market-metadata versions, fee-schedule versions, and relationship/rule versions. Replay order
  is the journal ordinal (docs/data-contracts.md). Wall-clock telemetry is excluded from equality.
- `consistency_pipeline.replay` restores the latest checkpoint at or before a timestamp, reapplies
  later ordinals, rebuilds books, recalculates detections, and diffs them.
- `PlaybackService` (the API Phase 11 will call): start, pause, resume, restart, step, seek,
  speeds 0.5 / 1 / 2 / 5 / 10. Each replay id has its own books and engine.
- `make replay` replays `fixtures/datasets/inconsistent` twice and prints the comparison, then
  checks seek-via-checkpoint against a prefix replay and prints S1–S8.
- `tests/replay/test_determinism.py`: the same recording twice matches books, classifications,
  certificate hashes, and event order; seek via checkpoint matches replay-from-start.
- `tests/integration/test_postgres_pipeline.py` runs the bundled inconsistent dataset through
  the worker and Postgres and checks S1–S8. The Test I loop in `tests/golden/replay_support.py`
  remains as the golden reference; the integration test is the production path.

Verification on this machine (Docker `postgres:16.15`, host port 5433 because 5432 is already
PostgreSQL 17): `make lint` clean, `make typecheck` clean (80 files), `make test` 368 passed.
Subsets: unit 296, golden 33, property 24, replay 8, integration 7. `alembic upgrade head` /
`downgrade base` / `upgrade head` succeeded. `make seed` loaded 27 markets, 15 relationships,
3 reviews, 2 fee schedules. `make replay` reported IDENTICAL books, classifications, certificate
hashes, and event order (63 events), and seek-via-checkpoint IDENTICAL.

## Deviations from the specification

- **Optimizer**: no SciPy/HiGHS. Only canonical templates with integer leg ratios are priced,
  and the quantity domain is searched exhaustively (≤ 2000 points) or by the bounded exact
  search (breakpoints + provably-bounded gap refinement; hardening pass). This
  is the "independently tested simple implementation" the spec asks to preserve; a general LP
  optimiser is deferred.
- **Fee configuration scope**: overrides are per series (exact ticker) plus KXMVE combo class.
  Event-level overrides and fee waivers (spec 6.5) are not modelled separately; a waiver can be
  expressed as an M = 0 row in a new schedule version.
- **Fee coefficients**: the coordinator's published-schedule instructions supersede the spec's
  "consult fee_rounding" note; the PDF was not fetched (see data-contracts), so the exception
  table is the owner's PARTIAL transcription.
- **Test E** maps to `THEORETICAL_ONLY` exactly as the spec suggests. **Test I** additionally
  checks every injected scenario's ground-truth label, not only replay identity.
- **Duration**: the spec does not define how `minimum_candidate_duration_ms` is measured. We use
  local-clock time of continuous "all gates pass except duration", sampled on leg updates and at
  least every 100 ms (so durations have ≤ 100 ms resolution).
- No spec number had to be changed: every golden expectation in the spec (A $0.35, B $0.10,
  C rejected, D not 10, E/F/G/H/I/J behaviours) is asserted as written.

## Known limitations (after Phase 9)

- No dashboard (Phase 10) and no public REST or SSE API (Phase 11). Health live/ready exist;
  readiness returns 503 `database_unavailable` when `DATABASE_URL` is set and Postgres is down.
- `make dev` starts Postgres, applies migrations, and runs the worker. It prints that the
  frontend is not started. `make build` and `make benchmark` still exit 2.
- Large-domain search is exact (`BOUNDED_EXACT`) unless refinement would exceed 50 000 extra
  points; it then falls back to a labelled `BREAKPOINT_APPROXIMATE` result that can miss optima
  (measured rates in the hardening section). Exhaustive search is used for all bundled fixtures.
- `max_future_ms` is a constructor parameter, not a `Settings` value.
- Real-venue fees are always `FEE_UNVERIFIED` in practice until member class, intermediary fees
  and schedule revisions are confirmed; the exception table is PARTIAL; the rebate-cap rule is
  an interpretation (mathematical-model §5.3).
- Older `ReviewRecord`s can have `review_id = null`; the id remains in `source`.
- Synthetic results support no empirical claim about real markets (docs/compliance.md).
- Execution is never atomic across markets; certificates say so.
- Host port 5432 may already be another Postgres. Compose reads `POSTGRES_PORT` (default 5432).
  This machine's integration run used Docker `postgres:16.15` published on **5433**.
- Backward playback seek rebuilds from the latest checkpoint at or before the target, then
  applies the suffix once. Forward play does not replay entries already applied.
- The Kalshi connector remains a refused skeleton. No Kalshi network access.

## Hardening pass (post b39f3eb) — 2026-10-09

Process per issue: regression first, confirmed failing on the pre-fix code, then the fix.
Golden files were not edited: `git diff -- fixtures/golden tests/golden` is empty.
Settlement, fee arithmetic, deterministic replay, and restart behaviour are unchanged.
`UPDATED` on the bundled inconsistent recording is 120 rather than 39 because a change in the
current quote (including a decrease) is now an update. `OPENED` 12, `EXPIRED` 7, and
`RESOLVED` 5 are unchanged, and no update repeats an unchanged quote.

| Issue | Regression | Pre-fix failure | Fix |
|---|---|---|---|
| P1 playback initialization | `test_fresh_and_restarted_playback_have_applied_zero_entries` | a new session's outcome already contained detection events while `cursor` was 0 (`_fresh(None, None)` replayed the recording) | construction and `restart` build an empty book manager, engine, and applier |
| P1 playback complexity | `test_forward_playback_applies_each_entry_once_and_seek_backward_matches`, `test_start_and_resume_apply_each_entry_once` | a forward walk applied ordinals `0, 0, 1, 0, 1, 2, ...` (each step replayed the prefix) | one persistent `JournalApplier`; forward play and forward seek apply only new entries; backward seek restores the latest checkpoint at or before the target and applies the suffix once |
| P2 journal flush | `test_failed_commit_preserves_buffer_and_next_flush_writes_once` | after `RuntimeError: commit failed` the buffer was `[]` | `BatchJournal.flush` drops the batch only after the commit returns |
| P2 checkpoint certificate | `test_reconcile_restores_checkpoint_certificate_not_a_later_one` | reconcile left `certificate_json is None` | the detection record stores the certificate of the latest open/update; reconcile writes that JSON back and rebuilds legs and scenarios when it differs from the row |
| P2 current metrics | `test_decreasing_net_edge_reports_current_value_and_keeps_maximum` | edge 0.10 then 0.04 emitted no update (`assert [] == ['UPDATED']`), so the stored quote stayed 0.10 | historical maxima stay maxima; the current quote is the latest economic figures; a repeated quote emits nothing; a real change, including a decrease, emits an update. Duration, freshness, and skew alone do not |
| P2 migration integrity | `test_initial_revision_does_not_follow_orm_metadata`; `test_upgrade_downgrade_upgrade` on Postgres 16 | `0001_initial` called `Base.metadata.create_all` / `drop_all` | frozen `op.create_table`, indexes, constraints, and matching `drop_table` downgrade. The revision does not import the ORM |

Phase 11 notifications are documented in [docs/architecture.md](docs/architecture.md) and are
not implemented. The API process cannot subscribe to the worker's in-memory broker. The
contract is an append-only `notification_outbox` (`id` cursor, `topic`, `session_id`,
`payload`, `created_at`) written in the detection transaction, polled by the API, with
resynchronization from the current `detections` rows. This pass does not create that table.

Verification on this machine (Docker `postgres:16.15`, host port 5433): `make lint` clean,
`make typecheck` clean (80 source files). Suites: unit 299 passed, golden 33 passed,
property 24 passed, replay 11 passed, integration 8 passed. Total 375 passed, 0 failed,
0 skipped.

One pre-existing race, not one of the audited issues: `test_worker_graceful_stop_on_live_source_closes_detections`
waits until an active detection has `event_count >= 2`, but the scripted live source reaches
end-of-stream inside one 10 ms poll and closes that detection first. The same timeout happens
with the pre-change engine. The wait now also accepts a detection that has already been
updated and closed. The shutdown assertions are unchanged.

## Hardening pass (post 6ad5552) — 2026-10-09

The six earlier fixes stay as they were. This pass adds a lock around journal flush and a
shutdown test that can actually observe `RUNNER_STOPPED`.

| Issue | Regression | Pre-fix failure | Fix |
|---|---|---|---|
| Concurrent journal flush | `test_overlapping_flushes_persist_each_ordinal_once_in_order` | the second flush inserted ordinal 0 while the first transaction still held it (`unique constraint violation on ordinal 0`) | `asyncio.Lock` covers the buffer read, the insert transaction, and the prefix delete. Appends during the await stay behind that prefix. The buffer is still dropped only after commit |
| Graceful stop while the stream is open | `test_worker_stop_while_stream_held_invalidates_with_runner_stopped` | coverage only. The existing live-source test can see `END_OF_STREAM` close the detection before shutdown; its assertions were not changed | the new source holds the subscription open after an active detection exists. Shutdown invalidates that detection with `RUNNER_STOPPED` |

Phase 11's `notification_outbox` is still not created. `docs/architecture.md` now records that
a `BIGSERIAL` id is assigned before commit, so assignment order is not commit order. A poller
that advances to the highest visible id can skip a row whose transaction commits later, and
waiting for every integer stalls on a rolled-back sequence value. The next phase has to
serialize outbox inserts or use a visibility rule before `id` is a resume token.

## How to resume Phase 10

Phase 10 is the Next.js dashboard (`apps/web`). Do not start it by extending health routes into
a detection API; public REST and SSE are Phase 11. Read persisted detections through
`consistency_persistence` (certificate JSON, legs, scenarios). Replay controls should call
`consistency_pipeline.playback.PlaybackService` rather than a second engine. `make dev` must
keep failing visibly if the frontend cannot start, and must not report success for a missing UI.

