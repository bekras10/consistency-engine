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

- Public REST and SSE (Phase 11) are not implemented. Health live/ready exist;
  readiness returns 503 `database_unavailable` when `DATABASE_URL` is set and Postgres is down.
- Phase 10's dashboard is recorded in the Phase 10 section below. `make benchmark` still exits 2.
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

## Phase 10 — dashboard ✅

The Next.js app in `apps/web` reads PostgreSQL through `consistency_persistence`. A local
gateway (`scripts/dashboard_gateway.py`, `http://127.0.0.1:8765/internal/...`) is the only
process that opens the database and the in-process `PlaybackService`. Next server components
call that gateway. The browser polls Next routes under `/app-data`, which proxy `/internal`.
Pages say the refresh is polling. There is no SSE stream and no public `/api/v1` catalog;
both stay in Phase 11. Health live/ready are unchanged.

Prices and quantities stay fixed-point strings. Depth charts scale those strings to integers
for geometry and label the axes from the same scale.

`make dev` starts Postgres, migrations, a fast synthetic session when no detections are stored
(`scripts/load_synthetic_session.py`), the synthetic worker, the gateway, and Next.js. It exits
non-zero if the gateway or the frontend does not answer. `make build` runs the Next production
build. `make benchmark` still exits 2.

| Page | Data |
|---|---|
| `/` | Latest session, source health, market and detection counts, verified relationship count (`verification_status = verified`), sync summary from the journal book, p50/p95 of stored `detection_completed_ns - processing_started_ns`, recent detections, activity buckets. A synthetic source shows a Synthetic simulation label. An empty database shows the seed hint and no invented rows. |
| `/relationships`, `/relationships/[id]` | Stored relationships, filters, constituents, constraints, and a link to the latest detection for that relationship. |
| `/detections`, `/detections/[id]` | Current net edge from `metrics`, with the historical maximum beside it. Detail includes the certificate, legs, scenarios, settlement text, latest book, lifecycle, and a copyable technical report. |
| `/markets`, `/markets/[id]` | Stored markets and a depth chart of the latest journal book. |
| `/replay/[id]` | `PlaybackService` start, pause, resume, restart, step, seek, and speed. The page polls the snapshot. |
| `/methodology`, `/about` | Text read from `docs/mathematical-model.md`, `docs/compliance.md`, and `README.md`. |

Playwright (`apps/web/e2e`, `scripts/e2e.sh`) covers the seeded dashboard, relationship filter,
detection proof, copy report, replay start/pause/seek, and a page rendered after the gateway
is stopped. That disconnected journey is a second spec because the first paint is server-rendered
and does not go through the browser's network stack. Playwright is local. CI runs frontend lint,
typecheck, Vitest, and `next build`. It does not start Postgres plus Next plus Chromium for e2e.

### Still incomplete (Phase 12 and later)

- `make benchmark` and deployment images.
- The dashboard still polls `/app-data`. `GET /api/v1/stream` is the SSE tail; pages are not
  switched onto it. A failed poll keeps the last successful payload.
- Market depth on a detection page is the latest journal book, not the book at first observation.
  Replay is the cursor-accurate book.
- Public GET routes are open on a local deployment. Replay mutations require `X-Replay-Token`.
  Viewer cursors are in the API process and disappear when that process restarts.
- Sessions recorded before the reference snapshot have no `session-reference` row and fall
  back to the current catalog.

## Phase 10 correctness — 2026-10-09

Regression first, confirmed failing, then the fix. Existing golden expectations were not edited.
Added `fixtures/golden/depth-multilevel.yaml` and `tests/golden/test_depth_chart.py`.

| Issue | Regression | Pre-fix failure | Fix |
|---|---|---|---|
| Depth chart | `test_multilevel_depth_chart_cumulative_quantities`; `lib/depth.test.ts` | bids summed low to high (`0.60` cumulative `36.75` instead of `10.00`); chart labels padded `0.40` to `0.4000` | bids accumulate high→low, asks low→high; displayed prices stay the input strings; integer scale is geometry only |
| BookTail rollback | `test_book_tail_rebuilds_after_same_session_truncate_and_replay`; `test_book_tail_rebuilds_when_cached_ordinal_entry_changes` | after truncate/replay the cache stayed at quantity `40` (fresh replay was `3`); a replaced tip stayed at `20` instead of `11` | if the cached ordinal is gone or its entry no longer matches, rebuild from the latest checkpoint that still anchors a journal row, then reapply |
| Historical replay | `test_replay_pins_recorded_reference_versions` | after reference edits, replay digest changed and detection events were empty | session open stores content-addressed market, relationship, and fee snapshots; replay loads those versions (journal stamps for relationship and fee) |
| Polling recovery | `lib/poll.test.ts` keeps the last successful poll | error envelope replaced the payload with `initial` | `reducePoll` keeps the last success and sets status `stale` (or `error` when nothing succeeded) |
| Market status | `test_market_status_follows_journal_after_catalog_row` | markets page status stayed `closed` (the catalog column) after the journal moved the market to `paused` | `apply_journal_market` copies `BookManager.market_status` onto the list and the detail |

`scripts/e2e.sh` exports `PYTHONPATH` and runs Playwright from `apps/web` so the config's base URL is loaded.

Verification on this machine (Docker `postgres:16.15`, host port 5433): `make lint` clean, `make typecheck` clean (83 source files). `make test` 388 passed, 0 failed, 0 skipped. Suites: unit 307, golden 34, property 24, replay 11, integration 12. Frontend: eslint clean, `tsc --noEmit` clean, Vitest 6 passed, `next build` succeeded. `scripts/e2e.sh`: dashboard spec 1 passed, disconnected spec 1 passed.

## Phase 11 — public API — 2026-10-09

`/api/v1` reads the same `dashboard.py` functions and `ReplayHost` as the gateway. The gateway
stays the dashboard's internal adapter. `POST /api/v1/replay/sessions/{id}/start` forks a
viewer id so two clients can seek the same recording independently. Mutations require
`X-Replay-Token` (`REPLAY_API_TOKEN`); `REPLAY_MUTATIONS_PUBLIC` defaults off. GET stays open
for a local deployment.

`notification_outbox` is Alembic `0002_notification_outbox`. The detection transaction inserts
the outbox row. Readers walk a contiguous committed prefix and hold a higher id while
`pg_snapshot_xip(pg_current_snapshot())` shows an in-progress transaction. An aborted hole is
skipped only after no other transaction is open. `GET /api/v1/stream` sends `resync` from
current detection rows, then tails the outbox. `Last-Event-ID` replays that boundary event.
Contract: `docs/api-reference.md`.

Verification on this machine (Docker `postgres:16.15`, host port 5433): `make lint` clean,
`make typecheck` clean (86 source files). `make test` 401 passed, 0 failed, 0 skipped.
Suites: unit 313, golden 34, property 24, replay 11, integration 19. Frontend: eslint clean,
`tsc --noEmit` clean, Vitest 6 passed, `next build` succeeded. `scripts/e2e.sh`: dashboard
spec 1 passed, disconnected spec 1 passed.

## Phase 12 — rigorous tests and audit fixes — 2026-10-10

Five audit fixes. Existing golden expectations in `fixtures/golden` and `tests/golden` were
not edited. No Alembic revision.

| Issue | Regression | Pre-fix failure | Fix |
|---|---|---|---|
| Replay authentication | `test_public_api_and_gateway_reject_unauthorized_replay_posts`; `test_gateway_viewers_seek_independently`; `lib/replay-proxy.test.ts`; `scripts/check_replay_proxy.py` during `scripts/e2e.sh` | `/app-data` proxied replay POSTs with no token. A gateway bound beyond loopback did the same. Two dashboard starts of one recording shared one cursor | The Next proxy returns `401` before it fetches. A non-loopback gateway checks `X-Replay-Token`. `ReplayHost.mutate` forks a viewer. Dashboard reads use `preview` so a page load does not open the shared recording cursor |
| SSE resynchronization | `test_resync_backlog_above_10000_matches_detection_snapshot`; `test_commit_during_resync_cannot_pair_a_new_snapshot_with_an_old_watermark` | watermark stopped at 10000 of 10001 (`assert 10000 == 10001`). A commit during the read paired detection status `4` with watermark version `3` | `_resync` uses one `REPEATABLE READ` snapshot. `contiguous_watermark` is SQL with no row cap. A commit during that read is invisible to both values |
| Outbox liveness | `test_unrelated_transaction_does_not_stall_committed_notifications` (the overlapping-commit test `test_higher_outbox_id_waits_for_the_lower_commit` still passes) | an open `INSERT` into `system_health` made `writers` true and held a higher outbox id (`assert writers is False` failed) | the wait is a write lock on `notification_outbox` (`RowExclusiveLock` and stronger), not every in-progress transaction. A missing id is held only while that lock exists. After the lock is gone, an aborted hole is skipped |
| Replay resource limits | `test_viewer_cap_rejects_another_session`; `test_expired_session_is_cancelled_and_a_live_session_stays` | viewers had no TTL, cap, or playback-task cancellation | `REPLAY_SESSION_TTL_S` default 1800, `REPLAY_MAX_VIEWERS` default 32. Idle expiry cancels the task. A viewer touched inside the TTL stays. Over the cap is `429` `replay_capacity` |
| SSE reliability | `test_slow_consumer_stops_at_the_queue_cap`; `test_idle_stream_emits_a_heartbeat_comment`; `test_idle_stream_sends_heartbeat_and_over_cap_is_refused` | the tail had no heartbeat, no connection cap, and no bounded queue | `: heartbeat` every `SSE_HEARTBEAT_SECONDS` (default 15). Over `SSE_MAX_CONNECTIONS` (default 32) is `503` `stream_unavailable`. A full `SSE_QUEUE_MAX` (default 32) stops the producer; the client reconnects with `Last-Event-ID` |

Also added, without changing existing expectations: `test_phase12_concurrency.py` (failed commit, retry, rollback, concurrent detection writes) and `test_worker_recovers_after_postgres_container_restart` (`docker restart` of the Postgres publishing `DATABASE_URL`, then the worker resumes the same session with no duplicate detection ids and no orphan legs). Hypothesis invariants 1–10 and golden fixtures A–J plus fee goldens were already present and were run unchanged. Historical replay (same recording twice, seek via checkpoint, recorded reference versions) was already covered.

Verification on this machine (Docker `postgres:16.15`, host port 5433): `make lint` clean, `make typecheck` clean (87 source files). `make test` 413 passed, 0 failed, 0 skipped. Suites: unit 315, golden 34, property 24, replay 13, integration 27. Frontend: eslint clean, `tsc --noEmit` clean, Vitest 8 passed, `next build` succeeded. `scripts/e2e.sh` printed the proxy line `Replay proxy: 401 without a token; viewers ... seek to cursors 0 and 12277`, then Playwright `dashboard.spec.ts` 1 passed (14.5s) and `disconnected.spec.ts` 1 passed (122ms), then `Playwright passed, including the disconnected page.`

GitHub Actions run [38086643349](https://github.com/bekras10/consistency-engine/actions/runs/38086643349)
finished success on `4e1a937` (backend, frontend, and Playwright). An earlier push,
[38086251147](https://github.com/bekras10/consistency-engine/actions/runs/38086251147),
failed because the dashboard seek assertion timed out while a full-journal seek was still
running; the spec now waits for that response. The position assertion is unchanged.

### Remaining limitations (superseded in part by the remediation below)

- No deployment images. Phase 14 has not been started.
- The dashboard still polls `/app-data`. `GET /api/v1/stream` is the SSE tail; pages are not switched onto it.
- Public GET routes stay open on a local deployment.
- Viewer cursors live in the process that opened them and disappear on process restart.
- A gateway bound to loopback trusts the Next server after the proxy has checked the capability. A bind that is not loopback checks `X-Replay-Token` itself.
- A slow SSE client is disconnected when its queue fills. Events stay in the outbox for `Last-Event-ID`.
- Market depth on a detection page is the latest journal book. Replay is the cursor-accurate book.
- `BREAKPOINT_APPROXIMATE` remains the fallback when an exact breakpoint is not found. Fee schedules for real venues stay unverified. No Kalshi network calls.

## Remediation before Phase 13 — 2026-10-10

Golden expectations in `fixtures/golden` and `tests/golden` were not edited. Alembic revision
`0003_outbox_claims` adds `outbox_claims`.

| Issue | Regression | Pre-fix failure | Design |
|---|---|---|---|
| Browser-exposed replay token | `lib/replay-proxy.test.ts` (`does not accept the shared token from the browser`, `keeps the shared token out of capability and proxy responses`, `lets two capabilities seek independently and refuses the other viewer`); `scripts/check_replay_proxy.py` in `scripts/e2e.sh`. Existing 401 cases stay | The replay page passed `expectedReplayToken()` into client `ReplayDesk`, so `REPLAY_API_TOKEN` was in the HTML | `POST /app-data/replay-capability` sets httpOnly `ce_replay_capability` (`REPLAY_CAPABILITY_TTL_S`, default 900). The body is `{ok:true}`. The first start binds the cookie to that viewer. The Next route checks the cookie and attaches `X-Replay-Token` only on the gateway hop. A browser-supplied token is ignored. Another viewer's id is `403` `replay_forbidden` |
| Outbox commit/snapshot race | `test_commit_between_snapshot_and_lock_check_is_not_skipped`; `test_committed_claim_is_not_skipped_when_the_row_misses_the_snapshot`; `test_committed_id_missing_from_the_snapshot_is_not_skipped`. Kept `test_higher_outbox_id_waits_for_the_lower_commit`, `test_aborted_outbox_hole_is_not_a_permanent_stall`, `test_unrelated_transaction_does_not_stall_committed_notifications` | `test_commit_between_snapshot_and_lock_check_is_not_skipped` delivered only id 2 (`assert [2] != [2]`) after id 1 committed between the snapshot and the `pg_locks` check | Claims, not locks. A short advisory-lock transaction commits `(id, xid)` into `outbox_claims` after `nextval`. The detection transaction inserts the outbox row with that id. The reader uses `pg_xact_status` of the stored xid. `committed` while the row is missing from the snapshot holds the cursor. `aborted` is skipped. No `pg_locks` query |
| SSE cursor past the log | `test_last_event_id_past_the_log_sends_a_full_resync` | A `Last-Event-ID` above every outbox id waited for an event the restored database would not produce | If `Last-Event-ID` is greater than `highest_outbox_id` (max of outbox ids and claim ids), the stream sends `resync` on connect |
| Database interruption | `test_terminate_backend_during_open_write_leaves_no_orphans`; `test_worker_recovers_after_postgres_container_kill` | The Phase 12 restart test stopped the worker before `docker restart` of the shared database | A disposable container `consistency-engine-crash-db` on host port 55432. `docker kill` while the worker is writing, then a new container on the same volume, migrate, resume. The CI-safe variant calls `pg_terminate_backend` on the writer connection during an open detection transaction. Neither test touches port 5432 or `consistency-engine-db-1` |
| Replay preview cost | `test_preview_cap_fails_fast_and_a_viewer_is_not_blocked` | `ReplayHost.preview` reconstructed a recording with no concurrency cap and could hold the book after the response | `REPLAY_MAX_PREVIEWS` default 4. Past the cap raises `ReplayPreviewBusyError` immediately (`429` `replay_preview_busy`). The reconstructed book is dropped before the snapshot is returned. An open viewer does not take a preview slot and is not queued behind a preview |

Verification on this machine (Docker `postgres:16.15`, host port 5433): `make lint` clean, `make typecheck` clean (87 source files). `make test` 420 passed, 0 failed, 0 skipped, in 86.01s. Suites: unit 316, golden 34, property 24, replay 14, integration 32. Frontend: eslint clean, `tsc --noEmit` clean, Vitest 10 passed, `next build` succeeded. `scripts/e2e.sh` (with `E2E_SKIP_COMPOSE=1` and `DATABASE_URL` on port 5433) printed `Replay proxy: 401 without a token; viewers viewer-c02f4646f15d466f99e78aa3153f9d43 and viewer-b9f62d56f32741a2860ec6c4b6bd1563 seek to cursors 0 and 12277.`, then Playwright `dashboard.spec.ts` 1 passed (14.1s) and `disconnected.spec.ts` 1 passed (128ms), then `Playwright passed, including the disconnected page.` The disposable crash tests passed in the same `make test` run (kill test 3.58s, terminate-backend test 1.89s when timed alone).

## Phase 13 — performance — 2026-10-10

`make benchmark` runs `scripts/benchmark.py` against a `consistency_bench` database on the
same Docker Postgres 16 as `DATABASE_URL` (port 5433). It does not contact Kalshi. CI does
not run it. Raw rows are in `docs/benchmark-results.json`. Methodology, hardware, and the
numbers below are in `docs/benchmarks.md`.

Machine: Apple M3, 16 GiB, Darwin 26.6.2, Python 3.12.11, PostgreSQL 16.15.
Command: `DATABASE_URL=postgresql+asyncpg://consistency:consistency@127.0.0.1:5433/consistency make benchmark`.

| Markets | Msg/s | Detection p95 (ms) | DB rows/s | RSS MiB |
|---|---:|---:|---:|---:|
| 54 | 1310.33 | 0.4797 | 1328.80 | 91.6 |
| 270 | 1093.72 | 0.4849 | 1163.40 | 110.5 |
| 1026 | 658.20 | 0.4873 | 789.43 | 195.5 |
| 5022 | 313.48 | 0.4884 | 460.61 | 866.4 |

The 1000 msg/s goal was hit at 54 and 270 markets and missed at 1026 and 5022. Focused
detection p95 was 0.4849 ms (goal 100 ms). Replay of the 270-market journal matched on the
second pass at 1817.44 entries/s. Inbound backlog stopped at the 10000 cap. A profile of a
270-market run, taken first, put the time in `evaluate` / canonical JSON and in session
open plus PostgreSQL waits. No pricing or detection code was changed after that profile.
Golden fixtures were not edited and were not re-run in this phase.

