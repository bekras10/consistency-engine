# MASTER BUILD SPECIFICATION: CONSISTENCY ENGINE

> This is the owner's master specification. It is the source of truth for scope and acceptance criteria.
> See "Owner checkpoint instructions" at the bottom for how the build is supervised.

## Your role

You are the lead engineer responsible for designing, implementing, testing, documenting, and preparing for deployment a complete production-quality application called **Consistency Engine**.

You have full responsibility for implementing the software, not simply proposing how it could work.

Work autonomously, use the terminal, inspect your code, run tests, resolve errors, and complete each development phase before moving to the next.

The end result must be a polished, open-source, full-stack prediction-market consistency analysis platform that demonstrates sophisticated backend engineering, financial mathematics, real-time data processing, and excellent product design.

The primary audience is software engineers, quantitative researchers, and recruiters at sophisticated prediction-market exchanges such as Kalshi.

This must be a real working system, not a collection of UI mockups.

---

# 1. Product overview

## 1.1 What we're building

Consistency Engine identifies logically related binary prediction-market contracts and detects situations where their market prices violate mathematical probability constraints.

It must distinguish between:

1. **Theoretical inconsistency:** Prices or probabilities appear to violate a valid relationship.
2. **Book-implied inconsistency:** An apparent discrepancy survives use of bid/ask prices rather than last-traded prices.
3. **Depth-supported opportunity:** There is sufficient order-book liquidity to construct the proposed portfolio at the calculated prices.
4. **Fee-adjusted candidate:** The portfolio's guaranteed terminal payoff exceeds estimated acquisition costs, including applicable fees, under an explicitly defined set of settlement scenarios.
5. **Execution-verified outcome:** An actual trade was independently confirmed to have filled. This category is NOT implemented in this project because live execution is out of scope.

Never call a candidate a realized profit or guaranteed executable trade.

Multiple contracts cannot necessarily be filled simultaneously. Account restrictions, execution latency, transaction costs, and settlement ambiguity can invalidate an apparent opportunity.

## 1.2 Core functionality

The system will:

- Ingest binary-market definitions and order-book data.
- Reconstruct live order books from snapshots and incremental updates.
- Normalize bids into executable YES/NO ask curves.
- Identify and store mathematical relationships between markets.
- Verify relationships against formal settlement constraints.
- Detect violations across related markets.
- Evaluate candidate portfolios against actual displayed depth.
- Account for fees, rounding, and configurable execution buffers.
- Compute worst-case terminal payoffs across admissible scenarios.
- Store detections with complete provenance.
- Replay historical data deterministically.
- Provide an interactive, visually polished dashboard.
- Expose documented REST and streaming APIs.
- Export reproducible technical reports.
- Run entirely using deterministic synthetic data without external accounts.

## 1.3 Data access and compliance requirements

This is mandatory.

Kalshi's Developer Agreement restricts API access, collection, storage, redistribution, and certain public disclosures. Do not assume publicly documented endpoints authorize every form of usage.

Implement three data-source modes:

**Mode A: SYNTHETIC** — Default. Fully functioning simulated prediction exchange with realistic markets, market movements, order-book depth, feeds, and controlled inconsistencies.

**Mode B: REPLAY** — Deterministically replays bundled synthetic datasets or other datasets that the operator is authorized to use.

**Mode C: KALSHI_AUTHORIZED** — An optional read-only adapter implementing Kalshi's documented API schemas. This adapter must remain disabled unless the operator has obtained appropriate permission and explicitly enables the integration.

Environment configuration:

- DATA_SOURCE=synthetic
- ENABLE_KALSHI_API=false
- KALSHI_AUTHORIZATION_CONFIRMED=false
- ENABLE_LIVE_TRADING=false

Do not access Kalshi's production or demo APIs, store its live data, or publicly redistribute such data without the appropriate authorization.

Do not implement live order placement, portfolio modification, or automated trading.

Do not publish Kalshi-branded promotional materials, real market data, or Kalshi-specific empirical findings without checking and satisfying the applicable permission requirements.

The public demonstration must work independently using synthetic data.

## 1.4 Out of scope

Do not waste time implementing: a trading account system; order submission or automatic execution; deposits, withdrawals, or portfolio management; consumer subscription billing; social features; arbitrary natural-language prediction models; machine learning for forecasting event outcomes; cryptocurrency wallets; support for perpetual futures or complex multivariate derivatives in the initial version.

Focus on correctly solving the market-consistency problem.

---

# 2. Technical stack

Use the following architecture unless a clearly documented technical blocker requires an alternative.

## Backend

Python 3.12+, FastAPI, Pydantic v2, SQLAlchemy 2.x, Alembic, PostgreSQL 16+, asyncpg, HTTPX, websockets, NumPy, SciPy/HiGHS for optional optimization, pytest, pytest-asyncio, Hypothesis, Ruff, mypy.

Use Python asyncio for ingestion and real-time processing.

## Frontend

Next.js with App Router, TypeScript (strict mode), React, Tailwind CSS, shadcn/ui, Recharts, Lucide icons, TanStack Query, Playwright for end-to-end tests, Vitest for frontend unit tests.

## Infrastructure

Docker, Docker Compose, PostgreSQL, GitHub Actions, structured JSON logging, configurable environment variables.

Use a monorepo.

Do not introduce Redis, Kafka, Kubernetes, Elasticsearch, or paid infrastructure unless there is a demonstrated requirement. Prefer fewer moving parts and excellent reliability.

---

# 3. Repository structure

consistency-engine/

- apps/
  - api/ — FastAPI application
  - worker/ — feed ingestion, detection, replay, scheduled jobs
  - web/ — Next.js frontend
- packages/
  - core/ — financial mathematics and shared domain models
  - connectors/ — data-source interfaces and provider adapters
  - simulation/ — synthetic exchange and scenario generators
- migrations/ — Alembic revisions
- fixtures/ — deterministic datasets and contract definitions
- tests/
  - unit/
  - property/
  - integration/
  - replay/
  - performance/
- docs/
  - architecture.md
  - mathematical-model.md
  - data-contracts.md
  - deployment.md
  - api-reference.md
  - limitations.md
  - compliance.md
- scripts/
- .github/workflows/
- docker-compose.yml
- .env.example
- Makefile
- README.md
- LICENSE
- AGENTS.md
- PROGRESS.md

Use appropriate Python package configuration so modules import cleanly. Do not duplicate core business logic across worker and API.

---

# 4. Phase 1 — Establish the project

## Step 1.1 — Inspect the environment

Before modifying files: determine whether an existing repository exists; inspect its architecture and dependencies; preserve useful existing code; check installed tooling; identify available databases and credentials; write a brief implementation plan into PROGRESS.md. Do not overwrite unrelated user work.

## Step 1.2 — Bootstrap the monorepo

Create the backend, frontend, shared packages, and development configuration. Configure: Python dependency management; TypeScript strict mode; import aliases; environment validation; Ruff linting and formatting; mypy checks; ESLint; Prettier; pytest; Vitest; Playwright; Docker Compose.

## Step 1.3 — Create one-command local startup

The following commands should work: `make setup`, `make dev`, `make test`, `make lint`, `make typecheck`, `make build`, `make seed`, `make replay`, `make benchmark`.

`make dev` should start the database, backend, worker, frontend, and synthetic exchange.

An engineer cloning the repository should be able to launch the entire synthetic demonstration without credentials.

## Step 1.4 — Create basic CI

GitHub Actions should execute: backend unit tests; backend property tests; frontend unit tests; static type checking; lint checks; production builds; database migration checks.

**Acceptance criteria:** The repository installs from scratch. The app starts without proprietary credentials. Health endpoints return successfully. CI configuration is functional. No secrets are committed.

---

# 5. Phase 2 — Research and implement exchange-compatible data models

Before implementing the optional Kalshi adapter, consult the current official documentation:

- https://docs.kalshi.com/
- https://docs.kalshi.com/llms.txt
- https://docs.kalshi.com/getting_started/orderbook_responses
- https://docs.kalshi.com/getting_started/fixed_point_migration
- https://docs.kalshi.com/websockets/orderbook-updates
- https://docs.kalshi.com/getting_started/quick_start_websockets
- https://docs.kalshi.com/getting_started/rate_limits
- https://docs.kalshi.com/getting_started/fee_rounding
- https://docs.kalshi.com/api-reference/market/get-markets
- https://docs.kalshi.com/api-reference/events/get-event
- https://docs.kalshi.com/api-reference/market/get-series

Treat documentation as authoritative over assumptions in this prompt if schemas change. (Reading public documentation pages is fine; calling Kalshi API endpoints is NOT.)

## Step 2.1 — Define domain models

**Market:** market_id, ticker, event_id, series_id, title, description, status, opening and closing timestamps, expiration timestamp, settlement source, settlement rules, rules version/hash, outcome type, price grid, exchange-specific metadata, data-source provenance.

**OrderBook:** market_id, source, source_sequence, connection_id, subscription_id, exchange timestamp (when supplied), local receive timestamp, last successful synchronization timestamp, YES bid levels, NO bid levels, synchronization status, health status.

**PriceLevel:** side, price, available quantity.

**Relationship:** relationship_id, relationship_type, constituent markets, mathematical constraints, admissible settlement scenarios, verification status, evidence and reasoning, rules hashes, reviewer, creation/update timestamps.

**Detection:** detection_id, relationship_id, detection timestamp, contributing snapshot references, theoretical deviation, gross portfolio edge, total estimated fees, net theoretical edge, maximum depth-supported quantity, worst-case payoff, timestamp skew, data freshness, validation classification, rejection reasons, proof certificate, simulation metadata.

## Step 2.2 — Handle fixed-point pricing correctly

Do not assume all prices are integer cents. Current Kalshi API conventions use: decimal dollar prices, potentially with four decimal places; fixed-point contract quantities, potentially with two decimal places; market-dependent tick grids; different response field names for REST and WebSocket payloads.

Build a normalization layer. Use Decimal arithmetic for monetary operations (or integer fixed-point units internally with rigorously documented scaling). Never use binary floating-point arithmetic as the authoritative money representation. Do not round prices prematurely. Parse the valid tick grid dynamically from market metadata. Support zero displayed quantities and empty books. Reject malformed, negative, or out-of-range book states.

## Step 2.3 — Implement the correct binary order-book representation

The exchange exposes YES bids and NO bids. Asks are derived from opposing bids:

YES ask = $1 - NO bid; NO ask = $1 - YES bid.

E.g., if the best NO bid is $0.6200, the implied best YES ask is $0.3800. The corresponding available quantity equals the quantity resting at that opposing bid level.

When constructing a buy-side ask curve: (1) obtain the opposite-side bids; (2) sort them from highest bid to lowest; (3) convert each to its complementary ask; (4) preserve available quantities; (5) walk the resulting asks from cheapest to most expensive.

Do not invent ask-side liquidity. Do not use last-traded prices as executable prices.

## Step 2.4 — Create normalization tests

Test: ordinary whole-cent prices; sub-cent price levels; fractional contract quantities; empty bid sides; multiple depth levels; prices at tick-grid boundaries; malformed input; rounding precision; complementary YES/NO price consistency.

**Acceptance criteria:** All domain models are typed, validated, tested, and independent of a specific exchange connector.

---

# 6. Phase 3 — Build the synthetic exchange

This phase is crucial because the product must work without external market access. Do not generate arbitrary disconnected random prices and call that a market. Build a deterministic simulation with coherent underlying event probabilities and order-book behavior.

## Step 3.1 — Implement a synthetic market generator

Generate market families involving: an election with several candidate outcomes; a numerical economic indicator with nested thresholds; a daily temperature with multiple intervals; a sports match with mutually exclusive outcomes; two semantically identical event contracts; two related events where one implies another.

Each generated market must have: a stable ID; a ticker; a descriptive title; formal settlement conditions; a set of valid outcomes; configurable spreads; multiple bid levels; available liquidity; timestamped book updates.

## Step 3.2 — Generate coherent underlying probabilities

Start from a valid probability distribution over possible settlement states. Derive individual contract probabilities from that distribution. Add realistic bid-ask spreads and depth. This establishes a correctly priced baseline.

## Step 3.3 — Inject controlled inconsistencies

Provide deterministic scenarios including: overpriced narrow threshold relative to wider threshold; underpriced complete outcome basket; apparent discrepancy caused by stale data; apparent discrepancy eliminated by fees; apparent discrepancy eliminated by insufficient depth; genuinely inconsistent executable quotes in a static synthetic environment; missing or ambiguous settlement outcomes; cross-market inconsistency that lasts only a few updates.

Every injected event must have a known expected classification.

## Step 3.4 — Implement a realistic event stream

Generate: initial order-book snapshots; incremental order-book deltas; market status changes; market creation/removal; connection interruptions; delayed messages; duplicate messages; sequence gaps; recovery snapshots.

Allow playback speeds of 0.5x, 1x, 2x, 5x, and 10x. Allow deterministic generation from a fixed random seed.

## Step 3.5 — Build multiple synthetic datasets

Bundle at least: (1) small smoke-test dataset; (2) normal trading session; (3) high-volatility session; (4) data-corruption and recovery session; (5) deliberately inconsistent market session; (6) large-scale performance dataset.

Every dataset must include metadata documenting the generator version, seed, and expected findings.

**Acceptance criteria:** The synthetic exchange creates repeatable, believable activity and contains ground-truth detections against which the analysis engine can be tested.

---

# 7. Phase 4 — Build real-time market-data ingestion

## Step 4.1 — Define a data-source interface

Abstract MarketDataSource interface supporting: get_markets(), get_events(), get_series(), get_market_orderbook(), subscribe_orderbooks(), unsubscribe_orderbooks(), get_market_rules(), get_connection_health().

Implement: SyntheticDataSource, ReplayDataSource, KalshiDataSource (authorization-gated). Core business logic must not care which source is supplying data.

## Step 4.2 — Implement snapshot processing

An order-book snapshot initializes the local book state and replaces the previous local state for that market. Validate that the snapshot: has a known market; contains valid sides and levels; contains valid quantities; uses the expected schema; has valid sequencing metadata. Mark the market synchronized only after receiving and validating a proper snapshot.

## Step 4.3 — Implement delta processing

For each update: (1) validate the message; (2) determine the target market and side; (3) apply the signed quantity change to the specified price level; (4) remove levels whose resulting quantity becomes zero; (5) reject states with negative quantity; (6) update sequence and timestamp metadata; (7) publish the revised book to the detection engine.

Kalshi's WebSocket examples include `orderbook_snapshot`, `orderbook_delta`, `sid`, and `seq`. Do not assume sequence numbering is independently scoped to each market. Respect the provider's documented sequencing and subscription behavior.

## Step 4.4 — Handle failures correctly

Implement: bounded exponential-backoff reconnection; authentication failure handling; heartbeats; ping/pong handling; sequence-gap detection; duplicate-message handling; resubscription after reconnect; fresh-snapshot recovery; message validation; timeouts; backpressure and bounded queues.

If sequencing becomes unreliable, mark affected books UNSYNCHRONIZED. Never continue issuing verified detections against an untrusted local book.

## Step 4.5 — Capture timing metadata

Record: exchange-generated timestamp (if available); local receive timestamp; processing-start timestamp; detection-completion timestamp; connection identifier; subscription identifier; source sequence. Track latency attributable to this application separately from network or exchange latency.

## Step 4.6 — Rate-limit and authorization design

For the optional authorized connector: respect current rate limits and token costs; use documented pagination; handle HTTP 429 with bounded exponential backoff; avoid unnecessary polling; use existing subscriptions where possible; do not attempt to circumvent restrictions; store and expose configuration health without leaking credentials. Do not execute this adapter unless expressly enabled by the operator after the required authorization.

**Acceptance criteria:** The ingestion engine handles thousands of synthetic incremental updates and recovers safely after malformed streams and simulated disconnections.

---

# 8. Phase 5 — Design the mathematical relationship engine

This is the most important intellectual component of the project. The objective is not to guess whether two markets seem related. The objective is to prove that specific market outcomes obey formal logical constraints.

## Step 5.1 — Implement relationship types

### Type A: Implication
A implies B. Example: A = temperature exceeds 90°F; B = temperature exceeds 85°F. If both contracts reference the same measurement, location, observation window, units, and settlement methodology, A implies B. Constraint: P(A) <= P(B). A violation exists when the narrower event is priced above the broader event.

### Type B: Mutually exclusive outcomes
A and B cannot both settle YES. Constraint: P(A) + P(B) <= 1; for larger sets sum(P(i)) <= 1. IMPORTANT: mutually exclusive means at most one outcome occurs. It does NOT prove that one outcome must occur.

### Type C: Exhaustive partitions
Exactly one outcome must occur. Constraint: sum(P(i)) = 1. Only apply this when exhaustiveness has been independently verified.

### Type D: Equivalent outcomes
Two contracts settle identically under every admissible state. Constraint: P(A) = P(B). Require sufficiently strong settlement-rule verification.

### Type E: Nested numerical thresholds
Example: X > 100 implies X > 90. Support exact comparisons: >, >=, <, <=. Pay special attention to inclusive boundaries, units, and rounding rules.

### Type F: Disjoint numerical intervals
Example: 80°F <= X < 85°F and 85°F <= X < 90°F. Disjoint if measurement and resolution conventions are identical. Only mark a complete interval set exhaustive if it covers the entire admissible numerical range, including boundary cases.

## Step 5.2 — Build a rules-verification layer

A relationship must contain evidence establishing: identical underlying measured quantity (where applicable); compatible event definitions; same geographical scope; same observation period; same settlement source and methodology; compatible rounding and boundary rules; compatible early-close and exceptional-resolution conditions; complete coverage when claiming exhaustiveness.

Do not infer logical equivalence from similar titles alone.

Three states: VERIFIED, CANDIDATE_REVIEW, REJECTED. Only VERIFIED relationships can produce confirmed mathematical classifications.

## Step 5.3 — Use deterministic discovery first

Automatically propose relationships when market metadata is sufficiently structured: grouping by event and series; threshold parsing; unit normalization; interval overlap checks; implication detection; contract-rule fingerprinting; settlement-rule compatibility checks.

Optional semantic matching may propose candidate relationships, but it must never certify them. Do not send proprietary market metadata to external LLMs without authorization.

## Step 5.4 — Implement manual verification

Administrative interface or configuration workflow allowing a developer to: review proposed relationships; inspect full rules; approve or reject them; specify admissible outcome scenarios; record why verification is valid; invalidate relationships after rule changes. Every verified relationship must retain a reproducible explanation.

## Step 5.5 — Create a formal scenario representation

Represent each admissible terminal state as a vector of binary market outcomes. E.g. if A implies B, valid states: (A=0,B=0), (A=0,B=1), (A=1,B=1); invalid: (A=1,B=0). Exclude logically impossible states. Generate truth tables for small groups. For larger relationships, use constraint-based representations rather than naively enumerating an exponential number of states.

## Step 5.6 — Prove settlement payoff assumptions

For every candidate portfolio, calculate its payoff in every admissible terminal state. Never assume the payout is constant unless demonstrated. A relationship is unsuitable for automated portfolio analysis if the possible settlement states cannot be modeled reliably.

**Acceptance criteria:** Unit tests prove that the relationship engine rejects false equivalences and correctly handles implication, exclusivity, exhaustiveness, thresholds, and edge cases.

---

# 9. Phase 6 — Build the pricing and portfolio engine

## Step 6.1 — Implement executable price calculations

For buying YES, consume the implied YES ask curve; for buying NO, consume the implied NO ask curve. Given a requested quantity: start at the cheapest ask; consume available size; move to the next level; continue until filled or liquidity exhausted; calculate total premium; preserve the individual levels consumed.

Return: requested quantity; available quantity; filled quantity; total premium; VWAP; marginal execution price; unfilled quantity; levels consumed. Do not assume top-of-book depth covers an entire portfolio.

## Step 6.2 — Implement canonical portfolio constructions

- **Implication** (A implies B): buy NO(A) and YES(B) in equal quantity. Every valid state pays at least $1 per paired unit. Check whether total cost + fees is below this guaranteed minimum.
- **Exhaustive partition** (exactly one of N): buy YES on every outcome; pays exactly $1 per basket. If total cost incl. fees < $1, positive worst-case edge.
- **Mutually exclusive** (at most one of N): buying NO on every outcome guarantees at least N-1 dollars per basket. Compare against acquisition cost. Do not reuse exhaustive YES-basket logic unless exhaustiveness is proved.
- **Equivalent**: compare YES(A)+NO(B) and NO(A)+YES(B). Each pays exactly $1 across verified states. Evaluate both directions.

## Step 6.3 — Implement general payoff verification

q_i = quantity of contract i; c_i = total acquisition cost; f_i = fees; payout_i(w) = settlement payout of contract i in admissible state w.

Total cost = sum(c_i) + sum(f_i)
Minimum terminal payout = min over valid w of sum(q_i × payout_i(w))
Worst-case theoretical profit = minimum terminal payout − total cost

Fee-adjusted positive only when worst-case theoretical profit > 0. Also report an explicitly separate execution-adjusted estimate after conservative buffers. Do not use average predicted probabilities; the guarantee comes from payoff constraints, not forecasts.

## Step 6.4 — Model real displayed liquidity

Calculate portfolio capacity using all required legs. For equal-sized baskets, max supported quantity is bounded by executable depth across every leg. For every candidate quantity: recalc average prices, fees, total cost, minimum payoff, net edge. Optimize capacity or total theoretical profit over feasible quantities. Use integer-compatible contract increments and exact money accounting. A general constrained optimizer may use SciPy/HiGHS, but preserve an independently tested simple implementation for canonical templates.

## Step 6.5 — Implement fee models

Modular FeeCalculator supporting: configurable quadratic fee models; different market/series fee configurations; event-level overrides; effective-date changes; fee waivers; taker-fee assumptions; fractional quantities; sub-cent pricing; rounding; conservative unknown-fee handling.

Use official fee definitions for any authorized real integration; consult https://docs.kalshi.com/getting_started/fee_rounding. Do not globally hardcode a single fee percentage. The synthetic exchange may use an explicitly documented fictional fee schedule. If a real fee cannot be verified, mark the candidate FEE_UNVERIFIED and do not classify it as fee-adjusted verified.

## Step 6.6 — Explicitly handle execution uncertainty

Calculate and store: max constituent book age; timestamp skew between constituent markets; local processing latency; required contract size; max displayed capacity; conservative slippage buffer; conservative fee buffer.

Configurable thresholds: max_book_age_ms, max_cross_market_skew_ms, minimum_net_edge, minimum_available_quantity, assumed_extra_slippage_per_leg, minimum_candidate_duration_ms. Cautious defaults, documented as assumptions. Reject stale candidates.

Independently observed order books do not establish an atomic multi-market execution opportunity. Never imply otherwise.

## Step 6.7 — Add classification results

Exactly one primary status per evaluation: INVALID_RELATIONSHIP, UNSYNCHRONIZED_DATA, STALE_DATA, INSUFFICIENT_LIQUIDITY, FEE_UNVERIFIED, THEORETICAL_ONLY, DEPTH_SUPPORTED, FEE_ADJUSTED_CANDIDATE, NO_OPPORTUNITY. Plus secondary reason codes for detailed diagnosis.

**Acceptance criteria:** The engine produces correct, independently reproducible calculations on synthetic fixtures, including counterexamples that appear profitable but are not.

---

# 10. Phase 7 — Build the detection pipeline

## Step 7.1 — Trigger incremental scans

On book change: (1) identify relationships containing that market; (2) check constituent books are synchronized; (3) check freshness; (4) evaluate theoretical constraints; (5) run portfolio evaluator if relevant; (6) classify; (7) persist meaningful state changes; (8) broadcast to frontend. Do not rescan every relationship after unrelated updates. Maintain efficient market→relationship indexes.

## Step 7.2 — Avoid alert spam

Lifecycle states: OPEN, UPDATED, EXPIRED, RESOLVED, INVALIDATED. Merge consecutive signals for the same relationship and portfolio direction. Store: first observed; last observed; max deviation; max simulated capacity; max net edge; current status; expiration reason.

## Step 7.3 — Preserve auditability

Every detection references: exact input book state; market rules version; relationship proof; fee schedule version; relevant configuration; candidate portfolio; calculated costs; admissible scenarios; worst-case settlement payoff; reason for final classification. Same inputs → same mathematical result.

**Acceptance criteria:** Known injected synthetic inconsistencies are correctly found, explained, and classified without excessive duplicate alerts.

---

# 11. Phase 8 — Design the PostgreSQL database

Normalized tables: data_sources, series, events, markets, market_rules, market_price_ranges, relationships, relationship_members, relationship_reviews, fee_schedules, ingestion_sessions, orderbook_snapshots, orderbook_updates, detections, detection_legs, detection_scenarios, replay_sessions, system_health, configuration_versions.

UUIDs or stable deterministic identifiers. Index: market ticker; event ticker; relationship membership; detection timestamp; detection classification; ingestion session and sequence; snapshot lookup; replay event ordering.

Alembic migrations. High-frequency events use bounded batching and retention policies; no unbounded stream of individual transactions. For synthetic data, retain enough to reconstruct every demo session exactly. For third-party data, do not store/publish/retain beyond permitted usage. Transactional consistency for saving detections + portfolio evaluations. Timezone-aware UTC timestamps.

**Acceptance criteria:** Database initialization and migrations work from scratch, and all relationships and detections remain consistent after service restarts.

---

# 12. Phase 9 — Build historical replay

## Step 9.1 — Record event streams
Record: initial snapshots; incremental updates; sequence metadata; event timestamps; receipt timestamps; market metadata versions; fee configuration versions; relationship rule versions. Preserve original ordering; deterministic replay order.

## Step 9.2 — Replay processor
Given session ID and position: restore latest valid checkpoint; reapply events deterministically; rebuild books; recalc relationship states; recalc detections; compare against previously persisted detections. Support: start, pause, resume, restart, step forward, jump to timestamp, change playback speed.

## Step 9.3 — Visual replay
Open a detection → "Replay." Show: markets involved; changing bid/ask prices; relevant book levels; the mathematical constraint; current theoretical deviation; fee-adjusted evaluation; moment first detected; moment it disappeared.

## Step 9.4 — Deterministic replay tests
Same recording twice → identical book states, classifications, mathematical outputs, event ordering. Wall-clock telemetry may differ.

**Acceptance criteria:** Historical replay is mathematically reproducible and usable directly from the frontend.

---

# 13. Phase 10 — Build the dashboard

Serious financial-infrastructure feel, not a startup landing page. Minimalist dark theme: charcoal background, muted borders, high-contrast typography, restrained green/red, monospaced numerics, dense readable tables, excellent alignment, subtle animations, responsive desktop/tablet. Do not copy Kalshi's branding, design, or interface.

- **Overview `/`:** current data source; source health; markets monitored; verified relationships; total detections; open detections; sync health; p50/p95 internal processing latency; recent detections table; detection activity over time; links to details. Prominent "Synthetic simulation" label when synthetic. Never label synthetic activity as real-world trading.
- **Relationship explorer `/relationships`:** type; constituents; verification status; category; current constraint evaluation; latest update; link to proof. Filters: type, status, category, market, number of constituents. Relationship detail page with formal constraints, supporting rules, admissible states, full provenance.
- **Detection explorer `/detections`:** sortable/filterable table. Columns: time, relationship, type, theoretical deviation, gross edge, fees, net edge, depth-supported qty, duration, classification. Filters: classification, relationship type, time range, min net edge, min quantity, market. Row → full report.
- **Detection details `/detections/[id]`:** short English explanation; mathematical relationship; constituent markets; relevant settlement rules; bid/ask curves; exact legs; walked book levels; transaction-cost breakdown; worst-case payout; net edge; execution caveats; full timestamps and sequence provenance; lifecycle; Replay button. Scenario-payoff table for every valid terminal state (manageable sizes). "Copy technical report" button.
- **Market explorer `/markets`:** market, category, status, best YES bid/ask, best NO bid/ask, spread, depth, last update, data source. Market detail page with real-time depth visualization with axes and labels.
- **Replay `/replay/[id]`:** replay controls; synchronized views of book evolution, prices, constraint deviations, detection states, timeline.
- **Methodology `/methodology`:** probability consistency; supported relationships; executable quote derivation; fee estimation; depth effects; why asynchronous quotes produce false signals; why a fee-adjusted candidate is not a realized trade; data-source limitations; how the engine is tested.
- **About `/about`:** open-source engineering project, architecture, independent status, links to source and docs.

Frontend behavior: loading, empty, error, reconnect states; live freshness indicators; consistent decimal precision; keyboard accessibility; mobile-friendly; copyable IDs; tooltips. Never invent data when backend is unavailable. Distinguish source-provided values from calculated metrics.

**Acceptance criteria:** All eight page categories function with backend data, navigation works, critical journeys pass browser E2E tests.

---

# 14. Phase 11 — Implement backend APIs

Versioned endpoints:

- GET `/api/v1/health/live`
- GET `/api/v1/health/ready`
- GET `/api/v1/system/status`
- GET `/api/v1/system/metrics`
- GET `/api/v1/markets`
- GET `/api/v1/markets/{id}`
- GET `/api/v1/markets/{id}/orderbook`
- GET `/api/v1/relationships`
- GET `/api/v1/relationships/{id}`
- GET `/api/v1/detections`
- GET `/api/v1/detections/{id}`
- GET `/api/v1/detections/{id}/proof`
- GET `/api/v1/replay/sessions`
- GET `/api/v1/replay/sessions/{id}`
- POST `/api/v1/replay/sessions/{id}/start`
- POST `/api/v1/replay/sessions/{id}/pause`
- POST `/api/v1/replay/sessions/{id}/seek`
- GET `/api/v1/stream`

SSE or backend-owned WebSocket for frontend updates. Never expose exchange credentials to the browser. Include: pagination; filtering; input validation; predictable error responses; API schema docs; rate limiting where appropriate; controlled CORS. Replay mutation endpoints protected appropriately if public (isolated, authenticated, or per-session replay state). All prices/quantities in JSON preserve exact fixed-point representations (strings). Do not silently convert Decimals into imprecise JS numbers.

**Acceptance criteria:** Every documented endpoint has contract tests and serves real values from the database or engine.

---

# 15. Phase 12 — Build a rigorous test suite

## Unit tests
Monetary arithmetic; price normalization; YES/NO complements; depth walking; relationship parsing; logical constraint evaluation; payoff calculation; fee models; detection classifications; timestamp freshness; snapshot/delta application.

## Property-based tests (Hypothesis)
1. All book quantities are nonnegative.
2. Buying additional quantity cannot reduce total gross purchase cost.
3. Increasing acquisition fees cannot improve a portfolio's net edge.
4. Adding valid settlement scenarios cannot increase the calculated minimum payoff.
5. A verified implication relationship excludes A=1, B=0.
6. A verified exclusive group never allows more than one YES outcome.
7. A verified exhaustive group always contains exactly one YES outcome.
8. Replaying the same ordered messages gives the same state.
9. Invalidating a relationship prevents verified classifications.
10. Increasing assumed slippage cannot increase the conservative edge estimate.

## Required golden fixtures
- **Test A — Correct implication:** A implies B. Buy NO(A) $0.30, buy YES(B) $0.35. Total premium $0.65. Min payout $1.00. Gross edge $0.35 per paired unit. Engine identifies relationship and edge before fees.
- **Test B — Exhaustive three-outcome basket:** YES asks $0.25, $0.30, $0.35. Basket cost $0.90. Guaranteed payoff $1.00. Gross edge $0.10.
- **Test C — Non-exhaustive outcomes:** three mutually exclusive outcomes but an additional outcome remains possible. Buying all three YES does not guarantee positive payout. Engine must reject the exhaustive-basket strategy.
- **Test D — Insufficient depth:** one leg 100 contracts, another 3. A 10-contract basket cannot be fully supported. Engine must not claim 10 are depth-supported.
- **Test E — Fees remove profitability:** positive pre-fee edge, negative post-fee edge. Expected THEORETICAL_ONLY or another documented non-actionable state.
- **Test F — Stale books:** otherwise profitable basket with stale quotes → STALE_DATA.
- **Test G — Boundary mismatch:** threshold contracts referring to differently rounded observations. Reject invalid implication.
- **Test H — Recovery:** inject missing WS sequence numbers. Book marked unsynchronized, no verified opportunities emitted, processing resumes after valid recovery snapshot.
- **Test I — Replay consistency:** replay identical synthetic events twice → identical detection output.
- **Test J — Fractional precision:** fractional quantities and 4-dp prices; exact calculation and serialization without float drift.

## Integration tests
Test containers or Docker services. Verify: migrations; real ingestion flow; synthetic generation; API responses; detection persistence; replay; frontend streaming updates.

## Frontend E2E (Playwright)
Opening dashboard; viewing live synthetic activity; filtering relationships; opening a detection; inspecting proof; starting replay; pausing replay; seeking to timestamp; copying technical report; handling backend disconnection.

**Acceptance criteria:** All mandatory tests pass before production deployment. Do not weaken tests to hide failures.

---

# 16. Phase 13 — Performance engineering

Benchmark harness, synthetic data only. Workloads: 50, 250, 1,000, and 5,000 (if practical) markets. Configurable message rates. Measure: messages/sec; internal p50/p95/p99 latency; detection calc latency; DB write throughput; memory; CPU; queue backlog; reconnection recovery time; replay throughput.

Targets (goals, not claims): ≥1,000 synthetic book updates/sec on documented dev hardware if attainable; p95 internal detection latency < 100 ms for the focused workload; no incorrect positive classifications in golden fixtures; no unbounded queue growth under backpressure; deterministic replay correctness.

Record actual hardware, load parameters, methodology, results. Never invent benchmark results. Do not benchmark Kalshi's service. Create `docs/benchmarks.md` after measurement. If targets not achieved, report actual results and bottlenecks.

---

# 17. Phase 14 — Reliability and security

Structured logs; health checks; readiness checks; worker restart handling; DB connection recovery; graceful shutdown; env validation; credential isolation; secret redaction; request timeouts; input validation; dependency vulnerability checks; reasonable public API rate limits.

Do not log private keys. Do not store third-party credentials in PostgreSQL. Do not expose stack traces publicly. No arbitrary URL requests or shell execution via user-controlled endpoints. Document all env vars. `.env.example` with safe placeholders only.

---

# 18. Phase 15 — Deployment

Production-ready config: Dockerfiles for backend and frontend; production config; migration handling; persistent synthetic data storage; worker/API process separation; graceful shutdown; health checks; restart policies; HTTPS-ready; env-specific settings. Low-cost, student-portfolio-suitable. Minimum: Next.js frontend, FastAPI service, async synthetic worker, PostgreSQL.

Exact deployment instructions for at least one practical hosting option. Do not purchase infrastructure or deploy to paid services without permission. If credentials unavailable, complete and test local deployment config and document the remaining external step.

**Acceptance criteria:** The full Docker Compose environment starts successfully and serves a functioning application.

---

# 19. Phase 16 — Documentation

Polished README: Introduction (~150 words on the engineering problem); Visual demo (actual captured screenshot/GIF — never AI-generated); Features; Architecture (Mermaid diagram); Mathematical model; Getting started (exact copy-and-run commands); Data sources (synthetic, replay, authorized live); Tests; Benchmarks (genuine measurements); Limitations (settlement-rule ambiguity, data-feed latency, non-atomic executions, liquidity disappearance, incomplete relationships, fee assumptions, market halts, capital/position constraints, why theoretical opportunities are not realized profits); Roadmap; Independent project disclosure (not an official Kalshi product, not endorsed by Kalshi).

A technically experienced reader should understand how data enters, how relationships are verified, how detections are computed, how incorrect detections are suppressed, how results can be reproduced. No exaggerated claims or marketing jargon.

---

# 20. Phase 17 — Demonstration

20–30 second sequence showing: (1) synthetic market moving in real time; (2) engine monitoring relationships; (3) injected logical price inconsistency; (4) immediate identification of the relationship; (5) liquidity evaluation; (6) fee-adjusted calculations; (7) detailed mathematical proof; (8) historical replay of the event. Easy to follow without narration. Synthetic labeling throughout. Repeatable script that initializes the scenario.

Create: demo init command; demo script; screenshot instructions; GIF/video recording instructions; short technical explanation; clear description of synthetic nature. If recording tools are available, record the actual app; otherwise provide exact capture steps.

---

# 21. Phase 18 — Optional authorized Kalshi connector

Complete the full synthetic project first. Implement connector code and offline contract tests using officially published schemas. Support (subject to authorization): REST market discovery; event metadata; series metadata; settlement rules; initial book snapshots; authenticated WS subscriptions; incremental updates; reconnection; subscription updates; fee schedule interpretation; pagination; rate-limit compliance.

Details: WS auth uses documented signed request headers; WS book feed sends snapshots and deltas; delta quantities are signed fixed-point strings; REST and WS book payloads differ; subscription sequence numbers must be tracked correctly; market fee configs vary; statuses and price grids can change.

Mock HTTP and WS transports for all automated connector tests. Must not require network access to pass CI. Not automatically enabled. Startup guard preventing live integration unless both explicit flags are enabled. If authorization absent, report connector as "Implemented, not activated."

---

# 22. Phase 19 — Final QA

Functional: all pages load; market updates reach frontend; relationships discoverable; detections mathematically correct; depth and fee calcs correct; replay works; reports work; filtering/sorting work; restarts don't corrupt state.

Engineering: no critical type errors; no lint failures; no placeholder handlers; no fabricated responses; no broken links; no unhandled exceptions on normal paths; no secrets; no live trading; no unauthorized real-market access.

Tests: run complete suite; record executed/passed/failed/skipped (and why); build results; performance measurements. Never report tests as passed unless executed successfully.

UI: real browser review of desktop, mobile, readability, numeric formatting, hierarchy, empty/error states, replay controls. Fix defects.

Docs: check every README command and env var against the real repo.

---

# 23. Required delivery artifacts

1. Working frontend. 2. Working FastAPI backend. 3. Working synthetic exchange. 4. Working ingestion pipeline. 5. Relationship discovery and verification. 6. Mathematical pricing engine. 7. Detection engine. 8. Fee modeling. 9. Order-book depth evaluation. 10. PostgreSQL persistence. 11. Deterministic historical replay. 12. Interactive dashboard. 13. Documented REST and streaming APIs. 14. Comprehensive test suite. 15. Reproducible benchmarks. 16. Docker deployment config. 17. Complete README. 18. Architecture docs. 19. Mathematical methodology. 20. Working demo scenario. 21. Authorization-gated Kalshi adapter with offline tests.

Do not replace incomplete functionality with hardcoded frontend data.

---

# 24. Implementation order and stopping rules

Order: 1 bootstrap; 2 core financial models; 3 synthetic exchange; 4 ingestion/normalization; 5 relationship verification; 6 payoff/portfolio math; 7 fee/depth; 8 detection engine; 9 DB/persistence; 10 APIs; 11 frontend; 12 replay UI; 13 tests/hardening; 14 performance; 15 deployment; 16 docs/demo; 17 optional authorized integration.

At the end of each phase: run relevant tests; fix failures; update PROGRESS.md; record technical decisions; identify remaining limitations; commit the milestone (git is initialized locally).

Do not skip the mathematics to make the UI look complete. Do not skip integration tests. If blocked by credentials/authorization/infrastructure: implement everything possible locally; use deterministic mocks; document the blocker; continue; never pretend blocked integration has been verified.

---

# 25. Definition of done

A new developer can clone the repository, run the documented startup command, open a functioning dashboard, observe simulated live markets, see correctly detected market inconsistencies, inspect the exact financial reasoning, replay the corresponding events, and run a test suite that verifies the calculations — without private credentials, exchange permissions, or paid cloud infrastructure. Real backend, database, streaming data, calculations, persistence, and replay. Correctness, reliability, and reproducibility take priority over visual polish. Do not claim completion if critical functionality remains unimplemented.

---

# Owner checkpoint instructions

- Build phase by phase with working, tested checkpoints. Commit each milestone.
- **Checkpoint 1 (owner inspects personally): the Phase 6 pricing engine.** Before significant frontend work, it must correctly calculate all golden fixtures, including cases that look profitable but aren't.
- **Checkpoint 2: the synthetic exchange triggers actual detections visible in the browser** (genuine end-to-end product).
- The synthetic version demonstrates algorithms and infrastructure; it cannot establish empirical claims about real Kalshi mispricing. No public Kalshi-specific findings or marketing without checking the Developer Agreement's disclosure restrictions (document this in docs/compliance.md).
- Target outcome: an exchange engineer can open the GitHub repo, inspect the mathematical model, run the tests, replay a detection, and recognize something technically substantial.
