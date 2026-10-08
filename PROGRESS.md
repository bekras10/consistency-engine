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
