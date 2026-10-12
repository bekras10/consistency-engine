# Delivery

This note is for an exchange engineer reading the synthetic build. It is not a
claim about live Kalshi prices, fills, or profit.

## What it is

Consistency Engine checks whether several binary books can be held together
under a settlement constraint. It rebuilds each book from a deterministic
synthetic exchange or a recorded journal, evaluates a relationship only after
the settlement checks pass, and stores a certificate with the detection. Money
is decimal. The default configuration does not call Kalshi.

This is an independent project. It is not an official Kalshi product and it is
not endorsed by Kalshi.

## Architecture

One worker reads the source, rebuilds books, and writes detections, legs,
scenarios, a journal, and checkpoints to PostgreSQL. A separate API process
reads that database. The dashboard polls an internal gateway. `GET
/api/v1/stream` tails the notification outbox. The diagram and the process
boundaries are in [architecture.md](architecture.md).

## What a passing run proves

A synthetic session can open, update, and close detections, store a qualifying
certificate whose SHA-256 matches the stored bytes, keep a different closing
evaluation, and replay the same journal to the same detection. An uncertified
row is not served as that proof. The local checks below are the evidence for
this tree.

## What it does not prove

It does not show a live Kalshi mispricing. It does not place orders. It does
not report a profit, a user, or a production opportunity. The Kalshi connector
is an unimplemented skeleton and stays disabled. Nothing in this delivery was
published to a public host.

## Certification

`GET /api/v1/detections/{id}/proof` returns the certificate only when
`proof_status` is `certified`, and that body includes the status. Any other
status is `403` with `{"error":"uncertified"}`. The certificate bytes are not
in that error. Detection listings and SSE resync snapshots include
`proof_status` and do not include the certificate document. The detection page
renders certificate sections only for `certified`. Otherwise the table says
"not a proof" and the page says the stored bytes are not a verified
certificate. The detail route can still return those bytes, with the status,
for investigation.

Historical recovery certifies a row only when a replay of that session's
journal, or a verified checkpoint of that same observation, reproduces the
hash already stored and agrees with the classification and any stored metrics,
legs, and scenarios. A different checkpoint is not substituted, and the
original hash is not rewritten. If that link is missing, the row stays
`uncertified`. `det-audit-rehash` is the regression for the old behavior: the
fake hash is kept and the row is uncertified. Each affected detection is one
savepoint, so a later failure does not undo an earlier certified repair.
Checkpoints are read one affected session at a time. A crash before Alembic
commits rolls the revision back.

## Checks on this machine

Host Postgres was `127.0.0.1:5433`. Port 5432 was not modified.

`make lint` was clean (`ruff check` and `ruff format --check`, 204 files).
`make typecheck` was clean (90 source files). `make test` was 514 passed,
0 failed, in 126.54s. Suites: unit 366, golden 34, property 24, replay 16,
integration 74. Golden expected values were not edited.
`test_upgrade_downgrade_upgrade` passed inside that run (downgrade to base,
upgrade to head, and the same pair again). Head in this tree is
`0006_certificate_audit`.

Frontend: `eslint .` clean, `tsc --noEmit` clean, Vitest 22 passed (7 files).
`scripts/e2e.sh` also ran `next build` (Next.js 15.5.27) before Playwright.
`node --test scripts/npm_audit_lib.test.mjs` was 4 passed.
`node scripts/check_npm_audit.mjs` printed `npm audit advisories: 2 advisory
id(s); production vulnerability count: 0` for `GHSA-vfj7-8cjw-p6xm` (braces)
and `GHSA-rj75-hqrm-r3gf` (postcss-selector-parser), then `npm audit matches
the allowlist. Production audit is empty.` Exit 0.
`uv export --frozen --all-groups --no-emit-workspace` and
`uvx pip-audit --requirement /tmp/ce-reqs.txt --disable-pip` printed
`No known vulnerabilities found` and exited 0.

`scripts/e2e.sh` with `E2E_SKIP_COMPOSE=1`, `POSTGRES_PORT=5433`, and
`WEB_PORT=3100` printed `Playwright passed, including the disconnected page.`
`e2e/dashboard.spec.ts` 1 passed (14.5s). `e2e/disconnected.spec.ts` 1 passed
(124ms).

`bash scripts/compose_smoke.sh` on project `consistency-smoke-local-92573`
printed `qualifying certificate sha256:0240383b5da7dea8aeb28ce478bd6e9655c5aa31468692fe1b938e0ebfb107fd matches stored JSON`,
`replay cursor 981/12277 shows det-f26eee89fc522826ab5d90f1`, and
`compose smoke passed: 12 detections, 1 completed session, restart kept the same ids`.
That project was removed by the script. Postgres was not published.

## Demo

The recordings were not replaced in this pass. `ffprobe` on the files present
in `docs/demo/`:

| File | Duration | Size |
|---|---|---|
| [demo.webm](demo/demo.webm) | 44.760000 s | 4,290,463 bytes |
| [demo.gif](demo/demo.gif) | 44.840000 s | 2,249,847 bytes |
| [demo-short.webm](demo/demo-short.webm) | 24.440000 s | 3,142,371 bytes |
| [demo-short.gif](demo/demo-short.gif) | 24.510000 s | 1,845,850 bytes |

The click path is in [demo.md](demo.md). On the already-running Compose
database, SHA-256 of the stored JSON for `det-f26eee89fc522826ab5d90f1` matched
both stored hashes: qualifying 14291 bytes,
`sha256:0240383b5da7dea8aeb28ce478bd6e9655c5aa31468692fe1b938e0ebfb107fd`
(`FEE_ADJUSTED_CANDIDATE`), closing 8281 bytes,
`sha256:1f837ec74d44b4012131a54da84259ea3443b646334fa0b336fc967364dc1140`
(`INSUFFICIENT_LIQUIDITY`). `proof_status` was `certified`.

## Benchmarks

Numbers below are copied from [benchmarks.md](benchmarks.md). They were not
remeasured for this delivery.

The 1000 msg/s line was met only on the earlier single-process run: 54 markets
at 1310.33 msg/s and 270 markets at 1093.72 msg/s. Later steady-state medians
are lower: 773, 794, 666, and 0 msg/s at 54, 270, 1026, and 5022 markets. On
the inconsistency workload, end-to-end detection latency is median 2281.125 ms
(n=144, p95 6614.0593 ms, p99 6811.6081 ms).

## Setup

From the repository root, matching the Makefile and [deployment.md](deployment.md):

```bash
make setup
docker compose -p consistency-engine -f compose.host-db.yml up -d db
export DATABASE_URL=postgresql+asyncpg://consistency:consistency@127.0.0.1:5433/consistency
make lint
make typecheck
make test
```

Host tests expect Postgres on `127.0.0.1:5433`. Do not point them at a
PostgreSQL instance on port 5432.

The local Compose stack, bound to loopback, is:

```bash
docker compose -p consistency-deploy up --build
```

`make demo` prints the synthetic click path. It does not start a public host.
`ENABLE_KALSHI_API` and `ENABLE_LIVE_TRADING` stay false.

## Remaining risks

Uncertified rows stay in the database for investigation. A reader that ignores
`proof_status` could treat the stored JSON as a proof; the proof route and the
dashboard do not. Replay viewer cursors live in the gateway process. The two
development npm advisories above stay allowlisted; the production count is 0.
The Kalshi connector is an unimplemented skeleton and stays disabled. Three
retention failures at the default `MAINTENANCE_MAX_FAILURES` of 3 stay healthy;
the fourth is unhealthy. A successful cycle older than
`MAINTENANCE_SUCCESS_MAX_AGE_S` (default 7200 seconds) is unhealthy even when
the process is alive. Downgrade of `0006_certificate_audit` drops
`proof_status` and does not undo a repaired certificate. The 2281.125 ms
end-to-end median was not remeasured.

GitHub Actions run
https://github.com/bekras10/consistency-engine/actions/runs/38189685534
finished with conclusion `success` on `c2a74f8` after a rerun of the backend
job. The first attempt failed while pulling the Postgres image
(`toomanyrequests: Data limit exceeded`) and did not run tests. The rerun's
backend job passed, including migrations up/down/up and integration tests.
Frontend, e2e, and Compose smoke passed on the first attempt.
