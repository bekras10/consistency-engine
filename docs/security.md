# Security

Phase 14. Nothing in this repository is a public deployment. Synthetic mode stays the
default. The Kalshi connector still requires both `ENABLE_KALSHI_API` and
`KALSHI_AUTHORIZATION_CONFIRMED`. No exchange credentials are required to run the
synthetic system, and none are stored in PostgreSQL.

## Logs

Worker and API logs are one JSON object per line on stderr (`ts`, `level`, `logger`,
`msg`, plus structured fields). The formatter redacts:

- passwords in `scheme://user:password@host` URLs, including `DATABASE_URL`
- `password=`, `token=`, `secret=`, `api_key=`, and `authorization=` assignments
- the value of `REPLAY_API_TOKEN` when it appears in a line
- `ce_replay_capability=...` cookies
- `Bearer` tokens
- fields whose names are `password`, `token`, `secret`, `database_url`, `cookie`, or
  `replay_api_token`

Exception text inside a log line is redacted the same way. Public HTTP error bodies
are not log lines: they are `{"error":"<code>"}` and do not include a stack trace or
the exception message.

## Health and readiness

`GET /api/v1/health/live` reports that the process is up.

`GET /api/v1/health/ready` reports the non-secret configuration summary. When
`DATABASE_URL` is set, readiness also runs `SELECT 1`. If that fails, or the check
raises, the response is `503` with `reason=database_unavailable`. The URL is not
included. The engine uses `pool_pre_ping`, so a connection taken after PostgreSQL
returns is checked before it is used. Worker restart after a database restart is
covered by `test_worker_recovers_after_postgres_container_restart` and
`test_restart_continues_lifecycle_without_duplicates`.

## Shutdown and worker restart

SIGINT and SIGTERM set the worker stop event. A deterministic session stops as
`interrupted` and can resume from its last checkpoint. A live session stops as
`stopped`. Background tasks (outbox, retention, ingestion supervisor) are cancelled
before the process returns. `test_worker_stop_on_deterministic_source_leaves_session_resumable`
covers the stop path.

The ingestion supervisor restarts a failed runner with backoff. Authentication
failures are not retried. The restart budget is `SUPERVISOR_MAX_RESTARTS`.

## Environment

Startup builds `Settings` and refuses live trading, a Kalshi mode without both
authorization flags, and non-positive timeouts, rate limits, and queue sizes.
`.env.example` lists every variable the process reads, with local placeholders only.
Do not commit a real `.env`.

## Credentials

PostgreSQL columns do not include password, token, or API-key fields.
`upsert_configuration` rejects a document that contains those keys, a URL with a
userinfo password, or a replay capability cookie. Session config stores a fingerprint
and content hashes, not `DATABASE_URL` or `REPLAY_API_TOKEN`.

Process entry points take configuration from the environment. The worker command
line is `python -m consistency_worker` and does not include the URL or the token.
`ps -o args=` for that process does not show them. The environment block of a
process listing still contains whatever the operator exported; that is how the
process receives `DATABASE_URL`.

## Public API

`/api/v1` requests, other than health and `GET /api/v1/stream`, are limited to
`API_RATE_LIMIT` per `API_RATE_WINDOW_S` per client (default 240 per 60 seconds).
The same routes time out after `API_REQUEST_TIMEOUT_S` (default 30 seconds) with
`504` `{"error":"timeout"}` only when the response has not started. If headers
have already been sent and the body stalls, the connection drops. A second
status line is not written. Over the rate limit is `429` `{"error":"rate_limited"}`.
Query validation failures are `422` `{"error":"invalid_request"}`.

The limiter key is the TCP peer. `X-Forwarded-For` is ignored unless
`TRUSTED_PROXY_COUNT` is at least 1 and that peer is listed in
`TRUSTED_PROXIES` (comma-separated exact addresses). The key is then the
address that many hops from the right, which is the hop a trusted proxy
appended. A prefix to the left does not open a new bucket. A client that is
not a configured proxy cannot rotate the header to evade the limit. Entries
whose newest hit is outside the window are removed. At most
`API_RATE_MAX_CLIENTS` identities are stored (default 4096). A new identity
past that cap is `429` and is not inserted.

No `/api/v1` handler fetches a caller-supplied URL or runs a shell command. Extra
`url` and `command` query parameters are ignored.

## Replay viewers and operators

These are different credentials.

| Credential | Who holds it | What it authorizes |
|---|---|---|
| `REPLAY_API_TOKEN` | The Next server, on the hop to the loopback gateway | Replay playback mutations only (`start`, `pause`, `resume`, `seek`, `restart`, `step`, `speed`) |
| `ce_replay_capability` cookie | One browser | That viewer's playback of one recording. Not retention, not configuration, not another viewer |

The cookie is `HttpOnly` and `SameSite=Lax`. It is `Secure` when the issuance
request is HTTPS or `REPLAY_COOKIE_SECURE=1`. Issuance is unauthenticated and
therefore capped: expired entries are deleted, at most `REPLAY_CAPABILITY_MAX`
capabilities are outstanding (default 256), and at most
`REPLAY_CAPABILITY_ISSUE_LIMIT` are issued per `REPLAY_CAPABILITY_ISSUE_WINDOW_S`
(default 30 per 60 seconds). A single client is also limited to
`REPLAY_CAPABILITY_CLIENT_LIMIT` (default 5) in that same window, so one
caller cannot spend the global budget. Past either cap the response is `429`.
The Next route does not see the TCP peer. It ignores `X-Forwarded-For` unless
both `TRUSTED_PROXY_COUNT` and `TRUSTED_PROXIES` are set, and then uses the
same rightmost-hop rule. While they are unset, every issuer shares one key,
so rotating the header does not create another bucket. Set those variables
only when Next is reached solely through that proxy.

Replay mutations reject a cross-site `Origin` with `403` `{"error":"cross_origin"}`.
Same-origin is allowed. A missing `Origin` is allowed because non-browser
clients (curl, the API test client, and other server-side tools) do not send
it. Browsers send `Origin` on cross-site POST. An unparsable or `null` origin
is rejected. This applies to `POST /api/v1/replay/...` and to dashboard replay
posts, including capability issuance. On the dashboard, `localhost` and
`127.0.0.1` are the same local site when the port and scheme match. Next
reports `localhost` in the route URL when the browser used `127.0.0.1`.
The `Host` header is accepted for the same comparison. A different host is
still rejected.

A viewer capability presented to any other POST, including retention or config,
is `403` `replay_forbidden`. The gateway is not called.

Viewer cursors live in the `ReplayHost` of the process that opened them. Two API
processes do not share that map. A restart starts an empty map. The recording and
the detection rows stay in PostgreSQL. `detections.detection_id` is the primary
key, so a resumed worker cannot insert a second row with the same id.
`test_restart_continues_lifecycle_without_duplicates` and
`test_worker_recovers_after_postgres_container_restart` check that a resume does
not leave duplicate detection ids.

## Outbox retention

`notification_outbox` and `outbox_claims` are read in pages (`LIMIT`, default 500),
not as one unbounded `SELECT` of the claims table. `pg_xact_status` is still the
cursor. A claim page that stops early does not classify an id past that page as
aborted.

`prune_consumed_outbox(consumed_through)` deletes rows and claims with
`id <= consumed_through` only when that value is at or below the commit-safe
watermark. Overlapping prunes take one advisory transaction lock and re-read
the floor, so the stored value only moves forward. The upsert uses `GREATEST`
as well. It records the floor in `outbox_retention.pruned_through`. It does not
rewind the id sequence. A reader still below the floor receives no notes and a
blocked cursor, and the SSE tail resyncs from the detection snapshot. The pruned
prefix is not delivered as a skip.

The worker retention loop, on `RETENTION_INTERVAL_S`, also runs that prune.
The consumer watermark is the contiguous commit-safe id. `OUTBOX_RETENTION_KEEP`
(default 10000) is how many of the newest ids stay. The delete target is
`watermark - OUTBOX_RETENTION_KEEP` and is never above the watermark. A client
that reconnects with `Last-Event-ID` still inside that suffix receives those
events, including the boundary id, and does not miss a later retained id. A
cursor at or below the floor gets `resync` (detection rows plus the new
watermark) instead of a tail that starts after the hole. Detection rows are
not deleted by this job, so the resync snapshot remains. 10000 is larger than
the bundled inconsistent session's detection-event count, so a normal synthetic
run does not prune a live cursor, and the outbox cannot grow without a bound.

## Dependency audits

`npm audit fix --force` was not run. Production dependencies were cleared by a
patch-level PostCSS override (`postcss@8.5.29` under Next 15.5.27). Vitest was
upgraded from 3.2.x to 5.0.3 after `vitest run`, `tsc --noEmit`, `eslint`, and
`next build` succeeded. That removed the tinypool and `@vitest/mocker`
advisories. `npm audit --omit=dev` then reported zero findings.

`uv pip audit` is not a subcommand (`unrecognized subcommand 'audit'`). The
Python audit is `pip-audit` 2.10.1:

```
uv export --frozen --all-groups --no-emit-workspace -o requirements.txt
uvx pip-audit --requirement requirements.txt --disable-pip
```

Workspace packages are omitted from that export because they are not on PyPI.
The command's output is in `PROGRESS.md`. CI runs the same check.

Residual `npm audit` findings are devDependencies only. Each advisory is named
in `apps/web/npm-audit-allowlist.json`. `node scripts/check_npm_audit.mjs` fails
when a full-audit advisory is missing from that file, when a listed advisory is
no longer reported, or when `npm audit --omit=dev` reports anything, including
a new critical production issue.

| Advisory | Scope | Why it remains | Upgrade that would clear it |
|---|---|---|---|
| GHSA-vfj7-8cjw-p6xm | dev | `braces@3.0.3` is the latest 3.x and is still affected. Tailwind CSS 3.4.19 pulls it through `chokidar` and `micromatch`. `eslint-config-next` 15 pulls it through `fast-glob`. These packages are not in the production bundle. | `tailwindcss@4.3.3` (major; different configuration) and a Tailwind 4-compatible ESLint config. `eslint-config-next`'s suggested fix is a downgrade to 14.2.35, which does not match Next 15. |
| GHSA-rj75-hqrm-r3gf | dev | `postcss-selector-parser` 6.x has no patched release. The fix is 7.1.6. Tailwind CSS 3 depends on `^6.1.2` through `postcss-nested`. Build-time only. | `tailwindcss@4.3.3`. Overriding the parser to 7.x was not done; Tailwind 3 does not declare that major. |
