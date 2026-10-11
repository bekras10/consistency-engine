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
`504` `{"error":"timeout"}`. Over the rate limit is `429` `{"error":"rate_limited"}`.
Query validation failures are `422` `{"error":"invalid_request"}`.

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
(default 30 per 60 seconds). Past the cap the response is `429`.

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
watermark. It records the floor in `outbox_retention.pruned_through`. It does not
rewind the id sequence. A reader still below the floor receives no notes and a
blocked cursor, and the SSE tail resyncs from the detection snapshot. The pruned
prefix is not delivered as a skip.

## Dependency audits

The commands and the counts from the Phase 14 run are in `PROGRESS.md`. A clean
audit is recorded only when the tool printed zero findings.
