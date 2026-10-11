# API reference

Public routes are served by `consistency_api` under `/api/v1`. OpenAPI is generated from those
routes at `/openapi.json` (interactive docs at `/docs`). Prices and quantities are fixed-point
strings. Error bodies are `{"error": "<code>"}` with no stack trace. A request that fails
validation is `422` `invalid_request`. An unknown id is `404` `not_found`.

The Next.js dashboard does not call these routes directly. It polls `/app-data`, which proxies
`scripts/dashboard_gateway.py` (`/internal/...`). That gateway is an internal adapter over the
same `consistency_persistence.dashboard` reads and the same `ReplayHost`. It is bound to
loopback and is not a second calculation. `GET /api/v1/stream` is available for a later switch;
polling stays the dashboard fallback.

## Reads

GET requests are open. This build assumes they are reached on a local or student deployment,
not on the public internet. Do not put exchange credentials in any response. There is no
account system.

| Method and path | Notes |
|---|---|
| `GET /api/v1/health/live` | Process is up. `{"status":"ok"}`. |
| `GET /api/v1/health/ready` | Configuration plus PostgreSQL when `DATABASE_URL` is set. `503` when not ready. |
| `GET /api/v1/system/status` | Data source, source health, and journal sync from `read_overview`. |
| `GET /api/v1/system/metrics` | Market, relationship, and detection counts, plus internal latency percentiles. |
| `GET /api/v1/markets` | Catalog rows. Journal status replaces the catalog status when the book manager has a later `market_status`. Query: `status`, `category`, `limit` (1–200, default 50), `offset`. |
| `GET /api/v1/markets/{id}` | One market, including the reconstructed book when the journal has it. |
| `GET /api/v1/markets/{id}/orderbook` | The same book object the market detail uses (`yes_bids`, `yes_asks`, and the depth chart). |
| `GET /api/v1/relationships` | Filters: `relationship_type`, `status`, `category`, `market_id`, `min_members`, `max_members`, plus `limit` and `offset`. |
| `GET /api/v1/relationships/{id}` | Relationship document, members, and provenance. |
| `GET /api/v1/detections` | Same filters as the dashboard query (classification, relationship type, time range, `min_net_edge`, `min_quantity`, market, sort). `limit` and `offset` page the filtered list. |
| `GET /api/v1/detections/{id}` | Stored detection, legs, scenarios, and certificate. |
| `GET /api/v1/detections/{id}/proof` | Certificate, legs, scenarios, and the technical report. |
| `GET /api/v1/replay/sessions` | Recorded ingestion sessions. |
| `GET /api/v1/replay/sessions/{id}` | A recording summary, or the live snapshot of a viewer id opened in this process. |
| `GET /api/v1/stream` | Server-sent events. See below. |

List payloads include `total`, `limit`, and `offset`. `limit` outside 1–200 or a negative `offset` is `422`.

## Replay controls

`POST /api/v1/replay/sessions/{id}/start` loads the recorded ingestion session and opens a
**new viewer**. The canonical journal is not updated. The response `replay_id` is the viewer
id (`viewer-...`). Pause, seek, resume, restart, step, and speed use that viewer id.
`PlaybackService` already isolates books by replay id; the fork is what keeps two clients
that start the same recording off a shared cursor.

The dashboard proxy does the same fork. Two browser tabs that start the same recording
receive different `viewer-...` ids and do not share a cursor.

| Method and path | Body | Effect |
|---|---|---|
| `POST .../start` | none | Fork a viewer and start playback. `{id}` is the recording. |
| `POST .../pause` | none | Pause that viewer. |
| `POST .../resume` | none | Continue playback. |
| `POST .../restart` | none | Cursor back to the start. Nothing is applied. |
| `POST .../step` | none | Apply one journal entry and pause. |
| `POST .../seek` | `{"timestamp_ms": <integer>}` | State after every entry with `now_ms` at or before that time. |
| `POST .../speed` | `{"speed": "0.5" \| "1" \| "2" \| "5" \| "10"}` | Fixed-point string. A JSON number is `422`. |

The gateway exposes the same actions on `/internal/replay/{id}/{action}`. A recording id
forks a viewer. None of these controls are gateway-only.

Viewer cursors live in the process that opened them. A restart drops them; the recording
does not. Idle viewers expire after `REPLAY_SESSION_TTL_S` seconds (default 1800). A viewer
that was read or commanded inside that window is not evicted, even if playback is still
running. An idle viewer's task is cancelled. More than `REPLAY_MAX_VIEWERS` open viewers
(default 32) is `429` `replay_capacity`.

### Authentication

Mutations require the header `X-Replay-Token` equal to the environment variable
`REPLAY_API_TOKEN`. If the variable is unset, mutations return `401`. Comparison uses
`secrets.compare_digest`.

`REPLAY_MUTATIONS_PUBLIC` defaults to false. Set it to `true` only to turn the check off
for a closed local demo. GET reads stay open either way, under the localhost assumption
above.

`401` body: `{"error":"unauthorized"}`.

The public API still requires `X-Replay-Token`. The dashboard does not. `POST /app-data/replay/...`
checks an httpOnly cookie, `ce_replay_capability`, issued by `POST /app-data/replay-capability`.
The cookie value is a random capability, not `REPLAY_API_TOKEN`. It lasts
`REPLAY_CAPABILITY_TTL_S` seconds (default 900). The first successful start binds it to the
forked viewer id. Later posts from that cookie may command only that viewer; a post aimed
at another viewer is `403` `{"error":"replay_forbidden"}`. A missing or expired capability
is `401` `{"error":"unauthorized"}`, including a request that presents `X-Replay-Token` and
no cookie. The Next server attaches `X-Replay-Token` only on the hop to the gateway. The
page HTML, the RSC payload, and `/app-data` responses do not contain the shared token.
`GET` stays open. The capability authorizes only playback actions (`start`, `pause`,
`resume`, `seek`, `restart`, `step`, `speed`) for that viewer. A post to retention,
configuration, or any other path is `403` `replay_forbidden` and is not proxied.
Issuance is capped (`REPLAY_CAPABILITY_MAX`, `REPLAY_CAPABILITY_ISSUE_LIMIT`). The
cookie is `HttpOnly` and `SameSite=Lax`, and `Secure` on HTTPS or when
`REPLAY_COOKIE_SECURE=1`. See `docs/security.md`.

Ordinary `/api/v1` requests, other than health and `GET /api/v1/stream`, are limited to
`API_RATE_LIMIT` per `API_RATE_WINDOW_S` (default 240 per 60 seconds) and time out after
`API_REQUEST_TIMEOUT_S` (default 30 seconds). The bodies are `429` `rate_limited` and
`504` `timeout`. Neither includes a stack trace.

If `DASHBOARD_GATEWAY_HOST` is not loopback (`127.0.0.1`, `::1`, or `localhost`), the
gateway requires the token on its own replay posts. The default bind is `127.0.0.1`.

## Stream

`GET /api/v1/stream` responds `text/event-stream`. Each event has an `id:` line (the outbox
id, or `0` when the log is empty), an `event:` name, and a `data:` JSON object.

On connect, without `Last-Event-ID`, the first event is `resync`: current detection rows
(`detection_id`, classification, status, certificate hash, session) and `outbox_id`, the
contiguous high-water mark. The connection then tails outbox rows with ids above that mark.
The dashboard may keep polling; this stream does not replace that path until a page is
switched, and polling remains correct on its own.

On reconnect, send `Last-Event-ID` with the last applied id. The stream emits that boundary
event again, then anything newer. Applying by id is idempotent, so the boundary is not a
second logical update. If that id is gone and a later id is already committed (the boundary
transaction aborted, or the row is no longer in the log), the stream sends `resync` and
tails from the new high-water mark instead of skipping ahead inside a hole. If
`Last-Event-ID` is greater than every reserved or committed outbox id, the database was
recreated or restored underneath the client. The stream sends `resync` on connect instead
of waiting for an id that will not appear.

`event:` for a tail row is the outbox topic (`detection`). `data` is
`{"topic", "session_id", "payload"}`. `payload` is the same document the in-process broker
publishes for a detection change.

## Notification outbox

Table `notification_outbox` (revision `0002_notification_outbox`):

| column | type | role |
|---|---|---|
| `id` | `BIGSERIAL` primary key | SSE id. Assigned at INSERT, before COMMIT. |
| `topic` | `text` | `detection` for detection writes. |
| `session_id` | `text` null | Ingestion session, when the event has one. |
| `payload` | `jsonb` | Broker document. Money stays strings. |
| `created_at` | `timestamptz` | Insert time. Not a cursor. |

The insert runs in the detection transaction. A consumer must not treat `id` order as commit
order. Two overlapping transactions can take ids 10 and 11, and 11 can commit first. A reader
that advances to 11 would lose 10 when it commits. A rollback of 10 leaves a permanent hole,
so blocking until every integer appears would stall.

`committed_notifications` reads the outbox rows in its snapshot, then `pg_xact_status` of
the xid stored in `outbox_claims` for each missing id. It does not query `pg_locks`. A
visible id is delivered. `aborted` is skipped. `in progress` holds every higher id.
`committed` while the row is missing from this snapshot also holds: the writer committed
after the snapshot, and the next read delivers the row. It is not treated as an aborted
hole. An unrelated transaction does not insert a claim, so it does not stall a contiguous
prefix. `created_at` is not used to order or delay delivery.

`resync` reads the detection rows and `contiguous_watermark` in one repeatable-read
transaction. The watermark is the contiguous high-water mark, not a 10 000-row prefix.
A commit that arrives during the read is not mixed into that snapshot.

## Limits and browsers

- About 240 requests per minute per client address on `/api/v1`, excluding the health probes.
  Excess is `429` `{"error":"rate_limited"}`.
- `GET /api/v1/stream` allows `SSE_MAX_CONNECTIONS` (default 32). The next client gets `503`
  `{"error":"stream_unavailable"}`.
- Idle streams send a `: heartbeat` comment every `SSE_HEARTBEAT_SECONDS` (default 15) so a
  proxy does not close a quiet socket.
- Each stream queues at most `SSE_QUEUE_MAX` frames (default 32). A slower client is
  disconnected instead of growing that queue. Reconnect with `Last-Event-ID`.
- Replay viewers: `REPLAY_MAX_VIEWERS` (default 32) and `REPLAY_SESSION_TTL_S` (default 1800).
- Recording previews: `REPLAY_MAX_PREVIEWS` (default 4). A preview past the cap is `429`
  `{"error":"replay_preview_busy","detail":"Too many replay previews are in flight."}`.
  The reconstructed book is not kept after the response. Open viewers do not use this cap.
- Dashboard replay capabilities: `REPLAY_CAPABILITY_TTL_S` (default 900).
- CORS origins come from `CORS_ORIGINS` (default `http://127.0.0.1:3000` and
  `http://localhost:3000`). Methods `GET`, `POST`, `OPTIONS`. Headers include
  `X-Replay-Token` and `Last-Event-ID`. Credentials are not allowed.
- The Kalshi connector stays authorization-gated. These routes do not call Kalshi and do
  not place orders.
