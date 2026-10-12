# Deployment

Local Docker Compose only. Nothing in this repository starts a public host, buys
infrastructure, or turns on the Kalshi connector. `ENABLE_KALSHI_API` and
`ENABLE_LIVE_TRADING` stay false.

The stack is Next.js, the FastAPI process, a separate synthetic worker process,
the internal dashboard gateway, and PostgreSQL 16. API and worker are the same
image with different commands. Images do not contain `.env` files or credentials.
The database password in the compose file is the local placeholder from
`.env.example`.

## What is published

| Port | Bind | Process |
|---|---|---|
| 3000 (`WEB_PORT`) | `127.0.0.1` only | Next.js frontend |

PostgreSQL, the API, and the gateway are on the compose networks and are not
published to the host. The browser talks only to the frontend. The frontend
calls the gateway at `http://gateway:8765` over the `app` network. Postgres is
on the `data` network, which is internal, and the frontend is not attached to
it. The gateway listens on `0.0.0.0` inside its container so the frontend can
reach it. That bind is not loopback, so replay posts on the gateway require
`X-Replay-Token`. The Next server attaches the token. The browser receives an
httpOnly capability cookie, not the token.

`TRUSTED_PROXY_COUNT` is 0 and `TRUSTED_PROXIES` is empty, so `X-Forwarded-For`
is ignored.

## Start

From the repository root:

```bash
docker compose -p consistency-deploy up --build
```

`docker-compose.yml` sets the project name `consistency-deploy`. That project
does not replace the host-test database container `consistency-engine-db-1`.
Host tests use `compose.host-db.yml` with project `consistency-engine` and
`POSTGRES_PORT` (5433 on this machine). That file publishes Postgres on
`127.0.0.1` only. `scripts/dev.sh` starts Next.js with `--hostname 127.0.0.1`.
The production compose file does not publish Postgres.

The `migrate` container runs `alembic upgrade head` and exits. The API
entrypoint runs the same upgrade again, then execs uvicorn so SIGTERM reaches
the server. The worker entrypoint starts two processes:
`python -m consistency_worker maintain` and `python -m consistency_worker`.
Maintenance applies retention on `RETENTION_INTERVAL_S` and keeps running after
ingestion finishes. The ingestion process stays up after a terminal session
until SIGTERM. The latest ingestion session for a fingerprint decides.
Ordinary startup resumes a `running`, `interrupted`, or `failed` session
from its checkpoint, including a later failed or interrupted reseed that
follows an older completed session. That resume keeps the reseed id and
does not open another one. If the latest session is `completed`, startup
skips ingest. An older interrupted row does not override that completed
session. `RESEED_SESSION=true` or `python -m consistency_worker run --reseed`
allocates a new session when the latest one is already completed. The
default is false. `stop_grace_period` is 30s on the worker and 20s on the
API. Restart policy is `unless-stopped` except for the one-shot migrate
container. If either worker process exits on its own, the entrypoint exits
non-zero so both come back together. A fingerprint whose latest session
completed is not opened twice by that restart.

Compose sets `DATA_SOURCE=synthetic`, `SYNTHETIC_PRESET=inconsistent`, and
`SYNTHETIC_PLAYBACK_SPEED=10`. Allowed playback speeds are 0.5, 1, 2, 5, and
10. The preset is 126000 ms of synthetic time. Speed 10 only shortens sleeps.
The worker still writes the detections. Detections are not loaded from a
fixture script.

## Stop

Stop and remove this project's containers, and leave the named volume:

```bash
docker compose -p consistency-deploy down
```

Remove the volume as well (this deletes the synthetic database):

```bash
docker compose -p consistency-deploy down -v
```

Do not run those commands against project `consistency-engine`. That is the
host-test Postgres.

## Environment

Placeholders live in `.env.example`. Compose overrides `DATABASE_URL` to
`postgresql+asyncpg://consistency:consistency@db:5432/consistency`. Other
values match the example: synthetic mode, both enable flags false, replay
token `local-replay-token`, `REPLAY_MUTATIONS_PUBLIC=false`, outbox keep
10000, rate-limit cap 4096. `REPLAY_COOKIE_SECURE` stays 0 because the
published frontend is HTTP on loopback.

## Detection latency

Pricing-only and book-update rates in `docs/benchmarks.md` are not
production-scale end-to-end detection throughput. On the inconsistency
workload, the measured end-to-end median from book update until the detection
transaction commits is **2281.125 ms** (n=144) on the Apple M3 run recorded in
that document. That is a latency limit of this engine, not a slogan. This
compose stack was not used for a new benchmark, and no new number is claimed
here.

## Worker health

`python -m consistency_worker health` is the compose healthcheck. It reads the
latest ingestion session and, when the entrypoint expects maintenance, the
heartbeat file the maintenance process rewrites. It does not treat a
successful PostgreSQL ping, or a historical `completed` row, as proof that
the maintenance process is alive.

The worker service sets `EXPECT_MAINTENANCE=1`. `maintain` writes
`MAINTENANCE_HEARTBEAT_PATH` (default `/tmp/consistency-worker/heartbeat`)
every `MAINTENANCE_HEARTBEAT_S` seconds (default 5). The file records
`consecutive_failures` and `last_success` for the retention cycle. If that
file is missing or older than `MAINTENANCE_STALE_S` (default 30), health
prints `unhealthy` and exits 1 even when the last ingestion session is
`completed`. The same word and exit are used when consecutive failed cycles
exceed `MAINTENANCE_MAX_FAILURES` (default 3), including when the process is
alive and the timestamp is fresh. A cycle that finishes resets the count and
writes `last_success`. A host `health` command that leaves
`EXPECT_MAINTENANCE` unset does not require the file. A `failed` ingestion
session exits 1 either way. A fresh heartbeat does not turn a failed session
into a healthy one.

| Status | Printed word | Exit | Meaning |
|---|---|---|---|
| `running`, heartbeat fresh or not required | `running` | 0 | Ingestion is in progress |
| `completed`, heartbeat fresh or not required | `completed` | 0 | The deterministic session finished. This is an expected terminal state |
| `interrupted`, heartbeat fresh or not required | `interrupted` | 0 | The journal stopped early. The next start resumes it |
| `completed` or `running`, maintenance stale | `unhealthy` | 1 | The entrypoint expects maintenance, and the heartbeat is missing or old |
| `completed` or `running`, retention failures above the threshold | `unhealthy` | 1 | The process is alive and the timestamp is fresh, and `consecutive_failures` is greater than `MAINTENANCE_MAX_FAILURES` (default 3) |
| `failed` | `failed` | 1 | Processing failed. The container is unhealthy |
| no row yet | `starting` | 1 | The session has not been opened |
| database error | `unhealthy` | 1 | The status could not be read |

A completed demo whose maintenance process is alive and whose retention cycle
succeeds stays healthy. A dead process, a process whose cycles keep failing,
and failed ingestion do not.
`running` and `completed` share exit 0 when maintenance is fresh so Compose
can mark both healthy; the printed word distinguishes them.

## CI

GitHub Actions runs the host test suites and `scripts/compose_smoke.sh`. That
job builds these images, starts a fresh database on the compose network
without publishing Postgres, waits until the synthetic worker has written
detections, calls `GET /api/v1/detections`, restarts the worker and the API,
and checks the detection ids are unchanged. It then GETs the containerized
dashboard and checks the HTML for the synthetic-simulation label and a
detection id read from Postgres, and posts replay start through the
frontend `/app-data` proxy with a capability cookie. It tears down only its
own project. The local command that builds and starts the stack on a machine is
still:

```bash
docker compose -p consistency-deploy up --build -d
```

## A real host, not performed

A public or rented machine is a separate step. It is not done here.

1. Run this compose project on a Linux host with Docker.
2. Put TLS in front of the frontend only. Point the proxy at
   `127.0.0.1:3000`. Set `REPLAY_COOKIE_SECURE=1` and set `CORS_ORIGINS` to
   the HTTPS origin.
3. Replace the placeholder database password outside the image. Do not commit
   it. Leave `ENABLE_KALSHI_API` and `ENABLE_LIVE_TRADING` false unless the
   operator holds authorization and changes those flags on purpose.
4. Keep Postgres unpublished. Do not publish the gateway.
