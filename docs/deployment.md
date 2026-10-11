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
`POSTGRES_PORT` (5433 on this machine).

The `migrate` container runs `alembic upgrade head` and exits. The API
entrypoint runs the same upgrade again, then execs uvicorn so SIGTERM reaches
the server. The worker runs `python -m consistency_worker`. When the synthetic
session finishes, the process stays up until SIGTERM so a restart policy does
not open a second session. `stop_grace_period` is 30s on the worker and 20s on
the API. Restart policy is `unless-stopped` except for the one-shot migrate
container.

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

## CI

GitHub Actions runs the host test suites. It does not build these images.
The image build and the compose run are local. The command that built and
started the stack on this machine is recorded in `PROGRESS.md`.

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
