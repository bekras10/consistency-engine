#!/usr/bin/env bash
# Start Postgres, migrations, the synthetic worker, the dashboard gateway, and Next.js.
# Exit non-zero if any of those pieces does not stay up. Do not report a UI that failed to start.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root"

port="${POSTGRES_PORT:-5432}"
export POSTGRES_PORT="$port"
export DATABASE_URL="${DATABASE_URL:-postgresql+asyncpg://consistency:consistency@127.0.0.1:${port}/consistency}"
export DASHBOARD_GATEWAY_PORT="${DASHBOARD_GATEWAY_PORT:-8765}"
export DASHBOARD_GATEWAY_URL="${DASHBOARD_GATEWAY_URL:-http://127.0.0.1:${DASHBOARD_GATEWAY_PORT}}"
export WEB_PORT="${WEB_PORT:-3000}"
export DATA_SOURCE="${DATA_SOURCE:-synthetic}"
export DASHBOARD_GATEWAY_HOST="${DASHBOARD_GATEWAY_HOST:-127.0.0.1}"
export REPLAY_API_TOKEN="${REPLAY_API_TOKEN:-local-replay-token}"
export REPLAY_MUTATIONS_PUBLIC="${REPLAY_MUTATIONS_PUBLIC:-false}"

if [[ ! -d apps/web/node_modules ]]; then
  echo "Frontend is not started: apps/web/node_modules is missing. Run npm install in apps/web." >&2
  exit 2
fi

docker compose up -d db
for _ in $(seq 1 30); do
  if docker compose exec -T db pg_isready -U consistency -d consistency >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
if ! docker compose exec -T db pg_isready -U consistency -d consistency >/dev/null 2>&1; then
  echo "Postgres did not become ready. The worker and frontend were not started." >&2
  exit 1
fi

uv run --frozen alembic upgrade head
uv run --frozen python scripts/load_synthetic_session.py

pids=()
cleanup() {
  for pid in "${pids[@]:-}"; do
    kill "$pid" >/dev/null 2>&1 || true
  done
}
trap cleanup EXIT

uv run --frozen python scripts/dashboard_gateway.py &
pids+=("$!")
uv run --frozen python -m consistency_worker &
pids+=("$!")
npm --prefix apps/web run dev -- --port "$WEB_PORT" &
pids+=("$!")

gateway_ok=0
web_ok=0
for _ in $(seq 1 60); do
  if curl -sf "${DASHBOARD_GATEWAY_URL}/internal/health" >/dev/null; then
    gateway_ok=1
  fi
  if curl -sf "http://127.0.0.1:${WEB_PORT}" >/dev/null; then
    web_ok=1
  fi
  if [[ "$gateway_ok" == 1 && "$web_ok" == 1 ]]; then
    break
  fi
  sleep 1
done

if [[ "$gateway_ok" != 1 ]]; then
  echo "Dashboard gateway did not become ready. Not claiming the UI is up." >&2
  exit 1
fi
if [[ "$web_ok" != 1 ]]; then
  echo "Frontend did not start on port ${WEB_PORT}." >&2
  exit 1
fi

echo "Postgres, migrations, synthetic worker, dashboard gateway (${DASHBOARD_GATEWAY_URL}), and frontend (http://127.0.0.1:${WEB_PORT}) are running."
echo "The dashboard polls the gateway. The public stream is GET /api/v1/stream."
while true; do
  for pid in "${pids[@]}"; do
    if ! kill -0 "$pid" 2>/dev/null; then
      echo "A dashboard process exited." >&2
      exit 1
    fi
  done
  sleep 2
done
