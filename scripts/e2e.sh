#!/usr/bin/env bash
# Local Playwright. Requires Docker, uv, and apps/web dependencies.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root"

port="${POSTGRES_PORT:-5432}"
export POSTGRES_PORT="$port"
export DATABASE_URL="${DATABASE_URL:-postgresql+asyncpg://consistency:consistency@127.0.0.1:${port}/consistency}"
export DASHBOARD_GATEWAY_PORT="${DASHBOARD_GATEWAY_PORT:-8765}"
export DASHBOARD_GATEWAY_URL="${DASHBOARD_GATEWAY_URL:-http://127.0.0.1:${DASHBOARD_GATEWAY_PORT}}"
export WEB_PORT="${WEB_PORT:-3000}"
export PLAYWRIGHT_BASE_URL="http://127.0.0.1:${WEB_PORT}"

docker compose up -d db
for _ in $(seq 1 30); do
  docker compose exec -T db pg_isready -U consistency -d consistency >/dev/null 2>&1 && break
  sleep 1
done

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
npm --prefix apps/web run build
npm --prefix apps/web run start -- --port "$WEB_PORT" &
pids+=("$!")

for _ in $(seq 1 60); do
  curl -sf "${DASHBOARD_GATEWAY_URL}/internal/health" >/dev/null && curl -sf "$PLAYWRIGHT_BASE_URL" >/dev/null && break
  sleep 1
done

npm --prefix apps/web exec playwright test e2e/dashboard.spec.ts
# Stop only the gateway so the next request observes a real disconnect.
kill "${pids[0]}"
pids=("${pids[1]}")
sleep 1
npm --prefix apps/web exec playwright test e2e/disconnected.spec.ts
echo "Playwright passed, including the disconnected page."
