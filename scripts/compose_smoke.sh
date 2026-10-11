#!/usr/bin/env bash
# Build the deployment images, start a fresh database on an internal network,
# and check the synthetic worker. Postgres is not published. The project name
# is not consistency-deploy or consistency-engine, and teardown removes only
# this project.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root"

project="consistency-smoke-${GITHUB_RUN_ID:-local}-$$"
export WEB_PORT="${SMOKE_WEB_PORT:-18080}"
files=(-f docker-compose.yml -f compose.smoke.yml)

cleanup() {
  docker compose -p "$project" "${files[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "compose smoke project ${project} (frontend 127.0.0.1:${WEB_PORT})"
# Build the two images first. Worker, gateway, and migrate reuse the API image
# and must not pull consistency-smoke-api from a registry.
docker compose -p "$project" "${files[@]}" build api frontend
docker compose -p "$project" "${files[@]}" up -d --no-build

published="$(docker compose -p "$project" "${files[@]}" port db 5432 2>/dev/null || true)"
if [[ -n "${published}" ]]; then
  echo "Postgres must stay off the host; compose reported ${published}" >&2
  exit 1
fi

wait_worker() {
  local want="$1"
  local out="" code=1
  local i
  for i in $(seq 1 90); do
    set +e
    out="$(docker compose -p "$project" "${files[@]}" exec -T worker python -m consistency_worker health 2>/dev/null)"
    code=$?
    set -e
    out="$(printf '%s' "$out" | tr -d '\r')"
    if [[ "$out" == "$want" && "$code" -eq 0 ]]; then
      return 0
    fi
    sleep 5
  done
  echo "worker health did not become ${want} (last=${out:-<none>} exit=${code})" >&2
  docker compose -p "$project" "${files[@]}" logs worker >&2 || true
  return 1
}

wait_api() {
  local i
  for i in $(seq 1 40); do
    if docker compose -p "$project" "${files[@]}" exec -T api python -c \
      'import urllib.request; urllib.request.urlopen("http://127.0.0.1:8000/api/v1/health/ready", timeout=3)' \
      >/dev/null 2>&1; then
      return 0
    fi
    sleep 3
  done
  echo "API did not become ready" >&2
  docker compose -p "$project" "${files[@]}" logs api >&2 || true
  return 1
}

psql_at() {
  docker compose -p "$project" "${files[@]}" exec -T db \
    psql -U consistency -d consistency -tAc "$1"
}

wait_worker completed
wait_api

sessions="$(psql_at "SELECT count(*) FROM ingestion_sessions WHERE status = 'completed'")"
detections="$(psql_at "SELECT count(*) FROM detections")"
sessions="$(printf '%s' "$sessions" | tr -d '[:space:]')"
detections="$(printf '%s' "$detections" | tr -d '[:space:]')"
if [[ "$sessions" != "1" ]]; then
  echo "expected 1 completed session, got ${sessions}" >&2
  exit 1
fi
if [[ "$detections" -lt 1 ]]; then
  echo "expected worker-written detections, got ${detections}" >&2
  exit 1
fi
before="$(psql_at "SELECT detection_id FROM detections ORDER BY 1")"

api_total="$(docker compose -p "$project" "${files[@]}" exec -T api python -c '
import json, sys, urllib.request
body = json.load(urllib.request.urlopen("http://127.0.0.1:8000/api/v1/detections?limit=200", timeout=15))
total = body.get("total")
if not isinstance(total, int) or total < 1:
    sys.stderr.write("detections total was %r\n" % (total,))
    sys.exit(1)
print(total)
')"
api_total="$(printf '%s' "$api_total" | tr -d '[:space:]')"
if [[ "$api_total" != "$detections" ]]; then
  echo "API total ${api_total} did not match database count ${detections}" >&2
  exit 1
fi

stamp="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
docker compose -p "$project" "${files[@]}" restart worker
skipped=0
for _ in $(seq 1 40); do
  if docker compose -p "$project" "${files[@]}" logs --since "$stamp" worker 2>/dev/null | grep -q "not ingesting"; then
    skipped=1
    break
  fi
  sleep 3
done
if [[ "$skipped" != 1 ]]; then
  echo "restarted worker did not skip the completed session" >&2
  docker compose -p "$project" "${files[@]}" logs worker >&2 || true
  exit 1
fi
wait_worker completed
after_sessions="$(psql_at "SELECT count(*) FROM ingestion_sessions")"
after_sessions="$(printf '%s' "$after_sessions" | tr -d '[:space:]')"
after="$(psql_at "SELECT detection_id FROM detections ORDER BY 1")"
if [[ "$after_sessions" != "1" || "$before" != "$after" ]]; then
  echo "worker restart changed sessions (${after_sessions}) or detection ids" >&2
  exit 1
fi

docker compose -p "$project" "${files[@]}" restart api
wait_api
after_api="$(docker compose -p "$project" "${files[@]}" exec -T api python -c '
import json, urllib.request
body = json.load(urllib.request.urlopen("http://127.0.0.1:8000/api/v1/detections?limit=200", timeout=15))
print(body.get("total"))
')"
after_api="$(printf '%s' "$after_api" | tr -d '[:space:]')"
if [[ "$after_api" != "$detections" ]]; then
  echo "API restart changed detection total from ${detections} to ${after_api}" >&2
  exit 1
fi

echo "compose smoke passed: ${detections} detections, 1 completed session, restart kept the same ids"
