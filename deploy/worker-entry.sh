#!/bin/sh
# Retention and ingestion run as two processes. Ingestion stays up after a
# terminal session (it does not open another one). If either process exits on
# its own, this script exits non-zero so the container comes back with both.
# A signal stops both and exits 0. There is no sleeping shell that leaves
# retention unscheduled.
set -eu
# The compose healthcheck is a separate process. It sees these variables from
# the container environment (docker-compose.yml sets the same defaults). A
# missing or old heartbeat is unhealthy while this script expects maintenance.
export EXPECT_MAINTENANCE="${EXPECT_MAINTENANCE:-1}"
export MAINTENANCE_HEARTBEAT_PATH="${MAINTENANCE_HEARTBEAT_PATH:-/tmp/consistency-worker/heartbeat}"
export MAINTENANCE_STALE_S="${MAINTENANCE_STALE_S:-30}"
export MAINTENANCE_HEARTBEAT_S="${MAINTENANCE_HEARTBEAT_S:-5}"
maint=0
child=0
stop=0
term() {
  stop=1
  if [ "$child" -ne 0 ]; then
    kill -TERM "$child" 2>/dev/null || true
  fi
  if [ "$maint" -ne 0 ]; then
    kill -TERM "$maint" 2>/dev/null || true
  fi
}
trap term TERM INT

python -m consistency_worker maintain &
maint=$!
python -m consistency_worker &
child=$!

while [ "$stop" -eq 0 ]; do
  if ! kill -0 "$child" 2>/dev/null; then
    break
  fi
  if ! kill -0 "$maint" 2>/dev/null; then
    break
  fi
  sleep 1
done

if [ "$stop" -eq 1 ]; then
  wait "$child" 2>/dev/null || true
  wait "$maint" 2>/dev/null || true
  exit 0
fi

kill -TERM "$child" 2>/dev/null || true
kill -TERM "$maint" 2>/dev/null || true
wait "$child" 2>/dev/null || true
wait "$maint" 2>/dev/null || true
exit 1
