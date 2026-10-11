#!/bin/sh
# Run one synthetic session. After it finishes, wait for SIGTERM instead of
# exiting, so the restart policy does not open another session.
set -eu
child=0
stopped=0
term() {
  stopped=1
  if [ "$child" -ne 0 ]; then
    kill -TERM "$child" 2>/dev/null || true
  fi
}
trap term TERM INT

python -m consistency_worker &
child=$!
set +e
wait "$child"
status=$?
set -e
if [ "$stopped" -eq 1 ]; then
  exit "$status"
fi
if [ "$status" -ne 0 ]; then
  exit "$status"
fi
echo "synthetic session finished; waiting for SIGTERM" >&2
trap 'exit 0' TERM INT
sleep infinity &
child=$!
set +e
wait "$child"
exit 0
