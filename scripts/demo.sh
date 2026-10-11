#!/usr/bin/env bash
# Start the local Compose deployment if needed and print the synthetic demo path.
# Reads the detection id from Postgres. Does not call Kalshi.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root"

project="${DEMO_PROJECT:-consistency-deploy}"
port="${WEB_PORT:-3000}"
base="http://127.0.0.1:${port}"

if ! curl -sf "${base}/" >/dev/null 2>&1; then
  echo "Starting Compose project ${project}."
  if [[ "${DEMO_BUILD:-0}" == "1" ]]; then
    docker compose -p "$project" up --build -d
  else
    docker compose -p "$project" up -d
  fi
fi

ready=0
for _ in $(seq 1 40); do
  if curl -sf "${base}/" | grep -q "Synthetic simulation"; then
    ready=1
    break
  fi
  sleep 3
done
if [[ "$ready" != 1 ]]; then
  echo "The frontend at ${base} did not serve the synthetic dashboard." >&2
  exit 1
fi

row="$(docker compose -p "$project" exec -T db psql -U consistency -d consistency -tAc \
  "SELECT detection_id || '|' || session_id || '|' || relationship_id || '|' || classification || '|' || coalesce(record_json::json->'metrics'->>'gross_edge','') || '|' || coalesce(record_json::json->'metrics'->>'total_fees','') || '|' || coalesce(record_json::json->'metrics'->>'net_edge','') || '|' || coalesce(record_json::json->'metrics'->>'depth_supported_quantity','') || '|' || coalesce(record_json::json->'metrics'->>'worst_case_payoff','') || '|' || coalesce(max_net_edge::text,'') || '|' || coalesce(certificate_json::json->'payoff'->>'min_payoff_per_unit','') || '|' || certificate_hash FROM detections WHERE classification = 'FEE_ADJUSTED_CANDIDATE' AND strategy_id LIKE 'implication|%' ORDER BY max_net_edge DESC NULLS LAST LIMIT 1")"
row="$(printf '%s' "$row" | tr -d '[:space:]')"
if [[ -z "$row" ]]; then
  echo "No implication classified FEE_ADJUSTED_CANDIDATE is stored." >&2
  exit 1
fi

IFS='|' read -r detection_id session_id relationship_id classification gross fees net quantity payoff max_net min_payoff cert_hash <<<"$row"

html="$(mktemp)"
overview="$(mktemp)"
trap 'rm -f "$html" "$overview"' EXIT
curl -sf "${base}/" -o "$overview"
curl -sf "${base}/detections/${detection_id}" -o "$html"
python3 - "$overview" "$html" "$detection_id" "$cert_hash" "$net" "$fees" "$gross" "$min_payoff" "$classification" <<'PY'
import pathlib, sys
overview = pathlib.Path(sys.argv[1]).read_text()
html = pathlib.Path(sys.argv[2]).read_text()
detection_id, cert_hash, net, fees, gross, min_payoff, classification = sys.argv[3:]
if "Synthetic simulation" not in overview:
    sys.exit("overview is missing the synthetic-simulation label")
missing = [
    label
    for label, value in (
        ("detection id", detection_id),
        ("certificate hash", cert_hash),
        ("net edge", net),
        ("fees", fees),
        ("gross edge", gross),
        ("min payoff per unit", min_payoff),
        ("classification", classification),
    )
    if value not in html
]
if missing:
    sys.exit("detection page is missing: " + ", ".join(missing))
print("page matches the stored detection row and certificate payoff")
PY

cat <<EOF
Synthetic simulation. Compose project ${project}.
Detection ${detection_id} (${classification})
Session ${session_id}
Relationship ${relationship_id}

Open ${base}/
Relationship ${base}/relationships/${relationship_id}
Detection ${base}/detections/${detection_id}
Replay ${base}/replay/${session_id}

Stored quote: gross edge ${gross}, fees ${fees}, net edge ${net}, depth quantity ${quantity}, worst-case payoff ${payoff}, maximum net edge ${max_net}.
Certificate payoff minimum per unit ${min_payoff}. Certificate ${cert_hash}.
EOF
