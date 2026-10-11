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

psql_at() {
  docker compose -p "$project" exec -T db psql -U consistency -d consistency -tAc "$1"
}

row="$(psql_at \
  "SELECT detection_id || '|' || session_id || '|' || relationship_id || '|' || classification || '|' || coalesce(record_json::json->'metrics'->>'gross_edge','') || '|' || coalesce(record_json::json->'metrics'->>'total_fees','') || '|' || coalesce(record_json::json->'metrics'->>'net_edge','') || '|' || coalesce(record_json::json->'metrics'->>'depth_supported_quantity','') || '|' || coalesce(record_json::json->'metrics'->>'worst_case_payoff','') || '|' || coalesce(max_net_edge::text,'') || '|' || coalesce(certificate_json::json->'payoff'->>'min_payoff_per_unit','') || '|' || certificate_hash || '|' || coalesce(closing_classification,'') || '|' || coalesce(close_reason,'') || '|' || last_observed_ms::text || '|' || last_position::text FROM detections WHERE classification = 'FEE_ADJUSTED_CANDIDATE' AND strategy_id LIKE 'implication|%' ORDER BY max_net_edge DESC NULLS LAST LIMIT 1")"
row="$(printf '%s' "$row" | tr -d '[:space:]')"
if [[ -z "$row" ]]; then
  echo "No implication classified FEE_ADJUSTED_CANDIDATE is stored." >&2
  exit 1
fi

IFS='|' read -r detection_id session_id relationship_id classification gross fees net quantity payoff max_net min_payoff cert_hash closing_classification close_reason replay_ms last_position <<<"$row"
cert_file="$(mktemp)"
psql_at "SELECT certificate_json FROM detections WHERE detection_id = '${detection_id}'" >"$cert_file"
python3 - "$cert_file" "$cert_hash" "$closing_classification" <<'PY'
import hashlib, json, pathlib, sys
raw = pathlib.Path(sys.argv[1]).read_text().strip()
stored, closing = sys.argv[2], sys.argv[3]
digest = "sha256:" + hashlib.sha256(raw.encode()).hexdigest()
if digest != stored:
    sys.exit(f"stored certificate hash {stored} != SHA-256 of stored JSON {digest}")
body = json.loads(raw)
if body.get("classification") != "FEE_ADJUSTED_CANDIDATE":
    sys.exit(f"qualifying certificate classification is {body.get('classification')}")
if closing == "FEE_ADJUSTED_CANDIDATE" or not closing:
    sys.exit(f"closing classification {closing!r} is not a separate later condition")
print("qualifying certificate hash matches the stored JSON")
PY
rm -f "$cert_file"
closed_ms="$(psql_at "SELECT coalesce(record_json->>'closed_at_ms','') FROM detections WHERE detection_id = '${detection_id}'")"
closed_ms="$(printf '%s' "$closed_ms" | tr -d '[:space:]')"
replay_cursor="$(psql_at "SELECT count(*) FROM orderbook_updates WHERE session_id = '${session_id}' AND ordinal <= ${last_position}")"
replay_cursor="$(printf '%s' "$replay_cursor" | tr -d '[:space:]')"
timestamp_cursor="$(psql_at "SELECT count(*) FROM orderbook_updates WHERE session_id = '${session_id}' AND received_ts_ms <= ${replay_ms}")"
timestamp_cursor="$(printf '%s' "$timestamp_cursor" | tr -d '[:space:]')"
if [[ -z "$replay_cursor" || -z "$timestamp_cursor" || "$replay_cursor" -ge "$timestamp_cursor" ]]; then
  echo "qualifying cursor ${replay_cursor} is not strictly before the close inside timestamp ${replay_ms} (timestamp cursor ${timestamp_cursor}, close ${closed_ms})" >&2
  exit 1
fi

html="$(mktemp)"
overview="$(mktemp)"
trap 'rm -f "$html" "$overview" "$cert_file"' EXIT
curl -sf "${base}/" -o "$overview"
curl -sf "${base}/detections/${detection_id}" -o "$html"
python3 - "$overview" "$html" "$detection_id" "$cert_hash" "$net" "$fees" "$gross" "$min_payoff" "$classification" "$closing_classification" <<'PY'
import pathlib, sys
overview = pathlib.Path(sys.argv[1]).read_text()
html = pathlib.Path(sys.argv[2]).read_text()
(
    detection_id,
    cert_hash,
    net,
    fees,
    gross,
    min_payoff,
    classification,
    closing_classification,
) = sys.argv[3:]
if "Synthetic simulation" not in overview:
    sys.exit("overview is missing the synthetic-simulation label")
if "live Kalshi" in overview and "not a live" not in overview:
    sys.exit("overview presents the simulation as a live Kalshi feed")
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
        ("qualifying quote label", "Last qualifying quote"),
        ("not executable", "not a currently executable opportunity"),
        ("closing condition", "Closing condition"),
        ("closing classification", closing_classification),
        ("synthetic detection label", "not a live Kalshi feed"),
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
Closing classification ${closing_classification} (${close_reason}). The qualifying quote is not a currently executable opportunity.
Replay cursor ${replay_cursor}/${timestamp_cursor} at pipeline timestamp ${replay_ms} (journal ordinal ${last_position}). The same millisecond continues into the close, so seek this cursor rather than the timestamp.

DEMO_DETECTION=${detection_id}
DEMO_RELATIONSHIP=${relationship_id}
DEMO_REPLAY_MS=${replay_ms}
DEMO_REPLAY_CURSOR=${replay_cursor}
DEMO_CERT_HASH=${cert_hash}
EOF
