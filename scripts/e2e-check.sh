#!/usr/bin/env bash
# Verifies the e2e stack after `docker compose -f docker-compose.e2e.yaml up -d --build`.
# Usage: scripts/e2e-check.sh [wait_seconds=200]   (needs curl + jq)
set -uo pipefail
# Load PROJECT_A etc. from .env.e2e (without exporting the credentials path elsewhere).
if [ -f .env.e2e ]; then set -a; . ./.env.e2e; set +a; fi
PROM=${PROM:-http://localhost:9090}
EXP=${EXP:-http://localhost:18080}
GRAF=${GRAF:-http://localhost:3000}
WAIT=${1:-200}
fail=0
ok()  { echo "PASS  $*"; }
bad() { echo "FAIL  $*"; fail=1; }
q()   { curl -sf --get "$PROM/api/v1/query" --data-urlencode "query=$1" | jq -r "${2:-.data.result}"; }

curl -sf "$EXP/healthz" >/dev/null && ok "exporter /healthz" || bad "exporter /healthz"

echo "waiting ${WAIT}s for scrapes..."; sleep "$WAIT"
series_before=$(q 'count({job=~"cm-.*|gmp-.*",__name__!~"up|scrape_.*"})' '.data.result[0].value[1] // "0"')

# Every e2e target must be up.
down=$(q 'up{job=~"cm-.*|gmp-.*"} == 0' '.data.result[] | .metric.job + " " + .metric.scrape_project')
[ -z "$down" ] && ok "all targets up" || bad "targets down:"$'\n'"$down"

# Prometheus must not have rejected samples.
for m in prometheus_target_scrapes_sample_out_of_order_total \
         prometheus_target_scrapes_sample_out_of_bounds_total \
         prometheus_target_scrapes_sample_duplicate_timestamp_total \
         prometheus_target_scrapes_exceeded_sample_limit_total; do
  v=$(q "sum($m)" '.data.result[0].value[1] // "0"')
  [ "$v" = "0" ] && ok "$m = 0" || bad "$m = $v"
done

# Exporter type check: GMP must be gauge only; report CM delta type.
curl -sf "$EXP/gmp-metrics" -G --data-urlencode "project=${PROJECT_A:-}" \
  --data-urlencode 'query=compute_googleapis_com:instance_network_sent_bytes_count' \
  --data-urlencode 'time_offset=4m' | grep -q '^# TYPE .* gauge' && ok "gmp delta is gauge" || bad "gmp delta not gauge (set PROJECT_A in env)"
cm_type=$(curl -sf "$EXP/metrics" -G --data-urlencode "project=${PROJECT_A:-}" \
  --data-urlencode 'metric_type=compute.googleapis.com/instance/network/sent_bytes_count' | grep -m1 '^# TYPE' | awk '{print $4}')
echo "INFO  cm delta exported as: ${cm_type:-<none>} (gauge expected after fix)"

# Series churn: wait two more scrape intervals, series count must not grow.
sleep 130
series_after=$(q 'count({job=~"cm-.*|gmp-.*",__name__!~"up|scrape_.*"})' '.data.result[0].value[1] // "0"')
if [ "$series_after" = "0" ]; then bad "no series at all (targets failing?)"
elif [ "$series_before" = "$series_after" ]; then ok "series count stable ($series_after)"
else bad "series churn: $series_before -> $series_after"; fi

curl -sf "$GRAF/api/health" | jq -e '.database=="ok"' >/dev/null && ok "grafana healthy" || bad "grafana health"
curl -sf "$GRAF/api/datasources/uid/prom-e2e/health" | jq -e '.status=="OK"' >/dev/null && ok "grafana datasource OK" || bad "grafana datasource"
curl -sf "$GRAF/api/dashboards/uid/vm-e2e" >/dev/null && ok "dashboard provisioned" || bad "dashboard missing"
echo "Open $GRAF/d/vm-e2e to inspect graphs."
exit $fail
