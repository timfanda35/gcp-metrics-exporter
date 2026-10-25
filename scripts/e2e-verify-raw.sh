#!/usr/bin/env bash
# Compares what Prometheus stored for the corrected DELTA jobs against the raw Cloud Monitoring
# points: same timestamp, same value (rate job: value * 60). Needs docker, python3, jq and the
# values in .env.e2e. Usage: scripts/e2e-verify-raw.sh
set -uo pipefail
cd "$(dirname "$0")/.."
[ -f .env.e2e ] && { set -a; . ./.env.e2e; set +a; }
PROM=${PROM:-http://localhost:9090}

raw=$(mktemp); trap 'rm -f "$raw"' EXIT
docker run --rm -e GOOGLE_APPLICATION_CREDENTIALS=/secrets/adc.json -e SA="${IMPERSONATE_SA:?}" \
  -e PROJECTS="$PROJECT_A,$PROJECT_B" \
  -v "$GOOGLE_APPLICATION_CREDENTIALS":/secrets/adc.json:ro \
  -v "$PWD/scripts/e2e-raw-points.py":/raw.py:ro \
  python:3.12-slim sh -c "pip install -q google-auth requests >/dev/null 2>&1; python /raw.py" > "$raw" || exit 1

PROM="$PROM" RAW="$raw" python3 - <<'PY'
import json, os, sys, time, urllib.parse, urllib.request

def q(expr):
    url = os.environ["PROM"] + "/api/v1/query?" + urllib.parse.urlencode({"query": expr})
    return json.load(urllib.request.urlopen(url))["data"]["result"]

raw = {}
for line in open(os.environ["RAW"]):
    r = json.loads(line)
    raw[(r["project"], r["metric"], r["instance_name"], r["end"])] = r["value"]

fail = 0
for job, scale in (("cm-delta-raw", 1.0), ("cm-delta-rate", 60.0)):
    compared = missing = bad = 0
    newest = 0
    for metric in ("sent_bytes_count", "received_bytes_count"):
        for s in q(f'compute_googleapis_com_instance_network_{metric}{{job="{job}"}}[20m]'):
            m = s["metric"]
            for ts, val in s["values"]:
                key = (m["project_id"], metric, m.get("instance_name", ""), int(round(float(ts))))
                newest = max(newest, float(ts))
                if key not in raw:
                    missing += 1   # sample timestamp does not match any raw point end time
                    continue
                compared += 1
                if abs(float(val) * scale - raw[key]) > 1e-6 * max(1.0, raw[key]):
                    bad += 1
                    print(f"  MISMATCH {job} {key}: prometheus*{scale:g}={float(val)*scale} raw={raw[key]}")
    lag = time.time() - newest if newest else float("nan")
    status = "PASS" if compared > 0 and bad == 0 and missing == 0 else "FAIL"
    fail |= status == "FAIL"
    print(f"{status}  {job}: compared={compared} value_mismatch={bad} timestamp_not_in_raw={missing} newest_sample_lag={lag:.0f}s")
sys.exit(fail)
PY
