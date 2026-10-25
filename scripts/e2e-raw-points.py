"""Print raw Cloud Monitoring points as JSON lines (used by e2e-verify-raw.sh).

Runs inside a throwaway python container. Env: SA (impersonation target),
PROJECTS (comma separated), GOOGLE_APPLICATION_CREDENTIALS. Prints no tokens.
"""
import datetime as dt
import json
import os

import google.auth
import google.auth.transport.requests as tr
import requests

creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
creds.refresh(tr.Request())
tok = requests.post(
    f"https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/{os.environ['SA']}:generateAccessToken",
    headers={"Authorization": "Bearer " + creds.token},
    json={"scope": ["https://www.googleapis.com/auth/monitoring.read"]},
).json()["accessToken"]

now = dt.datetime.now(dt.timezone.utc)
iso = lambda t: t.strftime("%Y-%m-%dT%H:%M:%SZ")
for project in os.environ["PROJECTS"].split(","):
    for metric in ("sent_bytes_count", "received_bytes_count"):
        resp = requests.get(
            f"https://monitoring.googleapis.com/v3/projects/{project}/timeSeries",
            params={
                "filter": f'metric.type="compute.googleapis.com/instance/network/{metric}"',
                "interval.startTime": iso(now - dt.timedelta(minutes=25)),
                "interval.endTime": iso(now),
            },
            headers={"Authorization": "Bearer " + tok},
        ).json()
        for ts in resp.get("timeSeries", []):
            for pt in ts["points"]:
                v = pt["value"]
                end = dt.datetime.fromisoformat(pt["interval"]["endTime"].replace("Z", "+00:00"))
                print(json.dumps({
                    "project": project, "metric": metric,
                    "instance_name": ts["metric"]["labels"].get("instance_name", ""),
                    "end": int(end.timestamp()), "value": float(v.get("int64Value", v.get("doubleValue", 0))),
                }))
