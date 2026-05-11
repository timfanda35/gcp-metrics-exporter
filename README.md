# GCP Metrics Exporter

An HTTP server that proxies Prometheus scrapes into GCP Cloud Monitoring API queries. Each `/metrics` request becomes a live `ListTimeSeries` call — no in-memory state, no scheduler, no buffer. Cross-project queries and Service Account impersonation are first-class.

## Features

- Stateless: every scrape is a live query
- Multi-project: each request can target a different GCP project
- Service Account impersonation per request or per deploy
- Native Prometheus exposition format (text v0.0.4)
- Streaming response, per-request timeout, concurrency cap
- Single static binary, distroless container

## Prerequisites

- Go 1.22+ (for building from source)
- A GCP project with the Monitoring API enabled
- Credentials with `roles/monitoring.viewer` (or narrower):
  - Application Default Credentials (`gcloud auth application-default login`), **or**
  - A Service Account JSON key, **or**
  - A privileged SA that can impersonate target SAs (`roles/iam.serviceAccountTokenCreator`)
- Optional: Docker / Docker Compose for the dev stack

## Quick start — local

```bash
git clone <this-repo>
cd gcp-metrics-exporter

# Authenticate
gcloud auth application-default login

# Run
go run ./cmd/server
```

In another shell:

```bash
curl 'http://localhost:8080/healthz'

curl 'http://localhost:8080/metrics?project=YOUR_PROJECT&metric_type=compute.googleapis.com/instance/cpu/utilization'
```

## Quick start — Docker Compose

Spins up the exporter plus a Prometheus instance pre-wired to scrape it.

```bash
# If you want ADC inside the container, mount your gcloud config (Linux/macOS):
mkdir -p secrets
# either: provide a SA JSON
cp /path/to/sa.json secrets/sa.json
# (then uncomment the GOOGLE_APPLICATION_CREDENTIALS env and the volume mount in docker-compose.yaml)

# Edit config/prometheus.yml — replace the placeholder targets with your project IDs
# Targets are formatted as "<project>;<metric_type>"

docker compose up --build
```

- Exporter: <http://localhost:8080>
- Prometheus UI: <http://localhost:9090>

## API

### `GET /metrics`

| Query Parameter | Required | Default | Description |
|---|---|---|---|
| `project` | yes | — | GCP Project ID |
| `metric_type` | yes | — | Cloud Monitoring metric type, e.g. `compute.googleapis.com/instance/cpu/utilization` |
| `filter` | no | — | Extra filter, AND-composed onto `metric.type = "<metric_type>"` (pass-through) |
| `aligner` | no | `ALIGN_MEAN` | Per-series aligner |
| `reducer` | no | `REDUCE_NONE` | Cross-series reducer |
| `group_by` | no | — | Comma-separated group-by fields (only when `reducer != REDUCE_NONE`) |
| `interval` | no | `5m` | Query window (Go duration: `30s`, `5m`, `1h`) |
| `alignment_period` | no | = `interval` | Alignment period |
| `impersonate_sa` | no | — | Override the impersonation target for this request |

Response: `text/plain; version=0.0.4; charset=utf-8` (Prometheus exposition format).

Example:

```bash
curl 'http://localhost:8080/metrics?project=my-project&metric_type=compute.googleapis.com/instance/cpu/utilization&interval=10m&aligner=ALIGN_MEAN'
```

### `GET /healthz`

Liveness only. Returns `200 OK` with `{"status":"ok"}`. Does not call GCP.

### Error mapping

| Condition | HTTP |
|---|---|
| Missing required param / bad duration / invalid aligner or reducer | 400 |
| Project or metric type not found | 404 |
| Concurrency cap hit | 429 |
| Quota exhausted (`Retry-After: 30`) or series cap exceeded | 503 |
| GCP `PermissionDenied` / `Unauthenticated` | 502 |
| Per-request timeout exceeded | 504 |

## Configuration (environment variables)

| Variable | Default | Description |
|---|---|---|
| `PORT` | `8080` | HTTP listen port |
| `GOOGLE_APPLICATION_CREDENTIALS` | — | Path to a Service Account JSON file (optional; ADC works too) |
| `DEFAULT_IMPERSONATE_SA` | — | Default impersonation target; overridden per-request by `?impersonate_sa=` |
| `LOG_LEVEL` | `info` | `debug` / `info` / `warn` / `error` |
| `LOG_FORMAT` | `json` | `json` / `text` |
| `SCRAPE_TIMEOUT` | `30s` | Per-request timeout for the upstream GCP call |
| `MAX_CONCURRENT_SCRAPES` | `16` | Concurrent `/metrics` requests; over-limit returns 429 |
| `MAX_SERIES_PER_REQUEST` | `10000` | Hard cap on series returned per response |
| `SHUTDOWN_GRACE` | `10s` | Graceful shutdown timeout |

## Authentication

Resolution order:

1. `GOOGLE_APPLICATION_CREDENTIALS` (Service Account JSON) if set
2. Application Default Credentials otherwise (`gcloud auth application-default login`, or workload identity in GKE/Cloud Run)

Impersonation, applied **on top** of the base credential:

- Set `DEFAULT_IMPERSONATE_SA` for a deploy-wide default
- Pass `?impersonate_sa=target@project.iam.gserviceaccount.com` to override per request

The base principal needs `roles/iam.serviceAccountTokenCreator` on the impersonated SA.

## Prometheus scrape config

Multi-target: one Prometheus job, one `static_configs.targets` entry per `(project, metric_type)` pair, separated by `;`. The included [config/prometheus.yml](config/prometheus.yml) shows the full pattern:

```yaml
scrape_configs:
  - job_name: gcp-metrics-exporter
    metrics_path: /metrics
    static_configs:
      - targets:
          - "my-project-a;compute.googleapis.com/instance/cpu/utilization"
          - "my-project-b;run.googleapis.com/request_count"
    relabel_configs:
      - source_labels: [__address__]
        regex: '([^;]+);(.+)'
        target_label: __param_project
        replacement: $1
      - source_labels: [__address__]
        regex: '([^;]+);(.+)'
        target_label: __param_metric_type
        replacement: $2
      - target_label: __address__
        replacement: exporter:8080
```

## Development

```bash
# Unit tests
go test -count=1 -race ./...

# Integration tests (in-process bufconn fake GCP server)
go test -tags=integration -count=1 -race -timeout=60s ./internal/integration/...

# Vet
go vet ./...
go vet -tags=integration ./internal/integration/...

# Build
go build -o /tmp/exporter ./cmd/server
```

Project layout:

```
cmd/server          # main entry point, env loading, graceful shutdown, --healthcheck flag
internal/auth       # GCP credentials + impersonation
internal/collector  # Cloud Monitoring queries → Prometheus metrics
internal/handler    # HTTP handlers (/metrics, /healthz)
internal/integration# end-to-end test (build tag: integration)
config/             # Prometheus scrape config (dev)
```

## Production notes

> **Security:** This service makes outbound calls as a privileged GCP Service Account. Anyone who can reach `/metrics` can read any metric the SA has permission to read. Deploy it on an internal network or behind authenticated infrastructure (IAP, internal LB, mesh policy) — **do not expose it to the public internet**.

- The container runs as `nonroot` on `gcr.io/distroless/static-debian12`; no shell, CA bundle included.
- `--healthcheck` flag (used by the docker-compose `HEALTHCHECK`) probes `/healthz` on loopback and exits 0/1.
- Per-SA gRPC clients are cached for the lifetime of the process; restart to pick up new credentials.
- Cumulative GCP series carry a `start_time_unix` Prometheus label so server resets create a new series rather than appearing as a counter decrement.
- Distribution metrics emit a Prometheus histogram with linear / exponential / explicit bucket support, including the implicit `+Inf` bucket.

## License

[MIT](LICENSE)
