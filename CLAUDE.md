# GCP Metrics Exporter — CLAUDE.md

> 這份文件是給 Claude（AI）看的開發指引，記錄架構決策、元件職責與開發規範。

---

## 專案概述

**GCP Metrics Exporter** 是一個 HTTP API server，負責：
1. 接收帶有查詢參數的 HTTP 請求
2. 以 GCP Service Account（支援 Impersonation）向 GCP 查詢指標
3. 將查詢結果轉換為 Prometheus exposition format 回傳
4. 支援跨專案（multi-project）動態查詢

支援兩種 backend，共用同一套 auth/impersonation，但查詢語言與結果型別不同：

| | `GET /metrics` | `GET /gmp-metrics` |
|---|---|---|
| 資料來源 | Cloud Monitoring（`ListTimeSeries` gRPC API） | GCP Managed Service for Prometheus（GMP）的 Prometheus-compatible instant query HTTP API |
| 查詢語言 | Cloud Monitoring filter + aligner/reducer | PromQL |
| 結果型別 | 依 GCP metric kind 轉換為 Gauge/Counter/Histogram | 一律轉換為 Prometheus gauge（instant query 無 cumulative 語意，不做型別轉換） |

---

## 目錄結構

```
gcp-metrics-exporter/
├── go.mod                        # Go module 定義
├── go.work                       # Go workspace（use .）
├── CLAUDE.md                     # 本文件
├── PLAN.md                       # 實作計畫
├── Dockerfile                    # 容器映像
├── docker-compose.yaml           # 開發環境（exporter + Prometheus）
├── cmd/
│   └── server/
│       └── main.go               # 程式進入點
├── internal/
│   ├── auth/
│   │   └── auth.go               # GCP 認證與 Impersonation（兩個 backend 共用）
│   ├── collector/
│   │   └── collector.go          # Cloud Monitoring 查詢 + Prometheus 轉換
│   ├── gmp/
│   │   ├── client.go             # GMP Prometheus-compatible instant query API client
│   │   ├── collector.go          # GMP 查詢結果 → Prometheus MetricFamily（一律為 gauge）
│   │   └── cache.go              # 依 impersonation target 快取 GMP client
│   ├── handler/
│   │   ├── handler.go            # HTTP handler（/metrics、/healthz 端點）
│   │   └── gmp_handler.go        # HTTP handler（/gmp-metrics 端點）
│   └── integration/
│       └── integration_test.go   # 端對端測試（build tag: integration，目前僅涵蓋 /metrics）
└── config/
    └── prometheus.yml            # Prometheus scrape 設定（開發用）
```

---

## 元件職責

### `internal/auth`
- 建立 GCP credentials（Application Default Credentials 或 Service Account JSON）
- 支援 **Impersonation**：透過 `ImpersonateServiceAccount` 取得 target service account 的 token
- 對外提供 `NewTokenSource(ctx, cfg)` 函式，回傳可注入其他 client 的 `oauth2.TokenSource`

### `internal/collector`
- 接受 `ProjectID`、`MetricType`、`Filter`、`Aligner`、`Reducer`、`GroupByFields`、`Interval`、`AlignmentPeriod` 等參數
- 呼叫 `cloud.google.com/go/monitoring/apiv3/v2` 的 `ListTimeSeries`
- 將 `TimeSeries` 轉換為 Prometheus `Metric`（GaugeVec / CounterVec / Histogram，依 GCP metric kind 對應）
- 每次 `/metrics` 請求都是 **即時查詢**（stateless）

### `internal/gmp`
- 職責對應 `internal/collector`，但資料來源是 GMP，不是 Cloud Monitoring
- `client.go`：以 HTTP 呼叫 GMP 的 Prometheus-compatible instant query API（`GET .../prometheus/api/v1/query`），非 gRPC `ListTimeSeries`
- `collector.go`：將查詢結果轉換為 Prometheus `MetricFamily`；**所有 metric 一律轉成 gauge**（instant query 沒有 cumulative 語意，不做 GAUGE/CUMULATIVE/DELTA/DISTRIBUTION 型別轉換）
- `cache.go`：依 impersonation target 快取 GMP client（與 `collector.ClientCache` 是各自獨立的快取，不共用）

### `internal/handler`
- `handler.go`：實作 `GET /metrics` HTTP endpoint（從 query parameters 解析請求參數，見 API 設計；組合 `auth` + `collector`，以 `text/plain; version=0.0.4` 格式回應）與 `GET /healthz` 健康檢查端點
- `gmp_handler.go`：實作 `GET /gmp-metrics` HTTP endpoint（組合 `auth` + `internal/gmp`，同樣以 Prometheus text format 回應）
- 兩個 handler 各自持有獨立的 concurrency semaphore，都以 `MAX_CONCURRENT_SCRAPES` 設定上限——**這不是一個全域共用的 cap**，而是「`/metrics` 最多 N 個並發」與「`/gmp-metrics` 最多 N 個並發」分開計算

### `cmd/server/main.go`
- 載入設定（環境變數）
- 初始化 HTTP server（使用標準 `net/http`）
- 註冊路由並啟動

---

## API 設計

### `GET /metrics`

| Query Parameter      | 必填 | 說明 |
|----------------------|------|------|
| `project`            | ✅   | GCP Project ID |
| `metric_type`        | ✅   | Cloud Monitoring metric type，例如 `compute.googleapis.com/instance/cpu/utilization` |
| `filter`             | ❌   | 額外 filter 條件，與 `metric.type = "<metric_type>"` 用 `AND` 串接（pass-through，不做 escaping） |
| `aligner`            | ❌   | Per-series aligner，預設 `ALIGN_MEAN` |
| `reducer`            | ❌   | Cross-series reducer，預設 `REDUCE_NONE` |
| `group_by`           | ❌   | 逗號分隔的 group-by fields，僅當 `reducer != REDUCE_NONE` 才有效 |
| `interval`           | ❌   | 查詢時間視窗（end-start），預設 `5m`（Go duration 格式） |
| `alignment_period`   | ❌   | 對齊週期，預設等於 `interval`（Go duration 格式） |
| `impersonate_sa`     | ❌   | 要 impersonate 的 service account email |

**回應格式**：Prometheus text format（`text/plain; version=0.0.4; charset=utf-8`）

**範例**：
```
GET /metrics?project=my-gcp-project&metric_type=compute.googleapis.com/instance/cpu/utilization&interval=10m
```

### `GET /gmp-metrics`

查詢 GCP Managed Service for Prometheus（GMP）的 Prometheus-compatible instant query API。GMP 裡的指標本身已是 Prometheus-native 格式，因此**不做**型別轉換，所有結果一律以 `gauge` 回傳。

| Query Parameter | 必填 | 說明 |
|------------------|------|------|
| `project`        | ✅   | GCP Project ID |
| `query`          | ✅   | PromQL 表達式，例如 `up{job="my-app"}` |
| `time_offset`    | ❌   | 查詢時間往前偏移，補償 GMP ingest 延遲（Go duration 格式，例如 `2m`），預設 `0` |
| `impersonate_sa` | ❌   | 覆寫此次請求的 impersonation 目標 |

**回應格式**：與 `/metrics` 相同，皆為 Prometheus text format（`text/plain; version=0.0.4; charset=utf-8`）

**範例**：
```
GET /gmp-metrics?project=my-gcp-project&query=up{job="my-app"}&time_offset=2m
```

### `GET /healthz`
回傳 `200 OK` + `{"status":"ok"}`

---

## 認證設定

優先順序：
1. 環境變數 `GOOGLE_APPLICATION_CREDENTIALS`（Service Account JSON 路徑）
2. Application Default Credentials（`gcloud auth application-default login`）

Impersonation 使用方式：
- 請求帶 `?impersonate_sa=target@project.iam.gserviceaccount.com`
- 或設定環境變數 `DEFAULT_IMPERSONATE_SA` 作為全域預設值

---

## 環境變數

| 變數名稱                    | 預設值 | 說明 |
|-----------------------------|--------|------|
| `PORT`                      | `8080` | HTTP server 監聽 port |
| `GOOGLE_APPLICATION_CREDENTIALS` | — | Service Account JSON 路徑 |
| `DEFAULT_IMPERSONATE_SA`    | — | 預設 impersonate 目標 SA |
| `LOG_LEVEL`                 | `info` | 日誌等級（debug/info/warn/error） |
| `LOG_FORMAT`                | `json` | 日誌格式（json/text） |
| `SCRAPE_TIMEOUT`            | `30s`  | 單次請求對 GCP 的 timeout（`/metrics`、`/gmp-metrics` 皆適用） |
| `MAX_CONCURRENT_SCRAPES`    | `16`   | 同時進行中的 scrape 上限，超過回 `429`。`/metrics` 與 `/gmp-metrics` 各自獨立計數（各自的 semaphore 都以此值為上限），不是兩者共用同一個全域 cap |
| `MAX_SERIES_PER_REQUEST`    | `10000` | 單次回應最多 series 數，超過回 `503` |
| `SHUTDOWN_GRACE`            | `10s`  | Graceful shutdown 允許時間 |

---

## 開發指引

### 執行測試
```bash
go test ./...
```

### 啟動開發環境
```bash
docker-compose up
```
- Exporter：http://localhost:8080
- Prometheus UI：http://localhost:9090

### 本地執行（需有 GCP credentials）
```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/sa.json
go run ./cmd/server
```

### 新增 `go.sum` 與 vendor
```bash
go mod tidy
go mod vendor   # 可選
```

---

## 編碼規範

- 所有 exported 函式必須有 godoc 註解
- error 一律向上傳遞（不在 internal 層直接 log + swallow）
- handler 層負責 error → HTTP status code 的對應
- 使用 `context.Context` 貫穿所有 GCP API 呼叫
- test 檔案與被測程式碼放在同一個 package（`_test.go`），使用 table-driven tests
- mock GCP API 使用 `google.golang.org/grpc` + 假的 gRPC server，或注入 interface
