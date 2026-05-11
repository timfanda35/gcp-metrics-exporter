# GCP Metrics Exporter — CLAUDE.md

> 這份文件是給 Claude（AI）看的開發指引，記錄架構決策、元件職責與開發規範。

---

## 專案概述

**GCP Metrics Exporter** 是一個 HTTP API server，負責：
1. 接收帶有查詢參數的 HTTP 請求
2. 以 GCP Service Account（支援 Impersonation）向 Cloud Monitoring API 查詢指標
3. 將查詢結果轉換為 Prometheus exposition format 回傳
4. 支援跨專案（multi-project）動態查詢

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
│   │   └── auth.go               # GCP 認證與 Impersonation
│   ├── collector/
│   │   └── collector.go          # Cloud Monitoring 查詢 + Prometheus 轉換
│   └── handler/
│       └── handler.go            # HTTP handler（/metrics 端點）
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
- 將 `TimeSeries` 轉換為 Prometheus `Metric`（GaugeVec / CounterVec）
- 每次 `/metrics` 請求都是 **即時查詢**（stateless）

### `internal/handler`
- 實作 `GET /metrics` HTTP endpoint
- 從 query parameters 解析請求參數（見 API 設計）
- 組合 `auth` + `collector`，執行查詢並以 `text/plain; version=0.0.4` 格式回應
- 實作 `GET /healthz` 健康檢查端點

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
| `SCRAPE_TIMEOUT`            | `30s`  | 單次 `/metrics` 請求對 GCP 的 timeout |
| `MAX_CONCURRENT_SCRAPES`    | `16`   | 同時進行中的 scrape 上限，超過回 `429` |
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
