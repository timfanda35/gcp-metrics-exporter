# GCP Metrics Exporter — 實作計畫

## 目標

建立一個可部署在容器環境的 HTTP API server，動態查詢 GCP Cloud Monitoring 指標並以 Prometheus exposition format 回傳，支援 Service Account Impersonation 與跨專案查詢。

---

## Phase 1：認證層（`internal/auth`）

**目標**：提供 GCP credentials，支援 ADC 與 Impersonation。

### 工作項目

- [ ] 定義 `Config` struct
  - `CredentialsFile string`（對應 `GOOGLE_APPLICATION_CREDENTIALS`）
  - `ImpersonateServiceAccount string`（target SA email，可空）
  - `Scopes []string`（預設 `["https://www.googleapis.com/auth/monitoring.read"]`；ADC 與 impersonation 都會套用，避免 SDK fallback 到過寬的 scope）
  - `Lifetime time.Duration`（impersonation token 有效期，0 表示 SDK 預設 1 小時）

- [ ] 實作 `NewTokenSource(ctx, cfg) (oauth2.TokenSource, error)`
  - 若 `ImpersonateServiceAccount` 為空：使用 `google.DefaultTokenSource`，傳入 `cfg.Scopes`
  - 若有設定：使用 `impersonate.CredentialsTokenSource`，將 `cfg.Scopes` 與 `cfg.Lifetime` forward 進 `impersonate.CredentialsConfig`

- [ ] 單元測試
  - 測試 ADC path（mock `google.DefaultTokenSource`）
  - 測試 Impersonation path（mock `impersonate.CredentialsTokenSource`）
  - 測試 credentials 設定錯誤時的 error 回傳
  - 測試預設 scopes 確實被套用

**關鍵依賴**：
```
golang.org/x/oauth2
google.golang.org/api/impersonate
```

---

## Phase 2：Collector 層（`internal/collector`）

**目標**：查詢 Cloud Monitoring 並轉換為 Prometheus metrics。

### 工作項目

- [ ] 定義 `QueryParams` struct
  ```go
  type QueryParams struct {
      ProjectID       string
      MetricType      string
      Filter          string        // 額外 filter 條件（可空），與 metric.type 用 AND 串接
      Aligner         string        // 預設 ALIGN_MEAN
      Reducer         string        // 預設 REDUCE_NONE
      GroupByFields   []string      // 僅當 Reducer != REDUCE_NONE 時有意義
      Interval        time.Duration // 查詢時間視窗（end-start），預設 5 分鐘
      AlignmentPeriod time.Duration // 對齊週期，預設 = Interval
  }
  ```

- [ ] 定義 `Collector` interface
  ```go
  type Collector interface {
      Collect(ctx context.Context, params QueryParams) ([]*dto.MetricFamily, error)
  }
  ```

- [ ] Filter 組合規則
  - 最終 filter = `metric.type = "<MetricType>"` + （`Filter` 非空時）` AND (<Filter>)`
  - 使用者 `Filter` 為 pass-through，不做語法驗證或 escaping（由 GCP 回 `InvalidArgument`）
  - 此規則需在 godoc 與測試中明確紀錄

- [ ] 實作 `GCPCollector struct`
  - 持有 `*monitoring.MetricClient`（apiv3/v2 的高階 client，可注入便於 mock）
  - `Collect()` 呼叫 `ListTimeSeries` API，**用 iterator 全部讀完**並設 `MaxSeries` 上限（預設 10_000，超過回傳 error）
  - Label 對應：`metric.labels` + `resource.labels`，並加上 `resource_type` label（來自 `resource.type`）
  - 依 `ValueType` 轉換：
    - `INT64` / `DOUBLE` + `MetricKind=GAUGE` → Prometheus Gauge
    - `INT64` / `DOUBLE` + `MetricKind=CUMULATIVE` → Prometheus Counter，加 `start_time_unix` label，使 GCP series 重啟時 Prom 視為新 series（避免假性 reset）
    - `INT64` / `DOUBLE` + `MetricKind=DELTA` → 累加同 series 的 delta 值後以 Counter 回傳（同樣帶 `start_time_unix`）
    - `DISTRIBUTION` → 詳見下節
  - **DISTRIBUTION 轉 Prometheus Histogram**
    - 三種 bucket layout 都要支援：`LinearBuckets{NumFiniteBuckets, Width, Offset}`、`ExponentialBuckets{NumFiniteBuckets, GrowthFactor, Scale}`、`ExplicitBuckets{Bounds}`
    - GCP 回傳每個 bucket 的 `BucketCounts`（per-bucket），需轉為 Prom 的 cumulative `le` 邊界
    - 輸出三組 series：`<name>_bucket{le="..."}`、`<name>_count`、`<name>_sum`

- [ ] 實作 `NewGCPCollector(ctx, tokenSource, opts) (*GCPCollector, error)`，opts 含 `MaxSeries int`

- [ ] **Client cache**：在 `cmd/server` 層維護一個 `map[string]*monitoring.MetricClient`，key = impersonated SA email（空字串代表 ADC），避免每次請求新建 gRPC 連線。搭配 `sync.RWMutex` 或 `sync.Map`，並提供 `Close()` 在 shutdown 時釋放所有 client

- [ ] 單元測試（table-driven）
  - 測試 GAUGE / CUMULATIVE / DELTA metric 的轉換
  - 測試 DISTRIBUTION 三種 bucket layout 的 cumulative 換算
  - 測試 CUMULATIVE 的 `start_time_unix` label
  - 測試 filter 組合（單獨 metric.type、metric.type AND user filter）
  - 測試 `MaxSeries` 觸發時回傳明確 error
  - 測試 API error 的 error propagation
  - 使用 fake gRPC server（`google.golang.org/grpc/test/bufconn`）注入假資料

**關鍵依賴**：
```
cloud.google.com/go/monitoring/apiv3/v2
cloud.google.com/go/monitoring/apiv3/v2/monitoringpb
github.com/prometheus/client_model/go
github.com/prometheus/common/expfmt
```

---

## Phase 3：Handler 層（`internal/handler`）

**目標**：提供 HTTP endpoint，整合 auth + collector。

### 工作項目

- [ ] 實作 `MetricsHandler` struct
  - 依賴注入：`auth.Config`（base config，從環境變數載入）、`*ClientCache`（見 Phase 2）、`Limits{ScrapeTimeout, MaxConcurrent}`
  - TokenSource 與 MetricClient 皆從 cache 取得（key = impersonated SA），不再 per-request 建立

- [ ] 實作 `ServeHTTP` / `HandleMetrics`
  - 解析 query parameters，驗證必填欄位（`project`, `metric_type`）
  - 若缺少必填：回傳 `400 Bad Request` + JSON error body
  - 覆寫 `impersonate_sa`：query param 優先於環境變數 `DEFAULT_IMPERSONATE_SA`
  - **Per-request timeout**：`ctx, cancel := context.WithTimeout(r.Context(), Limits.ScrapeTimeout)`（預設 30s，env `SCRAPE_TIMEOUT` 可調）
  - **Concurrency limit**：以 buffered channel / `golang.org/x/sync/semaphore` 限制同時進行的 scrape 數（預設 16，env `MAX_CONCURRENT_SCRAPES`）；取不到時直接 `429 Too Many Requests`
  - 呼叫 `Collector.Collect()`
  - 用 `expfmt.NewEncoder(w, expfmt.FmtText)` **streaming** 寫出，不在記憶體累積整份 `[]*dto.MetricFamily`
  - 設定 `Content-Type: text/plain; version=0.0.4; charset=utf-8`

- [ ] 錯誤對應表（從 `status.Code(err)` 判斷）

  | gRPC code | HTTP | 備註 |
  |-----------|------|------|
  | `InvalidArgument` | 400 | 使用者 filter / metric_type 語法錯誤 |
  | `NotFound` | 404 | project 不存在或 metric type 未啟用 |
  | `PermissionDenied` / `Unauthenticated` | 502 | exporter 設定問題，非 caller 過失 |
  | `DeadlineExceeded` | 504 | scrape 超時 |
  | `ResourceExhausted` | 503 | GCP quota，附 `Retry-After` |
  | 其他 | 502 | 上游錯誤 |
  | `MaxSeries` 觸發 | 503 | 訊息提示縮小 filter |

- [ ] 實作 `HandleHealthz`
  - 僅 liveness：process 起來就回 200，**不**驗證 token 或呼叫 GCP（避免 IAM 計費 / 與 GCP 故障耦合）
  - 回傳 `200 OK` + `{"status":"ok"}`

- [ ] **安全性備註**（README/runbook，不在程式碼）
  - 此 server 以 GCP SA 身份對 Cloud Monitoring 發請求，能讀到任何 SA 有權限的指標
  - 部署時必須放在內網 / 加上前置驗證（IAP、basic auth、network policy）
  - 不可直接暴露到公網

- [ ] 單元測試
  - 測試缺少 `project` / `metric_type` 時回傳 400
  - 測試 query parameter 正確對應到 `QueryParams`
  - 測試 impersonation SA 優先順序
  - Mock `Collector` interface，測試錯誤對應表每一列的狀態碼
  - 測試超過 concurrency 上限回傳 429
  - 測試超時情境回傳 504
  - 測試 Content-Type header 正確

---

## Phase 4：Server 進入點（`cmd/server`）

**目標**：載入設定、組裝元件、啟動 HTTP server。

### 工作項目

- [ ] 從環境變數讀取設定：
  ```go
  PORT                              （預設 8080）
  GOOGLE_APPLICATION_CREDENTIALS
  DEFAULT_IMPERSONATE_SA
  LOG_LEVEL                         （debug/info/warn/error，預設 info）
  LOG_FORMAT                        （text/json，預設 json）
  SCRAPE_TIMEOUT                    （Go duration，預設 30s）
  MAX_CONCURRENT_SCRAPES            （int，預設 16）
  MAX_SERIES_PER_REQUEST            （int，預設 10000）
  SHUTDOWN_GRACE                    （Go duration，預設 10s）
  ```

- [ ] 建立 `ClientCache`、`MetricsHandler`、`HealthzHandler` 並掛載路由：
  ```
  GET /metrics   → MetricsHandler
  GET /healthz   → HealthzHandler   （liveness only）
  ```

- [ ] Graceful shutdown
  - 監聽 `SIGINT` / `SIGTERM`
  - 收到訊號後 `http.Server.Shutdown(ctx)`，ctx timeout = `SHUTDOWN_GRACE`
  - shutdown context 應傳入 `ClientCache.Close()` 釋放 gRPC 連線
  - 仍在執行的 scrape 透過 ctx cancel 中斷

- [ ] 設定 structured logging（`log/slog`，Go 1.21+）
  - 預設 JSON handler（容器環境友善）
  - 每次 `/metrics` 請求 log 一行：`project`、`metric_type`、`impersonate_sa`、`status`、`series_count`、`gcp_latency_ms`、`total_latency_ms`

---

## Phase 5：容器化與開發環境

**目標**：提供 Dockerfile 與 docker-compose.yaml。

### 工作項目

- [ ] `Dockerfile`（multi-stage build；推薦 distroless 以同時拿到 CA bundle 與 nonroot user）
  ```dockerfile
  # Stage 1: build
  FROM golang:1.22-alpine AS builder
  WORKDIR /app
  COPY go.mod go.sum ./
  RUN go mod download
  COPY . .
  RUN CGO_ENABLED=0 GOOS=linux go build \
        -trimpath -ldflags="-s -w" \
        -o /exporter ./cmd/server

  # Stage 2: runtime（distroless 已含 ca-certificates 與 nonroot user）
  FROM gcr.io/distroless/static-debian12:nonroot
  COPY --from=builder /exporter /exporter
  EXPOSE 8080
  USER nonroot:nonroot
  ENTRYPOINT ["/exporter"]
  ```
  > 若改用 `alpine:3.19`，**必須** `RUN apk add --no-cache ca-certificates` 並 `USER 65532:65532`，否則 GCP HTTPS 呼叫會回 `x509: certificate signed by unknown authority`。

- [ ] `.dockerignore`（避免把 `.git` / SA JSON / vendor / 本地暫存帶進 build context）
  ```
  .git
  .github
  *.json
  !go.sum
  vendor
  *.md
  ```

- [ ] `docker-compose.yaml`
  - **exporter** service：mount SA JSON 為 read-only、設定 env vars
  - **prometheus** service：使用官方 image，mount `config/prometheus.yml`

- [ ] `config/prometheus.yml`（multi-project / multi-metric 用 `relabel_configs`）
  ```yaml
  scrape_configs:
    - job_name: 'gcp-metrics-exporter'
      metrics_path: /metrics
      static_configs:
        - targets:
            - 'my-project-a;compute.googleapis.com/instance/cpu/utilization'
            - 'my-project-b;run.googleapis.com/request_count'
      relabel_configs:
        - source_labels: [__address__]
          regex: '([^;]+);(.+)'
          target_label: __param_project
          replacement: '$1'
        - source_labels: [__address__]
          regex: '([^;]+);(.+)'
          target_label: __param_metric_type
          replacement: '$2'
        - target_label: __address__
          replacement: 'exporter:8080'
  ```

---

## Phase 6：整合測試與驗收

**目標**：確保所有元件端到端正常運作。

### 工作項目

- [ ] Integration test（`_test.go` with `//go:build integration`）
  - 啟動 fake gRPC server 模擬完整 Cloud Monitoring 回應
  - 發送 HTTP 請求到測試 server
  - 驗證 Prometheus 格式輸出正確
  - **Round-trip parse**：用 `expfmt.TextParser` 把 response body 反向 parse，assert 沒有錯誤、metric 數量與 labels 與預期相符（可抓到 escaping、label 名稱不合法等細節）

- [ ] 執行 `go test -count=1 -race -timeout=60s ./...` 確認無 race condition、繞過 test cache

- [ ] 用 docker-compose 啟動，在 Prometheus UI 驗證 scrape 正常

---

## 跨切面議題

### Cardinality 保護
- `REDUCE_NONE` + 多 resource label 容易產生大量 series，可能撐爆 Prometheus 記憶體
- 由 `MAX_SERIES_PER_REQUEST`（預設 10000）硬性限制；超過時 Collector 回傳 error，handler 回 503
- 接近上限（>80%）時 log warn，提示縮小 filter 或啟用 reducer

### Logging
- 預設 `log/slog` JSON handler；`LOG_FORMAT=text` 用於本地 dev
- `/metrics` 每筆請求 log 欄位：`project`、`metric_type`、`impersonate_sa`、`status`、`series_count`、`gcp_latency_ms`、`total_latency_ms`
- 不要 log `filter` 全文（可能含 PII）；可 hash 或截斷

### Singleflight（選配，後續優化）
- 多個 Prom replica 同時 scrape 相同 query 時，可用 `golang.org/x/sync/singleflight` dedupe，降低 GCP API 呼叫
- 預設不啟用；若觀察到 GCP quota 壓力再加上

---

## 技術決策記錄

| 決策 | 選擇 | 理由 |
|------|------|------|
| HTTP framework | 標準 `net/http` | 無額外依賴、足夠輕量 |
| GCP SDK | `cloud.google.com/go/monitoring/apiv3/v2` | 官方 Go SDK；types 來自 `monitoringpb` 子套件 |
| Prometheus 序列化 | 直接產出 `[]*dto.MetricFamily` + `expfmt.NewEncoder` streaming | 不依賴 `prometheus.Registry`，避免跨請求全域狀態，per-request 動態 metric 名稱較自然 |
| Logging | `log/slog`（Go 1.21+） JSON handler | 標準庫 structured logging，容器友善 |
| 查詢策略 | 每次請求即時查詢（stateless） | 簡化架構，Prometheus 控制 scrape interval |
| Client 重用 | per-SA `MetricClient` cache | 避免每次請求新建 gRPC 連線 |
| CUMULATIVE 重啟處理 | 加 `start_time_unix` label | GCP `start_time` 變動時 Prom 視為新 series，不會誤判 reset |
| Impersonation | `google.golang.org/api/impersonate` | 官方 impersonation 支援 |
| Mock GCP API | bufconn + fake gRPC server | 不需要真實 GCP 環境即可測試 |

---

## 里程碑順序

```
Phase 1（auth）→ Phase 2（collector）→ Phase 3（handler）
     ↓
Phase 4（server main）→ Phase 5（Docker）→ Phase 6（整合測試）
```

每個 Phase 完成後需確保：
1. `go test ./...` 全部通過
2. `go vet ./...` 無警告
3. 新增的 exported API 有 godoc 註解
