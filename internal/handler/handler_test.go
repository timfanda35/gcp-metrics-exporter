package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	commonmodel "github.com/prometheus/common/model"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/timfanda35/gcp-metrics-exporter/internal/collector"
)

// fakeCollector is the test seam for [collector.Collector]. Behaviour is
// driven by the function fields — assign collectFn for a specific test
// case and read seenParams afterwards. The mu guards seenParams against
// concurrent callers (the concurrency test races two goroutines).
type fakeCollector struct {
	mu         sync.Mutex
	collectFn  func(ctx context.Context, params collector.QueryParams) ([]*dto.MetricFamily, error)
	seenParams []collector.QueryParams
}

// Collect records params and delegates to collectFn (or returns nil/nil
// when collectFn is unset).
func (f *fakeCollector) Collect(ctx context.Context, params collector.QueryParams) ([]*dto.MetricFamily, error) {
	f.mu.Lock()
	f.seenParams = append(f.seenParams, params)
	fn := f.collectFn
	f.mu.Unlock()
	if fn == nil {
		return nil, nil
	}
	return fn(ctx, params)
}

// lastParams returns the most recent QueryParams seen, or zero value
// when none have been recorded.
func (f *fakeCollector) lastParams() collector.QueryParams {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.seenParams) == 0 {
		return collector.QueryParams{}
	}
	return f.seenParams[len(f.seenParams)-1]
}

// staticGauge returns a single gauge family with one metric — convenient
// canned data for the success path.
func staticGauge(name string, value float64) []*dto.MetricFamily {
	return []*dto.MetricFamily{{
		Name: proto.String(name),
		Type: dto.MetricType_GAUGE.Enum(),
		Help: proto.String("test gauge"),
		Metric: []*dto.Metric{{
			Gauge: &dto.Gauge{Value: proto.Float64(value)},
		}},
	}}
}

// newHandler constructs a MetricsHandler bound to the given fake
// collector and discard logger by default. Callers may override the
// logger to capture output.
func newHandler(t *testing.T, fc *fakeCollector, limits Limits, opts ...func(*MetricsHandler)) *MetricsHandler {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	factory := func(_ context.Context, _ string) (collector.Collector, error) {
		return fc, nil
	}
	h := NewMetricsHandler(factory, limits, logger)
	for _, o := range opts {
		o(h)
	}
	return h
}

// captureRecords lets a test capture (and assert on) request params and
// the SA passed into the factory.
type captureRecords struct {
	mu              sync.Mutex
	saSeen          []string
	factoryErr      error
	overrideFactory func(ctx context.Context, sa string) (collector.Collector, error)
}

func (cr *captureRecords) lastSA() string {
	cr.mu.Lock()
	defer cr.mu.Unlock()
	if len(cr.saSeen) == 0 {
		return ""
	}
	return cr.saSeen[len(cr.saSeen)-1]
}

// Test 1: missing project → 400, JSON error body.
func TestServeHTTP_MissingProject(t *testing.T) {
	h := newHandler(t, &fakeCollector{}, Limits{})
	req := httptest.NewRequest(http.MethodGet, "/metrics?metric_type=foo", nil)
	rr := httptest.NewRecorder()

	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rr.Code)
	}
	if ct := rr.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
	var body struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("invalid JSON body: %v", err)
	}
	if !strings.Contains(body.Error, "project") {
		t.Errorf("error message = %q, want it to mention project", body.Error)
	}
}

// Test 2: missing metric_type → 400.
func TestServeHTTP_MissingMetricType(t *testing.T) {
	h := newHandler(t, &fakeCollector{}, Limits{})
	req := httptest.NewRequest(http.MethodGet, "/metrics?project=p", nil)
	rr := httptest.NewRecorder()

	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rr.Code)
	}
	var body struct {
		Error string `json:"error"`
	}
	_ = json.Unmarshal(rr.Body.Bytes(), &body)
	if !strings.Contains(body.Error, "metric_type") {
		t.Errorf("error message = %q, want it to mention metric_type", body.Error)
	}
}

// Test 3: bad interval duration → 400.
func TestServeHTTP_BadInterval(t *testing.T) {
	h := newHandler(t, &fakeCollector{}, Limits{})
	req := httptest.NewRequest(http.MethodGet, "/metrics?project=p&metric_type=m&interval=banana", nil)
	rr := httptest.NewRecorder()

	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rr.Code)
	}
}

// Test 4: success path — body parses via expfmt.TextParser.
func TestServeHTTP_Success(t *testing.T) {
	fc := &fakeCollector{
		collectFn: func(_ context.Context, _ collector.QueryParams) ([]*dto.MetricFamily, error) {
			return staticGauge("test_metric", 42), nil
		},
	}
	h := newHandler(t, fc, Limits{})
	req := httptest.NewRequest(http.MethodGet, "/metrics?project=p&metric_type=test_metric&interval=10m", nil)
	rr := httptest.NewRecorder()

	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200, body=%q", rr.Code, rr.Body.String())
	}
	wantCT := "text/plain; version=0.0.4; charset=utf-8"
	if got := rr.Header().Get("Content-Type"); got != wantCT {
		t.Errorf("Content-Type = %q, want %q", got, wantCT)
	}

	parser := expfmt.NewTextParser(commonmodel.LegacyValidation)
	families, err := parser.TextToMetricFamilies(rr.Body)
	if err != nil {
		t.Fatalf("body did not parse: %v\nbody=%q", err, rr.Body.String())
	}
	if _, ok := families["test_metric"]; !ok {
		t.Errorf("parsed families missing test_metric: %v", families)
	}

	// Defaults propagated into QueryParams.
	got := fc.lastParams()
	if got.Aligner != "ALIGN_MEAN" {
		t.Errorf("Aligner default = %q, want ALIGN_MEAN", got.Aligner)
	}
	if got.Reducer != "REDUCE_NONE" {
		t.Errorf("Reducer default = %q, want REDUCE_NONE", got.Reducer)
	}
	if got.Interval != 10*time.Minute {
		t.Errorf("Interval = %v, want 10m", got.Interval)
	}
	if got.AlignmentPeriod != 10*time.Minute {
		t.Errorf("AlignmentPeriod = %v, want 10m (default to interval)", got.AlignmentPeriod)
	}
	if got.GroupByFields != nil {
		t.Errorf("GroupByFields = %v, want nil under REDUCE_NONE", got.GroupByFields)
	}
}

// Test 5: impersonate_sa query param overrides the env-derived default.
func TestServeHTTP_ImpersonationPrecedence(t *testing.T) {
	cr := &captureRecords{}
	fc := &fakeCollector{
		collectFn: func(_ context.Context, _ collector.QueryParams) ([]*dto.MetricFamily, error) {
			return nil, nil
		},
	}
	factory := func(_ context.Context, sa string) (collector.Collector, error) {
		cr.mu.Lock()
		cr.saSeen = append(cr.saSeen, sa)
		cr.mu.Unlock()
		return fc, nil
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	h := NewMetricsHandler(factory, Limits{
		DefaultImpersonateSA: "default@project.iam.gserviceaccount.com",
	}, logger)

	t.Run("default_used_when_no_query_param", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/metrics?project=p&metric_type=m", nil)
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("status = %d, body=%q", rr.Code, rr.Body.String())
		}
		if got := cr.lastSA(); got != "default@project.iam.gserviceaccount.com" {
			t.Errorf("SA passed to factory = %q, want default@…", got)
		}
	})

	t.Run("query_param_wins", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/metrics?project=p&metric_type=m&impersonate_sa=override@project.iam.gserviceaccount.com", nil)
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("status = %d, body=%q", rr.Code, rr.Body.String())
		}
		if got := cr.lastSA(); got != "override@project.iam.gserviceaccount.com" {
			t.Errorf("SA passed to factory = %q, want override@…", got)
		}
	})
}

// Test 6: error mapping table.
func TestServeHTTP_ErrorMapping(t *testing.T) {
	cases := []struct {
		name           string
		err            error
		wantStatus     int
		wantRetryAfter string // "" means header must be absent
		wantErrCode    string
	}{
		{
			name:        "max_series_exceeded",
			err:         fmt.Errorf("wrap: %w", collector.ErrMaxSeriesExceeded),
			wantStatus:  http.StatusServiceUnavailable,
			wantErrCode: "max_series_exceeded",
		},
		{
			name:        "invalid_aggregation",
			err:         fmt.Errorf("wrap: %w", collector.ErrInvalidAggregation),
			wantStatus:  http.StatusBadRequest,
			wantErrCode: "invalid_aggregation",
		},
		{
			name:        "deadline_exceeded_sentinel",
			err:         fmt.Errorf("wrap: %w", context.DeadlineExceeded),
			wantStatus:  http.StatusGatewayTimeout,
			wantErrCode: "deadline_exceeded",
		},
		{
			name:        "client_canceled",
			err:         fmt.Errorf("wrap: %w", context.Canceled),
			wantStatus:  http.StatusRequestTimeout, // wire form of 499
			wantErrCode: "client_canceled",
		},
		{
			name:        "grpc_invalid_argument",
			err:         grpcstatus.Error(codes.InvalidArgument, "bad filter"),
			wantStatus:  http.StatusBadRequest,
			wantErrCode: "InvalidArgument",
		},
		{
			name:        "grpc_not_found",
			err:         grpcstatus.Error(codes.NotFound, "no such project"),
			wantStatus:  http.StatusNotFound,
			wantErrCode: "NotFound",
		},
		{
			name:        "grpc_permission_denied",
			err:         grpcstatus.Error(codes.PermissionDenied, "denied"),
			wantStatus:  http.StatusBadGateway,
			wantErrCode: "PermissionDenied",
		},
		{
			name:        "grpc_unauthenticated",
			err:         grpcstatus.Error(codes.Unauthenticated, "no creds"),
			wantStatus:  http.StatusBadGateway,
			wantErrCode: "Unauthenticated",
		},
		{
			name:        "grpc_deadline_exceeded",
			err:         grpcstatus.Error(codes.DeadlineExceeded, "upstream timed out"),
			wantStatus:  http.StatusGatewayTimeout,
			wantErrCode: "DeadlineExceeded",
		},
		{
			name:           "grpc_resource_exhausted",
			err:            grpcstatus.Error(codes.ResourceExhausted, "quota"),
			wantStatus:     http.StatusServiceUnavailable,
			wantRetryAfter: "30",
			wantErrCode:    "ResourceExhausted",
		},
		{
			name:        "grpc_internal_default",
			err:         grpcstatus.Error(codes.Internal, "boom"),
			wantStatus:  http.StatusBadGateway,
			wantErrCode: "Internal",
		},
		{
			name:        "plain_error_default",
			err:         errors.New("something went wrong"),
			wantStatus:  http.StatusBadGateway,
			wantErrCode: "unknown",
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			fc := &fakeCollector{
				collectFn: func(_ context.Context, _ collector.QueryParams) ([]*dto.MetricFamily, error) {
					return nil, tc.err
				},
			}
			h := newHandler(t, fc, Limits{})
			req := httptest.NewRequest(http.MethodGet, "/metrics?project=p&metric_type=m", nil)
			rr := httptest.NewRecorder()

			h.ServeHTTP(rr, req)

			if rr.Code != tc.wantStatus {
				t.Errorf("status = %d, want %d (body=%q)", rr.Code, tc.wantStatus, rr.Body.String())
			}
			if got := rr.Header().Get("Retry-After"); got != tc.wantRetryAfter {
				t.Errorf("Retry-After = %q, want %q", got, tc.wantRetryAfter)
			}
			if ct := rr.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
				t.Errorf("Content-Type = %q, want application/json", ct)
			}
		})
	}
}

// Test 7: concurrency limit — the second of two concurrent requests
// while one is in flight gets 429 with Retry-After: 1.
func TestServeHTTP_ConcurrencyLimit(t *testing.T) {
	release := make(chan struct{})
	entered := make(chan struct{})

	fc := &fakeCollector{
		collectFn: func(ctx context.Context, _ collector.QueryParams) ([]*dto.MetricFamily, error) {
			select {
			case entered <- struct{}{}:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			return staticGauge("blocked_metric", 1), nil
		},
	}
	h := newHandler(t, fc, Limits{MaxConcurrent: 1, ScrapeTimeout: 5 * time.Second})

	rr1 := httptest.NewRecorder()
	rr2 := httptest.NewRecorder()
	req1 := httptest.NewRequest(http.MethodGet, "/metrics?project=p&metric_type=m", nil)
	req2 := httptest.NewRequest(http.MethodGet, "/metrics?project=p&metric_type=m", nil)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		h.ServeHTTP(rr1, req1)
	}()

	// Wait until the first request is inside the collector — then the
	// semaphore is held.
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("first request never entered fake collector")
	}

	// Second request should be rejected immediately.
	h.ServeHTTP(rr2, req2)
	if rr2.Code != http.StatusTooManyRequests {
		t.Fatalf("second request status = %d, want 429", rr2.Code)
	}
	if got := rr2.Header().Get("Retry-After"); got != "1" {
		t.Errorf("Retry-After = %q, want 1", got)
	}

	close(release)
	wg.Wait()

	if rr1.Code != http.StatusOK {
		t.Errorf("first request status = %d, want 200, body=%q", rr1.Code, rr1.Body.String())
	}
}

// Test 8: per-request timeout → 504.
func TestServeHTTP_Timeout(t *testing.T) {
	fc := &fakeCollector{
		collectFn: func(ctx context.Context, _ collector.QueryParams) ([]*dto.MetricFamily, error) {
			select {
			case <-time.After(200 * time.Millisecond):
				return staticGauge("never", 0), nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		},
	}
	h := newHandler(t, fc, Limits{ScrapeTimeout: 5 * time.Millisecond})
	req := httptest.NewRequest(http.MethodGet, "/metrics?project=p&metric_type=m", nil)
	rr := httptest.NewRecorder()

	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusGatewayTimeout {
		t.Errorf("status = %d, want 504, body=%q", rr.Code, rr.Body.String())
	}
}

// Test 9: HandleHealthz returns 200 with the canned body.
func TestHandleHealthz(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rr := httptest.NewRecorder()

	HandleHealthz(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", rr.Code)
	}
	if ct := rr.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
	if body := rr.Body.String(); body != `{"status":"ok"}` {
		t.Errorf("body = %q, want %q", body, `{"status":"ok"}`)
	}
}

// Test 10: JSON log lines for one success and one error contain the
// expected fields.
func TestServeHTTP_Logging(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		var buf bytes.Buffer
		logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
		fc := &fakeCollector{
			collectFn: func(_ context.Context, _ collector.QueryParams) ([]*dto.MetricFamily, error) {
				return staticGauge("ok_metric", 7), nil
			},
		}
		factory := func(_ context.Context, _ string) (collector.Collector, error) { return fc, nil }
		h := NewMetricsHandler(factory, Limits{}, logger)

		req := httptest.NewRequest(http.MethodGet, "/metrics?project=proj-1&metric_type=ok_metric&filter=resource.label.zone=%22us-central1-a%22", nil)
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)

		if rr.Code != http.StatusOK {
			t.Fatalf("status = %d", rr.Code)
		}
		log := lastJSONLine(t, &buf)
		if got := log["msg"]; got != "metrics request" {
			t.Errorf("msg = %v, want %q", got, "metrics request")
		}
		if got := log["project"]; got != "proj-1" {
			t.Errorf("project = %v", got)
		}
		if got := log["metric_type"]; got != "ok_metric" {
			t.Errorf("metric_type = %v", got)
		}
		if got, ok := log["status"].(float64); !ok || int(got) != http.StatusOK {
			t.Errorf("status = %v, want 200 (numeric)", log["status"])
		}
		if got, ok := log["series_count"].(float64); !ok || int(got) != 1 {
			t.Errorf("series_count = %v, want 1", log["series_count"])
		}
		if _, ok := log["gcp_latency_ms"]; !ok {
			t.Errorf("expected gcp_latency_ms in log: %v", log)
		}
		if _, ok := log["total_latency_ms"]; !ok {
			t.Errorf("expected total_latency_ms in log: %v", log)
		}
		if got, ok := log["filter_fingerprint"].(string); !ok || got == "" {
			t.Errorf("filter_fingerprint = %v, want non-empty hex prefix (filter must not be raw-logged)", log["filter_fingerprint"])
		}
		if got := log["filter"]; got != nil {
			t.Errorf("filter must not be logged raw, got %v", got)
		}
		if _, has := log["error_code"]; has {
			t.Errorf("error_code should be absent on success: %v", log)
		}
	})

	t.Run("error", func(t *testing.T) {
		var buf bytes.Buffer
		logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
		fc := &fakeCollector{
			collectFn: func(_ context.Context, _ collector.QueryParams) ([]*dto.MetricFamily, error) {
				return nil, grpcstatus.Error(codes.NotFound, "no such metric")
			},
		}
		factory := func(_ context.Context, _ string) (collector.Collector, error) { return fc, nil }
		h := NewMetricsHandler(factory, Limits{}, logger)

		req := httptest.NewRequest(http.MethodGet, "/metrics?project=proj-1&metric_type=missing", nil)
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)

		if rr.Code != http.StatusNotFound {
			t.Fatalf("status = %d", rr.Code)
		}
		log := lastJSONLine(t, &buf)
		if got := log["error_code"]; got != "NotFound" {
			t.Errorf("error_code = %v, want NotFound", got)
		}
		if got, ok := log["status"].(float64); !ok || int(got) != http.StatusNotFound {
			t.Errorf("status = %v, want 404", log["status"])
		}
	})
}

// lastJSONLine reads the buffer, parses the final non-empty line as JSON,
// and returns the resulting map. Fails the test on parse errors.
func lastJSONLine(t *testing.T, buf *bytes.Buffer) map[string]any {
	t.Helper()
	lines := strings.Split(strings.TrimRight(buf.String(), "\n"), "\n")
	if len(lines) == 0 {
		t.Fatal("no log lines emitted")
	}
	var out map[string]any
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &out); err != nil {
		t.Fatalf("parse log line %q: %v", lines[len(lines)-1], err)
	}
	return out
}

// Group-by parameter is only forwarded when the reducer is non-default.
func TestServeHTTP_GroupByOnlyWithReducer(t *testing.T) {
	fc := &fakeCollector{
		collectFn: func(_ context.Context, _ collector.QueryParams) ([]*dto.MetricFamily, error) {
			return nil, nil
		},
	}
	h := newHandler(t, fc, Limits{})

	t.Run("dropped_under_REDUCE_NONE", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/metrics?project=p&metric_type=m&group_by=zone,project_id", nil)
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("status = %d", rr.Code)
		}
		if gb := fc.lastParams().GroupByFields; gb != nil {
			t.Errorf("GroupByFields = %v, want nil under REDUCE_NONE", gb)
		}
	})

	t.Run("forwarded_when_reducing", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/metrics?project=p&metric_type=m&reducer=REDUCE_SUM&group_by=zone,project_id", nil)
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("status = %d", rr.Code)
		}
		got := fc.lastParams().GroupByFields
		if len(got) != 2 || got[0] != "zone" || got[1] != "project_id" {
			t.Errorf("GroupByFields = %v, want [zone project_id]", got)
		}
	})

	t.Run("empty_group_by_yields_nil", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/metrics?project=p&metric_type=m&reducer=REDUCE_SUM&group_by=", nil)
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("status = %d", rr.Code)
		}
		if gb := fc.lastParams().GroupByFields; gb != nil {
			t.Errorf("GroupByFields = %v, want nil for empty group_by", gb)
		}
	})
}
