package handler

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	"google.golang.org/protobuf/proto"

	"github.com/timfanda35/gcp-metrics-exporter/internal/gmp"
)

// fakeGMPClient is a test double for [gmp.Client].
type fakeGMPClient struct {
	mu      sync.Mutex
	samples []gmp.Sample
	err     error
	calls   int
}

func (f *fakeGMPClient) Query(_ context.Context, _, _ string, _ time.Time) ([]gmp.Sample, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	return f.samples, f.err
}

// staticGMPGauge returns a single gauge family — shared helper for success path.
func staticGMPGauge(name string, value float64) []*dto.MetricFamily {
	return []*dto.MetricFamily{{
		Name: proto.String(name),
		Type: dto.MetricType_GAUGE.Enum(),
		Help: proto.String("GMP metric " + name),
		Metric: []*dto.Metric{{
			Gauge: &dto.Gauge{Value: proto.Float64(value)},
		}},
	}}
}

// newGMPHandler builds a [GMPHandler] bound to the given fake client and
// discard logger.
func newGMPHandler(t *testing.T, fc *fakeGMPClient, limits Limits) *GMPHandler {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	factory := func(_ context.Context, _ string) (gmp.Client, error) {
		return fc, nil
	}
	return NewGMPHandler(factory, limits, logger)
}

func TestGMPHandler_Success(t *testing.T) {
	t.Parallel()
	fc := &fakeGMPClient{
		samples: []gmp.Sample{
			{Labels: map[string]string{"__name__": "up", "job": "test"}, Value: 1},
		},
	}
	h := newGMPHandler(t, fc, Limits{})
	req := httptest.NewRequest(http.MethodGet, "/gmp-metrics?project=my-proj&query=up", nil)
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "up") {
		t.Errorf("body does not contain metric name 'up': %s", body)
	}
}

func TestGMPHandler_MissingProject(t *testing.T) {
	t.Parallel()
	h := newGMPHandler(t, &fakeGMPClient{}, Limits{})
	req := httptest.NewRequest(http.MethodGet, "/gmp-metrics?query=up", nil)
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	var errBody struct{ Error string }
	_ = json.NewDecoder(rec.Body).Decode(&errBody)
	if !strings.Contains(errBody.Error, "project") {
		t.Errorf("error body = %q, want mention of 'project'", errBody.Error)
	}
}

func TestGMPHandler_MissingQuery(t *testing.T) {
	t.Parallel()
	h := newGMPHandler(t, &fakeGMPClient{}, Limits{})
	req := httptest.NewRequest(http.MethodGet, "/gmp-metrics?project=my-proj", nil)
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
}

func TestGMPHandler_InvalidTimeOffset(t *testing.T) {
	t.Parallel()
	h := newGMPHandler(t, &fakeGMPClient{}, Limits{})
	req := httptest.NewRequest(http.MethodGet, "/gmp-metrics?project=p&query=up&time_offset=notaduration", nil)
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
}

func TestGMPHandler_ConcurrencyLimit(t *testing.T) {
	t.Parallel()

	var (
		inflight sync.WaitGroup
		release  = make(chan struct{})
	)
	blocking := &fakeGMPClient{}
	blocking.err = nil

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	h := NewGMPHandler(func(_ context.Context, _ string) (gmp.Client, error) {
		// Block until released so we can saturate the semaphore.
		inflight.Done()
		<-release
		return &fakeGMPClient{}, nil
	}, Limits{MaxConcurrent: 1, ScrapeTimeout: 5 * time.Second}, logger)

	// Flood the semaphore with one request that blocks.
	inflight.Add(1)
	go func() {
		req := httptest.NewRequest(http.MethodGet, "/gmp-metrics?project=p&query=up", nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
	}()
	inflight.Wait() // block until first request has acquired the semaphore

	// Second request should be rejected with 429.
	req := httptest.NewRequest(http.MethodGet, "/gmp-metrics?project=p&query=up", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	close(release) // unblock first request

	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", rec.Code)
	}
	if rec.Header().Get("Retry-After") == "" {
		t.Error("expected Retry-After header on 429")
	}
}

func TestGMPHandler_ImpersonationPrecedence(t *testing.T) {
	t.Parallel()

	var gotSA string
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	h := NewGMPHandler(func(_ context.Context, sa string) (gmp.Client, error) {
		gotSA = sa
		return &fakeGMPClient{}, nil
	}, Limits{DefaultImpersonateSA: "default@proj.iam.gserviceaccount.com"}, logger)

	// Explicit impersonate_sa overrides the default.
	req := httptest.NewRequest(http.MethodGet,
		"/gmp-metrics?project=p&query=up&impersonate_sa=explicit@proj.iam.gserviceaccount.com", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if gotSA != "explicit@proj.iam.gserviceaccount.com" {
		t.Errorf("impersonateSA = %q, want explicit SA", gotSA)
	}

	// No per-request SA falls back to default.
	gotSA = ""
	req = httptest.NewRequest(http.MethodGet, "/gmp-metrics?project=p&query=up", nil)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if gotSA != "default@proj.iam.gserviceaccount.com" {
		t.Errorf("impersonateSA = %q, want default SA", gotSA)
	}
}

func TestGMPHandler_APIError_MapsTo400(t *testing.T) {
	t.Parallel()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	h := NewGMPHandler(func(_ context.Context, _ string) (gmp.Client, error) {
		return &fakeGMPClient{err: &gmp.APIError{StatusCode: 400, Msg: "bad query"}}, nil
	}, Limits{}, logger)

	req := httptest.NewRequest(http.MethodGet, "/gmp-metrics?project=p&query=bad{", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
}

func TestGMPHandler_TimeOffset(t *testing.T) {
	t.Parallel()

	var gotAt time.Time
	fixedNow := time.Unix(1609459200, 0)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	h := NewGMPHandler(func(_ context.Context, _ string) (gmp.Client, error) {
		return &fakeGMPClient{}, nil
	}, Limits{}, logger)
	h.WithClock(func() time.Time { return fixedNow })

	// Use a real collector to capture the query time. We override the factory
	// to record `at` via a custom client.
	h.clientForSA = func(_ context.Context, _ string) (gmp.Client, error) {
		return &captureTimeClient{capture: &gotAt}, nil
	}

	req := httptest.NewRequest(http.MethodGet, "/gmp-metrics?project=p&query=up&time_offset=3m", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	wantAt := fixedNow.Add(-3 * time.Minute)
	if !gotAt.Equal(wantAt) {
		t.Errorf("query time = %v, want %v (now - 3m)", gotAt, wantAt)
	}
}

// captureTimeClient records the `at` time passed to Query.
type captureTimeClient struct {
	capture *time.Time
}

func (c *captureTimeClient) Query(_ context.Context, _, _ string, at time.Time) ([]gmp.Sample, error) {
	*c.capture = at
	return nil, nil
}

func TestMapGMPError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		err        error
		wantStatus int
		wantCode   string
	}{
		{
			name:       "deadline exceeded",
			err:        context.DeadlineExceeded,
			wantStatus: http.StatusGatewayTimeout,
			wantCode:   "deadline_exceeded",
		},
		{
			name:       "context canceled",
			err:        context.Canceled,
			wantStatus: nonStandardClientClosed,
			wantCode:   "client_canceled",
		},
		{
			name:       "api 400",
			err:        &gmp.APIError{StatusCode: 400},
			wantStatus: http.StatusBadRequest,
			wantCode:   "bad_query",
		},
		{
			name:       "api 403",
			err:        &gmp.APIError{StatusCode: 403},
			wantStatus: http.StatusBadGateway,
			wantCode:   "auth_error",
		},
		{
			name:       "api 503",
			err:        &gmp.APIError{StatusCode: 503},
			wantStatus: http.StatusServiceUnavailable,
			wantCode:   "quota_exhausted",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			status, _, _, code, _ := mapGMPError(tc.err)
			if status != tc.wantStatus {
				t.Errorf("status = %d, want %d", status, tc.wantStatus)
			}
			if code != tc.wantCode {
				t.Errorf("errCode = %q, want %q", code, tc.wantCode)
			}
		})
	}
}
