//go:build integration

// Package integration_test wires the auth-less production stack —
// a real *monitoring.MetricClient against a bufconn-backed fake
// MetricServiceServer, the production GCPCollector, and the production
// MetricsHandler — behind an httptest.Server and exercises end-to-end
// request flows. It is intentionally external (package
// integration_test) so it consumes only the public APIs of the other
// packages.
package integration_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	monitoring "cloud.google.com/go/monitoring/apiv3/v2"
	"cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	dto "github.com/prometheus/client_model/go"
	"google.golang.org/api/option"
	distributionpb "google.golang.org/genproto/googleapis/api/distribution"
	metricpb "google.golang.org/genproto/googleapis/api/metric"
	monitoredres "google.golang.org/genproto/googleapis/api/monitoredres"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/timfanda35/gcp-metrics-exporter/internal/collector"
	"github.com/timfanda35/gcp-metrics-exporter/internal/handler"
)

// fixedNow is the deterministic "current" time used across all
// integration cases so the fake server's TimeInterval expectations are
// reproducible.
var fixedNow = time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)

// fakeMetricServer is a minimal monitoringpb.MetricServiceServer used by
// the integration tests.
//
// adapted from internal/collector/collector_test.go
type fakeMetricServer struct {
	monitoringpb.UnimplementedMetricServiceServer

	mu          sync.Mutex
	listFn      func(ctx context.Context, req *monitoringpb.ListTimeSeriesRequest) (*monitoringpb.ListTimeSeriesResponse, error)
	lastRequest *monitoringpb.ListTimeSeriesRequest
}

// ListTimeSeries records the request and dispatches to the per-test
// listFn. A nil listFn yields an empty response.
func (s *fakeMetricServer) ListTimeSeries(ctx context.Context, req *monitoringpb.ListTimeSeriesRequest) (*monitoringpb.ListTimeSeriesResponse, error) {
	s.mu.Lock()
	s.lastRequest = req
	fn := s.listFn
	s.mu.Unlock()
	if fn == nil {
		return &monitoringpb.ListTimeSeriesResponse{}, nil
	}
	return fn(ctx, req)
}

// snapshotLastRequest returns a defensive copy of the last seen request.
func (s *fakeMetricServer) snapshotLastRequest() *monitoringpb.ListTimeSeriesRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastRequest
}

// startFakeServer brings up a bufconn-backed gRPC server hosting the
// supplied fake MetricServiceServer and returns a real
// *monitoring.MetricClient dialled at it. Cleanup is registered via
// t.Cleanup so callers don't have to remember to tear down.
//
// adapted from internal/collector/collector_test.go (option.WithoutAuthentication
// added to mirror the production wiring code path even though the
// in-memory bufconn dial does not exercise credentials).
func startFakeServer(t *testing.T, fake *fakeMetricServer) *monitoring.MetricClient {
	t.Helper()

	lis := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer()
	monitoringpb.RegisterMetricServiceServer(srv, fake)
	go func() {
		_ = srv.Serve(lis)
	}()

	dialer := func(ctx context.Context, _ string) (net.Conn, error) {
		return lis.DialContext(ctx)
	}

	conn, err := grpc.NewClient(
		"passthrough:bufnet",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}

	client, err := monitoring.NewMetricClient(
		context.Background(),
		option.WithGRPCConn(conn),
		option.WithoutAuthentication(),
	)
	if err != nil {
		t.Fatalf("monitoring.NewMetricClient: %v", err)
	}

	t.Cleanup(func() {
		_ = client.Close()
		srv.Stop()
		_ = lis.Close()
	})

	return client
}

// startTestStack returns a fake server, a metric client wired through
// bufconn, and an httptest.Server hosting the production handler stack.
// The factory hands out a single shared collector regardless of
// impersonation target — cache behaviour is out of scope for these
// black-box round-trip tests.
func startTestStack(t *testing.T, fake *fakeMetricServer) (*httptest.Server, *fakeMetricServer) {
	t.Helper()

	client := startFakeServer(t, fake)

	c := collector.NewGCPCollector(
		collector.NewMetricClientFromGCP(client),
		collector.Options{
			MaxSeries: 10000,
			Now:       func() time.Time { return fixedNow },
		},
	)

	factory := func(_ context.Context, _ string) (collector.Collector, error) {
		return c, nil
	}

	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	mh := handler.NewMetricsHandler(factory, handler.Limits{
		ScrapeTimeout: 30 * time.Second,
		MaxConcurrent: 16,
		MaxSeries:     10000,
	}, logger)

	mux := http.NewServeMux()
	mux.Handle("/metrics", mh)
	mux.HandleFunc("/healthz", handler.HandleHealthz)

	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)

	return ts, fake
}

// mkPoint constructs a TimeSeries point with the given interval/value.
//
// adapted from internal/collector/collector_test.go
func mkPoint(start, end time.Time, value *monitoringpb.TypedValue) *monitoringpb.Point {
	return &monitoringpb.Point{
		Interval: &monitoringpb.TimeInterval{
			StartTime: timestamppb.New(start),
			EndTime:   timestamppb.New(end),
		},
		Value: value,
	}
}

// int64Value constructs an INT64 TypedValue.
//
// adapted from internal/collector/collector_test.go
func int64Value(v int64) *monitoringpb.TypedValue {
	return &monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_Int64Value{Int64Value: v}}
}

// distValue constructs a DISTRIBUTION TypedValue.
//
// adapted from internal/collector/collector_test.go
func distValue(d *distributionpb.Distribution) *monitoringpb.TypedValue {
	return &monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_DistributionValue{DistributionValue: d}}
}

// parseProm parses a Prometheus exposition body into the standard
// MetricFamily map. Test failures here indicate a malformed response
// (escaping bug, illegal label name, etc.).
func parseProm(t *testing.T, body []byte) map[string]*dto.MetricFamily {
	t.Helper()
	parser := expfmt.NewTextParser(model.UTF8Validation)
	fams, err := parser.TextToMetricFamilies(strings.NewReader(string(body)))
	if err != nil {
		t.Fatalf("expfmt parse: %v\nbody:\n%s", err, string(body))
	}
	return fams
}

// labelValue returns the value of the named label or "".
func labelValue(m *dto.Metric, name string) string {
	for _, lp := range m.GetLabel() {
		if lp.GetName() == name {
			return lp.GetValue()
		}
	}
	return ""
}

// doGET issues a GET against the test server and returns response status,
// headers, and body. Body is fully read and the response closed.
func doGET(t *testing.T, ts *httptest.Server, path string) (int, http.Header, []byte) {
	t.Helper()
	resp, err := ts.Client().Get(ts.URL + path)
	if err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return resp.StatusCode, resp.Header, body
}

// TestEndToEnd_GaugeFlow exercises the success path with a single GAUGE
// INT64 series. It also asserts the outgoing filter on the GCP request
// to prove the end-to-end filter composition.
func TestEndToEnd_GaugeFlow(t *testing.T) {
	end := fixedNow
	start := fixedNow.Add(-time.Minute)

	fake := &fakeMetricServer{
		listFn: func(_ context.Context, _ *monitoringpb.ListTimeSeriesRequest) (*monitoringpb.ListTimeSeriesResponse, error) {
			return &monitoringpb.ListTimeSeriesResponse{
				TimeSeries: []*monitoringpb.TimeSeries{{
					Metric: &metricpb.Metric{
						Type:   "compute.googleapis.com/instance/cpu/utilization",
						Labels: map[string]string{"instance_name": "vm-a"},
					},
					Resource: &monitoredres.MonitoredResource{
						Type:   "gce_instance",
						Labels: map[string]string{"zone": "us-central1-a", "project_id": "p"},
					},
					MetricKind: metricpb.MetricDescriptor_GAUGE,
					ValueType:  metricpb.MetricDescriptor_INT64,
					Points:     []*monitoringpb.Point{mkPoint(start, end, int64Value(42))},
				}},
			}, nil
		},
	}
	ts, fake := startTestStack(t, fake)

	status, hdr, body := doGET(t, ts, "/metrics?project=p&metric_type=compute.googleapis.com/instance/cpu/utilization")
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", status, body)
	}
	if got := hdr.Get("Content-Type"); got != "text/plain; version=0.0.4; charset=utf-8" {
		t.Errorf("Content-Type = %q, want text/plain; version=0.0.4; charset=utf-8", got)
	}

	fams := parseProm(t, body)
	fam, ok := fams["compute_googleapis_com_instance_cpu_utilization"]
	if !ok {
		t.Fatalf("missing metric family; got names=%v", famNames(fams))
	}
	if got := fam.GetType(); got != dto.MetricType_GAUGE {
		t.Errorf("family type = %v, want GAUGE", got)
	}
	if len(fam.GetMetric()) != 1 {
		t.Fatalf("want 1 metric, got %d", len(fam.GetMetric()))
	}
	m := fam.GetMetric()[0]
	if got := m.GetGauge().GetValue(); got != 42 {
		t.Errorf("gauge value = %v, want 42", got)
	}
	for _, want := range []struct{ name, value string }{
		{"instance_name", "vm-a"},
		{"zone", "us-central1-a"},
		{"project_id", "p"},
		{"resource_type", "gce_instance"},
	} {
		if got := labelValue(m, want.name); got != want.value {
			t.Errorf("label %s = %q, want %q", want.name, got, want.value)
		}
	}

	// Filter composition through the entire stack.
	last := fake.snapshotLastRequest()
	if last == nil {
		t.Fatal("fake server received no request")
	}
	if got, want := last.GetFilter(), `metric.type = "compute.googleapis.com/instance/cpu/utilization"`; got != want {
		t.Errorf("filter = %q, want %q", got, want)
	}
	if got, want := last.GetName(), "projects/p"; got != want {
		t.Errorf("name = %q, want %q", got, want)
	}
}

// TestEndToEnd_DistributionFlow exercises the histogram path with an
// explicit-buckets layout. Asserts cumulative bucket counts are
// non-decreasing and that the +Inf bucket / _count / _sum series exist
// after parse.
func TestEndToEnd_DistributionFlow(t *testing.T) {
	dist := &distributionpb.Distribution{
		Count: 4,
		Mean:  0,
		BucketOptions: &distributionpb.Distribution_BucketOptions{
			Options: &distributionpb.Distribution_BucketOptions_ExplicitBuckets{
				ExplicitBuckets: &distributionpb.Distribution_BucketOptions_Explicit{
					Bounds: []float64{-5, 0, 5},
				},
			},
		},
		BucketCounts: []int64{1, 1, 1, 1},
	}

	fake := &fakeMetricServer{
		listFn: func(_ context.Context, _ *monitoringpb.ListTimeSeriesRequest) (*monitoringpb.ListTimeSeriesResponse, error) {
			return &monitoringpb.ListTimeSeriesResponse{
				TimeSeries: []*monitoringpb.TimeSeries{{
					Metric:     &metricpb.Metric{Type: "svc.example.com/latency"},
					Resource:   &monitoredres.MonitoredResource{Type: "global"},
					MetricKind: metricpb.MetricDescriptor_CUMULATIVE,
					ValueType:  metricpb.MetricDescriptor_DISTRIBUTION,
					Points: []*monitoringpb.Point{
						mkPoint(fixedNow.Add(-time.Minute), fixedNow, distValue(dist)),
					},
				}},
			}, nil
		},
	}
	ts, _ := startTestStack(t, fake)

	status, hdr, body := doGET(t, ts, "/metrics?project=p&metric_type=svc.example.com/latency")
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", status, body)
	}
	if got := hdr.Get("Content-Type"); got != "text/plain; version=0.0.4; charset=utf-8" {
		t.Errorf("Content-Type = %q", got)
	}

	fams := parseProm(t, body)
	if len(fams) != 1 {
		t.Fatalf("want exactly 1 family, got %d (%v)", len(fams), famNames(fams))
	}
	fam, ok := fams["svc_example_com_latency"]
	if !ok {
		t.Fatalf("missing metric family svc_example_com_latency; got %v", famNames(fams))
	}
	if got := fam.GetType(); got != dto.MetricType_HISTOGRAM {
		t.Fatalf("family type = %v, want HISTOGRAM", got)
	}
	if len(fam.GetMetric()) != 1 {
		t.Fatalf("want 1 metric, got %d", len(fam.GetMetric()))
	}
	h := fam.GetMetric()[0].GetHistogram()
	if h == nil {
		t.Fatal("histogram nil")
	}
	if got := h.GetSampleCount(); got != 4 {
		t.Errorf("sample count = %d, want 4", got)
	}
	// Sample sum must be present (mean*count = 0*4 = 0). The expfmt parser
	// always produces a SampleSum field for histograms; assert it parsed.
	if h.SampleSum == nil {
		t.Error("histogram missing _sum")
	}
	// Bucket assertions: explicit bounds [-5, 0, 5] + implicit +Inf.
	buckets := h.GetBucket()
	if len(buckets) != 4 {
		t.Fatalf("want 4 buckets (3 finite + +Inf), got %d", len(buckets))
	}
	if got := buckets[len(buckets)-1].GetUpperBound(); !math.IsInf(got, 1) {
		t.Errorf("last bucket upper = %v, want +Inf", got)
	}
	// Cumulative counts must be non-decreasing.
	var prev uint64
	for i, b := range buckets {
		got := b.GetCumulativeCount()
		if got < prev {
			t.Errorf("bucket[%d] cum=%d decreased from %d", i, got, prev)
		}
		prev = got
	}
	// Final bucket equals total sample count.
	if got, want := buckets[len(buckets)-1].GetCumulativeCount(), uint64(4); got != want {
		t.Errorf("+Inf bucket cum = %d, want %d", got, want)
	}
}

// TestEndToEnd_HealthzFlow asserts the liveness probe contract.
func TestEndToEnd_HealthzFlow(t *testing.T) {
	ts, _ := startTestStack(t, &fakeMetricServer{})

	status, hdr, body := doGET(t, ts, "/healthz")
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", status, body)
	}
	if got := hdr.Get("Content-Type"); !strings.HasPrefix(got, "application/json") {
		t.Errorf("Content-Type = %q, want application/json...", got)
	}
	var parsed map[string]string
	if err := json.Unmarshal(body, &parsed); err != nil {
		t.Fatalf("body is not JSON: %v; body=%s", err, body)
	}
	if got, want := parsed["status"], "ok"; got != want {
		t.Errorf("status field = %q, want %q", got, want)
	}
}

// TestEndToEnd_MissingProject asserts the handler rejects a request that
// is missing the required "project" query parameter, with a JSON error
// body.
func TestEndToEnd_MissingProject(t *testing.T) {
	ts, _ := startTestStack(t, &fakeMetricServer{})

	status, hdr, body := doGET(t, ts, "/metrics?metric_type=foo")
	if status != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body=%s", status, body)
	}
	if got := hdr.Get("Content-Type"); !strings.HasPrefix(got, "application/json") {
		t.Errorf("Content-Type = %q, want application/json...", got)
	}
	var parsed struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		t.Fatalf("body is not JSON: %v; body=%s", err, body)
	}
	if !strings.Contains(parsed.Error, "project") {
		t.Errorf("error = %q, want substring 'project'", parsed.Error)
	}
}

// TestEndToEnd_GCPError exercises the gRPC-code → HTTP-status mapping by
// having the fake server return codes.NotFound. The handler must
// translate this to HTTP 404 with a JSON error body.
func TestEndToEnd_GCPError(t *testing.T) {
	fake := &fakeMetricServer{
		listFn: func(_ context.Context, _ *monitoringpb.ListTimeSeriesRequest) (*monitoringpb.ListTimeSeriesResponse, error) {
			return nil, status.Error(codes.NotFound, "no such project")
		},
	}
	ts, _ := startTestStack(t, fake)

	httpStatus, hdr, body := doGET(t, ts, "/metrics?project=p&metric_type=compute.googleapis.com/instance/cpu/utilization")
	if httpStatus != http.StatusNotFound {
		t.Fatalf("status = %d, want 404; body=%s", httpStatus, body)
	}
	if got := hdr.Get("Content-Type"); !strings.HasPrefix(got, "application/json") {
		t.Errorf("Content-Type = %q, want application/json...", got)
	}
	var parsed struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		t.Fatalf("body is not JSON: %v; body=%s", err, body)
	}
	if parsed.Error == "" {
		t.Error("error field is empty")
	}
}

// famNames returns the keys of fams for use in error messages.
func famNames(fams map[string]*dto.MetricFamily) []string {
	out := make([]string, 0, len(fams))
	for k := range fams {
		out = append(out, k)
	}
	return out
}
