package collector

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	"google.golang.org/api/iterator"
	metricpb "google.golang.org/genproto/googleapis/api/metric"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/timfanda35/gcp-metrics-exporter/internal/debugtrace"
)

// sliceIterator yields series then err (iterator.Done when err is nil).
type sliceIterator struct {
	series []*monitoringpb.TimeSeries
	err    error
}

func (s *sliceIterator) Next() (*monitoringpb.TimeSeries, error) {
	if len(s.series) > 0 {
		ts := s.series[0]
		s.series = s.series[1:]
		return ts, nil
	}
	if s.err != nil {
		return nil, s.err
	}
	return nil, iterator.Done
}

type stubMetricClient struct {
	it      TimeSeriesIterator
	descErr error
}

func (s *stubMetricClient) ListTimeSeries(context.Context, *monitoringpb.ListTimeSeriesRequest) TimeSeriesIterator {
	return s.it
}

func (s *stubMetricClient) GetMetricDescriptor(_ context.Context, req *monitoringpb.GetMetricDescriptorRequest) (*metricpb.MetricDescriptor, error) {
	if s.descErr != nil {
		return nil, s.descErr
	}
	return &metricpb.MetricDescriptor{Name: req.GetName(), Unit: "By"}, nil
}

func traceTestRequest() *monitoringpb.ListTimeSeriesRequest {
	return &monitoringpb.ListTimeSeriesRequest{
		Name:   "projects/p",
		Filter: `metric.type = "x"`,
		Interval: &monitoringpb.TimeInterval{
			StartTime: timestamppb.New(time.Date(2026, 5, 10, 11, 50, 0, 0, time.UTC)),
			EndTime:   timestamppb.New(time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)),
		},
		Aggregation: &monitoringpb.Aggregation{
			PerSeriesAligner:   monitoringpb.Aggregation_ALIGN_RATE,
			AlignmentPeriod:    durationpb.New(time.Minute),
			CrossSeriesReducer: monitoringpb.Aggregation_REDUCE_NONE,
		},
	}
}

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)) }

func drain(t *testing.T, it TimeSeriesIterator) (n int, err error) {
	t.Helper()
	for {
		_, e := it.Next()
		if errors.Is(e, iterator.Done) {
			return n, nil
		}
		if e != nil {
			return n, e
		}
		n++
	}
}

func TestTracingClient_RecordsListTimeSeries(t *testing.T) {
	inner := &stubMetricClient{it: &sliceIterator{series: []*monitoringpb.TimeSeries{{}, {}}}}
	c := NewTracingClient(inner, discardLogger())
	ctx, tr := debugtrace.WithTrace(context.Background())

	n, err := drain(t, c.ListTimeSeries(ctx, traceTestRequest()))
	if err != nil || n != 2 {
		t.Fatalf("drain: n=%d err=%v", n, err)
	}

	calls := tr.Calls()
	if len(calls) != 1 {
		t.Fatalf("len(calls) = %d, want 1", len(calls))
	}
	got := calls[0]
	if got.Backend != "cloud-monitoring" || got.Operation != "ListTimeSeries" || got.ResultCount != 2 || got.Error != "" {
		t.Errorf("call = %+v", got)
	}
	for k, want := range map[string]any{
		"name":             "projects/p",
		"filter":           `metric.type = "x"`,
		"aligner":          "ALIGN_RATE",
		"alignment_period": "1m0s",
		"reducer":          "REDUCE_NONE",
		"start_time":       "2026-05-10T11:50:00Z",
		"end_time":         "2026-05-10T12:00:00Z",
	} {
		if got.Request[k] != want {
			t.Errorf("Request[%q] = %v, want %v", k, got.Request[k], want)
		}
	}
}

func TestTracingClient_RecordsRawIteratorError(t *testing.T) {
	boom := errors.New("rpc error: code = Unauthenticated desc = invalid_scope")
	inner := &stubMetricClient{it: &sliceIterator{err: boom}}
	c := NewTracingClient(inner, discardLogger())
	ctx, tr := debugtrace.WithTrace(context.Background())

	if _, err := drain(t, c.ListTimeSeries(ctx, traceTestRequest())); !errors.Is(err, boom) {
		t.Fatalf("err = %v, want %v", err, boom)
	}
	if got := tr.Calls()[0].Error; !strings.Contains(got, "invalid_scope") {
		t.Errorf("trace Error = %q, want the raw upstream text", got)
	}
}

func TestTracingClient_PassThroughWhenNotWanted(t *testing.T) {
	it := &sliceIterator{}
	c := NewTracingClient(&stubMetricClient{it: it}, discardLogger()) // info level, no trace
	if got := c.ListTimeSeries(context.Background(), traceTestRequest()); got != TimeSeriesIterator(it) {
		t.Error("iterator should be returned unwrapped when neither a trace nor debug logging is active")
	}
}

func TestTracingClient_DebugLog(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	inner := &stubMetricClient{it: &sliceIterator{series: []*monitoringpb.TimeSeries{{}}}}
	c := NewTracingClient(inner, logger)

	if _, err := drain(t, c.ListTimeSeries(context.Background(), traceTestRequest())); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	for _, want := range []string{"monitoring request", "ListTimeSeries", "result_count=1", "aligner=ALIGN_RATE"} {
		if !strings.Contains(out, want) {
			t.Errorf("debug log %q missing %q", out, want)
		}
	}
}

func TestTracingClient_RecordsGetMetricDescriptor(t *testing.T) {
	c := NewTracingClient(&stubMetricClient{descErr: errors.New("permission denied")}, discardLogger())
	ctx, tr := debugtrace.WithTrace(context.Background())

	_, err := c.GetMetricDescriptor(ctx, &monitoringpb.GetMetricDescriptorRequest{Name: "projects/p/metricDescriptors/x"})
	if err == nil {
		t.Fatal("expected the inner error to be returned")
	}
	calls := tr.Calls()
	if len(calls) != 1 || calls[0].Operation != "GetMetricDescriptor" ||
		calls[0].Request["name"] != "projects/p/metricDescriptors/x" || !strings.Contains(calls[0].Error, "permission denied") {
		t.Errorf("calls = %+v", calls)
	}
}

// A consumer that stops reading (for example on ErrMaxSeriesExceeded) leaves
// the call unfinished; the trace must say so instead of looking successful.
func TestTracingClient_AbandonedIteratorIsIncomplete(t *testing.T) {
	inner := &stubMetricClient{it: &sliceIterator{series: []*monitoringpb.TimeSeries{{}, {}, {}}}}
	c := NewTracingClient(inner, discardLogger())
	ctx, tr := debugtrace.WithTrace(context.Background())

	it := c.ListTimeSeries(ctx, traceTestRequest())
	if _, err := it.Next(); err != nil {
		t.Fatal(err)
	}
	if !tr.Calls()[0].Incomplete {
		t.Error("call should be incomplete while the iterator has not finished")
	}
	if _, err := drain(t, it); err != nil {
		t.Fatal(err)
	}
	if got := tr.Calls()[0]; got.Incomplete || got.ResultCount != 3 {
		t.Errorf("after draining: %+v", got)
	}
}
