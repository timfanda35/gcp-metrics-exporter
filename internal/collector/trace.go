package collector

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"time"

	"cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	"google.golang.org/api/iterator"
	metricpb "google.golang.org/genproto/googleapis/api/metric"

	"github.com/timfanda35/gcp-metrics-exporter/internal/debugtrace"
)

const backendName = "cloud-monitoring"

// tracingClient decorates a [MetricClient] to record each upstream call into
// the request's [debugtrace.Trace] and, at debug level, to the log. When
// neither is active it forwards calls untouched.
type tracingClient struct {
	inner  MetricClient
	logger *slog.Logger
}

// NewTracingClient wraps inner so its calls are visible on the debug page
// (when the context carries a [debugtrace.Trace]) and in debug logs (when
// logger is enabled for debug). A nil logger uses [slog.Default].
func NewTracingClient(inner MetricClient, logger *slog.Logger) MetricClient {
	if logger == nil {
		logger = slog.Default()
	}
	return &tracingClient{inner: inner, logger: logger}
}

// ListTimeSeries records the request and, once the iterator finishes, the
// number of series, latency and any raw upstream error. A call whose iterator
// the consumer abandons stays marked incomplete in the trace.
func (t *tracingClient) ListTimeSeries(ctx context.Context, req *monitoringpb.ListTimeSeriesRequest) TimeSeriesIterator {
	rec := debugtrace.Start(ctx, t.logger, "monitoring request",
		debugtrace.Call{Backend: backendName, Operation: "ListTimeSeries", Request: listRequestFields(req)})
	it := t.inner.ListTimeSeries(ctx, req)
	if rec == nil {
		return it
	}
	return &tracingIterator{inner: it, rec: rec}
}

// GetMetricDescriptor records the lookup with its latency and raw error.
func (t *tracingClient) GetMetricDescriptor(ctx context.Context, req *monitoringpb.GetMetricDescriptorRequest) (*metricpb.MetricDescriptor, error) {
	rec := debugtrace.Start(ctx, t.logger, "monitoring request",
		debugtrace.Call{Backend: backendName, Operation: "GetMetricDescriptor", Request: map[string]any{"name": req.GetName()}})
	md, err := t.inner.GetMetricDescriptor(ctx, req)
	rec.Finish(0, err)
	return md, err
}

// tracingIterator counts series and finishes the recording on Done or error.
type tracingIterator struct {
	inner TimeSeriesIterator
	rec   *debugtrace.Recorder
	n     int
}

// Next returns the inner iterator's next series, finishing the recording when
// the stream ends or fails.
func (it *tracingIterator) Next() (*monitoringpb.TimeSeries, error) {
	ts, err := it.inner.Next()
	switch {
	case err == nil:
		it.n++
	case errors.Is(err, iterator.Done):
		it.rec.Finish(it.n, nil)
	default:
		it.rec.Finish(it.n, err)
	}
	return ts, err
}

// listRequestFields extracts the request parameters worth showing.
func listRequestFields(req *monitoringpb.ListTimeSeriesRequest) map[string]any {
	f := map[string]any{
		"name":   req.GetName(),
		"filter": req.GetFilter(),
	}
	if iv := req.GetInterval(); iv != nil {
		f["start_time"] = iv.GetStartTime().AsTime().UTC().Format(time.RFC3339)
		f["end_time"] = iv.GetEndTime().AsTime().UTC().Format(time.RFC3339)
	}
	if agg := req.GetAggregation(); agg != nil {
		f["aligner"] = agg.GetPerSeriesAligner().String()
		f["alignment_period"] = agg.GetAlignmentPeriod().AsDuration().String()
		f["reducer"] = agg.GetCrossSeriesReducer().String()
		if len(agg.GetGroupByFields()) > 0 {
			f["group_by_fields"] = strings.Join(agg.GetGroupByFields(), ",")
		}
	}
	return f
}
