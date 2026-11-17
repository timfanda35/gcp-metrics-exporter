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

// wanted reports whether calls under ctx need recording.
func (t *tracingClient) wanted(ctx context.Context) (*debugtrace.Trace, bool) {
	tr := debugtrace.FromContext(ctx)
	return tr, tr != nil || t.logger.Enabled(ctx, slog.LevelDebug)
}

// ListTimeSeries records the request and, once the iterator finishes, the
// number of series, latency and any raw upstream error.
func (t *tracingClient) ListTimeSeries(ctx context.Context, req *monitoringpb.ListTimeSeriesRequest) TimeSeriesIterator {
	tr, ok := t.wanted(ctx)
	if !ok {
		return t.inner.ListTimeSeries(ctx, req)
	}
	rec := &callRecorder{
		ctx: ctx, logger: t.logger, trace: tr, start: time.Now(),
		call: debugtrace.Call{Backend: backendName, Operation: "ListTimeSeries", Request: listRequestFields(req)},
	}
	rec.begin()
	return &tracingIterator{inner: t.inner.ListTimeSeries(ctx, req), rec: rec}
}

// GetMetricDescriptor records the lookup with its latency and raw error.
func (t *tracingClient) GetMetricDescriptor(ctx context.Context, req *monitoringpb.GetMetricDescriptorRequest) (*metricpb.MetricDescriptor, error) {
	tr, ok := t.wanted(ctx)
	if !ok {
		return t.inner.GetMetricDescriptor(ctx, req)
	}
	rec := &callRecorder{
		ctx: ctx, logger: t.logger, trace: tr, start: time.Now(),
		call: debugtrace.Call{Backend: backendName, Operation: "GetMetricDescriptor", Request: map[string]any{"name": req.GetName()}},
	}
	rec.begin()
	md, err := t.inner.GetMetricDescriptor(ctx, req)
	rec.finish(0, err)
	return md, err
}

// tracingIterator counts series and finishes the recording on Done or error.
type tracingIterator struct {
	inner TimeSeriesIterator
	rec   *callRecorder
	n     int
}

func (it *tracingIterator) Next() (*monitoringpb.TimeSeries, error) {
	ts, err := it.inner.Next()
	switch {
	case err == nil:
		it.n++
	case errors.Is(err, iterator.Done):
		it.rec.finish(it.n, nil)
	default:
		it.rec.finish(it.n, err)
	}
	return ts, err
}

// callRecorder owns one [debugtrace.Call]: it adds it to the trace up front
// and fills in the outcome exactly once.
type callRecorder struct {
	ctx    context.Context
	logger *slog.Logger
	trace  *debugtrace.Trace
	start  time.Time
	call   debugtrace.Call
	idx    int
	done   bool
}

func (r *callRecorder) begin() {
	if r.trace != nil {
		r.idx = r.trace.Add(r.call)
	}
}

func (r *callRecorder) finish(n int, err error) {
	if r.done {
		return
	}
	r.done = true
	r.call.ResultCount = n
	r.call.LatencyMs = time.Since(r.start).Milliseconds()
	if err != nil {
		r.call.Error = err.Error()
	}
	if r.trace != nil {
		r.trace.Update(r.idx, func(c *debugtrace.Call) {
			c.ResultCount, c.LatencyMs, c.Error = r.call.ResultCount, r.call.LatencyMs, r.call.Error
		})
	}
	if r.logger.Enabled(r.ctx, slog.LevelDebug) {
		attrs := []any{
			slog.String("operation", r.call.Operation),
			slog.Int("result_count", n),
			slog.Int64("latency_ms", r.call.LatencyMs),
		}
		for _, k := range []string{"name", "filter", "start_time", "end_time", "aligner", "alignment_period", "reducer", "group_by_fields"} {
			if v, ok := r.call.Request[k]; ok {
				attrs = append(attrs, slog.Any(k, v))
			}
		}
		if r.call.Error != "" {
			attrs = append(attrs, slog.String("error", r.call.Error))
		}
		r.logger.DebugContext(r.ctx, "monitoring request", attrs...)
	}
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
