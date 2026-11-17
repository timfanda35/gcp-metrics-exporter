package gmp

import (
	"context"
	"log/slog"
	"time"

	"github.com/timfanda35/gcp-metrics-exporter/internal/debugtrace"
)

// tracingClient decorates a [Client] to record each query into the request's
// [debugtrace.Trace] and, at debug level, to the log. When neither is active
// it forwards calls untouched.
type tracingClient struct {
	inner  Client
	logger *slog.Logger
}

// NewTracingClient wraps inner so its queries are visible on the debug page
// (when the context carries a [debugtrace.Trace]) and in debug logs (when
// logger is enabled for debug). A nil logger uses [slog.Default].
func NewTracingClient(inner Client, logger *slog.Logger) Client {
	if logger == nil {
		logger = slog.Default()
	}
	return &tracingClient{inner: inner, logger: logger}
}

// Query runs the inner query and records its parameters, result count,
// latency and raw upstream error.
func (t *tracingClient) Query(ctx context.Context, project, query string, at time.Time) ([]Sample, error) {
	tr := debugtrace.FromContext(ctx)
	debug := t.logger.Enabled(ctx, slog.LevelDebug)
	if tr == nil && !debug {
		return t.inner.Query(ctx, project, query, at)
	}

	call := debugtrace.Call{
		Backend:   "gmp",
		Operation: "Query",
		Request: map[string]any{
			"project": project,
			"query":   query,
			"time":    at.UTC().Format(time.RFC3339),
		},
	}
	idx := 0
	if tr != nil {
		idx = tr.Add(call)
	}

	start := time.Now()
	samples, err := t.inner.Query(ctx, project, query, at)
	call.ResultCount = len(samples)
	call.LatencyMs = time.Since(start).Milliseconds()
	if err != nil {
		call.Error = err.Error()
	}

	if tr != nil {
		tr.Update(idx, func(c *debugtrace.Call) {
			c.ResultCount, c.LatencyMs, c.Error = call.ResultCount, call.LatencyMs, call.Error
		})
	}
	if debug {
		attrs := []any{
			slog.String("project", project),
			slog.String("query", query),
			slog.String("time", call.Request["time"].(string)),
			slog.Int("result_count", call.ResultCount),
			slog.Int64("latency_ms", call.LatencyMs),
		}
		if call.Error != "" {
			attrs = append(attrs, slog.String("error", call.Error))
		}
		t.logger.DebugContext(ctx, "gmp request", attrs...)
	}
	return samples, err
}
