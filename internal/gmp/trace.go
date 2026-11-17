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
	rec := debugtrace.Start(ctx, t.logger, "gmp request", debugtrace.Call{
		Backend:   "gmp",
		Operation: "Query",
		Request: map[string]any{
			"project": project,
			"query":   query,
			"time":    at.UTC().Format(time.RFC3339),
		},
	})
	samples, err := t.inner.Query(ctx, project, query, at)
	rec.Finish(len(samples), err)
	return samples, err
}
