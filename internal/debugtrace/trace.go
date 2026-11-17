// Package debugtrace records the upstream GCP calls made while serving one
// request, so the debug page can show them. A [Trace] lives in the request
// context; code that makes upstream calls records into it only when one is
// present, so normal requests pay nothing.
package debugtrace

import (
	"context"
	"sync"
)

// Call describes one upstream call (a Cloud Monitoring RPC or a GMP HTTP
// query).
type Call struct {
	// Backend is "cloud-monitoring" or "gmp".
	Backend string `json:"backend"`
	// Operation is the upstream operation, for example "ListTimeSeries".
	Operation string `json:"operation"`
	// Request holds the call's parameters, for example project and filter.
	Request map[string]any `json:"request,omitempty"`
	// ResultCount is the number of series (or samples) returned.
	ResultCount int `json:"result_count"`
	// LatencyMs is the call duration in milliseconds.
	LatencyMs int64 `json:"latency_ms"`
	// Error is the raw upstream error text, empty on success. It is not
	// redacted: it is what the HTTP response deliberately hides.
	Error string `json:"error,omitempty"`
}

// Trace collects the [Call]s of one request. It is safe for concurrent use.
type Trace struct {
	mu    sync.Mutex
	calls []Call
}

type ctxKey struct{}

// WithTrace returns a context carrying a new, empty [Trace] and the trace.
func WithTrace(ctx context.Context) (context.Context, *Trace) {
	t := &Trace{}
	return context.WithValue(ctx, ctxKey{}, t), t
}

// FromContext returns the [Trace] in ctx, or nil when there is none.
func FromContext(ctx context.Context) *Trace {
	t, _ := ctx.Value(ctxKey{}).(*Trace)
	return t
}

// Add appends c and returns its index for a later [Trace.Update].
func (t *Trace) Add(c Call) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.calls = append(t.calls, c)
	return len(t.calls) - 1
}

// Update applies fn to the call at index i. An out-of-range index is ignored.
func (t *Trace) Update(i int, fn func(*Call)) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if i < 0 || i >= len(t.calls) {
		return
	}
	fn(&t.calls[i])
}

// Calls returns a copy of the recorded calls; never nil.
func (t *Trace) Calls() []Call {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]Call, len(t.calls))
	copy(out, t.calls)
	return out
}
