// Package debugtrace records the upstream GCP calls made while serving one
// request, so the debug page can show them. A [Trace] lives in the request
// context; code that makes upstream calls records into it only when one is
// present, so normal requests pay nothing.
package debugtrace

import (
	"context"
	"log/slog"
	"sort"
	"sync"
	"time"
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
	// Incomplete is true while the call has not finished. A call that stays
	// incomplete was abandoned by its consumer (for example when the series
	// limit was exceeded or the request was cancelled), so ResultCount and
	// LatencyMs are not final.
	Incomplete bool `json:"incomplete,omitempty"`
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

// Recorder tracks one upstream call: it adds it to the request's [Trace] (if
// any) as incomplete, and fills in the outcome on [Recorder.Finish], also
// writing a debug log line when the logger is enabled for debug. Both the
// Cloud Monitoring and the GMP tracing clients use it.
type Recorder struct {
	ctx    context.Context
	logger *slog.Logger
	msg    string
	trace  *Trace
	idx    int
	start  time.Time
	call   Call
	done   bool
}

// Start begins recording call. It returns nil, and records nothing, when ctx
// has no [Trace] and logger is not enabled for debug, so callers can skip all
// tracing work. The nil Recorder is safe to use. msg is the debug log message.
func Start(ctx context.Context, logger *slog.Logger, msg string, call Call) *Recorder {
	tr := FromContext(ctx)
	if tr == nil && !logger.Enabled(ctx, slog.LevelDebug) {
		return nil
	}
	r := &Recorder{ctx: ctx, logger: logger, msg: msg, trace: tr, start: time.Now(), call: call}
	if tr != nil {
		c := call
		c.Incomplete = true
		r.idx = tr.Add(c)
	}
	return r
}

// Finish records the outcome: the number of results, the elapsed time and err
// (nil on success). Only the first call has an effect.
func (r *Recorder) Finish(resultCount int, err error) {
	if r == nil || r.done {
		return
	}
	r.done = true
	r.call.ResultCount = resultCount
	r.call.LatencyMs = time.Since(r.start).Milliseconds()
	if err != nil {
		r.call.Error = err.Error()
	}
	if r.trace != nil {
		r.trace.Update(r.idx, func(c *Call) {
			c.ResultCount, c.LatencyMs, c.Error, c.Incomplete = r.call.ResultCount, r.call.LatencyMs, r.call.Error, false
		})
	}
	if r.logger.Enabled(r.ctx, slog.LevelDebug) {
		attrs := []any{
			slog.String("operation", r.call.Operation),
			slog.Int("result_count", r.call.ResultCount),
			slog.Int64("latency_ms", r.call.LatencyMs),
		}
		keys := make([]string, 0, len(r.call.Request))
		for k := range r.call.Request {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			attrs = append(attrs, slog.Any(k, r.call.Request[k]))
		}
		if r.call.Error != "" {
			attrs = append(attrs, slog.String("error", r.call.Error))
		}
		r.logger.DebugContext(r.ctx, r.msg, attrs...)
	}
}
