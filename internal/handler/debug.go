package handler

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"net/http"
	"time"

	"github.com/timfanda35/gcp-metrics-exporter/internal/debugtrace"
)

//go:embed debug.html
var debugPage []byte

// debugRunResponse is what /debug/run returns: the real handler's result plus
// the upstream calls made while producing it.
type debugRunResponse struct {
	Status    int               `json:"status"`
	Headers   http.Header       `json:"headers"`
	Body      string            `json:"body"`
	Calls     []debugtrace.Call `json:"calls"`
	ElapsedMs int64             `json:"elapsed_ms"`
}

// RegisterDebug mounts the debug page (GET /debug) and its runner
// (GET /debug/run) on mux. The page has no authentication and shows raw
// upstream errors and request details, so cmd/server registers it only when
// DEBUG_PAGE_ENABLED is set; enable it only on a non-public port.
//
// /debug/run?backend=metrics|gmp-metrics&... runs the matching handler
// in-process with the remaining query parameters, so concurrency limits and
// validation apply exactly as for a real scrape.
func RegisterDebug(mux *http.ServeMux, metrics, gmpMetrics http.Handler) {
	mux.HandleFunc("/debug", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(debugPage)
	})
	mux.Handle("/debug/run", debugRunHandler(metrics, gmpMetrics))
}

// captureWriter is a minimal [http.ResponseWriter] that keeps the status,
// headers and body of the handler it runs, so /debug/run can report exactly
// what a scraper would have received.
type captureWriter struct {
	header http.Header
	status int
	body   bytes.Buffer
}

func (c *captureWriter) Header() http.Header         { return c.header }
func (c *captureWriter) WriteHeader(status int)      { c.status = status }
func (c *captureWriter) Write(b []byte) (int, error) { return c.body.Write(b) }

// traceBuildClientFailure records a failure to build the upstream client
// (credential discovery, impersonation setup) when the request is being
// traced, since no upstream call exists to carry the error.
func traceBuildClientFailure(ctx context.Context, backend, impersonateSA string, err error) {
	if tr := debugtrace.FromContext(ctx); tr != nil {
		tr.Add(debugtrace.Call{
			Backend:   backend,
			Operation: "build_client",
			Request:   map[string]any{"impersonate_sa": impersonateSA},
			Error:     err.Error(),
		})
	}
}

func debugRunHandler(metrics, gmpMetrics http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}

		q := r.URL.Query()
		var target http.Handler
		var path string
		switch q.Get("backend") {
		case "metrics":
			target, path = metrics, "/metrics"
		case "gmp-metrics":
			target, path = gmpMetrics, "/gmp-metrics"
		default:
			writeJSONError(w, http.StatusBadRequest, `backend must be "metrics" or "gmp-metrics"`)
			return
		}
		q.Del("backend")

		ctx, trace := debugtrace.WithTrace(r.Context())
		inner := r.Clone(ctx)
		inner.URL.Path = path
		inner.URL.RawQuery = q.Encode()

		start := time.Now()
		rec := &captureWriter{header: http.Header{}, status: http.StatusOK}
		target.ServeHTTP(rec, inner)

		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		_ = json.NewEncoder(w).Encode(debugRunResponse{
			Status:    rec.status,
			Headers:   rec.header,
			Body:      rec.body.String(),
			Calls:     trace.Calls(),
			ElapsedMs: time.Since(start).Milliseconds(),
		})
	})
}
