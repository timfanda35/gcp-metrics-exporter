// Package handler provides HTTP handlers that expose GCP Cloud Monitoring
// time series in the Prometheus text exposition format.
//
// The handler layer is the only place where transport concerns live —
// per-request timeouts, concurrency limiting, query-parameter parsing,
// error → HTTP status mapping, and structured logging. The collector is
// kept stateless and is reached through the [CollectorFactory] seam so
// tests can inject fakes.
package handler

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/prometheus/common/expfmt"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/timfanda35/gcp-metrics-exporter/internal/collector"
)

// Default values applied by [NewMetricsHandler] when [Limits] fields are
// zero. Exposed as constants so tests and the cmd/server wiring can refer
// to them without duplication.
const (
	DefaultScrapeTimeout = 30 * time.Second
	DefaultMaxConcurrent = 16
	DefaultMaxSeries     = 10000
)

// nonStandardClientClosed is the unofficial HTTP status used by some
// servers (originally nginx) to mark a request whose client gave up
// before the server responded. We log with this code for observability;
// the client is gone so the wire status is irrelevant.
const nonStandardClientClosed = 499

// Limits configures request-time policy on a [MetricsHandler].
//
// Zero-value fields are filled in with their respective defaults by
// [NewMetricsHandler]; callers may set only the values they want to
// override.
type Limits struct {
	// ScrapeTimeout caps the total handler time per /metrics request,
	// including the GCP RPC. Zero defaults to [DefaultScrapeTimeout].
	ScrapeTimeout time.Duration

	// MaxConcurrent caps in-flight /metrics requests. New requests above
	// the cap receive HTTP 429. Zero defaults to [DefaultMaxConcurrent].
	MaxConcurrent int

	// MaxSeries is forwarded into [collector.Options] when constructing a
	// collector via the default [CollectorFactory]. Zero defaults to
	// [DefaultMaxSeries].
	MaxSeries int

	// DefaultImpersonateSA is used as the impersonation target when the
	// per-request impersonate_sa query parameter is empty. Typically
	// populated from the DEFAULT_IMPERSONATE_SA environment variable in
	// cmd/server.
	DefaultImpersonateSA string
}

// CollectorForSA produces a [collector.Collector] for the given
// impersonation target ("" means "no impersonation, use base credentials").
// It is the seam that decouples the handler from [collector.ClientCache];
// cmd/server wires the production implementation that reaches into the
// cache, while tests inject lightweight fakes.
type CollectorForSA func(ctx context.Context, impersonateSA string) (collector.Collector, error)

// MetricsHandler implements the GET /metrics endpoint described in the
// project README. Construct it with [NewMetricsHandler] and either let
// the default [CollectorForSA] be wired by cmd/server or override it
// with [MetricsHandler.WithCollectorForSA] in tests.
type MetricsHandler struct {
	collectorForSA CollectorForSA
	limits         Limits
	sem            chan struct{}
	logger         *slog.Logger
	now            func() time.Time
}

// NewMetricsHandler builds a [MetricsHandler] with the supplied collector
// factory, limits, and logger. Zero-valued [Limits] fields are replaced
// with the package defaults. The logger must be non-nil; pass
// [slog.Default] if you have nothing better.
//
// The collectorForSA seam is required — pass a function that, given an
// impersonation target, returns a [collector.Collector] suitable for
// querying GCP. cmd/server typically constructs this around a
// [collector.ClientCache] and [collector.NewMetricClientFromGCP].
func NewMetricsHandler(collectorForSA CollectorForSA, limits Limits, logger *slog.Logger) *MetricsHandler {
	if limits.ScrapeTimeout <= 0 {
		limits.ScrapeTimeout = DefaultScrapeTimeout
	}
	if limits.MaxConcurrent <= 0 {
		limits.MaxConcurrent = DefaultMaxConcurrent
	}
	if limits.MaxSeries <= 0 {
		limits.MaxSeries = DefaultMaxSeries
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &MetricsHandler{
		collectorForSA: collectorForSA,
		limits:         limits,
		sem:            make(chan struct{}, limits.MaxConcurrent),
		logger:         logger,
		now:            time.Now,
	}
}

// WithCollectorForSA replaces the collector factory and returns the
// handler for chaining. Intended for tests that need to swap the seam
// after construction.
func (h *MetricsHandler) WithCollectorForSA(f CollectorForSA) *MetricsHandler {
	h.collectorForSA = f
	return h
}

// WithClock replaces the time source used for latency measurements.
// Intended for tests that need deterministic timing.
func (h *MetricsHandler) WithClock(now func() time.Time) *MetricsHandler {
	h.now = now
	return h
}

// Limits returns the resolved [Limits] in use, including any defaults
// applied by [NewMetricsHandler]. Useful for sanity-checking the wiring
// at startup time.
func (h *MetricsHandler) Limits() Limits {
	return h.limits
}

// requestParams is the parsed shape of an inbound /metrics request.
type requestParams struct {
	project         string
	metricType      string
	filter          string
	aligner         string
	reducer         string
	groupBy         []string
	interval        time.Duration
	alignmentPeriod time.Duration
	impersonateSA   string
}

// parseRequest extracts and validates query parameters per the API
// contract documented in CLAUDE.md. The first return value is non-nil on
// success; on failure the second return value carries a user-facing
// error message and the third return value is the HTTP status to write.
func parseRequest(r *http.Request, limits Limits) (*requestParams, string, int) {
	q := r.URL.Query()

	project := q.Get("project")
	if project == "" {
		return nil, "missing required parameter: project", http.StatusBadRequest
	}
	metricType := q.Get("metric_type")
	if metricType == "" {
		return nil, "missing required parameter: metric_type", http.StatusBadRequest
	}

	aligner := q.Get("aligner")
	if aligner == "" {
		aligner = "ALIGN_MEAN"
	}
	reducer := q.Get("reducer")
	if reducer == "" {
		reducer = "REDUCE_NONE"
	}

	// group_by is only honoured when reducing; an empty/whitespace-only
	// value yields a nil slice, never []string{""}.
	var groupBy []string
	if reducer != "REDUCE_NONE" {
		raw := q.Get("group_by")
		if raw != "" {
			parts := strings.Split(raw, ",")
			groupBy = make([]string, 0, len(parts))
			for _, p := range parts {
				p = strings.TrimSpace(p)
				if p != "" {
					groupBy = append(groupBy, p)
				}
			}
			if len(groupBy) == 0 {
				groupBy = nil
			}
		}
	}

	intervalStr := q.Get("interval")
	interval := 5 * time.Minute
	if intervalStr != "" {
		d, err := time.ParseDuration(intervalStr)
		if err != nil || d <= 0 {
			return nil, fmt.Sprintf("invalid interval %q: must be a positive Go duration", intervalStr), http.StatusBadRequest
		}
		interval = d
	}

	alignmentStr := q.Get("alignment_period")
	alignment := interval
	if alignmentStr != "" {
		d, err := time.ParseDuration(alignmentStr)
		if err != nil || d <= 0 {
			return nil, fmt.Sprintf("invalid alignment_period %q: must be a positive Go duration", alignmentStr), http.StatusBadRequest
		}
		alignment = d
	}

	impersonate := q.Get("impersonate_sa")
	if impersonate == "" {
		impersonate = limits.DefaultImpersonateSA
	}

	return &requestParams{
		project:         project,
		metricType:      metricType,
		filter:          q.Get("filter"),
		aligner:         aligner,
		reducer:         reducer,
		groupBy:         groupBy,
		interval:        interval,
		alignmentPeriod: alignment,
		impersonateSA:   impersonate,
	}, "", 0
}

// ServeHTTP implements [http.Handler]. It parses the incoming request,
// applies the concurrency limit, runs the GCP query, and streams the
// Prometheus text exposition. Errors are mapped to HTTP via [mapError].
func (h *MetricsHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	startTotal := h.now()

	// Concurrency gate: non-blocking acquire. A 429 response carries
	// Retry-After: 1 so well-behaved clients back off briefly.
	select {
	case h.sem <- struct{}{}:
		defer func() { <-h.sem }()
	default:
		w.Header().Set("Retry-After", "1")
		writeJSONError(w, http.StatusTooManyRequests, "too many concurrent scrapes")
		h.logger.Info("metrics request rejected",
			slog.String("reason", "concurrency_limit"),
			slog.Int("status", http.StatusTooManyRequests),
			slog.Int("max_concurrent", h.limits.MaxConcurrent),
		)
		return
	}

	params, errMsg, errStatus := parseRequest(r, h.limits)
	if params == nil {
		writeJSONError(w, errStatus, errMsg)
		h.logger.Info("metrics request rejected",
			slog.String("reason", "bad_request"),
			slog.Int("status", errStatus),
			slog.String("error_msg", errMsg),
		)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), h.limits.ScrapeTimeout)
	defer cancel()

	logAttrs := []any{
		slog.String("project", params.project),
		slog.String("metric_type", params.metricType),
		slog.String("impersonate_sa", params.impersonateSA),
		slog.String("filter_fingerprint", filterFingerprint(params.filter)),
		slog.Int("filter_len", len(params.filter)),
	}

	c, err := h.collectorForSA(ctx, params.impersonateSA)
	if err != nil {
		err = fmt.Errorf("handler: build collector: %w", err)
		h.respondError(w, err, append(logAttrs, slog.Int64("total_latency_ms", elapsedMs(h.now(), startTotal)))...)
		return
	}

	gcpStart := h.now()
	families, err := c.Collect(ctx, collector.QueryParams{
		ProjectID:       params.project,
		MetricType:      params.metricType,
		Filter:          params.filter,
		Aligner:         params.aligner,
		Reducer:         params.reducer,
		GroupByFields:   params.groupBy,
		Interval:        params.interval,
		AlignmentPeriod: params.alignmentPeriod,
	})
	gcpLatency := elapsedMs(h.now(), gcpStart)

	if err != nil {
		h.respondError(w, err,
			append(logAttrs,
				slog.Int64("gcp_latency_ms", gcpLatency),
				slog.Int64("total_latency_ms", elapsedMs(h.now(), startTotal)),
			)...,
		)
		return
	}

	// Success: stream the families. Once WriteHeader is called the HTTP
	// status is fixed; mid-stream encoding errors are logged but the
	// client just sees a truncated body.
	w.Header().Set("Content-Type", string(expfmt.NewFormat(expfmt.TypeTextPlain)))
	w.WriteHeader(http.StatusOK)
	enc := expfmt.NewEncoder(w, expfmt.NewFormat(expfmt.TypeTextPlain))
	seriesCount := 0
	for _, mf := range families {
		seriesCount += len(mf.GetMetric())
		if encErr := enc.Encode(mf); encErr != nil {
			h.logger.Error("metrics encode failed mid-stream",
				slog.String("project", params.project),
				slog.String("metric_type", params.metricType),
				slog.String("err", encErr.Error()),
			)
			break
		}
	}

	h.logger.Info("metrics request",
		append(logAttrs,
			slog.Int("status", http.StatusOK),
			slog.Int("series_count", seriesCount),
			slog.Int64("gcp_latency_ms", gcpLatency),
			slog.Int64("total_latency_ms", elapsedMs(h.now(), startTotal)),
		)...,
	)
}

// respondError writes the JSON error body for err and logs the request
// outcome. logAttrs already contains the per-request fields we want on
// every line; respondError appends status, error_code, and series_count.
func (h *MetricsHandler) respondError(w http.ResponseWriter, err error, logAttrs ...any) {
	httpStatus, msg, headers, errCode, level := mapError(err)
	for k, v := range headers {
		w.Header().Set(k, v)
	}
	// 499 is non-standard and is for logging only. Don't actually try to
	// write it to the wire (the client is gone) — just emit a 0 by
	// hijacking the body write to a no-op. net/http will refuse codes
	// outside 100–999 anyway, so we coerce.
	wireStatus := httpStatus
	if wireStatus == nonStandardClientClosed {
		// Best effort: still try to write something, but the connection
		// is likely broken.
		wireStatus = http.StatusRequestTimeout
	}
	writeJSONError(w, wireStatus, msg)

	logAttrs = append(logAttrs,
		slog.Int("status", httpStatus),
		slog.Int("series_count", 0),
		slog.String("error_code", errCode),
		slog.String("error_msg", msg),
	)
	switch level {
	case slog.LevelError:
		h.logger.Error("metrics request failed", logAttrs...)
	default:
		h.logger.Info("metrics request failed", logAttrs...)
	}
}

// writeJSONError writes a {"error":"..."} body with the given status. It
// is safe to call multiple times only at most once — net/http enforces
// the WriteHeader-once contract.
func writeJSONError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	body, _ := json.Marshal(struct {
		Error string `json:"error"`
	}{Error: msg})
	_, _ = w.Write(body)
}

// mapError translates a collector / GCP error into:
//
//   - the HTTP status to surface,
//   - a user-facing message (intentionally terse to avoid leaking
//     principal/project hints beyond what the caller already supplied),
//   - any extra response headers (e.g. Retry-After on quota errors),
//   - a short error_code suitable for log aggregation,
//   - the slog level to use for the failure log line.
//
// The match order matches the table documented in CLAUDE.md.
func mapError(err error) (status int, msg string, headers map[string]string, errCode string, level slog.Level) {
	switch {
	case errors.Is(err, collector.ErrMaxSeriesExceeded):
		return http.StatusServiceUnavailable,
			"max series exceeded; narrow the filter or set a reducer",
			nil,
			"max_series_exceeded",
			slog.LevelError

	case errors.Is(err, collector.ErrInvalidAggregation):
		return http.StatusBadRequest,
			"invalid aligner or reducer",
			nil,
			"invalid_aggregation",
			slog.LevelInfo

	case errors.Is(err, context.DeadlineExceeded):
		return http.StatusGatewayTimeout,
			"scrape exceeded deadline",
			nil,
			"deadline_exceeded",
			slog.LevelError

	case errors.Is(err, context.Canceled):
		return nonStandardClientClosed,
			"client cancelled the request",
			nil,
			"client_canceled",
			slog.LevelInfo
	}

	if code := grpcstatus.Code(err); code != codes.OK && code != codes.Unknown {
		switch code {
		case codes.InvalidArgument:
			return http.StatusBadRequest,
				"invalid argument: check filter, aligner, reducer, or metric_type",
				nil,
				code.String(),
				slog.LevelInfo
		case codes.NotFound:
			return http.StatusNotFound,
				"project or metric type not found",
				nil,
				code.String(),
				slog.LevelInfo
		case codes.PermissionDenied, codes.Unauthenticated:
			return http.StatusBadGateway,
				"upstream authentication or authorization failed; check exporter credentials",
				nil,
				code.String(),
				slog.LevelError
		case codes.DeadlineExceeded:
			return http.StatusGatewayTimeout,
				"upstream deadline exceeded",
				nil,
				code.String(),
				slog.LevelError
		case codes.ResourceExhausted:
			return http.StatusServiceUnavailable,
				"upstream quota exhausted",
				map[string]string{"Retry-After": "30"},
				code.String(),
				slog.LevelError
		default:
			return http.StatusBadGateway,
				"upstream error from cloud monitoring",
				nil,
				code.String(),
				slog.LevelError
		}
	}

	return http.StatusBadGateway,
		"upstream error from cloud monitoring",
		nil,
		"unknown",
		slog.LevelError
}

// HandleHealthz is a liveness handler. It returns 200 with a static JSON
// body and never touches GCP — readiness/IAM checks are intentionally
// out of scope so a transient GCP outage does not flap the pod.
func HandleHealthz(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}

// filterFingerprint returns a short hex prefix of SHA-256(filter) so log
// aggregators can group identical filters without recording the raw
// (potentially PII-bearing) value. An empty filter returns the empty
// string so the log field is visibly empty rather than a hash of "".
func filterFingerprint(filter string) string {
	if filter == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(filter))
	return hex.EncodeToString(sum[:])[:12]
}

// elapsedMs returns the millisecond difference between end and start.
// Callers pass clock samples taken via the handler's now func.
func elapsedMs(end, start time.Time) int64 {
	d := end.Sub(start)
	if d < 0 {
		return 0
	}
	return d.Milliseconds()
}
