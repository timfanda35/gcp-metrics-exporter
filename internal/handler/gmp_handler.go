package handler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/prometheus/common/expfmt"

	"github.com/timfanda35/gcp-metrics-exporter/internal/gmp"
)

// GMPClientForSA produces a [gmp.Client] for the given impersonation target
// ("" means no impersonation). It is the seam that decouples [GMPHandler]
// from [gmp.ClientCache]; cmd/server wires the production implementation,
// while tests inject lightweight fakes.
type GMPClientForSA func(ctx context.Context, impersonateSA string) (gmp.Client, error)

// GMPHandler implements the GET /gmp-metrics endpoint. It accepts a PromQL
// query, runs it against GMP's Prometheus-compatible instant query API, and
// returns the vector results in Prometheus text exposition format as gauges.
type GMPHandler struct {
	clientForSA GMPClientForSA
	limits      Limits
	sem         chan struct{}
	logger      *slog.Logger
	now         func() time.Time
}

// NewGMPHandler builds a [GMPHandler] with the supplied client factory,
// limits, and logger. Zero-valued [Limits] fields are replaced with the
// package defaults. The logger must be non-nil; pass [slog.Default] if
// you have nothing better.
func NewGMPHandler(clientForSA GMPClientForSA, limits Limits, logger *slog.Logger) *GMPHandler {
	if limits.ScrapeTimeout <= 0 {
		limits.ScrapeTimeout = DefaultScrapeTimeout
	}
	if limits.MaxConcurrent <= 0 {
		limits.MaxConcurrent = DefaultMaxConcurrent
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &GMPHandler{
		clientForSA: clientForSA,
		limits:      limits,
		sem:         make(chan struct{}, limits.MaxConcurrent),
		logger:      logger,
		now:         time.Now,
	}
}

// WithClock replaces the time source used for latency measurements and query
// time calculation. Intended for tests that need deterministic timing.
func (h *GMPHandler) WithClock(now func() time.Time) *GMPHandler {
	h.now = now
	return h
}

// gmpRequestParams is the parsed shape of an inbound /gmp-metrics request.
type gmpRequestParams struct {
	project       string
	query         string
	timeOffset    time.Duration
	impersonateSA string
}

// parseGMPRequest extracts and validates query parameters for /gmp-metrics.
// Returns (nil, msg, status) on validation failure.
func parseGMPRequest(r *http.Request, limits Limits) (*gmpRequestParams, string, int) {
	q := r.URL.Query()

	project := q.Get("project")
	if project == "" {
		return nil, "missing required parameter: project", http.StatusBadRequest
	}

	query := q.Get("query")
	if query == "" {
		return nil, "missing required parameter: query", http.StatusBadRequest
	}

	var timeOffset time.Duration
	if v := q.Get("time_offset"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d < 0 {
			return nil, fmt.Sprintf("invalid time_offset %q: must be a non-negative Go duration", v), http.StatusBadRequest
		}
		timeOffset = d
	}

	impersonateSA := q.Get("impersonate_sa")
	if impersonateSA == "" {
		impersonateSA = limits.DefaultImpersonateSA
	}

	return &gmpRequestParams{
		project:       project,
		query:         query,
		timeOffset:    timeOffset,
		impersonateSA: impersonateSA,
	}, "", 0
}

// ServeHTTP implements [http.Handler]. It applies the concurrency limit,
// runs the GMP query, and streams the Prometheus text exposition.
func (h *GMPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	startTotal := h.now()

	select {
	case h.sem <- struct{}{}:
		defer func() { <-h.sem }()
	default:
		w.Header().Set("Retry-After", "1")
		writeJSONError(w, http.StatusTooManyRequests, "too many concurrent scrapes")
		h.logger.Info("gmp-metrics request rejected",
			slog.String("reason", "concurrency_limit"),
			slog.Int("status", http.StatusTooManyRequests),
			slog.Int("max_concurrent", h.limits.MaxConcurrent),
		)
		return
	}

	params, errMsg, errStatus := parseGMPRequest(r, h.limits)
	if params == nil {
		writeJSONError(w, errStatus, errMsg)
		h.logger.Info("gmp-metrics request rejected",
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
		slog.String("impersonate_sa", params.impersonateSA),
	}

	client, err := h.clientForSA(ctx, params.impersonateSA)
	if err != nil {
		err = fmt.Errorf("handler: build gmp client: %w", err)
		h.respondGMPError(w, err, append(logAttrs, slog.Int64("total_latency_ms", elapsedMs(h.now(), startTotal)))...)
		return
	}

	c := gmp.NewCollector(client)
	at := h.now().Add(-params.timeOffset)

	gmpStart := h.now()
	families, err := c.Collect(ctx, params.project, params.query, at)
	gmpLatency := elapsedMs(h.now(), gmpStart)

	if err != nil {
		h.respondGMPError(w, err,
			append(logAttrs,
				slog.Int64("gmp_latency_ms", gmpLatency),
				slog.Int64("total_latency_ms", elapsedMs(h.now(), startTotal)),
			)...,
		)
		return
	}

	w.Header().Set("Content-Type", string(expfmt.NewFormat(expfmt.TypeTextPlain)))
	w.WriteHeader(http.StatusOK)
	enc := expfmt.NewEncoder(w, expfmt.NewFormat(expfmt.TypeTextPlain))
	seriesCount := 0
	for _, mf := range families {
		seriesCount += len(mf.GetMetric())
		if encErr := enc.Encode(mf); encErr != nil {
			h.logger.Error("gmp-metrics encode failed mid-stream",
				slog.String("project", params.project),
				slog.String("err", encErr.Error()),
			)
			break
		}
	}

	h.logger.Info("gmp-metrics request",
		append(logAttrs,
			slog.Int("status", http.StatusOK),
			slog.Int("series_count", seriesCount),
			slog.Int64("gmp_latency_ms", gmpLatency),
			slog.Int64("total_latency_ms", elapsedMs(h.now(), startTotal)),
		)...,
	)
}

func (h *GMPHandler) respondGMPError(w http.ResponseWriter, err error, logAttrs ...any) {
	httpStatus, msg, headers, errCode, level := mapGMPError(err)
	for k, v := range headers {
		w.Header().Set(k, v)
	}
	wireStatus := httpStatus
	if wireStatus == nonStandardClientClosed {
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
		h.logger.Error("gmp-metrics request failed", logAttrs...)
	default:
		h.logger.Info("gmp-metrics request failed", logAttrs...)
	}
}

// mapGMPError translates a GMP client error into HTTP status, user-facing
// message, extra response headers, a short error code, and a log level.
func mapGMPError(err error) (status int, msg string, headers map[string]string, errCode string, level slog.Level) {
	switch {
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

	var apiErr *gmp.APIError
	if errors.As(err, &apiErr) {
		switch {
		case apiErr.StatusCode == http.StatusBadRequest:
			return http.StatusBadRequest,
				"invalid PromQL query",
				nil,
				"bad_query",
				slog.LevelInfo
		case apiErr.StatusCode == http.StatusUnauthorized || apiErr.StatusCode == http.StatusForbidden:
			return http.StatusBadGateway,
				"upstream authentication or authorization failed; check exporter credentials",
				nil,
				"auth_error",
				slog.LevelError
		case apiErr.StatusCode == http.StatusServiceUnavailable || apiErr.StatusCode == http.StatusTooManyRequests:
			return http.StatusServiceUnavailable,
				"upstream quota exhausted",
				map[string]string{"Retry-After": "30"},
				"quota_exhausted",
				slog.LevelError
		}
	}

	return http.StatusBadGateway,
		"upstream error from GMP",
		nil,
		"unknown",
		slog.LevelError
}
