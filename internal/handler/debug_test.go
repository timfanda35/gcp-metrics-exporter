package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	dto "github.com/prometheus/client_model/go"

	"github.com/timfanda35/gcp-metrics-exporter/internal/collector"
	"github.com/timfanda35/gcp-metrics-exporter/internal/debugtrace"
	"github.com/timfanda35/gcp-metrics-exporter/internal/gmp"
)

// debugMux wires both real handlers behind RegisterDebug. The Cloud
// Monitoring collector records into the trace itself, as the production
// tracing client would; the GMP side uses the real tracing decorator.
func debugMux(t *testing.T, cmFactoryErr error, gmpErr error) *http.ServeMux {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	fc := &fakeCollector{collectFn: func(ctx context.Context, _ collector.QueryParams) ([]*dto.MetricFamily, error) {
		if tr := debugtrace.FromContext(ctx); tr != nil {
			tr.Add(debugtrace.Call{Backend: "cloud-monitoring", Operation: "ListTimeSeries", ResultCount: 1})
		}
		return staticGauge("test_metric", 7), nil
	}}
	metricsH := NewMetricsHandler(func(context.Context, string) (collector.Collector, error) {
		if cmFactoryErr != nil {
			return nil, cmFactoryErr
		}
		return fc, nil
	}, Limits{}, logger)

	gmpH := NewGMPHandler(func(context.Context, string) (gmp.Client, error) {
		return gmp.NewTracingClient(&fakeGMPClient{
			samples: []gmp.Sample{{Labels: map[string]string{"__name__": "up"}, Value: 1}},
			err:     gmpErr,
		}, logger), nil
	}, Limits{}, logger)

	mux := http.NewServeMux()
	RegisterDebug(mux, metricsH, gmpH)
	return mux
}

func doDebug(mux http.Handler, method, target string) *httptest.ResponseRecorder {
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, httptest.NewRequest(method, target, nil))
	return rr
}

func decodeRun(t *testing.T, rr *httptest.ResponseRecorder) debugRunResponse {
	t.Helper()
	var out debugRunResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &out); err != nil {
		t.Fatalf("response is not JSON: %v\n%s", err, rr.Body.String())
	}
	return out
}

func TestDebug_Page(t *testing.T) {
	mux := debugMux(t, nil, nil)
	rr := doDebug(mux, http.MethodGet, "/debug")
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d", rr.Code)
	}
	if ct := rr.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/html") {
		t.Errorf("Content-Type = %q", ct)
	}
	if !strings.Contains(rr.Body.String(), "<form") {
		t.Error("page should contain a form")
	}
	if rr := doDebug(mux, http.MethodGet, "/debug/nope"); rr.Code != http.StatusNotFound {
		t.Errorf("unknown debug path status = %d, want 404", rr.Code)
	}
}

func TestDebug_NotRegisteredByDefault(t *testing.T) {
	mux := http.NewServeMux()
	for _, p := range []string{"/debug", "/debug/run"} {
		if rr := doDebug(mux, http.MethodGet, p); rr.Code != http.StatusNotFound {
			t.Errorf("%s status = %d, want 404 when not registered", p, rr.Code)
		}
	}
}

func TestDebug_RunMetrics(t *testing.T) {
	out := decodeRun(t, doDebug(debugMux(t, nil, nil), http.MethodGet,
		"/debug/run?backend=metrics&project=p&metric_type=m"))
	if out.Status != http.StatusOK || !strings.Contains(out.Body, "test_metric") {
		t.Errorf("status=%d body=%q", out.Status, out.Body)
	}
	if len(out.Calls) != 1 || out.Calls[0].Operation != "ListTimeSeries" {
		t.Errorf("calls = %+v", out.Calls)
	}
}

func TestDebug_RunGMP(t *testing.T) {
	out := decodeRun(t, doDebug(debugMux(t, nil, nil), http.MethodGet,
		"/debug/run?backend=gmp-metrics&project=p&query=up"))
	if out.Status != http.StatusOK || !strings.Contains(out.Body, "gcp_up") {
		t.Errorf("status=%d body=%q", out.Status, out.Body)
	}
	if len(out.Calls) != 1 || out.Calls[0].Backend != "gmp" || out.Calls[0].Request["query"] != "up" {
		t.Errorf("calls = %+v", out.Calls)
	}
}

// The generic upstream message hides the cause; the trace must carry it.
func TestDebug_RunShowsRawUpstreamError(t *testing.T) {
	raw := errors.New("oauth2: cannot fetch token: invalid_scope")
	out := decodeRun(t, doDebug(debugMux(t, nil, raw), http.MethodGet,
		"/debug/run?backend=gmp-metrics&project=p&query=up"))
	if out.Status != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", out.Status)
	}
	if strings.Contains(out.Body, "invalid_scope") {
		t.Error("the normal response body must keep hiding the raw error")
	}
	if len(out.Calls) != 1 || !strings.Contains(out.Calls[0].Error, "invalid_scope") {
		t.Errorf("calls = %+v, want the raw error in the trace", out.Calls)
	}
}

// A failure before any upstream call (building the client) is traced too.
func TestDebug_RunTracesClientBuildFailure(t *testing.T) {
	out := decodeRun(t, doDebug(debugMux(t, errors.New("find default credentials: not found"), nil), http.MethodGet,
		"/debug/run?backend=metrics&project=p&metric_type=m&impersonate_sa=sa@x.iam.gserviceaccount.com"))
	if out.Status < 500 {
		t.Fatalf("status = %d, want a 5xx", out.Status)
	}
	if len(out.Calls) != 1 || out.Calls[0].Operation != "build_client" ||
		!strings.Contains(out.Calls[0].Error, "find default credentials") ||
		out.Calls[0].Request["impersonate_sa"] != "sa@x.iam.gserviceaccount.com" {
		t.Errorf("calls = %+v", out.Calls)
	}
}

func TestDebug_RunValidation(t *testing.T) {
	mux := debugMux(t, nil, nil)
	if rr := doDebug(mux, http.MethodGet, "/debug/run?backend=bogus"); rr.Code != http.StatusBadRequest {
		t.Errorf("unknown backend status = %d, want 400", rr.Code)
	}
	if rr := doDebug(mux, http.MethodGet, "/debug/run"); rr.Code != http.StatusBadRequest {
		t.Errorf("missing backend status = %d, want 400", rr.Code)
	}
	for _, p := range []string{"/debug", "/debug/run?backend=metrics"} {
		rr := doDebug(mux, http.MethodPost, p)
		if rr.Code != http.StatusMethodNotAllowed {
			t.Errorf("POST %s status = %d, want 405", p, rr.Code)
		}
	}
	// A validation error from the real handler surfaces inside the result.
	out := decodeRun(t, doDebug(mux, http.MethodGet, "/debug/run?backend=metrics&metric_type=m"))
	if out.Status != http.StatusBadRequest || !strings.Contains(out.Body, "project") {
		t.Errorf("status=%d body=%q, want the handler's 400 about project", out.Status, out.Body)
	}
}

// error_detail carries the raw upstream error, at debug level only.
func TestErrorDetailLoggedOnlyAtDebug(t *testing.T) {
	for _, tc := range []struct {
		level slog.Level
		want  bool
	}{{slog.LevelDebug, true}, {slog.LevelInfo, false}} {
		var buf bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: tc.level}))
		h := NewGMPHandler(func(context.Context, string) (gmp.Client, error) {
			return &fakeGMPClient{err: errors.New("oauth2: invalid_scope")}, nil
		}, Limits{}, logger)
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/gmp-metrics?project=p&query=up", nil))

		got := strings.Contains(buf.String(), "error_detail") && strings.Contains(buf.String(), "invalid_scope")
		if got != tc.want {
			t.Errorf("level %v: error_detail logged = %v, want %v\n%s", tc.level, got, tc.want, buf.String())
		}
		if strings.Contains(rr.Body.String(), "invalid_scope") {
			t.Errorf("level %v: raw error leaked into the HTTP response", tc.level)
		}
	}
}
