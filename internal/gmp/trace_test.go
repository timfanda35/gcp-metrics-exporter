package gmp

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/timfanda35/gcp-metrics-exporter/internal/debugtrace"
)

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)) }

func TestTracingClient_RecordsQuery(t *testing.T) {
	inner := &fakeClient{samples: []Sample{{Value: 1}, {Value: 2}, {Value: 3}}}
	c := NewTracingClient(inner, quietLogger())
	ctx, tr := debugtrace.WithTrace(context.Background())
	at := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)

	got, err := c.Query(ctx, "proj", "up", at)
	if err != nil || len(got) != 3 {
		t.Fatalf("Query: n=%d err=%v", len(got), err)
	}
	calls := tr.Calls()
	if len(calls) != 1 {
		t.Fatalf("len(calls) = %d, want 1", len(calls))
	}
	c0 := calls[0]
	if c0.Backend != "gmp" || c0.Operation != "Query" || c0.ResultCount != 3 || c0.Error != "" {
		t.Errorf("call = %+v", c0)
	}
	for k, want := range map[string]any{"project": "proj", "query": "up", "time": "2026-05-10T12:00:00Z"} {
		if c0.Request[k] != want {
			t.Errorf("Request[%q] = %v, want %v", k, c0.Request[k], want)
		}
	}
}

func TestTracingClient_RecordsRawError(t *testing.T) {
	boom := errors.New("oauth2: cannot fetch token: invalid_scope")
	c := NewTracingClient(&fakeClient{err: boom}, quietLogger())
	ctx, tr := debugtrace.WithTrace(context.Background())

	if _, err := c.Query(ctx, "p", "q", time.Unix(0, 0)); !errors.Is(err, boom) {
		t.Fatalf("err = %v, want %v", err, boom)
	}
	if got := tr.Calls()[0].Error; !strings.Contains(got, "invalid_scope") {
		t.Errorf("trace Error = %q, want the raw upstream text", got)
	}
}

func TestTracingClient_PassThroughWhenNotWanted(t *testing.T) {
	inner := &fakeClient{}
	c := NewTracingClient(inner, quietLogger())
	if tc, ok := c.(*tracingClient); !ok || tc.inner != Client(inner) {
		t.Fatalf("unexpected wrapper %T", c)
	}
	// No trace and info level: nothing recorded, result passed through.
	if _, err := c.Query(context.Background(), "p", "q", time.Unix(0, 0)); err != nil {
		t.Fatal(err)
	}
}

func TestTracingClient_DebugLog(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	c := NewTracingClient(&fakeClient{err: errors.New("upstream says no")}, logger)

	_, _ = c.Query(context.Background(), "proj", "up{job=\"x\"}", time.Unix(0, 0))
	out := buf.String()
	for _, want := range []string{"gmp request", "project=proj", "upstream says no"} {
		if !strings.Contains(out, want) {
			t.Errorf("debug log %q missing %q", out, want)
		}
	}
}
