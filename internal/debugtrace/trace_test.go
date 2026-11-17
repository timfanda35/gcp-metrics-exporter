package debugtrace

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
)

func TestFromContext_NoTrace(t *testing.T) {
	if got := FromContext(context.Background()); got != nil {
		t.Fatalf("FromContext without a trace = %v, want nil", got)
	}
}

func TestTrace_AddCallsUpdate(t *testing.T) {
	ctx, tr := WithTrace(context.Background())
	if FromContext(ctx) != tr {
		t.Fatal("FromContext should return the trace stored by WithTrace")
	}
	if got := tr.Calls(); got == nil || len(got) != 0 {
		t.Fatalf("empty trace Calls() = %#v, want non-nil empty slice", got)
	}

	i := tr.Add(Call{Backend: "cloud-monitoring", Operation: "ListTimeSeries", Request: map[string]any{"project": "p"}})
	tr.Update(i, func(c *Call) {
		c.ResultCount = 3
		c.LatencyMs = 12
		c.Error = "boom"
	})

	calls := tr.Calls()
	if len(calls) != 1 {
		t.Fatalf("len(calls) = %d, want 1", len(calls))
	}
	if c := calls[0]; c.ResultCount != 3 || c.LatencyMs != 12 || c.Error != "boom" || c.Request["project"] != "p" {
		t.Errorf("call = %+v", c)
	}

	// Calls returns a copy: mutating it must not change the trace.
	calls[0].Error = "mutated"
	if got := tr.Calls()[0].Error; got != "boom" {
		t.Errorf("trace was modified through the returned slice: %q", got)
	}
}

func TestTrace_UpdateOutOfRangeIsIgnored(t *testing.T) {
	_, tr := WithTrace(context.Background())
	tr.Update(5, func(c *Call) { c.Error = "x" }) // must not panic
	tr.Update(-1, func(c *Call) { c.Error = "x" })
}

func TestTrace_ConcurrentAdds(t *testing.T) {
	_, tr := WithTrace(context.Background())
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			idx := tr.Add(Call{Operation: "op"})
			tr.Update(idx, func(c *Call) { c.ResultCount++ })
		}()
	}
	wg.Wait()
	if got := len(tr.Calls()); got != 50 {
		t.Fatalf("len(calls) = %d, want 50", got)
	}
}

func debugLogger(buf *bytes.Buffer) *slog.Logger {
	return slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

func infoLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))
}

func TestStart_NilWhenNothingWantsIt(t *testing.T) {
	if r := Start(context.Background(), infoLogger(), "msg", Call{}); r != nil {
		t.Fatal("Start should return nil without a trace or debug logging")
	}
	var r *Recorder
	r.Finish(1, errors.New("x")) // nil-safe, must not panic
}

func TestRecorder_TraceLifecycle(t *testing.T) {
	ctx, tr := WithTrace(context.Background())
	r := Start(ctx, infoLogger(), "msg", Call{Backend: "b", Operation: "op", Request: map[string]any{"k": "v"}})
	if r == nil {
		t.Fatal("Start returned nil with a trace in the context")
	}
	if c := tr.Calls()[0]; !c.Incomplete {
		t.Errorf("a call that has not finished must be marked incomplete: %+v", c)
	}

	r.Finish(4, errors.New("boom"))
	r.Finish(99, nil) // only the first Finish counts
	c := tr.Calls()[0]
	if c.Incomplete || c.ResultCount != 4 || c.Error != "boom" {
		t.Errorf("finished call = %+v", c)
	}
}

func TestRecorder_DebugLog(t *testing.T) {
	var buf bytes.Buffer
	r := Start(context.Background(), debugLogger(&buf), "my request",
		Call{Operation: "op", Request: map[string]any{"project": "p", "filter": "f"}})
	r.Finish(2, errors.New("bad"))

	out := buf.String()
	for _, want := range []string{"my request", "operation=op", "result_count=2", "project=p", "filter=f", "error=bad"} {
		if !strings.Contains(out, want) {
			t.Errorf("debug log %q missing %q", out, want)
		}
	}
}
