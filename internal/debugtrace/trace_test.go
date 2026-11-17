package debugtrace

import (
	"context"
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
