package gmp

import (
	"context"
	"errors"
	"testing"
	"time"
)

// fakeClient is a test double for [Client] that returns a fixed set of
// samples or an error.
type fakeClient struct {
	samples []Sample
	err     error
}

func (f *fakeClient) Query(_ context.Context, _, _ string, _ time.Time) ([]Sample, error) {
	return f.samples, f.err
}

func TestCollector_Collect(t *testing.T) {
	t.Parallel()

	at := time.Unix(1609459200, 0)

	tests := []struct {
		name        string
		samples     []Sample
		clientErr   error
		wantFams    int
		wantMetrics map[string]int // family name → metric count
		wantErr     bool
	}{
		{
			name: "single metric",
			samples: []Sample{
				{Labels: map[string]string{"__name__": "up", "job": "a"}, Value: 1},
			},
			wantFams:    1,
			wantMetrics: map[string]int{"up": 1},
		},
		{
			name: "multiple series same family",
			samples: []Sample{
				{Labels: map[string]string{"__name__": "http_requests_total", "job": "a"}, Value: 10},
				{Labels: map[string]string{"__name__": "http_requests_total", "job": "b"}, Value: 20},
			},
			wantFams:    1,
			wantMetrics: map[string]int{"http_requests_total": 2},
		},
		{
			name: "multiple families",
			samples: []Sample{
				{Labels: map[string]string{"__name__": "up", "job": "a"}, Value: 1},
				{Labels: map[string]string{"__name__": "go_goroutines", "job": "a"}, Value: 5},
			},
			wantFams:    2,
			wantMetrics: map[string]int{"up": 1, "go_goroutines": 1},
		},
		{
			name: "drops samples without __name__",
			samples: []Sample{
				{Labels: map[string]string{"job": "a"}, Value: 1},
				{Labels: map[string]string{"__name__": "up", "job": "b"}, Value: 1},
			},
			wantFams:    1,
			wantMetrics: map[string]int{"up": 1},
		},
		{
			name:    "propagates client error",
			samples: nil,
			clientErr: errors.New("query failed"),
			wantErr: true,
		},
		{
			name:    "empty result",
			samples: []Sample{},
			wantFams: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := NewCollector(&fakeClient{samples: tc.samples, err: tc.clientErr})
			got, err := c.Collect(context.Background(), "proj", "up", at)
			if (err != nil) != tc.wantErr {
				t.Fatalf("Collect() error = %v, wantErr %v", err, tc.wantErr)
			}
			if tc.wantErr {
				return
			}
			if len(got) != tc.wantFams {
				t.Errorf("got %d families, want %d", len(got), tc.wantFams)
			}
			for _, fam := range got {
				name := fam.GetName()
				wantCount, ok := tc.wantMetrics[name]
				if !ok {
					t.Errorf("unexpected family %q", name)
					continue
				}
				if got := len(fam.GetMetric()); got != wantCount {
					t.Errorf("family %q: got %d metrics, want %d", name, got, wantCount)
				}
			}
		})
	}
}

func TestCollector_Collect_NoNameLabel(t *testing.T) {
	t.Parallel()
	c := NewCollector(&fakeClient{samples: []Sample{
		{Labels: map[string]string{"__name__": "up", "job": "a"}, Value: 42},
	}})
	families, err := c.Collect(context.Background(), "proj", "up", time.Now())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(families) != 1 {
		t.Fatalf("want 1 family, got %d", len(families))
	}
	metrics := families[0].GetMetric()
	if len(metrics) != 1 {
		t.Fatalf("want 1 metric, got %d", len(metrics))
	}
	// __name__ must not appear in the label set.
	for _, lp := range metrics[0].GetLabel() {
		if lp.GetName() == "__name__" {
			t.Error("__name__ must not appear in exported labels")
		}
	}
	// Value must be exported as gauge.
	if g := metrics[0].GetGauge(); g == nil {
		t.Error("metric must have Gauge set")
	} else if g.GetValue() != 42 {
		t.Errorf("Gauge.Value = %v, want 42", g.GetValue())
	}
}
