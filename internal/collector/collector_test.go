package collector

import (
	"context"
	"errors"
	"math"
	"net"
	"strconv"
	"testing"
	"time"

	monitoring "cloud.google.com/go/monitoring/apiv3/v2"
	"cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	dto "github.com/prometheus/client_model/go"
	"google.golang.org/api/option"
	distributionpb "google.golang.org/genproto/googleapis/api/distribution"
	metricpb "google.golang.org/genproto/googleapis/api/metric"
	monitoredres "google.golang.org/genproto/googleapis/api/monitoredres"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// fixedClock returns a deterministic time-source so request-time intervals
// are stable across runs.
func fixedClock(t time.Time) func() time.Time {
	return func() time.Time { return t }
}

// fakeMetricServer is a minimal monitoringpb.MetricServiceServer used by
// the collector tests. Tests assign listFn / lastRequest before invoking
// Collect.
type fakeMetricServer struct {
	monitoringpb.UnimplementedMetricServiceServer

	listFn      func(ctx context.Context, req *monitoringpb.ListTimeSeriesRequest) (*monitoringpb.ListTimeSeriesResponse, error)
	lastRequest *monitoringpb.ListTimeSeriesRequest
}

func (s *fakeMetricServer) ListTimeSeries(ctx context.Context, req *monitoringpb.ListTimeSeriesRequest) (*monitoringpb.ListTimeSeriesResponse, error) {
	s.lastRequest = req
	if s.listFn == nil {
		return &monitoringpb.ListTimeSeriesResponse{}, nil
	}
	return s.listFn(ctx, req)
}

// startFakeServer brings up a bufconn-backed gRPC server hosting the
// supplied fake MetricServiceServer and returns a real
// *monitoring.MetricClient dialled at it. The returned cleanup closes
// both the client and the server.
func startFakeServer(t *testing.T, fake *fakeMetricServer) (*monitoring.MetricClient, func()) {
	t.Helper()

	lis := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer()
	monitoringpb.RegisterMetricServiceServer(srv, fake)
	go func() {
		// Errors here happen at shutdown; the test will fail elsewhere
		// if the server never becomes ready.
		_ = srv.Serve(lis)
	}()

	dialer := func(ctx context.Context, _ string) (net.Conn, error) {
		return lis.DialContext(ctx)
	}

	conn, err := grpc.NewClient(
		"passthrough:bufnet",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}

	client, err := monitoring.NewMetricClient(
		context.Background(),
		option.WithGRPCConn(conn),
	)
	if err != nil {
		t.Fatalf("monitoring.NewMetricClient: %v", err)
	}

	cleanup := func() {
		_ = client.Close()
		srv.Stop()
		_ = lis.Close()
	}
	return client, cleanup
}

// helper constructors

func mkPoint(start, end time.Time, value *monitoringpb.TypedValue) *monitoringpb.Point {
	return &monitoringpb.Point{
		Interval: &monitoringpb.TimeInterval{
			StartTime: timestamppb.New(start),
			EndTime:   timestamppb.New(end),
		},
		Value: value,
	}
}

func int64Value(v int64) *monitoringpb.TypedValue {
	return &monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_Int64Value{Int64Value: v}}
}

func doubleValue(v float64) *monitoringpb.TypedValue {
	return &monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_DoubleValue{DoubleValue: v}}
}

func distValue(d *distributionpb.Distribution) *monitoringpb.TypedValue {
	return &monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_DistributionValue{DistributionValue: d}}
}

// findFamily returns the *dto.MetricFamily with the given name or nil.
func findFamily(fams []*dto.MetricFamily, name string) *dto.MetricFamily {
	for _, f := range fams {
		if f.GetName() == name {
			return f
		}
	}
	return nil
}

// labelValue returns the value of the named label or "".
func labelValue(m *dto.Metric, name string) string {
	for _, lp := range m.GetLabel() {
		if lp.GetName() == name {
			return lp.GetValue()
		}
	}
	return ""
}

// upperBounds returns the buckets' upper bounds in order.
func upperBounds(h *dto.Histogram) []float64 {
	out := make([]float64, 0, len(h.GetBucket()))
	for _, b := range h.GetBucket() {
		out = append(out, b.GetUpperBound())
	}
	return out
}

// TestCollect_GaugeInt64Double exercises required test case 1: GAUGE
// INT64 + GAUGE DOUBLE — labels and values match.
func TestCollect_GaugeInt64Double(t *testing.T) {
	now := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
	end := now
	start := now.Add(-time.Minute)

	cases := []struct {
		name      string
		valueType metricpb.MetricDescriptor_ValueType
		point     *monitoringpb.TypedValue
		want      float64
	}{
		{"int64", metricpb.MetricDescriptor_INT64, int64Value(42), 42},
		{"double", metricpb.MetricDescriptor_DOUBLE, doubleValue(3.14), 3.14},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			fake := &fakeMetricServer{
				listFn: func(_ context.Context, _ *monitoringpb.ListTimeSeriesRequest) (*monitoringpb.ListTimeSeriesResponse, error) {
					return &monitoringpb.ListTimeSeriesResponse{
						TimeSeries: []*monitoringpb.TimeSeries{{
							Metric: &metricpb.Metric{
								Type:   "compute.googleapis.com/instance/cpu/utilization",
								Labels: map[string]string{"instance_name": "vm-a"},
							},
							Resource: &monitoredres.MonitoredResource{
								Type:   "gce_instance",
								Labels: map[string]string{"zone": "us-central1-a", "project_id": "p"},
							},
							MetricKind: metricpb.MetricDescriptor_GAUGE,
							ValueType:  tc.valueType,
							Points:     []*monitoringpb.Point{mkPoint(start, end, tc.point)},
						}},
					}, nil
				},
			}
			client, cleanup := startFakeServer(t, fake)
			t.Cleanup(cleanup)

			c := NewGCPCollector(NewMetricClientFromGCP(client), Options{Now: fixedClock(now)})
			fams, err := c.Collect(context.Background(), QueryParams{
				ProjectID:  "p",
				MetricType: "compute.googleapis.com/instance/cpu/utilization",
			})
			if err != nil {
				t.Fatalf("Collect: %v", err)
			}
			if len(fams) != 1 {
				t.Fatalf("want 1 family, got %d", len(fams))
			}
			fam := fams[0]
			if got := fam.GetName(); got != "compute_googleapis_com_instance_cpu_utilization" {
				t.Errorf("family name = %q", got)
			}
			if got := fam.GetType(); got != dto.MetricType_GAUGE {
				t.Errorf("family type = %v, want GAUGE", got)
			}
			if len(fam.GetMetric()) != 1 {
				t.Fatalf("want 1 metric, got %d", len(fam.GetMetric()))
			}
			m := fam.GetMetric()[0]
			if got := m.GetGauge().GetValue(); got != tc.want {
				t.Errorf("gauge value = %v, want %v", got, tc.want)
			}
			if got := labelValue(m, "instance_name"); got != "vm-a" {
				t.Errorf("instance_name label = %q", got)
			}
			if got := labelValue(m, "zone"); got != "us-central1-a" {
				t.Errorf("zone label = %q", got)
			}
			if got := labelValue(m, "resource_type"); got != "gce_instance" {
				t.Errorf("resource_type label = %q", got)
			}
		})
	}
}

// TestCollect_CumulativeStartTimeUnix exercises required test case 2.
func TestCollect_CumulativeStartTimeUnix(t *testing.T) {
	now := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
	start := now.Add(-time.Hour)
	end := now

	fake := &fakeMetricServer{
		listFn: func(_ context.Context, _ *monitoringpb.ListTimeSeriesRequest) (*monitoringpb.ListTimeSeriesResponse, error) {
			return &monitoringpb.ListTimeSeriesResponse{
				TimeSeries: []*monitoringpb.TimeSeries{{
					Metric:     &metricpb.Metric{Type: "x", Labels: map[string]string{"k": "v"}},
					Resource:   &monitoredres.MonitoredResource{Type: "global"},
					MetricKind: metricpb.MetricDescriptor_CUMULATIVE,
					ValueType:  metricpb.MetricDescriptor_INT64,
					Points: []*monitoringpb.Point{
						mkPoint(start, end, int64Value(7)),
					},
				}},
			}, nil
		},
	}
	client, cleanup := startFakeServer(t, fake)
	t.Cleanup(cleanup)

	c := NewGCPCollector(NewMetricClientFromGCP(client), Options{Now: fixedClock(now)})
	fams, err := c.Collect(context.Background(), QueryParams{
		ProjectID:  "p",
		MetricType: "svc.example.com/cumulative_metric",
	})
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if len(fams) != 1 || fams[0].GetType() != dto.MetricType_COUNTER {
		t.Fatalf("expected single counter family, got %+v", fams)
	}
	m := fams[0].GetMetric()[0]
	if got := m.GetCounter().GetValue(); got != 7 {
		t.Errorf("counter value = %v", got)
	}
	wantStart := strconv.FormatInt(start.Unix(), 10)
	if got := labelValue(m, "start_time_unix"); got != wantStart {
		t.Errorf("start_time_unix = %q, want %q", got, wantStart)
	}
}

// TestCollect_DeltaSummation exercises required test case 3.
func TestCollect_DeltaSummation(t *testing.T) {
	now := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
	// GCP returns most-recent-first. Build three points whose start
	// times step backwards from now.
	p1 := mkPoint(now.Add(-2*time.Minute), now.Add(-time.Minute), doubleValue(1.5))
	p2 := mkPoint(now.Add(-3*time.Minute), now.Add(-2*time.Minute), doubleValue(2.5))
	earliestStart := now.Add(-4 * time.Minute)
	p3 := mkPoint(earliestStart, now.Add(-3*time.Minute), doubleValue(0.25))

	fake := &fakeMetricServer{
		listFn: func(_ context.Context, _ *monitoringpb.ListTimeSeriesRequest) (*monitoringpb.ListTimeSeriesResponse, error) {
			return &monitoringpb.ListTimeSeriesResponse{
				TimeSeries: []*monitoringpb.TimeSeries{{
					Metric:     &metricpb.Metric{Type: "x"},
					Resource:   &monitoredres.MonitoredResource{Type: "global"},
					MetricKind: metricpb.MetricDescriptor_DELTA,
					ValueType:  metricpb.MetricDescriptor_DOUBLE,
					Points:     []*monitoringpb.Point{p1, p2, p3},
				}},
			}, nil
		},
	}
	client, cleanup := startFakeServer(t, fake)
	t.Cleanup(cleanup)

	c := NewGCPCollector(NewMetricClientFromGCP(client), Options{Now: fixedClock(now)})
	fams, err := c.Collect(context.Background(), QueryParams{
		ProjectID:  "p",
		MetricType: "svc.example.com/delta_metric",
	})
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if len(fams) != 1 {
		t.Fatalf("want 1 family, got %d", len(fams))
	}
	if fams[0].GetType() != dto.MetricType_COUNTER {
		t.Fatalf("want COUNTER, got %v", fams[0].GetType())
	}
	m := fams[0].GetMetric()[0]
	if got := m.GetCounter().GetValue(); math.Abs(got-(1.5+2.5+0.25)) > 1e-9 {
		t.Errorf("counter value = %v, want 4.25", got)
	}
	wantStart := strconv.FormatInt(earliestStart.Unix(), 10)
	if got := labelValue(m, "start_time_unix"); got != wantStart {
		t.Errorf("start_time_unix = %q, want %q", got, wantStart)
	}
}

// TestCollect_DistributionLayouts exercises required test case 4 across
// linear, exponential, and explicit bucket layouts. Empty bucket counts
// are also covered (linear layout with all-zero counts).
func TestCollect_DistributionLayouts(t *testing.T) {
	now := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)

	cases := []struct {
		name       string
		opts       *distributionpb.Distribution_BucketOptions
		counts     []int64
		count      int64
		mean       float64
		wantBounds []float64
		wantCum    []uint64 // cumulative counts including +Inf
		wantSum    float64
	}{
		{
			name: "linear",
			opts: &distributionpb.Distribution_BucketOptions{
				Options: &distributionpb.Distribution_BucketOptions_LinearBuckets{
					LinearBuckets: &distributionpb.Distribution_BucketOptions_Linear{
						NumFiniteBuckets: 3,
						Width:            10,
						Offset:           0,
					},
				},
			},
			counts:     []int64{2, 3, 5, 1},
			count:      11,
			mean:       12.5,
			wantBounds: []float64{10, 20, 30, math.Inf(1)},
			wantCum:    []uint64{2, 5, 10, 11},
			wantSum:    12.5 * 11,
		},
		{
			name: "exponential",
			opts: &distributionpb.Distribution_BucketOptions{
				Options: &distributionpb.Distribution_BucketOptions_ExponentialBuckets{
					ExponentialBuckets: &distributionpb.Distribution_BucketOptions_Exponential{
						NumFiniteBuckets: 3,
						GrowthFactor:     2,
						Scale:            1,
					},
				},
			},
			counts:     []int64{1, 1, 1, 1},
			count:      4,
			mean:       3,
			wantBounds: []float64{2, 4, 8, math.Inf(1)},
			wantCum:    []uint64{1, 2, 3, 4},
			wantSum:    12,
		},
		{
			name: "explicit",
			opts: &distributionpb.Distribution_BucketOptions{
				Options: &distributionpb.Distribution_BucketOptions_ExplicitBuckets{
					ExplicitBuckets: &distributionpb.Distribution_BucketOptions_Explicit{
						Bounds: []float64{-5, 0, 5},
					},
				},
			},
			counts:     []int64{1, 1, 1, 1},
			count:      4,
			mean:       0,
			wantBounds: []float64{-5, 0, 5, math.Inf(1)},
			wantCum:    []uint64{1, 2, 3, 4},
			wantSum:    0,
		},
		{
			name: "empty_counts",
			opts: &distributionpb.Distribution_BucketOptions{
				Options: &distributionpb.Distribution_BucketOptions_LinearBuckets{
					LinearBuckets: &distributionpb.Distribution_BucketOptions_Linear{
						NumFiniteBuckets: 2,
						Width:            1,
						Offset:           0,
					},
				},
			},
			counts:     nil,
			count:      0,
			mean:       0,
			wantBounds: []float64{1, 2, math.Inf(1)},
			wantCum:    []uint64{0, 0, 0},
			wantSum:    0,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			dist := &distributionpb.Distribution{
				Count:         tc.count,
				Mean:          tc.mean,
				BucketOptions: tc.opts,
				BucketCounts:  tc.counts,
			}
			fake := &fakeMetricServer{
				listFn: func(_ context.Context, _ *monitoringpb.ListTimeSeriesRequest) (*monitoringpb.ListTimeSeriesResponse, error) {
					return &monitoringpb.ListTimeSeriesResponse{
						TimeSeries: []*monitoringpb.TimeSeries{{
							Metric:     &metricpb.Metric{Type: "x"},
							Resource:   &monitoredres.MonitoredResource{Type: "global"},
							MetricKind: metricpb.MetricDescriptor_CUMULATIVE,
							ValueType:  metricpb.MetricDescriptor_DISTRIBUTION,
							Points: []*monitoringpb.Point{
								mkPoint(now.Add(-time.Minute), now, distValue(dist)),
							},
						}},
					}, nil
				},
			}
			client, cleanup := startFakeServer(t, fake)
			t.Cleanup(cleanup)

			c := NewGCPCollector(NewMetricClientFromGCP(client), Options{Now: fixedClock(now)})
			fams, err := c.Collect(context.Background(), QueryParams{
				ProjectID:  "p",
				MetricType: "svc.example.com/distribution_metric",
			})
			if err != nil {
				t.Fatalf("Collect: %v", err)
			}
			if len(fams) != 1 || fams[0].GetType() != dto.MetricType_HISTOGRAM {
				t.Fatalf("expected single histogram family, got %+v", fams)
			}
			h := fams[0].GetMetric()[0].GetHistogram()
			if h == nil {
				t.Fatal("histogram is nil")
			}
			gotBounds := upperBounds(h)
			if len(gotBounds) != len(tc.wantBounds) {
				t.Fatalf("bucket count = %d, want %d (bounds=%v)", len(gotBounds), len(tc.wantBounds), gotBounds)
			}
			for i, ub := range tc.wantBounds {
				if math.IsInf(ub, 1) {
					if !math.IsInf(gotBounds[i], 1) {
						t.Errorf("bucket[%d] upper = %v, want +Inf", i, gotBounds[i])
					}
					continue
				}
				if gotBounds[i] != ub {
					t.Errorf("bucket[%d] upper = %v, want %v", i, gotBounds[i], ub)
				}
			}
			for i, want := range tc.wantCum {
				if got := h.GetBucket()[i].GetCumulativeCount(); got != want {
					t.Errorf("bucket[%d] cum = %d, want %d", i, got, want)
				}
			}
			if got := h.GetSampleCount(); got != tc.wantCum[len(tc.wantCum)-1] {
				t.Errorf("sample count = %d, want %d", got, tc.wantCum[len(tc.wantCum)-1])
			}
			if got := h.GetSampleSum(); math.Abs(got-tc.wantSum) > 1e-9 {
				t.Errorf("sample sum = %v, want %v", got, tc.wantSum)
			}
		})
	}
}

// TestCollect_FilterComposition exercises required test case 5 in both
// directions (empty user filter, non-empty user filter).
func TestCollect_FilterComposition(t *testing.T) {
	now := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)

	cases := []struct {
		name       string
		userFilter string
		want       string
	}{
		{
			name:       "empty",
			userFilter: "",
			want:       `metric.type = "compute.googleapis.com/instance/cpu/utilization"`,
		},
		{
			name:       "with_user_filter",
			userFilter: `resource.labels.zone = "us-central1-a"`,
			want:       `metric.type = "compute.googleapis.com/instance/cpu/utilization" AND (resource.labels.zone = "us-central1-a")`,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			fake := &fakeMetricServer{}
			client, cleanup := startFakeServer(t, fake)
			t.Cleanup(cleanup)

			c := NewGCPCollector(NewMetricClientFromGCP(client), Options{Now: fixedClock(now)})
			_, err := c.Collect(context.Background(), QueryParams{
				ProjectID:  "p",
				MetricType: "compute.googleapis.com/instance/cpu/utilization",
				Filter:     tc.userFilter,
			})
			if err != nil {
				t.Fatalf("Collect: %v", err)
			}
			if fake.lastRequest == nil {
				t.Fatal("server received no request")
			}
			if got := fake.lastRequest.GetFilter(); got != tc.want {
				t.Errorf("Filter = %q,\nwant     %q", got, tc.want)
			}
			if got := fake.lastRequest.GetName(); got != "projects/p" {
				t.Errorf("Name = %q, want %q", got, "projects/p")
			}
			// Default aligner / reducer applied.
			agg := fake.lastRequest.GetAggregation()
			if got := agg.GetPerSeriesAligner(); got != monitoringpb.Aggregation_ALIGN_MEAN {
				t.Errorf("aligner = %v, want ALIGN_MEAN", got)
			}
			if got := agg.GetCrossSeriesReducer(); got != monitoringpb.Aggregation_REDUCE_NONE {
				t.Errorf("reducer = %v, want REDUCE_NONE", got)
			}
		})
	}
}

// TestCollect_MaxSeriesExceeded exercises required test case 6.
func TestCollect_MaxSeriesExceeded(t *testing.T) {
	now := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
	// Build 5 trivial gauge series; cap at 3.
	mkSeries := func(i int) *monitoringpb.TimeSeries {
		return &monitoringpb.TimeSeries{
			Metric:     &metricpb.Metric{Type: "x", Labels: map[string]string{"i": strconv.Itoa(i)}},
			Resource:   &monitoredres.MonitoredResource{Type: "global"},
			MetricKind: metricpb.MetricDescriptor_GAUGE,
			ValueType:  metricpb.MetricDescriptor_INT64,
			Points:     []*monitoringpb.Point{mkPoint(now.Add(-time.Minute), now, int64Value(int64(i)))},
		}
	}
	all := []*monitoringpb.TimeSeries{mkSeries(0), mkSeries(1), mkSeries(2), mkSeries(3), mkSeries(4)}

	fake := &fakeMetricServer{
		listFn: func(_ context.Context, _ *monitoringpb.ListTimeSeriesRequest) (*monitoringpb.ListTimeSeriesResponse, error) {
			return &monitoringpb.ListTimeSeriesResponse{TimeSeries: all}, nil
		},
	}
	client, cleanup := startFakeServer(t, fake)
	t.Cleanup(cleanup)

	c := NewGCPCollector(NewMetricClientFromGCP(client), Options{
		MaxSeries: 3,
		Now:       fixedClock(now),
	})
	_, err := c.Collect(context.Background(), QueryParams{
		ProjectID:  "p",
		MetricType: "svc.example.com/count",
	})
	if err == nil {
		t.Fatal("Collect returned nil error; expected ErrMaxSeriesExceeded")
	}
	if !errors.Is(err, ErrMaxSeriesExceeded) {
		t.Fatalf("err = %v, want errors.Is(ErrMaxSeriesExceeded)", err)
	}
}

// TestCollect_ServerErrorPropagated exercises required test case 7.
func TestCollect_ServerErrorPropagated(t *testing.T) {
	now := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
	fake := &fakeMetricServer{
		listFn: func(_ context.Context, _ *monitoringpb.ListTimeSeriesRequest) (*monitoringpb.ListTimeSeriesResponse, error) {
			return nil, status.Error(codes.PermissionDenied, "no access")
		},
	}
	client, cleanup := startFakeServer(t, fake)
	t.Cleanup(cleanup)

	c := NewGCPCollector(NewMetricClientFromGCP(client), Options{Now: fixedClock(now)})
	_, err := c.Collect(context.Background(), QueryParams{
		ProjectID:  "p",
		MetricType: "x",
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if got := status.Code(err); got != codes.PermissionDenied {
		t.Fatalf("status.Code(err) = %v, want PermissionDenied", got)
	}
}

// TestCollect_EmptyResponse exercises required test case 8.
func TestCollect_EmptyResponse(t *testing.T) {
	now := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
	fake := &fakeMetricServer{
		listFn: func(_ context.Context, _ *monitoringpb.ListTimeSeriesRequest) (*monitoringpb.ListTimeSeriesResponse, error) {
			return &monitoringpb.ListTimeSeriesResponse{}, nil
		},
	}
	client, cleanup := startFakeServer(t, fake)
	t.Cleanup(cleanup)

	c := NewGCPCollector(NewMetricClientFromGCP(client), Options{Now: fixedClock(now)})
	fams, err := c.Collect(context.Background(), QueryParams{
		ProjectID:  "p",
		MetricType: "x",
	})
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if len(fams) != 0 {
		t.Errorf("len(fams) = %d, want 0", len(fams))
	}
}

// TestBuildRequest_DefaultsAndInterval verifies QueryParams defaults and
// the use of the injected clock for the TimeInterval.
func TestBuildRequest_DefaultsAndInterval(t *testing.T) {
	now := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
	c := NewGCPCollector(nil, Options{Now: fixedClock(now)})
	req, err := c.buildRequest(QueryParams{
		ProjectID:  "p",
		MetricType: "x",
	})
	if err != nil {
		t.Fatalf("buildRequest: %v", err)
	}
	if got := req.GetInterval().GetEndTime().AsTime(); !got.Equal(now) {
		t.Errorf("end = %v, want %v", got, now)
	}
	if got := req.GetInterval().GetStartTime().AsTime(); !got.Equal(now.Add(-defaultInterval)) {
		t.Errorf("start = %v, want %v", got, now.Add(-defaultInterval))
	}
	if got := req.GetAggregation().GetAlignmentPeriod().AsDuration(); got != defaultInterval {
		t.Errorf("alignment period = %v, want %v", got, defaultInterval)
	}
	if got := req.GetView(); got != monitoringpb.ListTimeSeriesRequest_FULL {
		t.Errorf("view = %v, want FULL", got)
	}
}

// TestBuildRequest_InvalidEnums covers the 400-mappable paths. Both errors
// must wrap [ErrInvalidAggregation] so the handler can map them to HTTP
// 400 via [errors.Is].
func TestBuildRequest_InvalidEnums(t *testing.T) {
	c := NewGCPCollector(nil, Options{Now: fixedClock(time.Now())})

	_, err := c.buildRequest(QueryParams{
		ProjectID:  "p",
		MetricType: "x",
		Aligner:    "ALIGN_BOGUS",
	})
	if err == nil {
		t.Fatal("expected error for bogus aligner, got nil")
	}
	if !errors.Is(err, ErrInvalidAggregation) {
		t.Errorf("aligner err = %v, want errors.Is(ErrInvalidAggregation)", err)
	}

	_, err = c.buildRequest(QueryParams{
		ProjectID:  "p",
		MetricType: "x",
		Reducer:    "REDUCE_BOGUS",
	})
	if err == nil {
		t.Fatal("expected error for bogus reducer, got nil")
	}
	if !errors.Is(err, ErrInvalidAggregation) {
		t.Errorf("reducer err = %v, want errors.Is(ErrInvalidAggregation)", err)
	}
}

// TestBuildRequest_GroupByOnlyWithReducer asserts GroupByFields is dropped
// when reducer is REDUCE_NONE (the default).
func TestBuildRequest_GroupByOnlyWithReducer(t *testing.T) {
	c := NewGCPCollector(nil, Options{Now: fixedClock(time.Now())})

	req, err := c.buildRequest(QueryParams{
		ProjectID:     "p",
		MetricType:    "x",
		GroupByFields: []string{"resource.label.zone"},
	})
	if err != nil {
		t.Fatalf("buildRequest: %v", err)
	}
	if got := req.GetAggregation().GetGroupByFields(); len(got) != 0 {
		t.Errorf("group_by_fields = %v, want empty (reducer=REDUCE_NONE)", got)
	}

	req, err = c.buildRequest(QueryParams{
		ProjectID:     "p",
		MetricType:    "x",
		Reducer:       "REDUCE_SUM",
		GroupByFields: []string{"resource.label.zone"},
	})
	if err != nil {
		t.Fatalf("buildRequest: %v", err)
	}
	if got := req.GetAggregation().GetGroupByFields(); len(got) != 1 || got[0] != "resource.label.zone" {
		t.Errorf("group_by_fields = %v, want [resource.label.zone]", got)
	}
}

// silence unused-helper warnings if any future test removes references.
var _ = findFamily
