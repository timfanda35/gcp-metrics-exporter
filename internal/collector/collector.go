// Package collector queries GCP Cloud Monitoring and converts the resulting
// time series into Prometheus [*dto.MetricFamily] values.
//
// The package is deliberately stateless: each call to [GCPCollector.Collect]
// performs an on-demand ListTimeSeries RPC and serialises the response.
// All side effects (timeouts, concurrency limiting, HTTP encoding) live in
// the handler layer — this package's only responsibility is the GCP-to-Prom
// translation.
//
// The package depends on the GCP SDK only through the small [MetricClient]
// interface so that tests can swap in a bufconn-backed fake. The real
// adapter lives in [NewMetricClientFromGCP].
package collector

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"strconv"
	"strings"
	"time"

	monitoring "cloud.google.com/go/monitoring/apiv3/v2"
	"cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	dto "github.com/prometheus/client_model/go"
	"google.golang.org/api/iterator"
	distributionpb "google.golang.org/genproto/googleapis/api/distribution"
	metricpb "google.golang.org/genproto/googleapis/api/metric"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ErrMaxSeriesExceeded is returned by [GCPCollector.Collect] when a single
// query would yield more series than [Options.MaxSeries] allows. Callers
// should detect this with [errors.Is] and surface it as HTTP 503 so the
// operator knows to narrow the filter or enable a reducer.
var ErrMaxSeriesExceeded = errors.New("collector: max series exceeded")

// ErrInvalidAggregation is wrapped by [GCPCollector.Collect] when the
// caller-supplied [QueryParams.Aligner] or [QueryParams.Reducer] does not
// match a known [monitoringpb.Aggregation] enum value. Callers should
// detect this with [errors.Is] and surface it as HTTP 400 since the fault
// is in the request, not the upstream API.
var ErrInvalidAggregation = errors.New("collector: invalid aggregation")

// defaultInterval is the query window used when [QueryParams.Interval] is
// zero.
const defaultInterval = 5 * time.Minute

// defaultMaxSeries caps the number of series returned per Collect call.
const defaultMaxSeries = 10000

// QueryParams describes a single Cloud Monitoring time-series query.
//
// Zero-value defaults: Aligner = "ALIGN_MEAN", Reducer = "REDUCE_NONE",
// Interval = 5 minutes, AlignmentPeriod = Interval.
type QueryParams struct {
	// ProjectID is the GCP project whose time series to query. Required.
	ProjectID string

	// MetricType is the fully-qualified GCP metric type, for example
	// "compute.googleapis.com/instance/cpu/utilization". Required. Always
	// emitted as the leading clause of the outgoing filter.
	MetricType string

	// Filter is an additional Cloud Monitoring filter expression that will
	// be AND-composed onto the mandatory metric.type clause. The value is
	// passed through verbatim — no quoting, escaping, or syntax validation
	// is performed by this package. Any syntactic error surfaces as a gRPC
	// InvalidArgument from the upstream API.
	//
	// The composed filter is:
	//
	//	metric.type = "<MetricType>"                       (Filter empty)
	//	metric.type = "<MetricType>" AND (<Filter>)        (Filter set)
	Filter string

	// Aligner is the per-series aligner name, parsed via
	// [monitoringpb.Aggregation_Aligner_value]. Empty means "ALIGN_MEAN".
	Aligner string

	// Reducer is the cross-series reducer name, parsed via
	// [monitoringpb.Aggregation_Reducer_value]. Empty means "REDUCE_NONE".
	Reducer string

	// GroupByFields lists the labels to group by when reducing. It is only
	// forwarded to the API when Reducer is set to something other than the
	// (empty default) "REDUCE_NONE".
	GroupByFields []string

	// Interval is the query window (end - start). Zero means 5 minutes.
	Interval time.Duration

	// AlignmentPeriod is the alignment bucket width. Zero defaults to
	// Interval.
	AlignmentPeriod time.Duration
}

// Options configures a [GCPCollector].
type Options struct {
	// MaxSeries caps the number of time series consumed from a single
	// ListTimeSeries response. When the cap is reached the collector
	// aborts iteration and returns [ErrMaxSeriesExceeded]; partial results
	// are never emitted. Zero defaults to 10000.
	MaxSeries int

	// Now is the clock used to build the [TimeInterval] of outgoing
	// requests. Tests inject a deterministic clock; production leaves
	// it nil and the collector uses [time.Now].
	Now func() time.Time
}

// MetricClient is the narrow interface the collector consumes from the GCP
// monitoring SDK. The single method mirrors
// [*monitoring.MetricClient.ListTimeSeries] and exposes a
// [TimeSeriesIterator] so the production path is exercised verbatim by
// tests using a bufconn-backed fake server.
type MetricClient interface {
	// ListTimeSeries issues a Cloud Monitoring ListTimeSeries RPC and
	// returns an iterator over the response.
	ListTimeSeries(ctx context.Context, req *monitoringpb.ListTimeSeriesRequest) TimeSeriesIterator
}

// TimeSeriesIterator is the small iterator surface the collector consumes.
// [*monitoring.TimeSeriesIterator] from the GCP SDK satisfies it directly.
type TimeSeriesIterator interface {
	// Next returns the next series or [iterator.Done] when the stream is
	// exhausted. Other errors are propagated.
	Next() (*monitoringpb.TimeSeries, error)
}

// gcpMetricClient adapts the high-level [*monitoring.MetricClient] to the
// local [MetricClient] interface so the rest of the package never depends
// on the concrete SDK type.
type gcpMetricClient struct {
	client *monitoring.MetricClient
}

// NewMetricClientFromGCP wraps the provided [*monitoring.MetricClient] so
// it can be injected into a [GCPCollector]. The wrapper is intentionally
// trivial — it only narrows the API surface.
func NewMetricClientFromGCP(c *monitoring.MetricClient) MetricClient {
	return &gcpMetricClient{client: c}
}

// ListTimeSeries delegates to the wrapped SDK client.
func (g *gcpMetricClient) ListTimeSeries(ctx context.Context, req *monitoringpb.ListTimeSeriesRequest) TimeSeriesIterator {
	return g.client.ListTimeSeries(ctx, req)
}

// Collector is the abstraction handlers depend on.
type Collector interface {
	// Collect performs the query described by params and converts the
	// resulting time series into Prometheus metric families.
	Collect(ctx context.Context, params QueryParams) ([]*dto.MetricFamily, error)
}

// GCPCollector implements [Collector] against the Cloud Monitoring v3 API.
type GCPCollector struct {
	client    MetricClient
	maxSeries int
	now       func() time.Time
}

// NewGCPCollector constructs a collector that issues ListTimeSeries calls
// against the supplied [MetricClient]. Defaults from [Options] are
// applied here; callers should pass zero-value Options to opt into them.
func NewGCPCollector(client MetricClient, opts Options) *GCPCollector {
	c := &GCPCollector{
		client:    client,
		maxSeries: opts.MaxSeries,
		now:       opts.Now,
	}
	if c.maxSeries <= 0 {
		c.maxSeries = defaultMaxSeries
	}
	if c.now == nil {
		c.now = time.Now
	}
	return c
}

// Collect builds a [monitoringpb.ListTimeSeriesRequest] from params,
// streams the response, and converts each [monitoringpb.TimeSeries] into
// Prometheus metric families. See the package doc for label and naming
// conventions, and [QueryParams.Filter] for filter composition rules.
func (c *GCPCollector) Collect(ctx context.Context, params QueryParams) ([]*dto.MetricFamily, error) {
	req, err := c.buildRequest(params)
	if err != nil {
		return nil, err
	}

	it := c.client.ListTimeSeries(ctx, req)

	// Bucket series by (MetricKind, ValueType) so that mixed responses
	// (rare, but possible if GCP changes the kind for an aligned series)
	// produce well-formed families. The metric type plus kind/value
	// determines the family name and dto type.
	type familyKey struct {
		name      string
		kind      metricpb.MetricDescriptor_MetricKind
		valueType metricpb.MetricDescriptor_ValueType
	}
	families := make(map[familyKey]*dto.MetricFamily)
	order := make([]familyKey, 0, 1)

	count := 0
	for {
		series, iterErr := it.Next()
		if errors.Is(iterErr, iterator.Done) {
			break
		}
		if iterErr != nil {
			// Preserve the gRPC status so the handler can map to HTTP.
			return nil, iterErr
		}

		count++
		if count > c.maxSeries {
			return nil, fmt.Errorf("%w: limit=%d", ErrMaxSeriesExceeded, c.maxSeries)
		}

		converted, ok := convertSeries(params.MetricType, series)
		if !ok {
			continue
		}

		key := familyKey{
			name:      converted.familyName,
			kind:      series.GetMetricKind(),
			valueType: series.GetValueType(),
		}
		fam, exists := families[key]
		if !exists {
			fam = &dto.MetricFamily{
				Name: proto.String(converted.familyName),
				Type: converted.familyType.Enum(),
				Help: proto.String(fmt.Sprintf("GCP metric %s", params.MetricType)),
			}
			families[key] = fam
			order = append(order, key)
		}
		fam.Metric = append(fam.Metric, converted.metric)
	}

	out := make([]*dto.MetricFamily, 0, len(order))
	for _, k := range order {
		out = append(out, families[k])
	}
	return out, nil
}

// buildRequest assembles the outgoing ListTimeSeriesRequest, applying all
// QueryParams defaults and validating the aligner/reducer enum values.
func (c *GCPCollector) buildRequest(params QueryParams) (*monitoringpb.ListTimeSeriesRequest, error) {
	if params.ProjectID == "" {
		return nil, fmt.Errorf("collector: ProjectID is required")
	}
	if params.MetricType == "" {
		return nil, fmt.Errorf("collector: MetricType is required")
	}

	interval := params.Interval
	if interval <= 0 {
		interval = defaultInterval
	}
	alignment := params.AlignmentPeriod
	if alignment <= 0 {
		alignment = interval
	}

	alignerName := params.Aligner
	if alignerName == "" {
		alignerName = "ALIGN_MEAN"
	}
	alignerVal, ok := monitoringpb.Aggregation_Aligner_value[alignerName]
	if !ok {
		return nil, fmt.Errorf("invalid aligner %q: %w", alignerName, ErrInvalidAggregation)
	}

	reducerName := params.Reducer
	if reducerName == "" {
		reducerName = "REDUCE_NONE"
	}
	reducerVal, ok := monitoringpb.Aggregation_Reducer_value[reducerName]
	if !ok {
		return nil, fmt.Errorf("invalid reducer %q: %w", reducerName, ErrInvalidAggregation)
	}

	now := c.now()
	timeInterval := &monitoringpb.TimeInterval{
		StartTime: timestamppb.New(now.Add(-interval)),
		EndTime:   timestamppb.New(now),
	}

	agg := &monitoringpb.Aggregation{
		AlignmentPeriod:    durationpb.New(alignment),
		PerSeriesAligner:   monitoringpb.Aggregation_Aligner(alignerVal),
		CrossSeriesReducer: monitoringpb.Aggregation_Reducer(reducerVal),
	}
	if reducerName != "REDUCE_NONE" && len(params.GroupByFields) > 0 {
		agg.GroupByFields = append([]string(nil), params.GroupByFields...)
	}

	return &monitoringpb.ListTimeSeriesRequest{
		Name:        "projects/" + params.ProjectID,
		Filter:      composeFilter(params.MetricType, params.Filter),
		Interval:    timeInterval,
		Aggregation: agg,
		View:        monitoringpb.ListTimeSeriesRequest_FULL,
	}, nil
}

// composeFilter applies the rule documented on [QueryParams.Filter].
func composeFilter(metricType, userFilter string) string {
	base := fmt.Sprintf("metric.type = %q", metricType)
	userFilter = strings.TrimSpace(userFilter)
	if userFilter == "" {
		return base
	}
	return base + " AND (" + userFilter + ")"
}

// convertedMetric carries the Prometheus-side artefacts produced from a
// single GCP TimeSeries.
type convertedMetric struct {
	familyName string
	familyType dto.MetricType
	metric     *dto.Metric
}

// convertSeries translates a single TimeSeries into a (family-name,
// metric) pair. Returns ok=false for unsupported value types so the caller
// can skip the series without aborting the whole response.
func convertSeries(metricType string, series *monitoringpb.TimeSeries) (convertedMetric, bool) {
	name := promMetricName(metricType)
	labels := buildLabelPairs(series)

	kind := series.GetMetricKind()
	valueType := series.GetValueType()

	switch valueType {
	case metricpb.MetricDescriptor_DISTRIBUTION:
		// Histograms render the most-recent point in cumulative form.
		points := series.GetPoints()
		if len(points) == 0 {
			return convertedMetric{}, false
		}
		dist := points[0].GetValue().GetDistributionValue()
		if dist == nil {
			return convertedMetric{}, false
		}
		hist := buildHistogram(dist)
		return convertedMetric{
			familyName: name,
			familyType: dto.MetricType_HISTOGRAM,
			metric: &dto.Metric{
				Label:     labels,
				Histogram: hist,
			},
		}, true

	case metricpb.MetricDescriptor_INT64, metricpb.MetricDescriptor_DOUBLE:
		// fall through to scalar handling below.
	default:
		// BOOL, STRING, MONEY, UNSPECIFIED — not representable as
		// Prometheus scalars without lossy re-encoding. Skip.
		slog.Debug("collector: skipping series with unsupported value type",
			"metric_type", metricType,
			"value_type", valueType.String(),
			"metric_kind", kind.String(),
		)
		return convertedMetric{}, false
	}

	switch kind {
	case metricpb.MetricDescriptor_GAUGE:
		points := series.GetPoints()
		if len(points) == 0 {
			return convertedMetric{}, false
		}
		val := scalarValue(points[0], valueType)
		return convertedMetric{
			familyName: name,
			familyType: dto.MetricType_GAUGE,
			metric: &dto.Metric{
				Label: labels,
				Gauge: &dto.Gauge{Value: proto.Float64(val)},
			},
		}, true

	case metricpb.MetricDescriptor_CUMULATIVE:
		points := series.GetPoints()
		if len(points) == 0 {
			return convertedMetric{}, false
		}
		// Cloud Monitoring returns most-recent-first; the latest value is
		// the cumulative count we want.
		val := scalarValue(points[0], valueType)
		startUnix := pointStartUnix(points[0])
		labels = appendLabel(labels, "start_time_unix", strconv.FormatInt(startUnix, 10))
		return convertedMetric{
			familyName: name,
			familyType: dto.MetricType_COUNTER,
			metric: &dto.Metric{
				Label:   labels,
				Counter: &dto.Counter{Value: proto.Float64(val)},
			},
		}, true

	case metricpb.MetricDescriptor_DELTA:
		// DELTA metrics: GCP returns one delta value per alignment bucket.
		// We sum across all returned points so a Prometheus counter sees
		// the total delta covered by the response window. The earliest
		// point's start time anchors the synthetic counter so a restart
		// (start time changes) appears as a new series and Prometheus does
		// not report a false reset.
		points := series.GetPoints()
		if len(points) == 0 {
			return convertedMetric{}, false
		}
		var sum float64
		for _, p := range points {
			sum += scalarValue(p, valueType)
		}
		// Points are most-recent-first; earliest is the last element.
		earliest := points[len(points)-1]
		startUnix := pointStartUnix(earliest)
		labels = appendLabel(labels, "start_time_unix", strconv.FormatInt(startUnix, 10))
		return convertedMetric{
			familyName: name,
			familyType: dto.MetricType_COUNTER,
			metric: &dto.Metric{
				Label:   labels,
				Counter: &dto.Counter{Value: proto.Float64(sum)},
			},
		}, true

	default:
		slog.Debug("collector: skipping series with unsupported metric kind",
			"metric_type", metricType,
			"value_type", valueType.String(),
			"metric_kind", kind.String(),
		)
		return convertedMetric{}, false
	}
}

// scalarValue extracts a float64 out of a TypedValue, choosing between the
// int64 and double oneof fields according to the series' declared
// ValueType.
func scalarValue(p *monitoringpb.Point, valueType metricpb.MetricDescriptor_ValueType) float64 {
	v := p.GetValue()
	switch valueType {
	case metricpb.MetricDescriptor_INT64:
		return float64(v.GetInt64Value())
	case metricpb.MetricDescriptor_DOUBLE:
		return v.GetDoubleValue()
	default:
		return 0
	}
}

// pointStartUnix returns the Unix start time of a Point, defaulting to 0
// when the interval or start timestamp is missing (defensive — GCP always
// sets it for cumulative/delta series).
func pointStartUnix(p *monitoringpb.Point) int64 {
	if p == nil {
		return 0
	}
	iv := p.GetInterval()
	if iv == nil {
		return 0
	}
	st := iv.GetStartTime()
	if st == nil {
		return 0
	}
	return st.AsTime().Unix()
}

// buildLabelPairs returns Prometheus label pairs derived from the series'
// metric labels, resource labels, and the resource type. Order is
// deterministic only insofar as the Go map iteration is — Prometheus does
// not require any particular label ordering, and [expfmt] will sort
// alphabetically when rendering text format.
func buildLabelPairs(series *monitoringpb.TimeSeries) []*dto.LabelPair {
	metricLabels := series.GetMetric().GetLabels()
	resource := series.GetResource()
	resLabels := resource.GetLabels()

	out := make([]*dto.LabelPair, 0, len(metricLabels)+len(resLabels)+1)
	for k, v := range metricLabels {
		out = append(out, &dto.LabelPair{
			Name:  proto.String(k),
			Value: proto.String(v),
		})
	}
	for k, v := range resLabels {
		out = append(out, &dto.LabelPair{
			Name:  proto.String(k),
			Value: proto.String(v),
		})
	}
	if resource != nil {
		out = append(out, &dto.LabelPair{
			Name:  proto.String("resource_type"),
			Value: proto.String(resource.GetType()),
		})
	}
	return out
}

// appendLabel returns a new slice with a fresh label pair appended; the
// input slice is not modified beyond what append normally does.
func appendLabel(labels []*dto.LabelPair, name, value string) []*dto.LabelPair {
	return append(labels, &dto.LabelPair{
		Name:  proto.String(name),
		Value: proto.String(value),
	})
}

// promMetricName converts a GCP metric type into a Prometheus-safe metric
// name by replacing "/", ".", and "-" with "_". No fixed prefix is added —
// the GCP namespace already disambiguates ("compute_googleapis_com_...").
func promMetricName(metricType string) string {
	var b strings.Builder
	b.Grow(len(metricType))
	for _, r := range metricType {
		switch r {
		case '/', '.', '-':
			b.WriteRune('_')
		default:
			b.WriteRune(r)
		}
	}
	return b.String()
}

// buildHistogram converts a GCP [*distributionpb.Distribution] into a
// Prometheus [*dto.Histogram].
//
// The conversion is:
//   - bucket upper bounds are derived per the GCP bucket layout (linear,
//     exponential, or explicit),
//   - GCP's per-bucket counts are summed into Prometheus's cumulative
//     counts,
//   - SampleSum is approximated as Mean * Count (GCP does not expose the
//     raw sum; this is the conventional derivation),
//   - the implicit +Inf bucket is always emitted with cumulative count
//     equal to the total sample count.
func buildHistogram(dist *distributionpb.Distribution) *dto.Histogram {
	bounds := bucketBounds(dist.GetBucketOptions())
	counts := dist.GetBucketCounts()

	// The cumulative count for the +Inf bucket must equal SampleCount,
	// even if individual BucketCounts are missing. We accumulate per-
	// bucket counts and synthesize the trailing +Inf bucket.
	buckets := make([]*dto.Bucket, 0, len(bounds)+1)
	var cumulative uint64
	for i, ub := range bounds {
		var c int64
		if i < len(counts) {
			c = counts[i]
		}
		if c < 0 {
			c = 0
		}
		cumulative += uint64(c)
		buckets = append(buckets, &dto.Bucket{
			UpperBound:      proto.Float64(ub),
			CumulativeCount: proto.Uint64(cumulative),
		})
	}
	// Any bucket counts beyond the last finite bound (e.g. an explicit
	// "+Inf" overflow bucket from GCP) roll into the +Inf bucket.
	for i := len(bounds); i < len(counts); i++ {
		c := counts[i]
		if c < 0 {
			c = 0
		}
		cumulative += uint64(c)
	}
	// Total sample count: prefer the distribution's authoritative count
	// when it exceeds what we accumulated (e.g., when all per-bucket
	// counts are zero but Count > 0 — defensive).
	totalCount := cumulative
	if dc := dist.GetCount(); dc > 0 && uint64(dc) > totalCount {
		totalCount = uint64(dc)
	}

	// Always emit a +Inf bucket so the output is well-formed.
	buckets = append(buckets, &dto.Bucket{
		UpperBound:      proto.Float64(math.Inf(1)),
		CumulativeCount: proto.Uint64(totalCount),
	})

	sampleSum := dist.GetMean() * float64(dist.GetCount())

	return &dto.Histogram{
		SampleCount: proto.Uint64(totalCount),
		SampleSum:   proto.Float64(sampleSum),
		Bucket:      buckets,
	}
}

// bucketBounds returns the finite upper bounds for the bucket layout, per
// the simplified contract documented in the package spec:
//
//	Linear:        offset + width*1, offset + width*2, ..., offset + width*N
//	Exponential:   scale * growthFactor^1, ..., scale * growthFactor^N
//	Explicit:      bounds verbatim
//
// The +Inf overflow bucket is always implicit and is appended by
// [buildHistogram], not here.
func bucketBounds(opt *distributionpb.Distribution_BucketOptions) []float64 {
	if opt == nil {
		return nil
	}
	switch o := opt.GetOptions().(type) {
	case *distributionpb.Distribution_BucketOptions_LinearBuckets:
		lin := o.LinearBuckets
		if lin == nil {
			return nil
		}
		n := int(lin.GetNumFiniteBuckets())
		if n < 0 {
			n = 0
		}
		out := make([]float64, n)
		for i := 0; i < n; i++ {
			out[i] = lin.GetOffset() + lin.GetWidth()*float64(i+1)
		}
		return out
	case *distributionpb.Distribution_BucketOptions_ExponentialBuckets:
		exp := o.ExponentialBuckets
		if exp == nil {
			return nil
		}
		n := int(exp.GetNumFiniteBuckets())
		if n < 0 {
			n = 0
		}
		out := make([]float64, n)
		factor := 1.0
		for i := 0; i < n; i++ {
			factor *= exp.GetGrowthFactor()
			out[i] = exp.GetScale() * factor
		}
		return out
	case *distributionpb.Distribution_BucketOptions_ExplicitBuckets:
		exp := o.ExplicitBuckets
		if exp == nil {
			return nil
		}
		bounds := exp.GetBounds()
		out := make([]float64, len(bounds))
		copy(out, bounds)
		return out
	default:
		return nil
	}
}
