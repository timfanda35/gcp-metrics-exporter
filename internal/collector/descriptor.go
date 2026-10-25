package collector

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// DescriptorCache remembers the unit of each metric descriptor per
// (project, metric type) for the life of the process. ListTimeSeries does not
// return units, so the collector looks them up once with GetMetricDescriptor.
// Share one cache across collectors: collectors are built per request.
type DescriptorCache struct {
	m sync.Map // key string -> unit string ("" when the descriptor is unavailable)
}

// NewDescriptorCache returns an empty [DescriptorCache].
func NewDescriptorCache() *DescriptorCache {
	return &DescriptorCache{}
}

func descriptorKey(project, metricType string) string {
	return project + "|" + metricType
}

// unit returns the base unit of the metric, or "" when it cannot be
// determined. Successful lookups and permanent failures (not found, no
// permission, unsupported) are cached; transient errors are not, so a later
// scrape retries. A failed lookup never fails the scrape.
func (c *GCPCollector) unit(ctx context.Context, project, metricType string) string {
	key := descriptorKey(project, metricType)
	if v, ok := c.descriptors.m.Load(key); ok {
		return v.(string)
	}
	md, err := c.client.GetMetricDescriptor(ctx, &monitoringpb.GetMetricDescriptorRequest{
		Name: fmt.Sprintf("projects/%s/metricDescriptors/%s", project, metricType),
	})
	if err != nil {
		switch status.Code(err) {
		case codes.NotFound, codes.PermissionDenied, codes.Unimplemented:
			c.descriptors.m.Store(key, "")
		}
		return ""
	}
	c.descriptors.m.Store(key, md.GetUnit())
	return md.GetUnit()
}

// resultUnit derives the unit of the returned values from the metric's base
// unit and the aggregation applied: ALIGN_RATE divides by time, counts and
// fractions become dimensionless, percent change is a percentage; everything
// else keeps the base unit. The reducer is applied after the aligner.
func resultUnit(base string, aligner monitoringpb.Aggregation_Aligner, reducer monitoringpb.Aggregation_Reducer) string {
	unit := base
	switch aligner {
	case monitoringpb.Aggregation_ALIGN_RATE:
		if unit == "" {
			unit = "1"
		}
		unit += "/s"
	case monitoringpb.Aggregation_ALIGN_COUNT,
		monitoringpb.Aggregation_ALIGN_COUNT_TRUE,
		monitoringpb.Aggregation_ALIGN_COUNT_FALSE,
		monitoringpb.Aggregation_ALIGN_FRACTION_TRUE:
		unit = "1"
	case monitoringpb.Aggregation_ALIGN_PERCENT_CHANGE:
		unit = "%"
	}
	switch reducer {
	case monitoringpb.Aggregation_REDUCE_COUNT,
		monitoringpb.Aggregation_REDUCE_COUNT_TRUE,
		monitoringpb.Aggregation_REDUCE_COUNT_FALSE,
		monitoringpb.Aggregation_REDUCE_FRACTION_TRUE:
		unit = "1"
	}
	return unit
}

// helpText builds the # HELP line: the metric type plus how the value was
// produced, so series of the same name from different settings can be told
// apart in the Prometheus UI.
func helpText(metricType string, agg *monitoringpb.Aggregation, baseUnit string) string {
	parts := []string{
		"aligner=" + agg.GetPerSeriesAligner().String(),
		"alignment_period=" + agg.GetAlignmentPeriod().AsDuration().String(),
	}
	if agg.GetCrossSeriesReducer() != monitoringpb.Aggregation_REDUCE_NONE {
		parts = append(parts, "reducer="+agg.GetCrossSeriesReducer().String())
	}
	if u := resultUnit(baseUnit, agg.GetPerSeriesAligner(), agg.GetCrossSeriesReducer()); baseUnit != "" && u != "" {
		parts = append(parts, "unit="+u)
	}
	return fmt.Sprintf("GCP metric %s (%s)", metricType, strings.Join(parts, ", "))
}
