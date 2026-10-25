package gmp

import (
	"context"
	"fmt"
	"strings"
	"time"

	dto "github.com/prometheus/client_model/go"
	"google.golang.org/protobuf/proto"
)

// metricNamePrefix is prepended to every exported metric name; it matches the
// prefix used by the Cloud Monitoring collector.
const metricNamePrefix = "gcp_"

// Collector converts GMP instant query results into Prometheus MetricFamily
// values. All metrics are emitted as GAUGE — the point-in-time nature of an
// instant query carries no cumulative semantics.
type Collector struct {
	client Client
}

// NewCollector constructs a [Collector] backed by the provided [Client].
func NewCollector(client Client) *Collector {
	return &Collector{client: client}
}

// Collect runs an instant PromQL query against GMP at time at and converts
// the resulting vector into [*dto.MetricFamily] slices. Each unique
// __name__ label value becomes one family; samples without __name__ are
// silently dropped. Families preserve the order in which they first appear.
//
// Names and labels are normalised to match the Cloud Monitoring backend: names
// get the "gcp_" prefix, ':' in metric names becomes '_', monitored_resource is exposed as
// resource_type. project_id is left to the sample (add it with a Prometheus
// relabel rule when a series lacks it).
func (c *Collector) Collect(ctx context.Context, project, query string, at time.Time) ([]*dto.MetricFamily, error) {
	samples, err := c.client.Query(ctx, project, query, at)
	if err != nil {
		return nil, err
	}

	families := make(map[string]*dto.MetricFamily)
	order := make([]string, 0, len(samples))

	for _, s := range samples {
		orig, ok := s.Labels["__name__"]
		if !ok || orig == "" {
			continue
		}
		name := metricNamePrefix + strings.ReplaceAll(orig, ":", "_")

		fam, exists := families[name]
		if !exists {
			fam = &dto.MetricFamily{
				Name: proto.String(name),
				Type: dto.MetricType_GAUGE.Enum(),
				Help: proto.String(fmt.Sprintf("GMP metric %s (instant query, exported as gauge)", orig)),
			}
			families[name] = fam
			order = append(order, name)
		}

		labels := normalizeLabels(s.Labels)

		fam.Metric = append(fam.Metric, &dto.Metric{
			Label: labels,
			Gauge: &dto.Gauge{Value: proto.Float64(s.Value)},
		})
	}

	out := make([]*dto.MetricFamily, 0, len(order))
	for _, name := range order {
		out = append(out, families[name])
	}
	return out, nil
}

// normalizeLabels converts a GMP sample's labels into label pairs, dropping
// __name__, renaming monitored_resource to resource_type (unless the sample
// already has one).
func normalizeLabels(in map[string]string) []*dto.LabelPair {
	_, hasResourceType := in["resource_type"]
	labels := make([]*dto.LabelPair, 0, len(in))
	for k, v := range in {
		if k == "__name__" {
			continue
		}
		if k == "monitored_resource" && !hasResourceType {
			k = "resource_type"
		}
		labels = append(labels, &dto.LabelPair{
			Name:  proto.String(k),
			Value: proto.String(v),
		})
	}
	return labels
}
