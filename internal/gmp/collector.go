package gmp

import (
	"context"
	"fmt"
	"time"

	dto "github.com/prometheus/client_model/go"
	"google.golang.org/protobuf/proto"
)

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
func (c *Collector) Collect(ctx context.Context, project, query string, at time.Time) ([]*dto.MetricFamily, error) {
	samples, err := c.client.Query(ctx, project, query, at)
	if err != nil {
		return nil, err
	}

	families := make(map[string]*dto.MetricFamily)
	order := make([]string, 0, len(samples))

	for _, s := range samples {
		name, ok := s.Labels["__name__"]
		if !ok || name == "" {
			continue
		}

		fam, exists := families[name]
		if !exists {
			fam = &dto.MetricFamily{
				Name: proto.String(name),
				Type: dto.MetricType_GAUGE.Enum(),
				Help: proto.String(fmt.Sprintf("GMP metric %s", name)),
			}
			families[name] = fam
			order = append(order, name)
		}

		labels := make([]*dto.LabelPair, 0, len(s.Labels))
		for k, v := range s.Labels {
			if k == "__name__" {
				continue
			}
			k, v := k, v
			labels = append(labels, &dto.LabelPair{
				Name:  proto.String(k),
				Value: proto.String(v),
			})
		}

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
