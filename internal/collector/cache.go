package collector

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	monitoring "cloud.google.com/go/monitoring/apiv3/v2"
	"google.golang.org/api/option"

	"github.com/timfanda35/gcp-metrics-exporter/internal/auth"
)

// ClientCache lazily constructs and caches one [*monitoring.MetricClient]
// per impersonation target so repeated /metrics scrapes do not pay the
// cost of a fresh gRPC dial each time.
//
// The cache is keyed by the impersonation target service-account email;
// the empty string represents "no impersonation, use the base credentials
// directly". Construction is rare and the underlying client is safe for
// concurrent use, so a single mutex around get-or-insert is sufficient.
//
// This is the only file in the collector package that may import the
// auth package — the conversion code in collector.go must remain free of
// auth dependencies so it stays independently testable with bufconn.
type ClientCache struct {
	authCfg auth.Config

	mu      sync.Mutex
	clients map[string]*monitoring.MetricClient

	// dialer overrides client construction in tests. When nil, the
	// default GCP SDK dialer is used.
	dialer func(ctx context.Context, opts ...option.ClientOption) (*monitoring.MetricClient, error)
}

// NewClientCache returns an empty cache that will build clients from the
// supplied base auth config. The provided config's
// [auth.Config.ImpersonateServiceAccount] is treated as a fallback only;
// callers pass the per-request impersonation target to [ClientCache.Get].
func NewClientCache(authCfg auth.Config) *ClientCache {
	return &ClientCache{
		authCfg: authCfg,
		clients: make(map[string]*monitoring.MetricClient),
	}
}

// Get returns the cached [*monitoring.MetricClient] for the given
// impersonation target, constructing one on first use. Pass the empty
// string to use the base credentials with no impersonation (subject to
// any default impersonation target on the cache's [auth.Config]).
//
// The returned client is owned by the cache; do not call Close on it.
// Use [ClientCache.Close] to release all cached clients at shutdown.
func (c *ClientCache) Get(ctx context.Context, impersonateSA string) (*monitoring.MetricClient, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if cli, ok := c.clients[impersonateSA]; ok {
		return cli, nil
	}

	cfg := c.authCfg
	// Per-request override of the impersonation target; an empty
	// impersonateSA falls back to the cache-level default in cfg.
	if impersonateSA != "" {
		cfg.ImpersonateServiceAccount = impersonateSA
	}

	ts, err := auth.NewTokenSource(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("collector: build token source: %w", err)
	}

	dial := c.dialer
	if dial == nil {
		dial = monitoring.NewMetricClient
	}
	cli, err := dial(ctx, option.WithTokenSource(ts))
	if err != nil {
		return nil, fmt.Errorf("collector: dial monitoring client: %w", err)
	}

	c.clients[impersonateSA] = cli
	return cli, nil
}

// Close releases every cached client. The first non-nil error encountered
// is returned; subsequent close errors are logged via [slog] and
// otherwise discarded so a single misbehaving client cannot prevent the
// rest from being closed.
func (c *ClientCache) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var firstErr error
	for sa, cli := range c.clients {
		if err := cli.Close(); err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("collector: close client for %q: %w", sa, err)
			} else {
				slog.Warn("collector: close client",
					"impersonate_sa", sa,
					"err", err,
				)
			}
		}
		delete(c.clients, sa)
	}
	if firstErr != nil && errors.Is(firstErr, context.Canceled) {
		// Surface but do not wrap further; defensive — Close paths in
		// gRPC rarely return ctx errors.
		return firstErr
	}
	return firstErr
}
