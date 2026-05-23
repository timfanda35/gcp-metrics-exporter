package gmp

import (
	"context"
	"fmt"
	"sync"

	"github.com/timfanda35/gcp-metrics-exporter/internal/auth"
)

// ClientCache lazily constructs and caches one [*HTTPClient] per
// impersonation target, avoiding repeated token-source construction on every
// request. The empty key represents "no impersonation, use base credentials".
//
// Unlike the gRPC-based collector cache, HTTP clients do not hold persistent
// connections that need explicit cleanup, so there is no Close method.
type ClientCache struct {
	authCfg auth.Config

	mu      sync.Mutex
	clients map[string]*HTTPClient
}

// NewClientCache returns an empty cache that builds HTTP clients from the
// supplied base auth config. The config's ImpersonateServiceAccount is used
// as a fallback only; callers pass the per-request target to [ClientCache.Get].
func NewClientCache(authCfg auth.Config) *ClientCache {
	return &ClientCache{
		authCfg: authCfg,
		clients: make(map[string]*HTTPClient),
	}
}

// Get returns the cached [*HTTPClient] for the given impersonation target,
// constructing one on first use. Pass the empty string to use base credentials
// with no per-request impersonation (subject to the cache's base config).
//
// The returned client is owned by the cache; its lifetime matches the cache.
func (c *ClientCache) Get(ctx context.Context, impersonateSA string) (*HTTPClient, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if cli, ok := c.clients[impersonateSA]; ok {
		return cli, nil
	}

	cfg := c.authCfg
	if impersonateSA != "" {
		cfg.ImpersonateServiceAccount = impersonateSA
	}

	ts, err := auth.NewTokenSource(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("gmp: build token source: %w", err)
	}

	cli := New(ts)
	c.clients[impersonateSA] = cli
	return cli, nil
}
