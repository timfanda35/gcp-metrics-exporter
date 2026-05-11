// Command server is the GCP Metrics Exporter HTTP entry point.
//
// It wires the auth, collector, and handler packages into a single
// http.Server, applies graceful-shutdown semantics on SIGINT / SIGTERM, and
// exposes a self-targeted /healthz probe (`--healthcheck`) for use as a
// container HEALTHCHECK command.
//
// Configuration is read entirely from environment variables — see CLAUDE.md
// for the documented matrix and defaults.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/timfanda35/gcp-metrics-exporter/internal/auth"
	"github.com/timfanda35/gcp-metrics-exporter/internal/collector"
	"github.com/timfanda35/gcp-metrics-exporter/internal/handler"
)

// config is the resolved, validated set of process-level settings derived
// from environment variables. All durations and integers are pre-parsed so
// the rest of main() does not have to repeat type conversion.
type config struct {
	Port                 int
	CredentialsFile      string
	DefaultImpersonateSA string
	LogLevel             string
	LogFormat            string
	ScrapeTimeout        time.Duration
	MaxConcurrent        int
	MaxSeries            int
	ShutdownGrace        time.Duration
}

// defaultPort is the listen port used when PORT is unset or empty.
const defaultPort = 8080

// healthcheckTimeout caps the loopback HTTP probe issued by --healthcheck.
const healthcheckTimeout = 2 * time.Second

// loadConfig parses every supported environment variable, applies defaults,
// and validates each value. It returns a fully populated config or an
// error mentioning the offending variable. The function has no side
// effects beyond reading os.Getenv.
func loadConfig() (config, error) {
	cfg := config{
		Port:                 defaultPort,
		CredentialsFile:      os.Getenv("GOOGLE_APPLICATION_CREDENTIALS"),
		DefaultImpersonateSA: os.Getenv("DEFAULT_IMPERSONATE_SA"),
		LogLevel:             "info",
		LogFormat:            "json",
		ScrapeTimeout:        30 * time.Second,
		MaxConcurrent:        16,
		MaxSeries:            10000,
		ShutdownGrace:        10 * time.Second,
	}

	if v := os.Getenv("PORT"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 || n > 65535 {
			return config{}, fmt.Errorf("invalid PORT %q: must be an integer in 1..65535", v)
		}
		cfg.Port = n
	}

	if v := os.Getenv("LOG_LEVEL"); v != "" {
		switch strings.ToLower(v) {
		case "debug", "info", "warn", "error":
			cfg.LogLevel = strings.ToLower(v)
		default:
			return config{}, fmt.Errorf("invalid LOG_LEVEL %q: must be one of debug, info, warn, error", v)
		}
	}

	if v := os.Getenv("LOG_FORMAT"); v != "" {
		switch strings.ToLower(v) {
		case "json", "text":
			cfg.LogFormat = strings.ToLower(v)
		default:
			return config{}, fmt.Errorf("invalid LOG_FORMAT %q: must be one of json, text", v)
		}
	}

	if v := os.Getenv("SCRAPE_TIMEOUT"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d <= 0 {
			return config{}, fmt.Errorf("invalid SCRAPE_TIMEOUT %q: must be a positive Go duration", v)
		}
		cfg.ScrapeTimeout = d
	}

	if v := os.Getenv("MAX_CONCURRENT_SCRAPES"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			return config{}, fmt.Errorf("invalid MAX_CONCURRENT_SCRAPES %q: must be a positive integer", v)
		}
		cfg.MaxConcurrent = n
	}

	if v := os.Getenv("MAX_SERIES_PER_REQUEST"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			return config{}, fmt.Errorf("invalid MAX_SERIES_PER_REQUEST %q: must be a positive integer", v)
		}
		cfg.MaxSeries = n
	}

	if v := os.Getenv("SHUTDOWN_GRACE"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d <= 0 {
			return config{}, fmt.Errorf("invalid SHUTDOWN_GRACE %q: must be a positive Go duration", v)
		}
		cfg.ShutdownGrace = d
	}

	return cfg, nil
}

// buildLogger constructs a *slog.Logger from the validated logLevel and
// logFormat strings. logLevel must be one of debug/info/warn/error and
// logFormat must be json or text — loadConfig guarantees both.
func buildLogger(logLevel, logFormat string) *slog.Logger {
	var level slog.Level
	switch logLevel {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}
	opts := &slog.HandlerOptions{Level: level}

	var h slog.Handler
	if logFormat == "text" {
		h = slog.NewTextHandler(os.Stderr, opts)
	} else {
		h = slog.NewJSONHandler(os.Stderr, opts)
	}
	return slog.New(h)
}

// runHealthcheck issues a single GET against url with the provided client
// and returns a process exit code: 0 on HTTP 200, 1 otherwise. The HTTP
// request is governed by a context.WithTimeout(healthcheckTimeout).
//
// The function is factored out of main so tests can drive it against a
// stub httptest.Server without invoking os.Exit.
func runHealthcheck(url string, client *http.Client) int {
	ctx, cancel := context.WithTimeout(context.Background(), healthcheckTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 1
	}
	resp, err := client.Do(req)
	if err != nil {
		return 1
	}
	defer resp.Body.Close()
	// Drain the body so the connection can be reused / closed cleanly,
	// even though we exit immediately afterwards.
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode == http.StatusOK {
		return 0
	}
	return 1
}

func main() {
	// Parse the --healthcheck flag first so the probe path never builds
	// the GCP client cache. Using a private FlagSet keeps us out of the
	// global flag.CommandLine's way and avoids surprising other callers
	// that might add flags later.
	fs := flag.NewFlagSet("gcp-metrics-exporter", flag.ExitOnError)
	healthcheck := fs.Bool("healthcheck", false, "issue a loopback GET /healthz against this exporter and exit")
	if err := fs.Parse(os.Args[1:]); err != nil {
		// flag.ExitOnError already terminates; defensive in case it is
		// changed in the future.
		os.Exit(2)
	}

	if *healthcheck {
		// Resolve PORT silently — failures collapse to "unhealthy".
		port := defaultPort
		if v := os.Getenv("PORT"); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 65535 {
				port = n
			}
		}
		url := fmt.Sprintf("http://127.0.0.1:%d/healthz", port)
		os.Exit(runHealthcheck(url, &http.Client{Timeout: healthcheckTimeout}))
	}

	cfg, err := loadConfig()
	if err != nil {
		// The logger has not been built yet, so use a minimal one that
		// matches what the rest of the process would have produced for
		// error-level entries.
		fallback := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
		fallback.Error("invalid configuration", slog.String("err", err.Error()))
		os.Exit(1)
	}

	logger := buildLogger(cfg.LogLevel, cfg.LogFormat)
	slog.SetDefault(logger)

	// Auth config: forward CredentialsFile and the cache-level fallback
	// impersonation target. Scopes are left nil so the auth package's
	// monitoring-read scope default applies.
	authCfg := auth.Config{
		CredentialsFile:           cfg.CredentialsFile,
		ImpersonateServiceAccount: cfg.DefaultImpersonateSA,
		Scopes:                    nil,
	}

	cache := collector.NewClientCache(authCfg)

	limits := handler.Limits{
		ScrapeTimeout:        cfg.ScrapeTimeout,
		MaxConcurrent:        cfg.MaxConcurrent,
		MaxSeries:            cfg.MaxSeries,
		DefaultImpersonateSA: cfg.DefaultImpersonateSA,
	}

	// Factory closes over the cache so each request reuses the SDK
	// client for its impersonation target. Lazy: the first call per SA
	// performs auth discovery + a gRPC dial; subsequent calls hit the
	// in-memory cache directly.
	factory := func(ctx context.Context, sa string) (collector.Collector, error) {
		client, err := cache.Get(ctx, sa)
		if err != nil {
			return nil, err
		}
		return collector.NewGCPCollector(
			collector.NewMetricClientFromGCP(client),
			collector.Options{MaxSeries: limits.MaxSeries},
		), nil
	}

	metricsHandler := handler.NewMetricsHandler(factory, limits, logger)

	mux := http.NewServeMux()
	mux.Handle("/metrics", metricsHandler)
	mux.HandleFunc("/healthz", handler.HandleHealthz)

	srv := &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.Port),
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second, // mitigate Slowloris
	}

	// Trap SIGINT / SIGTERM. NotifyContext returns a context that is
	// cancelled on the first signal; we use it as the trigger for the
	// graceful-shutdown sequence below.
	sigCtx, stopSignals := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stopSignals()

	listenErrCh := make(chan error, 1)
	go func() {
		listenErrCh <- srv.ListenAndServe()
	}()

	logger.Info("server started",
		slog.Int("port", cfg.Port),
		slog.String("log_level", cfg.LogLevel),
		slog.String("log_format", cfg.LogFormat),
		slog.Duration("scrape_timeout", cfg.ScrapeTimeout),
		slog.Int("max_concurrent", cfg.MaxConcurrent),
		slog.Int("max_series", cfg.MaxSeries),
		slog.Duration("shutdown_grace", cfg.ShutdownGrace),
		slog.Bool("default_impersonate_sa_set", cfg.DefaultImpersonateSA != ""),
	)

	// Block on either a fatal listener error or a shutdown signal.
	select {
	case err := <-listenErrCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("listener failed", slog.String("err", err.Error()))
			// Best-effort cache cleanup before exit.
			if cerr := cache.Close(); cerr != nil {
				logger.Warn("client cache close after listener failure", slog.String("err", cerr.Error()))
			}
			os.Exit(1)
		}
	case <-sigCtx.Done():
		logger.Info("shutdown signal received, draining in-flight requests",
			slog.Duration("grace", cfg.ShutdownGrace),
		)

		shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownGrace)
		defer cancel()

		exitCode := 0
		if err := srv.Shutdown(shutdownCtx); err != nil {
			logger.Error("graceful shutdown failed", slog.String("err", err.Error()))
			exitCode = 1
		}
		// Always release cached gRPC connections, even if Shutdown
		// returned an error — leaving them open would mask the failure
		// behind a leaking process.
		if err := cache.Close(); err != nil {
			logger.Warn("client cache close", slog.String("err", err.Error()))
		}
		// Drain the listener goroutine so we do not leak it on exit.
		if err := <-listenErrCh; err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("listener returned error during shutdown", slog.String("err", err.Error()))
			exitCode = 1
		}

		logger.Info("shutdown complete")
		if exitCode != 0 {
			os.Exit(exitCode)
		}
	}
}
