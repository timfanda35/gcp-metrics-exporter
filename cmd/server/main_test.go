package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// clearAllConfigEnv unsets every env var loadConfig might consult so that
// each test starts from a clean slate. t.Setenv("X", "") would set the var
// to the empty string rather than unsetting it; the loader treats empty
// the same as unset, but unsetting is closer to the documented contract.
func clearAllConfigEnv(t *testing.T) {
	t.Helper()
	for _, k := range []string{
		"PORT",
		"GOOGLE_APPLICATION_CREDENTIALS",
		"DEFAULT_IMPERSONATE_SA",
		"LOG_LEVEL",
		"LOG_FORMAT",
		"SCRAPE_TIMEOUT",
		"MAX_CONCURRENT_SCRAPES",
		"MAX_SERIES_PER_REQUEST",
		"SHUTDOWN_GRACE",
	} {
		t.Setenv(k, "")
	}
}

func TestLoadConfigDefaults(t *testing.T) {
	clearAllConfigEnv(t)

	cfg, err := loadConfig()
	if err != nil {
		t.Fatalf("loadConfig returned error: %v", err)
	}

	want := config{
		Port:                 8080,
		CredentialsFile:      "",
		DefaultImpersonateSA: "",
		LogLevel:             "info",
		LogFormat:            "json",
		ScrapeTimeout:        30 * time.Second,
		MaxConcurrent:        16,
		MaxSeries:            10000,
		ShutdownGrace:        10 * time.Second,
	}
	if cfg != want {
		t.Errorf("loadConfig defaults mismatch:\n got %+v\nwant %+v", cfg, want)
	}
}

func TestLoadConfigOverrides(t *testing.T) {
	cases := []struct {
		name   string
		env    map[string]string
		assert func(t *testing.T, cfg config)
	}{
		{
			name: "PORT",
			env:  map[string]string{"PORT": "9091"},
			assert: func(t *testing.T, cfg config) {
				if cfg.Port != 9091 {
					t.Errorf("Port = %d, want 9091", cfg.Port)
				}
			},
		},
		{
			name: "GOOGLE_APPLICATION_CREDENTIALS",
			env:  map[string]string{"GOOGLE_APPLICATION_CREDENTIALS": "/secrets/sa.json"},
			assert: func(t *testing.T, cfg config) {
				if cfg.CredentialsFile != "/secrets/sa.json" {
					t.Errorf("CredentialsFile = %q, want %q", cfg.CredentialsFile, "/secrets/sa.json")
				}
			},
		},
		{
			name: "DEFAULT_IMPERSONATE_SA",
			env:  map[string]string{"DEFAULT_IMPERSONATE_SA": "reader@example.iam.gserviceaccount.com"},
			assert: func(t *testing.T, cfg config) {
				if cfg.DefaultImpersonateSA != "reader@example.iam.gserviceaccount.com" {
					t.Errorf("DefaultImpersonateSA = %q, want %q", cfg.DefaultImpersonateSA, "reader@example.iam.gserviceaccount.com")
				}
			},
		},
		{
			name: "LOG_LEVEL",
			env:  map[string]string{"LOG_LEVEL": "debug"},
			assert: func(t *testing.T, cfg config) {
				if cfg.LogLevel != "debug" {
					t.Errorf("LogLevel = %q, want debug", cfg.LogLevel)
				}
			},
		},
		{
			name: "LOG_LEVEL_uppercase_normalized",
			env:  map[string]string{"LOG_LEVEL": "WARN"},
			assert: func(t *testing.T, cfg config) {
				if cfg.LogLevel != "warn" {
					t.Errorf("LogLevel = %q, want warn", cfg.LogLevel)
				}
			},
		},
		{
			name: "LOG_FORMAT",
			env:  map[string]string{"LOG_FORMAT": "text"},
			assert: func(t *testing.T, cfg config) {
				if cfg.LogFormat != "text" {
					t.Errorf("LogFormat = %q, want text", cfg.LogFormat)
				}
			},
		},
		{
			name: "SCRAPE_TIMEOUT",
			env:  map[string]string{"SCRAPE_TIMEOUT": "45s"},
			assert: func(t *testing.T, cfg config) {
				if cfg.ScrapeTimeout != 45*time.Second {
					t.Errorf("ScrapeTimeout = %v, want 45s", cfg.ScrapeTimeout)
				}
			},
		},
		{
			name: "MAX_CONCURRENT_SCRAPES",
			env:  map[string]string{"MAX_CONCURRENT_SCRAPES": "32"},
			assert: func(t *testing.T, cfg config) {
				if cfg.MaxConcurrent != 32 {
					t.Errorf("MaxConcurrent = %d, want 32", cfg.MaxConcurrent)
				}
			},
		},
		{
			name: "MAX_SERIES_PER_REQUEST",
			env:  map[string]string{"MAX_SERIES_PER_REQUEST": "5000"},
			assert: func(t *testing.T, cfg config) {
				if cfg.MaxSeries != 5000 {
					t.Errorf("MaxSeries = %d, want 5000", cfg.MaxSeries)
				}
			},
		},
		{
			name: "SHUTDOWN_GRACE",
			env:  map[string]string{"SHUTDOWN_GRACE": "20s"},
			assert: func(t *testing.T, cfg config) {
				if cfg.ShutdownGrace != 20*time.Second {
					t.Errorf("ShutdownGrace = %v, want 20s", cfg.ShutdownGrace)
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			clearAllConfigEnv(t)
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			cfg, err := loadConfig()
			if err != nil {
				t.Fatalf("loadConfig returned error: %v", err)
			}
			tc.assert(t, cfg)
		})
	}
}

func TestLoadConfigValidation(t *testing.T) {
	cases := []struct {
		name        string
		env         map[string]string
		wantSubstrs []string // each must appear in the error message
	}{
		{
			name:        "bad PORT non-numeric",
			env:         map[string]string{"PORT": "abc"},
			wantSubstrs: []string{"PORT", "abc"},
		},
		{
			name:        "bad PORT zero",
			env:         map[string]string{"PORT": "0"},
			wantSubstrs: []string{"PORT", "0"},
		},
		{
			name:        "bad LOG_LEVEL",
			env:         map[string]string{"LOG_LEVEL": "trace"},
			wantSubstrs: []string{"LOG_LEVEL", "trace"},
		},
		{
			name:        "bad LOG_FORMAT",
			env:         map[string]string{"LOG_FORMAT": "yaml"},
			wantSubstrs: []string{"LOG_FORMAT", "yaml"},
		},
		{
			name:        "bad SCRAPE_TIMEOUT no unit",
			env:         map[string]string{"SCRAPE_TIMEOUT": "5"},
			wantSubstrs: []string{"SCRAPE_TIMEOUT", "5"},
		},
		{
			name:        "bad MAX_CONCURRENT_SCRAPES zero",
			env:         map[string]string{"MAX_CONCURRENT_SCRAPES": "0"},
			wantSubstrs: []string{"MAX_CONCURRENT_SCRAPES", "0"},
		},
		{
			name:        "bad MAX_CONCURRENT_SCRAPES negative",
			env:         map[string]string{"MAX_CONCURRENT_SCRAPES": "-3"},
			wantSubstrs: []string{"MAX_CONCURRENT_SCRAPES", "-3"},
		},
		{
			name:        "bad MAX_SERIES_PER_REQUEST negative",
			env:         map[string]string{"MAX_SERIES_PER_REQUEST": "-1"},
			wantSubstrs: []string{"MAX_SERIES_PER_REQUEST", "-1"},
		},
		{
			name:        "bad SHUTDOWN_GRACE",
			env:         map[string]string{"SHUTDOWN_GRACE": "forever"},
			wantSubstrs: []string{"SHUTDOWN_GRACE", "forever"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			clearAllConfigEnv(t)
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			_, err := loadConfig()
			if err == nil {
				t.Fatalf("loadConfig succeeded; expected error mentioning %v", tc.wantSubstrs)
			}
			msg := err.Error()
			for _, want := range tc.wantSubstrs {
				if !strings.Contains(msg, want) {
					t.Errorf("error %q missing substring %q", msg, want)
				}
			}
		})
	}
}

func TestRunHealthcheck(t *testing.T) {
	t.Run("returns 0 on 200 OK", func(t *testing.T) {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"ok"}`))
		}))
		defer ts.Close()

		client := &http.Client{Timeout: 2 * time.Second}
		got := runHealthcheck(ts.URL+"/healthz", client)
		if got != 0 {
			t.Errorf("runHealthcheck = %d, want 0", got)
		}
	})

	t.Run("returns 1 on 500", func(t *testing.T) {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}))
		defer ts.Close()

		client := &http.Client{Timeout: 2 * time.Second}
		got := runHealthcheck(ts.URL+"/healthz", client)
		if got != 1 {
			t.Errorf("runHealthcheck = %d, want 1", got)
		}
	})

	t.Run("returns 1 on connection refused", func(t *testing.T) {
		// 127.0.0.1:1 is reserved (TCPMUX) and almost always closed.
		// The probe should fail quickly and return non-zero.
		client := &http.Client{Timeout: 500 * time.Millisecond}
		got := runHealthcheck("http://127.0.0.1:1/healthz", client)
		if got != 1 {
			t.Errorf("runHealthcheck = %d, want 1", got)
		}
	})

	t.Run("returns 1 on malformed URL", func(t *testing.T) {
		client := &http.Client{Timeout: 500 * time.Millisecond}
		got := runHealthcheck("://not a url", client)
		if got != 1 {
			t.Errorf("runHealthcheck = %d, want 1", got)
		}
	})
}
