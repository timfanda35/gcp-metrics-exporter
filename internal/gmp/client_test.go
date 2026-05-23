package gmp

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestParseQueryResponse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		body        string
		wantSamples []Sample
		wantErr     bool
	}{
		{
			name: "success vector",
			body: `{
				"status": "success",
				"data": {
					"resultType": "vector",
					"result": [
						{
							"metric": {"__name__": "up", "job": "my-app", "instance": "0"},
							"value": [1609459200, "1"]
						},
						{
							"metric": {"__name__": "up", "job": "other", "instance": "1"},
							"value": [1609459200, "0"]
						}
					]
				}
			}`,
			wantSamples: []Sample{
				{
					Labels:    map[string]string{"__name__": "up", "job": "my-app", "instance": "0"},
					Value:     1,
					Timestamp: time.Unix(1609459200, 0),
				},
				{
					Labels:    map[string]string{"__name__": "up", "job": "other", "instance": "1"},
					Value:     0,
					Timestamp: time.Unix(1609459200, 0),
				},
			},
		},
		{
			name: "empty result",
			body: `{"status":"success","data":{"resultType":"vector","result":[]}}`,
		},
		{
			name:    "status error",
			body:    `{"status":"error","errorType":"bad_data","error":"invalid query"}`,
			wantErr: true,
		},
		{
			name:    "malformed JSON",
			body:    `not json`,
			wantErr: true,
		},
		{
			name: "float timestamp",
			body: `{
				"status": "success",
				"data": {
					"resultType": "vector",
					"result": [{"metric":{"__name__":"x"},"value":[1609459200.5,"42.5"]}]
				}
			}`,
			wantSamples: []Sample{
				{
					Labels:    map[string]string{"__name__": "x"},
					Value:     42.5,
					Timestamp: time.Unix(1609459200, 0),
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := parseQueryResponse([]byte(tc.body))
			if (err != nil) != tc.wantErr {
				t.Fatalf("parseQueryResponse() error = %v, wantErr %v", err, tc.wantErr)
			}
			if tc.wantErr {
				return
			}
			if len(got) != len(tc.wantSamples) {
				t.Fatalf("got %d samples, want %d", len(got), len(tc.wantSamples))
			}
			for i, s := range got {
				want := tc.wantSamples[i]
				if s.Value != want.Value {
					t.Errorf("sample[%d].Value = %v, want %v", i, s.Value, want.Value)
				}
				if !s.Timestamp.Equal(want.Timestamp) {
					t.Errorf("sample[%d].Timestamp = %v, want %v", i, s.Timestamp, want.Timestamp)
				}
				for k, v := range want.Labels {
					if got := s.Labels[k]; got != v {
						t.Errorf("sample[%d].Labels[%q] = %q, want %q", i, k, got, v)
					}
				}
			}
		})
	}
}

func TestHTTPClient_Query(t *testing.T) {
	t.Parallel()

	successBody := func(name, value string) []byte {
		payload := map[string]any{
			"status": "success",
			"data": map[string]any{
				"resultType": "vector",
				"result": []any{
					map[string]any{
						"metric": map[string]string{"__name__": name, "job": "test"},
						"value":  []any{1609459200, value},
					},
				},
			},
		}
		b, _ := json.Marshal(payload)
		return b
	}

	tests := []struct {
		name       string
		serverFn   http.HandlerFunc
		wantSample *Sample
		wantErrFn  func(err error) bool
	}{
		{
			name: "success",
			serverFn: func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Query().Get("query") == "" {
					t.Errorf("query param missing")
				}
				if r.URL.Query().Get("time") == "" {
					t.Errorf("time param missing")
				}
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write(successBody("http_requests_total", "42"))
			},
			wantSample: &Sample{
				Labels: map[string]string{"__name__": "http_requests_total", "job": "test"},
				Value:  42,
			},
		},
		{
			name: "GMP 400",
			serverFn: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(`{"error":"bad query syntax"}`))
			},
			wantErrFn: func(err error) bool {
				var apiErr *APIError
				return isAPIError(err, &apiErr) && apiErr.StatusCode == 400
			},
		},
		{
			name: "GMP 403",
			serverFn: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusForbidden)
				_, _ = w.Write([]byte(`{"error":"permission denied"}`))
			},
			wantErrFn: func(err error) bool {
				var apiErr *APIError
				return isAPIError(err, &apiErr) && apiErr.StatusCode == 403
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := httptest.NewServer(tc.serverFn)
			defer srv.Close()

			// Override gmpBaseURL for test using a client with custom transport.
			cli := &HTTPClient{httpClient: srv.Client()}
			// Patch the URL by constructing the request path manually via a
			// wrapper that intercepts and rewrites the request URL.
			cli.httpClient.Transport = &urlRewriter{
				base:     srv.Client().Transport,
				original: gmpBaseURL,
				replace:  srv.URL,
			}

			at := time.Unix(1609459200, 0)
			got, err := cli.Query(context.Background(), "my-project", "up", at)

			if tc.wantErrFn != nil {
				if !tc.wantErrFn(err) {
					t.Fatalf("error mismatch: got %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tc.wantSample != nil {
				if len(got) == 0 {
					t.Fatal("got no samples, want at least one")
				}
				if got[0].Value != tc.wantSample.Value {
					t.Errorf("Value = %v, want %v", got[0].Value, tc.wantSample.Value)
				}
			}
		})
	}
}

// urlRewriter replaces a URL prefix in outgoing requests — used to redirect
// production GMP URLs to the test server without changing the client code.
type urlRewriter struct {
	base     http.RoundTripper
	original string
	replace  string
}

func (u *urlRewriter) RoundTrip(req *http.Request) (*http.Response, error) {
	clone := req.Clone(req.Context())
	raw := clone.URL.String()
	if len(raw) >= len(u.original) && raw[:len(u.original)] == u.original {
		newURL := u.replace + raw[len(u.original):]
		parsed, err := clone.URL.Parse(newURL)
		if err != nil {
			return nil, err
		}
		clone.URL = parsed
		clone.Host = parsed.Host
	}
	if u.base == nil {
		return http.DefaultTransport.RoundTrip(clone)
	}
	return u.base.RoundTrip(clone)
}

// isAPIError is a helper for checking APIError in tests without importing
// errors.As repeatedly.
func isAPIError(err error, target **APIError) bool {
	if err == nil || target == nil {
		return false
	}
	if ae, ok := err.(*APIError); ok {
		*target = ae
		return true
	}
	return false
}
