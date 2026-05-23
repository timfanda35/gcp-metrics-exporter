// Package gmp queries Google Managed Service for Prometheus (GMP) using its
// Prometheus-compatible instant query API and returns results as standard
// Prometheus metric families.
//
// Authentication is provided by an [oauth2.TokenSource]; callers typically
// build one via [auth.NewTokenSource] and cache it per impersonation target
// using [ClientCache].
package gmp

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"golang.org/x/oauth2"
)

const gmpBaseURL = "https://monitoring.googleapis.com/v1"

// Client queries the GMP Prometheus-compatible instant query API.
type Client interface {
	// Query performs an instant PromQL query for the given project at time
	// at. It returns the resulting vector samples or an error. When GMP
	// returns a non-200 response, the error is of type [*APIError].
	Query(ctx context.Context, project, query string, at time.Time) ([]Sample, error)
}

// Sample is a single result from a GMP instant query vector.
type Sample struct {
	// Labels includes __name__ when present in the GMP response.
	Labels    map[string]string
	Value     float64
	Timestamp time.Time
}

// APIError is returned when the GMP API responds with a non-200 HTTP status.
// Callers can inspect StatusCode to decide the appropriate HTTP response code.
type APIError struct {
	StatusCode int
	Msg        string
}

// Error implements the error interface.
func (e *APIError) Error() string {
	return fmt.Sprintf("gmp: API error %d: %s", e.StatusCode, e.Msg)
}

// HTTPClient implements [Client] by calling the GMP Prometheus HTTP API
// authenticated with an OAuth2 token source.
type HTTPClient struct {
	httpClient *http.Client
}

// New returns an [HTTPClient] that signs every request with the supplied
// token source using Bearer token authentication.
func New(tokenSource oauth2.TokenSource) *HTTPClient {
	return &HTTPClient{
		httpClient: &http.Client{
			Transport: &oauth2.Transport{
				Source: tokenSource,
			},
		},
	}
}

// Query performs a GMP instant PromQL query at time at and returns the
// resulting vector samples. The query time is expressed as a Unix timestamp
// (second precision). GMP's staleness window (default 5 minutes) handles
// minor ingest delays; use a non-zero time_offset in the handler for metrics
// with longer delays.
func (c *HTTPClient) Query(ctx context.Context, project, query string, at time.Time) ([]Sample, error) {
	url := fmt.Sprintf("%s/projects/%s/location/global/prometheus/api/v1/query", gmpBaseURL, project)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("gmp: build request: %w", err)
	}

	q := req.URL.Query()
	q.Set("query", query)
	q.Set("time", strconv.FormatFloat(float64(at.Unix()), 'f', 0, 64))
	req.URL.RawQuery = q.Encode()

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("gmp: HTTP request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("gmp: read response body: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		var errPayload struct {
			Error string `json:"error"`
		}
		_ = json.Unmarshal(body, &errPayload)
		msg := errPayload.Error
		if msg == "" {
			msg = resp.Status
		}
		return nil, &APIError{StatusCode: resp.StatusCode, Msg: msg}
	}

	return parseQueryResponse(body)
}

// apiResponse mirrors the Prometheus HTTP API JSON envelope for instant queries.
type apiResponse struct {
	Status string  `json:"status"`
	Error  string  `json:"error,omitempty"`
	Data   apiData `json:"data"`
}

type apiData struct {
	ResultType string      `json:"resultType"`
	Result     []apiSample `json:"result"`
}

type apiSample struct {
	Metric map[string]string  `json:"metric"`
	Value  [2]json.RawMessage `json:"value"` // [unix_timestamp, "value_string"]
}

// parseQueryResponse decodes a GMP API JSON body into a slice of [Sample].
// Malformed individual samples are silently skipped; a corrupt envelope
// returns an error.
func parseQueryResponse(body []byte) ([]Sample, error) {
	var r apiResponse
	if err := json.Unmarshal(body, &r); err != nil {
		return nil, fmt.Errorf("gmp: decode response: %w", err)
	}
	if r.Status != "success" {
		return nil, &APIError{StatusCode: http.StatusBadGateway, Msg: r.Error}
	}

	out := make([]Sample, 0, len(r.Data.Result))
	for _, s := range r.Data.Result {
		if len(s.Value) < 2 {
			continue
		}
		var ts float64
		if err := json.Unmarshal(s.Value[0], &ts); err != nil {
			continue
		}
		var valStr string
		if err := json.Unmarshal(s.Value[1], &valStr); err != nil {
			continue
		}
		val, err := strconv.ParseFloat(valStr, 64)
		if err != nil {
			continue
		}
		out = append(out, Sample{
			Labels:    s.Metric,
			Value:     val,
			Timestamp: time.Unix(int64(ts), 0),
		})
	}
	return out, nil
}
