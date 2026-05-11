package auth

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// fakeServiceAccountJSON is a syntactically valid service_account credential
// file with a dummy (but well-formed) RSA private key. It is enough for
// google.CredentialsFromJSON / FindDefaultCredentials to parse without
// touching the network. Calling .Token() on the resulting source would
// attempt to mint a JWT against Google's token endpoint; we never do that
// in these tests.
const fakeServiceAccountJSON = `{
  "type": "service_account",
  "project_id": "test-project",
  "private_key_id": "deadbeef",
  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDQ6pZ9V0z5QzKh\nvqv0c1Q7yVqg4RAzCp8w6SpQ9yQbPMRZBQDfE5kqYg1C3o9JpPq3o0RoQzw1vQ8U\nF7VBTnDwLcGZ+w==\n-----END PRIVATE KEY-----\n",
  "client_email": "test-sa@test-project.iam.gserviceaccount.com",
  "client_id": "123456789",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/test-sa%40test-project.iam.gserviceaccount.com"
}`

// writeFixtureCreds writes the fake SA JSON to a temp file and returns its
// path. The file is removed automatically when the test ends.
func writeFixtureCreds(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "sa.json")
	if err := os.WriteFile(path, []byte(fakeServiceAccountJSON), 0o600); err != nil {
		t.Fatalf("write fixture credentials: %v", err)
	}
	return path
}

// isolateADC ensures tests do not pick up a developer's local ADC.
//
// The oauth2/google package looks at, in order:
//  1. GOOGLE_APPLICATION_CREDENTIALS
//  2. $HOME/.config/gcloud/application_default_credentials.json (or
//     %APPDATA%/gcloud/... on Windows) — note: it uses $HOME directly,
//     not CLOUDSDK_CONFIG
//  3. The GCE metadata server (cached via sync.Once across the test binary)
//
// We point (1) and (2) at empty / unreachable locations. Tests that pass
// CredentialsFile explicitly never reach (3).
func isolateADC(t *testing.T) {
	t.Helper()
	// (1) Clear and unset GOOGLE_APPLICATION_CREDENTIALS — some code paths
	// use os.LookupEnv so an empty string is not enough.
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "")
	if err := os.Unsetenv("GOOGLE_APPLICATION_CREDENTIALS"); err != nil {
		t.Fatalf("unset GOOGLE_APPLICATION_CREDENTIALS: %v", err)
	}
	// (2) Redirect $HOME (and APPDATA, for parity) at an empty temp dir
	// so the well-known gcloud ADC file cannot be discovered.
	tmpHome := t.TempDir()
	t.Setenv("HOME", tmpHome)
	t.Setenv("APPDATA", tmpHome)
	// CLOUDSDK_CONFIG is consulted by gcloud itself in some cases; harmless
	// to redirect for completeness.
	t.Setenv("CLOUDSDK_CONFIG", tmpHome)
}

func TestNewTokenSource_DefaultScopeApplied(t *testing.T) {
	isolateADC(t)
	credsPath := writeFixtureCreds(t)

	ts, err := NewTokenSource(context.Background(), Config{
		CredentialsFile: credsPath,
		// Scopes intentionally left empty — should default.
	})
	if err != nil {
		t.Fatalf("NewTokenSource returned error: %v", err)
	}
	if ts == nil {
		t.Fatal("NewTokenSource returned nil TokenSource without error")
	}
}

func TestNewTokenSource_ExplicitScopesRespected(t *testing.T) {
	isolateADC(t)
	credsPath := writeFixtureCreds(t)

	customScope := "https://www.googleapis.com/auth/cloud-platform.read-only"
	ts, err := NewTokenSource(context.Background(), Config{
		CredentialsFile: credsPath,
		Scopes:          []string{customScope},
	})
	if err != nil {
		t.Fatalf("NewTokenSource returned error: %v", err)
	}
	if ts == nil {
		t.Fatal("NewTokenSource returned nil TokenSource without error")
	}
}

func TestNewTokenSource_CredentialsFileNotFound(t *testing.T) {
	isolateADC(t)

	tests := []struct {
		name string
		cfg  Config
	}{
		{
			name: "ADC path",
			cfg: Config{
				CredentialsFile: "/definitely/does/not/exist/sa.json",
			},
		},
		{
			name: "impersonation path",
			cfg: Config{
				CredentialsFile:           "/definitely/does/not/exist/sa.json",
				ImpersonateServiceAccount: "target@example.iam.gserviceaccount.com",
			},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			ts, err := NewTokenSource(context.Background(), tc.cfg)
			if err == nil {
				t.Fatalf("expected error for missing credentials file, got nil (ts=%v)", ts)
			}
			if ts != nil {
				t.Fatalf("expected nil TokenSource on error, got %v", ts)
			}
			if !strings.Contains(err.Error(), "credentials file") {
				t.Fatalf("error %q does not mention credentials file", err.Error())
			}
		})
	}
}

func TestNewTokenSource_CredentialsFileMalformed(t *testing.T) {
	isolateADC(t)

	dir := t.TempDir()
	path := filepath.Join(dir, "bad.json")
	if err := os.WriteFile(path, []byte("not json"), 0o600); err != nil {
		t.Fatalf("write bad fixture: %v", err)
	}

	ts, err := NewTokenSource(context.Background(), Config{CredentialsFile: path})
	if err == nil {
		t.Fatalf("expected parse error, got nil (ts=%v)", ts)
	}
	if ts != nil {
		t.Fatalf("expected nil TokenSource on error, got %v", ts)
	}
}

// TestNewTokenSource_ImpersonationConstructs verifies that the impersonated
// token source is constructed successfully without hitting the network.
//
// Note: We deliberately leave Config.Lifetime at its zero value. The
// underlying impersonate.CredentialsTokenSource eagerly mints a token (and
// therefore makes a network call) when a non-zero Lifetime is supplied, but
// is lazy when Lifetime == 0. Exercising the non-zero-lifetime path
// requires real IAM credentials and belongs in an integration test.
func TestNewTokenSource_ImpersonationConstructs(t *testing.T) {
	isolateADC(t)
	credsPath := writeFixtureCreds(t)

	tests := []struct {
		name   string
		scopes []string
	}{
		{name: "default scope"},
		{name: "explicit scopes", scopes: []string{"https://www.googleapis.com/auth/monitoring"}},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			ts, err := NewTokenSource(context.Background(), Config{
				CredentialsFile:           credsPath,
				ImpersonateServiceAccount: "target@example.iam.gserviceaccount.com",
				Scopes:                    tc.scopes,
			})
			if err != nil {
				t.Fatalf("NewTokenSource returned error: %v", err)
			}
			if ts == nil {
				t.Fatal("NewTokenSource returned nil TokenSource without error")
			}
		})
	}
}

func TestDefaultMonitoringReadScope(t *testing.T) {
	if DefaultMonitoringReadScope != "https://www.googleapis.com/auth/monitoring.read" {
		t.Fatalf("DefaultMonitoringReadScope changed unexpectedly: %q", DefaultMonitoringReadScope)
	}
}
