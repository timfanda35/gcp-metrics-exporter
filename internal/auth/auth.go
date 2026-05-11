// Package auth handles GCP authentication and service account impersonation.
//
// It produces an [oauth2.TokenSource] that downstream packages (notably the
// collector) can inject into the GCP SDK clients. Two modes are supported:
//
//  1. Application Default Credentials (ADC) or an explicit Service Account
//     JSON key file referenced by [Config.CredentialsFile].
//  2. Service Account impersonation via
//     [google.golang.org/api/impersonate.CredentialsTokenSource], using the
//     base credentials above as the impersonator.
package auth

import (
	"context"
	"fmt"
	"os"
	"time"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/impersonate"
	"google.golang.org/api/option"
)

// DefaultMonitoringReadScope is the OAuth2 scope required to read time series
// data from GCP Cloud Monitoring. It is used as the default scope when
// [Config.Scopes] is empty so that the SDK does not silently fall back to a
// broader scope.
const DefaultMonitoringReadScope = "https://www.googleapis.com/auth/monitoring.read"

// Config controls how a token source is constructed.
//
// All fields are optional. A zero-value Config falls back to Application
// Default Credentials with the [DefaultMonitoringReadScope] scope and no
// impersonation.
type Config struct {
	// CredentialsFile is the filesystem path to a Service Account JSON key.
	// If empty, Application Default Credentials are used (which honours
	// the GOOGLE_APPLICATION_CREDENTIALS environment variable, gcloud's
	// well-known location, and the metadata server in that order).
	CredentialsFile string

	// ImpersonateServiceAccount is the email of the target service account
	// to impersonate. When empty, no impersonation is performed and the base
	// credentials' token source is returned directly.
	ImpersonateServiceAccount string

	// Scopes are the OAuth2 scopes requested for the resulting token. When
	// empty, [DefaultMonitoringReadScope] is used.
	Scopes []string

	// Lifetime controls the lifetime of impersonated tokens. A zero value
	// means "use the SDK default" (currently one hour). It is only consulted
	// when ImpersonateServiceAccount is non-empty.
	Lifetime time.Duration
}

// NewTokenSource builds an [oauth2.TokenSource] according to cfg.
//
// When cfg.ImpersonateServiceAccount is empty, the returned token source is
// derived directly from the base credentials (either an explicit JSON file or
// ADC). When set, the base credentials are used as the impersonator and the
// returned token source mints tokens for the target service account via the
// IAM Credentials API.
//
// Any error from credentials discovery, JSON parsing, or impersonation client
// construction is returned to the caller without being logged.
func NewTokenSource(ctx context.Context, cfg Config) (oauth2.TokenSource, error) {
	scopes := cfg.Scopes
	if len(scopes) == 0 {
		scopes = []string{DefaultMonitoringReadScope}
	}

	if cfg.ImpersonateServiceAccount == "" {
		creds, err := loadBaseCredentials(ctx, cfg.CredentialsFile, scopes)
		if err != nil {
			return nil, err
		}
		return creds.TokenSource, nil
	}

	// Impersonation path: build base credentials (without scopes — the
	// impersonator only needs IAM Credentials access, which is granted by
	// cloud-platform / IAM bindings on the impersonator SA), then wrap with
	// impersonate.CredentialsTokenSource.
	baseCreds, err := loadBaseCredentials(ctx, cfg.CredentialsFile, nil)
	if err != nil {
		return nil, err
	}

	ts, err := impersonate.CredentialsTokenSource(ctx, impersonate.CredentialsConfig{
		TargetPrincipal: cfg.ImpersonateServiceAccount,
		Scopes:          scopes,
		Lifetime:        cfg.Lifetime,
	}, option.WithTokenSource(baseCreds.TokenSource))
	if err != nil {
		return nil, fmt.Errorf("auth: build impersonated token source: %w", err)
	}
	return ts, nil
}

// loadBaseCredentials returns google credentials from either an explicit
// JSON key file or Application Default Credentials. The provided scopes are
// applied to whichever path is taken; pass nil to skip scope application
// (useful for the impersonator credentials).
func loadBaseCredentials(ctx context.Context, credentialsFile string, scopes []string) (*google.Credentials, error) {
	if credentialsFile != "" {
		data, err := os.ReadFile(credentialsFile)
		if err != nil {
			return nil, fmt.Errorf("auth: read credentials file %q: %w", credentialsFile, err)
		}
		creds, err := google.CredentialsFromJSON(ctx, data, scopes...)
		if err != nil {
			return nil, fmt.Errorf("auth: parse credentials file %q: %w", credentialsFile, err)
		}
		return creds, nil
	}

	creds, err := google.FindDefaultCredentials(ctx, scopes...)
	if err != nil {
		return nil, fmt.Errorf("auth: find default credentials: %w", err)
	}
	return creds, nil
}
