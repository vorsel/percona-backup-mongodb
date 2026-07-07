package gcs

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	storagegcs "cloud.google.com/go/storage"
	"github.com/googleapis/gax-go/v2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"

	"github.com/percona/percona-backup-mongodb/pbm/errors"
)

func (g *GCS) initClient() error {
	ctx := context.Background()
	cfg := g.cfg

	opts, err := authOptions(ctx, cfg)
	if err != nil {
		return err
	}

	var cli *storagegcs.Client
	if cfg.ClientType == ClientTypeGRPC {
		opts = append(opts, storagegcs.WithDisabledClientMetrics())
		cli, err = storagegcs.NewGRPCClient(ctx, opts...)
	} else {
		cli, err = storagegcs.NewClient(ctx, opts...)
	}
	if err != nil {
		return errors.Wrap(err, "new GCS client")
	}

	cli.SetRetry(
		storagegcs.WithBackoff(gax.Backoff{
			Initial:    cfg.Retryer.BackoffInitial,
			Max:        cfg.Retryer.BackoffMax,
			Multiplier: cfg.Retryer.BackoffMultiplier,
		}),
		storagegcs.WithMaxAttempts(cfg.Retryer.MaxAttempts),
		storagegcs.WithPolicy(storagegcs.RetryAlways),
		storagegcs.WithErrorFunc(shouldRetryExtended),
	)

	g.client = cli
	g.bucketHandle = cli.Bucket(cfg.Bucket)
	return nil
}

func authOptions(ctx context.Context, cfg *Config) ([]option.ClientOption, error) {
	if cfg.Credentials.PrivateKey != "" && cfg.Credentials.ClientEmail != "" {
		creds, err := serviceAccountCredentialsJSON(cfg)
		if err != nil {
			return nil, err
		}
		return []option.ClientOption{option.WithCredentialsJSON(creds)}, nil
	}

	if !cfg.Credentials.WorkloadIdentity {
		errMsg := "clientEmail and privateKey are required for GCS credentials when workloadIdentity is not enabled"
		return nil, errors.New(errMsg)
	}

	// No explicit credentials: validate ADC resolves to an allowed Workload Identity type.
	// We only check the credentials type; the scope used here doesn't matter.
	adc, err := google.FindDefaultCredentials(ctx, storagegcs.ScopeReadOnly)
	if err != nil {
		return nil, fmt.Errorf("finding default credentials: %w", err)
	}
	if err := validateDefaultCredentialType(adc); err != nil {
		return nil, fmt.Errorf("validate default credential type: %w", err)
	}

	return nil, nil
}

func serviceAccountCredentialsJSON(cfg *Config) ([]byte, error) {
	creds, err := json.Marshal(ServiceAccountCredentials{
		Type:                "service_account",
		PrivateKey:          string(cfg.Credentials.PrivateKey),
		ClientEmail:         string(cfg.Credentials.ClientEmail),
		AuthURI:             "https://accounts.google.com/o/oauth2/auth",
		TokenURI:            "https://oauth2.googleapis.com/token",
		UniverseDomain:      "googleapis.com",
		AuthProviderCertURL: "https://www.googleapis.com/oauth2/v1/certs",
		ClientCertURL: fmt.Sprintf(
			"https://www.googleapis.com/robot/v1/metadata/x509/%s",
			string(cfg.Credentials.ClientEmail),
		),
	})
	if err != nil {
		return nil, errors.Wrap(err, "marshal GCS credentials")
	}
	return creds, nil
}

// validateDefaultCredentialType validates that credentials are of type "external_account" used for Workload Identity
func validateDefaultCredentialType(creds *google.Credentials) error {
	// Empty JSON means metadata server (GKE/GCE Workload Identity)
	if len(creds.JSON) == 0 {
		return nil
	}

	var jsonCreds struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(creds.JSON, &jsonCreds); err != nil {
		return fmt.Errorf("parsing default credentials: %w", err)
	}

	if jsonCreds.Type != "external_account" {
		msg := "unsupported type %q; use Workload Identity or explicit config credentials"
		return fmt.Errorf(msg, jsonCreds.Type)
	}
	return nil
}

// shouldRetryExtended extends default shouldRetry with mainly
// `client connection lost` error from std library's http package.
func shouldRetryExtended(err error) bool {
	if err == nil {
		return false
	}
	if storagegcs.ShouldRetry(err) {
		return true
	}
	if strings.Contains(err.Error(), "http2: client connection lost") ||
		strings.Contains(err.Error(), "connect: network is unreachable") {
		return true
	}

	return false
}
