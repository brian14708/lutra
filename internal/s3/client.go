// Package s3 provides the server-side S3 client used by Lutra.
package s3

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// Config contains the connection settings for an S3-compatible object store.
// The AWS_* environment names are intentionally used so the same settings can
// be shared with AWS tooling and SDKs.
type Config struct {
	Endpoint        string
	Bucket          string
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
	Region          string
	Secure          bool
}

// ConfigFromEnv reads the canonical AWS environment variables.
func ConfigFromEnv() (Config, error) {
	config := Config{
		Endpoint:        os.Getenv("AWS_ENDPOINT_URL_S3"),
		Bucket:          os.Getenv("AWS_S3_BUCKET"),
		AccessKeyID:     os.Getenv("AWS_ACCESS_KEY_ID"),
		SecretAccessKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
		SessionToken:    os.Getenv("AWS_SESSION_TOKEN"),
		Region:          os.Getenv("AWS_REGION"),
		Secure:          true,
	}
	if value := os.Getenv("AWS_S3_SECURE"); value != "" {
		secure, err := strconv.ParseBool(value)
		if err != nil {
			return Config{}, fmt.Errorf("parse AWS_S3_SECURE: %w", err)
		}
		config.Secure = secure
	}
	if config.Region == "" {
		config.Region = "us-east-1"
	}
	if config.Endpoint == "" || config.Bucket == "" || config.AccessKeyID == "" || config.SecretAccessKey == "" {
		return Config{}, errors.New("S3 endpoint, bucket, AWS access key, and AWS secret key are required")
	}
	return config, nil
}

// NewFromEnv creates an S3 client using ConfigFromEnv.
func NewFromEnv() (*minio.Client, Config, error) {
	config, err := ConfigFromEnv()
	if err != nil {
		return nil, Config{}, err
	}
	client, err := New(config)
	if err != nil {
		return nil, Config{}, err
	}
	return client, config, nil
}

// EnsureBucket creates the configured bucket when it does not exist. It is
// safe for multiple server replicas to call this concurrently.
func EnsureBucket(ctx context.Context, client *minio.Client, config Config) error {
	exists, err := client.BucketExists(ctx, config.Bucket)
	if err != nil {
		return fmt.Errorf("check object bucket %q: %w", config.Bucket, err)
	}
	if exists {
		return nil
	}
	if err := client.MakeBucket(ctx, config.Bucket, minio.MakeBucketOptions{Region: config.Region}); err != nil {
		response := minio.ToErrorResponse(err)
		if response.Code == "BucketAlreadyExists" || response.Code == "BucketAlreadyOwnedByYou" {
			return nil
		}
		return fmt.Errorf("create object bucket %q: %w", config.Bucket, err)
	}
	return nil
}

// New creates an S3-compatible client from explicit settings.
func New(config Config) (*minio.Client, error) {
	endpoint := config.Endpoint
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Host == "" {
		return nil, fmt.Errorf("invalid S3 endpoint %q", endpoint)
	}
	if parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || (parsed.Path != "" && parsed.Path != "/") {
		return nil, fmt.Errorf("invalid S3 endpoint %q", endpoint)
	}
	switch parsed.Scheme {
	case "https":
		config.Secure = true
	case "http":
		config.Secure = false
	default:
		return nil, fmt.Errorf("invalid S3 endpoint scheme in %q", endpoint)
	}
	endpoint = parsed.Host
	endpoint = strings.TrimSuffix(endpoint, "/")
	return minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(config.AccessKeyID, config.SecretAccessKey, config.SessionToken),
		Secure: config.Secure,
		Region: config.Region,
	})
}
