package blob

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// s3Config contains the connection settings for an S3-compatible object store.
// The AWS_* environment names are intentionally used so the same settings can
// be shared with AWS tooling and SDKs.
type s3Config struct {
	Endpoint        string
	PublicEndpoint  string
	Bucket          string
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
	Region          string
	Secure          bool
}

// s3ConfigFromEnv reads the canonical AWS environment variables.
func s3ConfigFromEnv() (s3Config, error) {
	config := s3Config{
		Endpoint:        os.Getenv("AWS_ENDPOINT_URL_S3"),
		PublicEndpoint:  os.Getenv("AWS_S3_PUBLIC_ENDPOINT_URL"),
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
			return s3Config{}, fmt.Errorf("parse AWS_S3_SECURE: %w", err)
		}
		config.Secure = secure
	}
	if config.Region == "" {
		config.Region = "us-east-1"
	}
	if config.Endpoint == "" || config.Bucket == "" || config.AccessKeyID == "" || config.SecretAccessKey == "" {
		return s3Config{}, errors.New("S3 endpoint, bucket, AWS access key, and AWS secret key are required")
	}
	return config, nil
}

// NewFromEnv configures blob storage and download signing using AWS settings.
func NewFromEnv(pool *pgxpool.Pool) (Service, error) {
	config, err := s3ConfigFromEnv()
	if err != nil {
		return Service{}, err
	}
	client, err := newS3Client(config)
	if err != nil {
		return Service{}, err
	}
	// Sign URLs for the endpoint clients actually reach.
	if config.PublicEndpoint != "" {
		config.Endpoint = config.PublicEndpoint
	}
	signer, err := newS3Client(config)
	if err != nil {
		return Service{}, fmt.Errorf("configure blob signer: %w", err)
	}
	return Service{DB: pool, Store: &minio.Core{Client: client}, Signer: signer, Bucket: config.Bucket}, nil
}

func newS3Client(config s3Config) (*minio.Client, error) {
	endpoint := config.Endpoint
	if parsed, err := url.Parse(endpoint); err == nil && parsed.Host != "" {
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
	} else {
		return nil, fmt.Errorf("invalid S3 endpoint %q", endpoint)
	}
	endpoint = strings.TrimSuffix(endpoint, "/")
	return minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(config.AccessKeyID, config.SecretAccessKey, config.SessionToken),
		Secure: config.Secure,
		Region: config.Region,
	})
}
