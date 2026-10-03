package blob

import "testing"

func TestNewFromEnvEndpoints(t *testing.T) {
	t.Setenv("AWS_ENDPOINT_URL_S3", "http://storage.internal:9000")
	t.Setenv("AWS_S3_BUCKET", "blobs")
	t.Setenv("AWS_ACCESS_KEY_ID", "test-access-key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test-secret-key")
	t.Setenv("AWS_SESSION_TOKEN", "")
	t.Setenv("AWS_REGION", "us-east-1")
	t.Setenv("AWS_S3_SECURE", "")
	for _, tc := range []struct {
		public string
		signer string
	}{
		{"", "http://storage.internal:9000"},
		{"http://localhost:9000", "http://localhost:9000"},
		{"https://blobs.example.com", "https://blobs.example.com"},
	} {
		t.Run(tc.signer, func(t *testing.T) {
			t.Setenv("AWS_S3_PUBLIC_ENDPOINT_URL", tc.public)
			service, err := NewFromEnv(nil)
			if err != nil {
				t.Fatal(err)
			}
			if got := service.Store.EndpointURL().String(); got != "http://storage.internal:9000" {
				t.Fatalf("storage endpoint = %q", got)
			}
			if got := service.Signer.EndpointURL().String(); got != tc.signer {
				t.Fatalf("signing endpoint = %q; want %q", got, tc.signer)
			}
			if service.Bucket != "blobs" {
				t.Fatalf("bucket = %q", service.Bucket)
			}
		})
	}
	t.Setenv("AWS_S3_PUBLIC_ENDPOINT_URL", "https://blobs.example.com/path")
	if _, err := NewFromEnv(nil); err == nil {
		t.Fatal("accepted invalid signing endpoint")
	}
}
