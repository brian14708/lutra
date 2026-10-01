package lutra

import (
	"context"
	"crypto/rand"
	"errors"
	"os"
	"testing"

	lutrav1 "github.com/brian14708/lutra/gen/lutra/v1"
	"github.com/brian14708/lutra/internal/cache"
	"github.com/jackc/pgx/v5/pgxpool"
)

type imageBuilder struct{ build func() (*Image, error) }

func (b imageBuilder) ImageKey(*lutrav1.EnvironmentSpec) ([]byte, error) { return nil, nil }
func (b imageBuilder) Build(context.Context, *EnvironmentExecution) (*Image, error) {
	return b.build()
}

func (imageBuilder) Run(context.Context, *Image, *EnvironmentExecution) (Job, error) {
	panic("image test ran a task")
}

func TestImageAdapterReusesArtifactAndRetriesFailure(t *testing.T) {
	url := os.Getenv("LUTRA_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("set LUTRA_TEST_DATABASE_URL to run database image tests")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	worker := &Worker{Cache: cache.New(pool)}
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	builds := 0
	builder := imageBuilder{build: func() (*Image, error) {
		builds++
		return &Image{ArtifactURI: "artifact-one"}, nil
	}}
	for range 2 {
		image, err := worker.ensureImage(ctx, key, builder, &EnvironmentExecution{})
		if err != nil || image.ArtifactURI != "artifact-one" {
			t.Fatalf("image reuse: %v, %v", image, err)
		}
	}
	if builds != 1 {
		t.Fatalf("build count: %d", builds)
	}
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	failed := imageBuilder{build: func() (*Image, error) { return nil, errors.New("build failed") }}
	if _, err := worker.ensureImage(ctx, key, failed, &EnvironmentExecution{}); err == nil || err.Error() != "build failed" {
		t.Fatalf("build failure: %v", err)
	}
	image, err := worker.ensureImage(ctx, key, builder, &EnvironmentExecution{})
	if err != nil || image.ArtifactURI != "artifact-one" || builds != 2 {
		t.Fatalf("later generation: %v, %v, builds=%d", image, err, builds)
	}
}
