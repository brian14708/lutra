package lutra

import (
	"context"
	"crypto/rand"
	"errors"
	"os"
	"testing"

	"github.com/brian14708/lutra/internal/cache"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestTaskCacheAdapterReleasesOrdinaryFailureAndReplaysCacheableError(t *testing.T) {
	url := os.Getenv("LUTRA_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("set LUTRA_TEST_DATABASE_URL to run database task cache tests")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	driver := &runDriver{worker: &RunWorker{Cache: cache.New(pool)}}
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	_, _, first, err := driver.Acquire(ctx, key)
	if err != nil || first == nil {
		t.Fatalf("first claim: %v", err)
	}
	if err := first.Finish(ctx, nil, errors.New("ordinary failure")); err != nil {
		t.Fatal(err)
	}
	_, _, second, err := driver.Acquire(ctx, key)
	if err != nil || second == nil {
		t.Fatalf("ordinary failure did not release claim: %v", err)
	}
	if err := second.Finish(ctx, nil, &CacheableError{Code: "invalid_input", Details: []byte{0xf6}}); err != nil {
		t.Fatal(err)
	}
	output, cachedErr, third, err := driver.Acquire(ctx, key)
	if err != nil || third != nil || len(output) == 0 || cachedErr == nil || cachedErr.Error() != "invalid_input" {
		t.Fatalf("cached error replay: output=%x, task=%v, lease=%v, service=%v", output, cachedErr, third, err)
	}
}
