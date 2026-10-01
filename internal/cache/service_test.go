package cache

import (
	"context"
	"crypto/rand"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func databaseService(t *testing.T) (*Service, *pgxpool.Pool) {
	t.Helper()
	url := os.Getenv("LUTRA_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("set LUTRA_TEST_DATABASE_URL to run database cache tests")
	}
	pool, err := pgxpool.New(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return New(pool), pool
}

func testKey(t *testing.T, kind Kind) Key {
	t.Helper()
	key := Key{Kind: kind}
	if _, err := rand.Read(key.Digest[:]); err != nil {
		t.Fatal(err)
	}
	return key
}

func TestTaskGeneration(t *testing.T) {
	service, _ := databaseService(t)
	ctx := context.Background()
	key := testKey(t, KindTaskResult)
	_, lease, err := service.Acquire(ctx, key)
	if err != nil || lease == nil {
		t.Fatalf("first claim: %v, %v", lease, err)
	}
	wait := make(chan Result, 1)
	waitErr := make(chan error, 1)
	go func() {
		result, _, err := service.Acquire(ctx, key)
		wait <- result
		waitErr <- err
	}()
	if err := lease.Complete(ctx, TaskResult{OutputCBOR: []byte{0xf6}}); err != nil {
		t.Fatal(err)
	}
	if err := <-waitErr; err != nil {
		t.Fatal(err)
	}
	if result := (<-wait).(TaskResult); len(result.OutputCBOR) != 1 || result.OutputCBOR[0] != 0xf6 {
		t.Fatalf("waiter output: %x", result.OutputCBOR)
	}
	result, next, err := service.Acquire(ctx, key)
	if err != nil || next != nil || result.(TaskResult).OutputCBOR[0] != 0xf6 {
		t.Fatalf("ready task result: %v, %v, %v", result, next, err)
	}

	other := testKey(t, KindTaskResult)
	_, first, err := service.Acquire(ctx, other)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Release(ctx); err != nil {
		t.Fatal(err)
	}
	_, second, err := service.Acquire(ctx, other)
	if err != nil || second == nil {
		t.Fatalf("claim after release: %v", err)
	}
	if err := second.Complete(ctx, TaskResult{ErrorCode: "invalid", ErrorDetailsCBOR: []byte{0xf6}}); err != nil {
		t.Fatal(err)
	}
	negative, _, err := service.Acquire(ctx, other)
	if err != nil || negative.(TaskResult).ErrorCode != "invalid" {
		t.Fatalf("cached task error: %v, %v", negative, err)
	}
}

func TestImageFailureAndFencing(t *testing.T) {
	service, pool := databaseService(t)
	ctx := context.Background()
	key := testKey(t, KindImage)
	_, first, err := service.Acquire(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	wait := make(chan error, 1)
	go func() {
		_, _, err := service.Acquire(ctx, key)
		wait <- err
	}()
	time.Sleep(200 * time.Millisecond)
	if err := first.Fail(ctx, "build failed"); err != nil {
		t.Fatal(err)
	}
	if err := <-wait; err == nil || err.Error() != "build failed" {
		t.Fatalf("pinned waiter failure: %v", err)
	}
	_, second, err := service.Acquire(ctx, key)
	if err != nil || second == nil {
		t.Fatalf("new image generation: %v", err)
	}
	if _, err := pool.Exec(ctx, "UPDATE lutra.cache_entries SET lease_until = now() - interval '1 second' WHERE id = $1", second.id); err != nil {
		t.Fatal(err)
	}
	_, third, err := service.Acquire(ctx, key)
	if err != nil || third == nil {
		t.Fatalf("claim after expiry: %v", err)
	}
	if err := second.Complete(ctx, ImageResult{ArtifactURI: "stale"}); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale owner completed: %v", err)
	}
	if err := third.Complete(ctx, ImageResult{ArtifactURI: "artifact"}); err != nil {
		t.Fatal(err)
	}
	result, lease, err := service.Acquire(ctx, key)
	if err != nil || lease != nil || result.(ImageResult).ArtifactURI != "artifact" {
		t.Fatalf("ready image result: %v, %v, %v", result, lease, err)
	}
}

func TestWaitCancellation(t *testing.T) {
	service, _ := databaseService(t)
	ctx := context.Background()
	key := testKey(t, KindTaskResult)
	_, lease, err := service.Acquire(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	deadline, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
	defer cancel()
	_, _, err = service.Acquire(deadline, key)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("wait cancellation: %v", err)
	}
	if err := lease.Release(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestRenewalFailureCancelsLease(t *testing.T) {
	service, pool := databaseService(t)
	service.renewEvery = 20 * time.Millisecond
	ctx := context.Background()
	_, lease, err := service.Acquire(ctx, testKey(t, KindTaskResult))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "UPDATE lutra.cache_entries SET lease_until = now() - interval '1 second' WHERE id = $1", lease.id); err != nil {
		t.Fatal(err)
	}
	select {
	case <-lease.Context().Done():
	case <-time.After(time.Second):
		t.Fatal("renewal failure did not cancel the lease")
	}
	if err := lease.Complete(ctx, TaskResult{OutputCBOR: []byte{0xf6}}); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("expired lease published: %v", err)
	}
}

func TestReservedExceptionTagCannotBeTaskOutput(t *testing.T) {
	service, _ := databaseService(t)
	ctx := context.Background()
	_, lease, err := service.Acquire(ctx, testKey(t, KindTaskResult))
	if err != nil {
		t.Fatal(err)
	}
	if err := lease.Complete(ctx, TaskResult{OutputCBOR: []byte{0xd9, 0xea, 0x60, 0xf6}}); err == nil {
		t.Fatal("reserved exception tag was accepted as task output")
	}
	if err := lease.Release(ctx); err != nil {
		t.Fatal(err)
	}
}
