package cache

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/brian14708/lutra/db/migrations"
	"github.com/brian14708/lutra/internal/result"
	"github.com/fxamacker/cbor/v2"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
)

func TestExceptionTagEncodings(t *testing.T) {
	encoded, err := result.EncodeFailure(result.Failure{Cacheable: true, Message: "failed"})
	if err != nil {
		t.Fatal(err)
	}
	if err := validateCBOR(encoded); err != nil {
		t.Fatal(err)
	}
	encoded, err = result.EncodeFailure(result.Failure{Message: "failed"})
	if err != nil {
		t.Fatal(err)
	}
	if err := validateCBOR(encoded); err == nil {
		t.Fatal("cached runtime failure")
	}
}

func TestDecodeExceptionDistinguishesTaskOutput(t *testing.T) {
	for _, value := range []any{nil, 1, "output", cbor.Tag{Number: 32, Content: "blob:text/plain,digest"}} {
		encoded, err := cbor.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		if _, tagged, err := result.DecodeFailure(encoded); tagged || err != nil {
			t.Fatalf("ordinary output %v: tagged=%v err=%v", value, tagged, err)
		}
	}
	encoded, err := cbor.Marshal(cbor.Tag{Number: result.ErrorTag, Content: "invalid envelope"})
	if err != nil {
		t.Fatal(err)
	}
	if _, tagged, err := result.DecodeFailure(encoded); !tagged || err == nil {
		t.Fatalf("invalid exception: tagged=%v err=%v", tagged, err)
	}
}

func TestTaskCacheLifecycle(t *testing.T) {
	url := os.Getenv("LUTRA_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("set LUTRA_TEST_DATABASE_URL to a dedicated PostgreSQL test database")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	sqlDB := stdlib.OpenDBFromPool(pool)
	defer func() { _ = sqlDB.Close() }()
	if err := migrations.Run(ctx, sqlDB); err != nil {
		t.Fatal(err)
	}
	service := New(pool)
	for _, want := range []Result{
		{ResultCBOR: []byte{0x01}},
		{ResultCBOR: []byte{0xf6}},
		{ResultCBOR: []byte{0x01}},
	} {
		key := sha256.Sum256([]byte(uuid.NewString()))
		_, lease, err := service.Acquire(ctx, key)
		if err != nil || lease == nil {
			t.Fatalf("acquire: lease=%v err=%v", lease, err)
		}
		defer func() { _ = lease.Release(context.Background()) }()
		// Existing cache keys used kind byte 1 before the task digest.
		legacyKey := sha256.Sum256(append([]byte{1}, key[:]...))
		defer func() {
			_, _ = pool.Exec(context.Background(), "DELETE FROM lutra.cache_entries WHERE key = $1", legacyKey[:])
		}()
		var count int
		if err := pool.QueryRow(ctx, "SELECT count(*) FROM lutra.cache_entries WHERE key = $1", legacyKey[:]).Scan(&count); err != nil || count != 1 {
			t.Fatalf("legacy key compatibility: count=%d err=%v", count, err)
		}
		waiting, stop := context.WithTimeout(ctx, 20*time.Millisecond)
		_, _, err = service.Acquire(waiting, key)
		stop()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("waiting on owner: %v", err)
		}
		if err := lease.Complete(ctx, want); err != nil {
			t.Fatal(err)
		}
		got, next, err := service.Acquire(ctx, key)
		if err != nil || next != nil || !bytes.Equal(got.ResultCBOR, want.ResultCBOR) {
			t.Fatalf("cache hit: got=%+v lease=%v err=%v; want=%+v", got, next, err, want)
		}
		if err := lease.Complete(ctx, want); !errors.Is(err, ErrLeaseLost) {
			t.Fatalf("complete twice: %v", err)
		}
	}

	key := sha256.Sum256([]byte(uuid.NewString()))
	_, owner, err := service.Acquire(ctx, key)
	if err != nil || owner == nil {
		t.Fatalf("claim: lease=%v err=%v", owner, err)
	}
	defer func() { _ = owner.Release(context.Background()) }()
	if _, err := pool.Exec(ctx, "UPDATE lutra.cache_entries SET lease_until = clock_timestamp() - interval '1 second' WHERE id = $1", owner.id); err != nil {
		t.Fatal(err)
	}
	_, replacement, err := service.Acquire(ctx, key)
	if err != nil || replacement == nil || replacement.id == owner.id {
		t.Fatalf("replace expired claim: lease=%v err=%v", replacement, err)
	}
	defer func() { _ = replacement.Release(context.Background()) }()
	if err := owner.Complete(ctx, Result{ResultCBOR: []byte{0x01}}); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("expired owner published result: %v", err)
	}
	if err := replacement.Release(ctx); err != nil {
		t.Fatal(err)
	}
	_, retry, err := service.Acquire(ctx, key)
	if err != nil || retry == nil || retry.id == replacement.id {
		t.Fatalf("reacquire released claim: lease=%v err=%v", retry, err)
	}
	if err := retry.Release(ctx); err != nil {
		t.Fatal(err)
	}
}
