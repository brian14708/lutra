package cache

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/brian14708/lutra/db/migrations"
	"github.com/fxamacker/cbor/v2"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
)

func TestExceptionTagEncodings(t *testing.T) {
	// These all encode tag 60000, including valid noncanonical widths.
	for _, encoded := range []string{
		"d9ea6082666661696c6564f6",
		"da0000ea6082666661696c6564f6",
		"db000000000000ea6082666661696c6564f6",
	} {
		t.Run(encoded, func(t *testing.T) {
			data, err := hex.DecodeString(encoded)
			if err != nil {
				t.Fatal(err)
			}
			message, details, tagged, err := decodeException(data)
			if err != nil || !tagged || message != "failed" || !bytes.Equal(details, []byte{0xf6}) {
				t.Fatalf("decode: message=%q details=%x tagged=%v err=%v", message, details, tagged, err)
			}
			var lease Lease
			if err := lease.Complete(t.Context(), Result{OutputCBOR: data}); err == nil || err.Error() != "task output uses reserved cache exception tag" {
				t.Fatalf("reserved task output: %v", err)
			}
		})
	}
}

func TestDecodeExceptionDistinguishesTaskOutput(t *testing.T) {
	for _, value := range []any{nil, 1, "output", cbor.Tag{Number: 32, Content: "blob:text/plain,digest"}} {
		encoded, err := cbor.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		if _, _, tagged, err := decodeException(encoded); tagged || err != nil {
			t.Fatalf("ordinary output %v: tagged=%v err=%v", value, tagged, err)
		}
	}
	encoded, err := cbor.Marshal(cbor.Tag{Number: exceptionTag, Content: "invalid envelope"})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, tagged, err := decodeException(encoded); !tagged || err == nil {
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
		{OutputCBOR: []byte{0x01}},
		{OutputCBOR: []byte{0xf6}},
		{ErrorCode: "task.failed", ErrorDetailsCBOR: []byte{0xa0}},
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
		if err != nil || next != nil || got.ErrorCode != want.ErrorCode || !bytes.Equal(got.OutputCBOR, want.OutputCBOR) || !bytes.Equal(got.ErrorDetailsCBOR, want.ErrorDetailsCBOR) {
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
	if err := owner.Complete(ctx, Result{OutputCBOR: []byte{0x01}}); !errors.Is(err, ErrLeaseLost) {
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
