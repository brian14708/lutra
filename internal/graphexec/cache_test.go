package graphexec

import (
	"context"
	"testing"

	"github.com/google/uuid"
)

type hitCache struct{}

func (hitCache) Lookup(context.Context, uuid.UUID, []byte) ([]byte, error, bool) {
	return []byte{0x01}, nil, true
}

func (hitCache) Store(context.Context, uuid.UUID, []byte, []byte, error) error {
	panic("cache hit attempted publication")
}

func TestCacheHitSkipsImageAndRunner(t *testing.T) {
	store := &memoryStore{}
	executor := New([]Node{{ID: id(), CacheKey: []byte{1}, ImageKey: []byte{2}}}, Options{
		Store: store, Cache: hitCache{},
		Images: imageFunc(func(context.Context, []byte) error { panic("cache hit requested image") }),
		Runner: runnerFunc(func(context.Context, Attempt) ([]byte, error) { panic("cache hit ran task") }),
	})
	if err := executor.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	last := store.events[len(store.events)-1]
	if last.State != Done || len(last.Output) != 1 || last.Output[0] != 0x01 || last.Attempt != 0 || !last.CacheHit {
		t.Fatalf("cache hit transition: %+v", last)
	}
}
