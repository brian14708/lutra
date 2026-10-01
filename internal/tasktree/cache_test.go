package tasktree

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

type leaseMarker struct{}

type leaseCache struct{ lease *recordingLease }

func (c leaseCache) Acquire(ctx context.Context, _ []byte) ([]byte, error, CacheLease, error) {
	c.lease.ctx = context.WithValue(ctx, leaseMarker{}, true)
	return nil, nil, c.lease, nil
}

type recordingLease struct {
	ctx       context.Context
	finished  bool
	finishErr error
	released  bool
}

func (l *recordingLease) Context() context.Context { return l.ctx }
func (l *recordingLease) Finish(_ context.Context, output []byte, err error) error {
	if err == nil && len(output) == 1 && output[0] == 0x02 {
		l.finished = true
	}
	return l.finishErr
}
func (l *recordingLease) Release(context.Context) error { l.released = true; return nil }

type markedSlots struct{}

func (markedSlots) Acquire(ctx context.Context) error {
	if ctx.Value(leaseMarker{}) != true {
		panic("slot acquisition did not use lease context")
	}
	return nil
}
func (markedSlots) Release() {}

type cancelingCache struct{ lease *cancelingLease }

func (c cancelingCache) Acquire(ctx context.Context, _ []byte) ([]byte, error, CacheLease, error) {
	c.lease.ctx, c.lease.cancel = context.WithCancel(ctx)
	return nil, nil, c.lease, nil
}

type cancelingLease struct {
	ctx      context.Context
	cancel   context.CancelFunc
	released bool
}

func (l *cancelingLease) Context() context.Context { return l.ctx }
func (*cancelingLease) Finish(context.Context, []byte, error) error {
	panic("lost lease published")
}

func (l *cancelingLease) Release(context.Context) error {
	l.released = true
	return nil
}

type hitCache struct{}

func (hitCache) Acquire(context.Context, []byte) ([]byte, error, CacheLease, error) {
	return []byte{0x01}, nil, nil, nil
}

type failingCache struct{ err error }

func (c failingCache) Acquire(context.Context, []byte) ([]byte, error, CacheLease, error) {
	return nil, nil, nil, c.err
}

func TestCacheLookupFailureLeavesActionForReplay(t *testing.T) {
	want := errors.New("cache database unavailable")
	root := id()
	executor := New([]Node{{ID: root, CacheKey: []byte{1}}}, Options{
		Cache:  failingCache{err: want},
		Runner: runnerFunc(func(context.Context, Attempt) ([]byte, error) { panic("cache lookup ran task") }),
	})
	if err := executor.Run(context.Background()); !errors.Is(err, want) {
		t.Fatalf("lookup outcome: %v", err)
	}
	node, err := executor.Get(context.Background(), root)
	if err != nil || node.State.Terminal() {
		t.Fatalf("lookup failure terminalized action: %+v, %v", node, err)
	}
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

func TestCacheLeaseCoversImageSlotAndRunner(t *testing.T) {
	lease := &recordingLease{}
	executor := New([]Node{{ID: id(), CacheKey: []byte{1}, ImageKey: []byte{2}}}, Options{
		Cache: leaseCache{lease: lease}, SlotPool: markedSlots{},
		Images: imageFunc(func(ctx context.Context, _ []byte) error {
			if ctx.Value(leaseMarker{}) != true {
				panic("image wait did not use lease context")
			}
			return nil
		}),
		Runner: runnerFunc(func(ctx context.Context, _ Attempt) ([]byte, error) {
			if ctx.Value(leaseMarker{}) != true {
				panic("runner did not use lease context")
			}
			return []byte{0x02}, nil
		}),
	})
	if err := executor.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !lease.finished {
		t.Fatal("task result was not published")
	}
	if lease.released {
		t.Fatal("completed lease was released")
	}
}

func TestLostLeaseDuringImageWaitFinishesAction(t *testing.T) {
	lease := &cancelingLease{}
	executor := New([]Node{{ID: id(), CacheKey: []byte{1}, ImageKey: []byte{2}}}, Options{
		Cache: cancelingCache{lease: lease},
		Images: imageFunc(func(ctx context.Context, _ []byte) error {
			lease.cancel()
			return ctx.Err()
		}),
		Runner: runnerFunc(func(context.Context, Attempt) ([]byte, error) { panic("lost lease ran task") }),
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := executor.Run(ctx); err == nil || !strings.Contains(err.Error(), "cache lease lost") {
		t.Fatalf("lost lease outcome: %v", err)
	}
	if !lease.released {
		t.Fatal("lost lease was not released")
	}
	for _, node := range executor.nodes {
		if node.State.Terminal() {
			t.Fatalf("lost lease terminalized action: %+v", node)
		}
	}
}

func TestCachePublicationFailurePreservesTaskResult(t *testing.T) {
	lease := &recordingLease{finishErr: errors.New("cache unavailable")}
	root := id()
	executor := New([]Node{{ID: root, CacheKey: []byte{1}}}, Options{
		Cache:  leaseCache{lease: lease},
		Runner: runnerFunc(func(context.Context, Attempt) ([]byte, error) { return []byte{0x02}, nil }),
	})
	if err := executor.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	node, err := executor.Get(context.Background(), root)
	if err != nil || node.State != Done || len(node.Output) != 1 || node.Output[0] != 0x02 {
		t.Fatalf("task result changed by cache publication: %+v, %v", node, err)
	}
	if !lease.released {
		t.Fatal("failed cache publication left lease open")
	}
}

func TestLostLeaseCannotPublishRunnerSuccess(t *testing.T) {
	lease := &cancelingLease{}
	executor := New([]Node{{ID: id(), CacheKey: []byte{1}}}, Options{
		Cache: cancelingCache{lease: lease},
		Runner: runnerFunc(func(context.Context, Attempt) ([]byte, error) {
			lease.cancel()
			return []byte{0x02}, nil
		}),
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := executor.Run(ctx); err == nil || !strings.Contains(err.Error(), "cache lease lost") {
		t.Fatalf("lost lease outcome: %v", err)
	}
	if !lease.released {
		t.Fatal("lost lease was not released")
	}
}
