package graphexec

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

type testClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []testTimer
}

type testTimer struct {
	at time.Time
	ch chan time.Time
}

func newTestClock() *testClock      { return &testClock{now: time.Unix(100, 0)} }
func (c *testClock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.now }
func (c *testClock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	ch := make(chan time.Time, 1)
	c.timers = append(c.timers, testTimer{at: c.now.Add(d), ch: ch})
	return ch
}

func (c *testClock) advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	now := c.now
	ts := append([]testTimer(nil), c.timers...)
	c.timers = nil
	for _, timer := range ts {
		if timer.at.After(now) {
			c.timers = append(c.timers, timer)
		} else {
			timer.ch <- now
		}
	}
	c.mu.Unlock()
}

type memoryStore struct {
	mu     sync.Mutex
	events []Transition
	fail   error
}

func (s *memoryStore) Transition(_ context.Context, t Transition) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fail != nil {
		return s.fail
	}
	s.events = append(s.events, t)
	return nil
}

type runnerFunc func(context.Context, Attempt) ([]byte, error)

func (f runnerFunc) Run(c context.Context, a Attempt) ([]byte, error) { return f(c, a) }
func id() uuid.UUID                                                   { return uuid.New() }

func TestImageWaitDoesNotConsumeSlot(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	img := imageFunc(func(context.Context, []byte) error { close(started); <-release; return nil })
	root := Node{ID: id(), MaxAttempts: 1, ImageKey: []byte{1}}
	e := New([]Node{root}, Options{Slots: 1, Images: img, Runner: runnerFunc(func(context.Context, Attempt) ([]byte, error) { return nil, nil })})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- e.Run(ctx) }()
	<-started
	if err := e.opts.SlotPool.Acquire(ctx); err != nil {
		t.Fatal(err)
	}
	e.opts.SlotPool.Release()
	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("run did not finish")
	}
}

type imageFunc func(context.Context, []byte) error

func (f imageFunc) Ensure(c context.Context, k []byte) error { return f(c, k) }

func TestWaitHandsBackAndReacquiresSharedSlot(t *testing.T) {
	root, child := id(), id()
	runParent := make(chan struct{})
	finishParent := make(chan struct{})
	finishChild := make(chan struct{})
	childStarted := make(chan struct{})
	var e *Executor
	e = New([]Node{{ID: root, MaxAttempts: 1}}, Options{Slots: 1, Runner: runnerFunc(func(ctx context.Context, a Attempt) ([]byte, error) {
		if a.NodeID == root {
			if err := eAddWait(e, ctx, root, a.Number, child, runParent, finishParent); err != nil {
				return nil, err
			}
			return nil, nil
		}
		close(childStarted)
		<-finishChild
		return []byte("ok"), nil
	})})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- e.Run(ctx) }()
	select {
	case <-runParent:
	case <-time.After(time.Second):
		t.Fatal("parent did not start")
	}
	select {
	case <-childStarted:
	case <-time.After(time.Second):
		t.Fatal("child did not acquire handed-back slot")
	}
	close(finishChild)
	close(finishParent)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("parent failed to resume")
	}
}

func eAddWait(e *Executor, ctx context.Context, p uuid.UUID, attempt int32, c uuid.UUID, started, finish chan struct{}) error {
	if err := e.Add(ctx, p, attempt, Node{ID: c, MaxAttempts: 1}); err != nil {
		return err
	}
	close(started)
	_, err := e.Wait(ctx, p, attempt, c)
	if err != nil {
		return err
	}
	<-finish
	return nil
}

func TestStoreFailureStopsRun(t *testing.T) {
	want := errors.New("database unavailable")
	store := &memoryStore{fail: want}
	root := Node{ID: id(), MaxAttempts: 1}
	e := New([]Node{root}, Options{Store: store, Runner: runnerFunc(func(context.Context, Attempt) ([]byte, error) { return nil, nil })})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := e.Run(ctx); !errors.Is(err, want) {
		t.Fatalf("Run() = %v, want %v", err, want)
	}
}

func TestRetryUsesInjectedClock(t *testing.T) {
	clock := newTestClock()
	root := Node{ID: id(), MaxAttempts: 2}
	var mu sync.Mutex
	calls := 0
	e := New([]Node{root}, Options{Clock: clock, RetryBackoff: func(int32) time.Duration { return 5 * time.Second }, Runner: runnerFunc(func(context.Context, Attempt) ([]byte, error) {
		mu.Lock()
		defer mu.Unlock()
		calls++
		if calls == 1 {
			return nil, errors.New("retry")
		}
		return nil, nil
	})})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- e.Run(ctx) }()
	deadline := time.After(time.Second)
	for {
		mu.Lock()
		n := calls
		mu.Unlock()
		if n == 1 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("first attempt did not run")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	time.Sleep(10 * time.Millisecond)
	clock.advance(5 * time.Second)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("retry did not run after clock advance")
	}
	mu.Lock()
	defer mu.Unlock()
	if calls != 2 {
		t.Fatalf("attempts=%d, want 2", calls)
	}
}

func TestCancelCancelsRunner(t *testing.T) {
	root := Node{ID: id(), MaxAttempts: 1}
	started := make(chan struct{})
	canceled := make(chan struct{})
	e := New([]Node{root}, Options{Runner: runnerFunc(func(ctx context.Context, _ Attempt) ([]byte, error) {
		close(started)
		<-ctx.Done()
		close(canceled)
		return nil, ctx.Err()
	})})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- e.Run(ctx) }()
	<-started
	e.Cancel(ctx)
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("runner context not canceled")
	}
	<-done
	cancel()
}

func TestRejectInvalidSnapshots(t *testing.T) {
	root, child := id(), id()
	for name, nodes := range map[string][]Node{
		"duplicate":        {{ID: root}, {ID: root}},
		"missing parent":   {{ID: child, ParentID: &root}},
		"dependency cycle": {{ID: root, Prerequisites: []uuid.UUID{child}}, {ID: child, ParentID: &root, Prerequisites: []uuid.UUID{root}}},
		"parent cycle":     {{ID: root, ParentID: &child}, {ID: child, ParentID: &root}},
	} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := New(nodes, Options{}).Run(ctx); err == nil || errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("invalid snapshot returned %v", err)
			}
		})
	}
}

func TestFailedPrerequisiteDoesNotStopUnrelatedBranch(t *testing.T) {
	root, pending, failed, dependent, independent := id(), id(), id(), id(), id()
	var e *Executor
	e = New([]Node{
		{ID: root},
		{ID: pending, ParentID: &root, ImageKey: []byte{1}},
		{ID: failed, ParentID: &root, State: Failed, Error: "boom"},
		{ID: dependent, ParentID: &root, Prerequisites: []uuid.UUID{pending, failed}},
		{ID: independent, ParentID: &root},
	}, Options{Slots: 1, Images: imageFunc(func(ctx context.Context, _ []byte) error {
		<-ctx.Done()
		return ctx.Err()
	}), Runner: runnerFunc(func(ctx context.Context, attempt Attempt) ([]byte, error) {
		if attempt.NodeID == root {
			if _, err := e.Wait(ctx, root, attempt.Number, dependent); err == nil {
				return nil, errors.New("dependent unexpectedly succeeded")
			}
			returnValue, err := e.Wait(ctx, root, attempt.Number, independent)
			return returnValue.Output, err
		}
		if attempt.NodeID != independent {
			return nil, errors.New("dependent must never execute")
		}
		return []byte("unrelated output"), nil
	})})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := e.Run(ctx); err != nil {
		t.Fatal(err)
	}
	node, _ := e.Get(ctx, root)
	if node.State != Done || string(node.Output) != "unrelated output" {
		t.Fatalf("root: %+v", node)
	}
	node, _ = e.Get(ctx, dependent)
	if node.State != Failed || node.Error != "prerequisite failed" {
		t.Fatalf("dependent: %+v", node)
	}
}

func TestRecoveryReusesCompletedChildAndPreservesFailures(t *testing.T) {
	root, child := id(), id()
	var e *Executor
	e = New([]Node{{ID: root, Attempt: 4, Failures: 1, MaxAttempts: 2}, {ID: child, ParentID: &root, State: Done, Output: []byte("stored")}}, Options{Slots: 1, Runner: runnerFunc(func(ctx context.Context, attempt Attempt) ([]byte, error) {
		if attempt.NodeID != root || attempt.Number != 5 {
			return nil, errors.New("completed child executed or replay attempt incorrect")
		}
		if err := e.Add(ctx, root, attempt.Number, Node{ID: child}); err != nil {
			return nil, err
		}
		node, err := e.Wait(ctx, root, attempt.Number, child)
		return node.Output, err
	})})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := e.Run(ctx); err != nil {
		t.Fatal(err)
	}
	node, _ := e.Get(ctx, root)
	if node.Failures != 1 || node.Attempt != 5 || string(node.Output) != "stored" {
		t.Fatalf("replayed root: %+v", node)
	}
}

func TestWorkerLossLeavesNodesRecoverable(t *testing.T) {
	root := id()
	started := make(chan struct{})
	store := &memoryStore{}
	e := New([]Node{{ID: root, MaxAttempts: 1}}, Options{Store: store, Runner: runnerFunc(func(ctx context.Context, _ Attempt) ([]byte, error) {
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	})})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- e.Run(ctx) }()
	<-started
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("shutdown: %v", err)
	}
	node, _ := e.Get(context.Background(), root)
	if node.State != Running || node.Failures != 0 {
		t.Fatalf("worker loss consumed retry or terminalized action: %+v", node)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	for _, event := range store.events {
		if event.State.Terminal() {
			t.Fatalf("worker loss wrote terminal status: %+v", event)
		}
	}
}

func TestConcurrentChildWaitsWithOneSlot(t *testing.T) {
	root, first, second := id(), id(), id()
	var e *Executor
	e = New([]Node{{ID: root}}, Options{Slots: 1, Runner: runnerFunc(func(ctx context.Context, attempt Attempt) ([]byte, error) {
		if attempt.NodeID != root {
			return []byte("child"), nil
		}
		for _, child := range []uuid.UUID{first, second} {
			if err := e.Add(ctx, root, attempt.Number, Node{ID: child}); err != nil {
				return nil, err
			}
		}
		results := make(chan error, 2)
		for _, child := range []uuid.UUID{first, second} {
			go func() {
				node, err := e.Wait(ctx, root, attempt.Number, child)
				if err == nil && string(node.Output) != "child" {
					err = errors.New("wrong child output")
				}
				results <- err
			}()
		}
		for range 2 {
			if err := <-results; err != nil {
				return nil, err
			}
		}
		return []byte("joined"), nil
	})})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := e.Run(ctx); err != nil {
		t.Fatal(err)
	}
	node, _ := e.Get(ctx, root)
	if node.State != Done || string(node.Output) != "joined" {
		t.Fatalf("root: %+v", node)
	}
}

func TestRestoreRetryDeadline(t *testing.T) {
	clock := newTestClock()
	started := make(chan struct{})
	root := Node{ID: id(), Attempt: 2, Failures: 1, MaxAttempts: 3, NextAttemptAt: clock.Now().Add(10 * time.Second)}
	e := New([]Node{root}, Options{Clock: clock, Runner: runnerFunc(func(context.Context, Attempt) ([]byte, error) {
		close(started)
		return nil, nil
	})})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- e.Run(ctx) }()
	for {
		clock.mu.Lock()
		registered := len(clock.timers) == 1
		clock.mu.Unlock()
		if registered {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("retry timer not restored")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	clock.advance(9 * time.Second)
	select {
	case <-started:
		t.Fatal("restored retry dispatched before its deadline")
	default:
	}
	clock.advance(time.Second)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestImageFailureFailsNodeWithoutRunningTask(t *testing.T) {
	want := errors.New("image build failed")
	e := New([]Node{{ID: id(), ImageKey: []byte{1}}}, Options{Images: imageFunc(func(context.Context, []byte) error { return want }), Runner: runnerFunc(func(context.Context, Attempt) ([]byte, error) {
		return nil, errors.New("task must never execute")
	})})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := e.Run(ctx); err == nil || err.Error() != want.Error() {
		t.Fatalf("image failure: %v", err)
	}
}
