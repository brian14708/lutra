package tasktree

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

type testClock struct {
	mu           sync.Mutex
	now          time.Time
	timers       []*testTimer
	timerCreated chan struct{}
}

type testTimer struct {
	clock  *testClock
	at     time.Time
	ch     chan time.Time
	active bool
}

func newTestClock() *testClock {
	return &testClock{now: time.Unix(100, 0), timerCreated: make(chan struct{}, 1)}
}
func (c *testClock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.now }
func (c *testClock) NewTimer(d time.Duration) Timer {
	c.mu.Lock()
	defer c.mu.Unlock()
	timer := &testTimer{clock: c, at: c.now.Add(d), ch: make(chan time.Time, 1), active: true}
	if d <= 0 {
		timer.active = false
		timer.ch <- c.now
	}
	c.timers = append(c.timers, timer)
	c.timerCreated <- struct{}{}
	return timer
}

func (t *testTimer) C() <-chan time.Time { return t.ch }
func (t *testTimer) Reset(d time.Duration) {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	select {
	case <-t.ch:
	default:
	}
	t.at = t.clock.now.Add(d)
	t.active = true
	if d <= 0 {
		t.active = false
		t.ch <- t.clock.now
	}
}

func (t *testTimer) Stop() {
	t.clock.mu.Lock()
	t.active = false
	t.clock.mu.Unlock()
}

func (c *testClock) advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	now := c.now
	for _, timer := range c.timers {
		if timer.active && !timer.at.After(now) {
			timer.active = false
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

type unlockedStore struct{ executor *Coordinator }

func (s *unlockedStore) Transition(context.Context, Transition) error {
	acquired := make(chan bool, 1)
	go func() {
		s.executor.mu.Lock()
		transitioning := len(s.executor.committing) > 0
		s.executor.mu.Unlock()
		acquired <- transitioning
	}()
	select {
	case transitioning := <-acquired:
		if transitioning {
			return nil
		}
		return errors.New("transition was not marked in progress")
	case <-time.After(time.Second):
		return errors.New("state lock held during database transition")
	}
}

func TestTransitionsDoNotHoldStateLock(t *testing.T) {
	store := &unlockedStore{}
	store.executor = New([]Node{{ID: id()}}, Options{Store: store, Runner: runnerFunc(func(context.Context, Attempt) ([]byte, error) {
		return []byte("ok"), nil
	})})
	if err := store.executor.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
}

type parallelStore struct {
	blockedID uuid.UUID
	otherID   uuid.UUID
	blocked   chan struct{}
	other     chan struct{}
	release   chan struct{}
}

func (s parallelStore) Transition(_ context.Context, transition Transition) error {
	if transition.State != Running {
		return nil
	}
	if transition.NodeID == s.blockedID {
		close(s.blocked)
		<-s.release
	}
	if transition.NodeID == s.otherID {
		close(s.other)
	}
	return nil
}

func TestUnrelatedTaskTransitionsCanCommitConcurrently(t *testing.T) {
	root, first, second := id(), id(), id()
	store := parallelStore{blockedID: first, otherID: second, blocked: make(chan struct{}), other: make(chan struct{}), release: make(chan struct{})}
	var executor *Coordinator
	executor = New([]Node{{ID: root}}, Options{Slots: 2, Store: store, Runner: runnerFunc(func(ctx context.Context, attempt Attempt) ([]byte, error) {
		if attempt.NodeID != root {
			return []byte("child"), nil
		}
		for _, child := range []uuid.UUID{first, second} {
			if err := executor.Add(ctx, root, attempt.Number, Node{ID: child}); err != nil {
				return nil, err
			}
		}
		results := make(chan error, 2)
		for _, child := range []uuid.UUID{first, second} {
			go func() { _, err := executor.Wait(ctx, root, attempt.Number, child); results <- err }()
		}
		for range 2 {
			if err := <-results; err != nil {
				return nil, err
			}
		}
		return []byte("done"), nil
	})})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- executor.Run(ctx) }()
	select {
	case <-store.blocked:
	case <-ctx.Done():
		t.Fatal("first child did not enter its transition")
	}
	select {
	case <-store.other:
	case <-ctx.Done():
		t.Fatal("second child was blocked by first child's transition")
	}
	close(store.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestProcessCeilingFailsChildInsteadOfHanging(t *testing.T) {
	root, child := id(), id()
	var executor *Coordinator
	executor = New([]Node{{ID: root}}, Options{Slots: 1, ProcessPool: NewProcessPool(1, 1), Runner: runnerFunc(func(ctx context.Context, attempt Attempt) ([]byte, error) {
		if attempt.NodeID != root {
			return nil, errors.New("child ran past process ceiling")
		}
		if err := executor.Add(ctx, root, attempt.Number, Node{ID: child}); err != nil {
			return nil, err
		}
		_, _ = executor.Wait(ctx, root, attempt.Number, child)
		return []byte("handled"), nil
	})})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := executor.Run(ctx); err != nil {
		t.Fatal(err)
	}
	node, err := executor.Get(ctx, child)
	if err != nil || node.State != Failed || node.Error != ErrProcessCapacity.Error() {
		t.Fatalf("capacity outcome: %+v, %v", node, err)
	}
}

func TestReadyAdmissionReleasedWhenParentRuns(t *testing.T) {
	root, child := id(), id()
	var executor *Coordinator
	executor = New([]Node{{ID: root}}, Options{Slots: 1, ReadyQueue: 1, Runner: runnerFunc(func(ctx context.Context, attempt Attempt) ([]byte, error) {
		if attempt.NodeID == child {
			return []byte("child result"), nil
		}
		if err := executor.Add(ctx, root, attempt.Number, Node{ID: child}); err != nil {
			return nil, err
		}
		node, err := executor.Wait(ctx, root, attempt.Number, child)
		return node.Output, err
	})})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := executor.Run(ctx); err != nil {
		t.Fatal(err)
	}
	node, err := executor.Get(ctx, root)
	if err != nil || node.State != Done || string(node.Output) != "child result" {
		t.Fatalf("root result: %+v, %v", node, err)
	}
}

func TestCanceledChildWaitResumesLiveParent(t *testing.T) {
	root, child := id(), id()
	childWaiting := make(chan struct{})
	var executor *Coordinator
	executor = New([]Node{{ID: root}}, Options{Slots: 1, Images: imageFunc(func(ctx context.Context, _ []byte) error {
		close(childWaiting)
		<-ctx.Done()
		return ctx.Err()
	}), Runner: runnerFunc(func(ctx context.Context, attempt Attempt) ([]byte, error) {
		if attempt.NodeID == child {
			<-ctx.Done()
			return nil, ctx.Err()
		}
		if err := executor.Add(ctx, root, attempt.Number, Node{ID: child, ImageKey: []byte{1}}); err != nil {
			return nil, err
		}
		waitCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		go func() {
			select {
			case <-childWaiting:
				cancel()
			case <-ctx.Done():
			}
		}()
		if _, err := executor.Wait(waitCtx, root, attempt.Number, child); !errors.Is(err, context.Canceled) {
			return nil, errors.New("wait did not return cancellation")
		}
		node, err := executor.Get(ctx, root)
		if err != nil || node.State != Running {
			return nil, errors.New("live parent did not resume after canceled wait")
		}
		return []byte("handled cancellation"), nil
	})})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := executor.Run(ctx); err != nil {
		t.Fatal(err)
	}
	node, err := executor.Get(ctx, root)
	if err != nil || string(node.Output) != "handled cancellation" {
		t.Fatalf("root result: %+v, %v", node, err)
	}
}

type cancellationStore struct {
	leaves  map[uuid.UUID]bool
	blocked chan uuid.UUID
	release chan struct{}
	once    sync.Once
}

func (s *cancellationStore) Transition(_ context.Context, transition Transition) error {
	if transition.State == Canceled && s.leaves[transition.NodeID] {
		s.once.Do(func() {
			s.blocked <- transition.NodeID
			<-s.release
		})
	}
	return nil
}

func TestSubtreeCancellationIncludesChildAddedDuringCommit(t *testing.T) {
	root, branch, first, second, late := id(), id(), id(), id(), id()
	store := &cancellationStore{leaves: map[uuid.UUID]bool{first: true, second: true}, blocked: make(chan uuid.UUID, 1), release: make(chan struct{})}
	leavesStarted := make(chan struct{}, 2)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var executor *Coordinator
	executor = New([]Node{{ID: root}}, Options{Slots: 4, Store: store, Runner: runnerFunc(func(ctx context.Context, attempt Attempt) ([]byte, error) {
		switch attempt.NodeID {
		case root:
			if err := executor.Add(ctx, root, attempt.Number, Node{ID: branch}); err != nil {
				return nil, err
			}
			_, _ = executor.Wait(ctx, root, attempt.Number, branch)
		case branch:
			for _, leaf := range []uuid.UUID{first, second} {
				if err := executor.Add(ctx, branch, attempt.Number, Node{ID: leaf}); err != nil {
					return nil, err
				}
			}
		case first, second:
			leavesStarted <- struct{}{}
		}
		<-ctx.Done()
		return nil, ctx.Err()
	})})
	runDone := make(chan error, 1)
	go func() { runDone <- executor.Run(ctx) }()
	defer func() {
		cancel()
		<-runDone
	}()
	for range 2 {
		select {
		case <-leavesStarted:
		case <-ctx.Done():
			t.Fatal("leaves did not start")
		}
	}
	canceled := make(chan error, 1)
	go func() { canceled <- executor.CancelSubtree(ctx, branch) }()
	var blocked uuid.UUID
	select {
	case blocked = <-store.blocked:
	case <-ctx.Done():
		t.Fatal("cancellation did not enter store")
	}
	other := first
	if blocked == first {
		other = second
	}
	addErr := executor.Add(ctx, other, 1, Node{ID: late})
	close(store.release)
	if addErr != nil {
		t.Fatal(addErr)
	}
	if err := <-canceled; err != nil {
		t.Fatal(err)
	}
	node, err := executor.Get(ctx, late)
	if err != nil || node.State != Canceled || node.Error != "ancestor canceled" {
		t.Fatalf("late descendant: %+v, %v", node, err)
	}
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
	img := imageFunc(func(ctx context.Context, _ []byte) error {
		close(started)
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	root := Node{ID: id(), MaxAttempts: 1, ImageKey: []byte{1}}
	e := New([]Node{root}, Options{Slots: 1, Images: img, Runner: runnerFunc(func(context.Context, Attempt) ([]byte, error) { return nil, nil })})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- e.Run(ctx) }()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("image wait did not start")
	}
	node, err := e.Get(ctx, root.ID)
	if err != nil || node.State != Building {
		t.Fatalf("state during image build = %q, error = %v", node.State, err)
	}
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
	var e *Coordinator
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

func eAddWait(e *Coordinator, ctx context.Context, p uuid.UUID, attempt int32, c uuid.UUID, started, finish chan struct{}) error {
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
	root := Node{ID: id(), Attempt: 1, MaxAttempts: 3}
	firstAttempt := make(chan struct{})
	backoffAttempt := make(chan int32, 1)
	calls := 0
	e := New([]Node{root}, Options{Clock: clock, RetryBackoff: func(attempt int32) time.Duration {
		backoffAttempt <- attempt
		return 5 * time.Second
	}, Runner: runnerFunc(func(context.Context, Attempt) ([]byte, error) {
		calls++
		if calls == 1 {
			close(firstAttempt)
			return nil, errors.New("retry")
		}
		return nil, nil
	})})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- e.Run(ctx) }()
	select {
	case <-firstAttempt:
	case <-ctx.Done():
		t.Fatal("first attempt did not run")
	}
	select {
	case <-clock.timerCreated:
	case <-ctx.Done():
		t.Fatal("retry timer was not registered")
	}
	if attempt := <-backoffAttempt; attempt != 2 {
		t.Fatalf("backoff attempt=%d, want 2", attempt)
	}
	clock.advance(5 * time.Second)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("retry did not run after clock advance")
	}
	if calls != 2 {
		t.Fatalf("attempts=%d, want 2", calls)
	}
	clock.mu.Lock()
	timers := len(clock.timers)
	clock.mu.Unlock()
	if timers != 1 {
		t.Fatalf("retry timers=%d, want 1", timers)
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
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- e.Run(ctx) }()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("runner did not start")
	}
	e.Cancel(ctx)
	select {
	case <-canceled:
	case <-ctx.Done():
		t.Fatal("runner context not canceled")
	}
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("run did not stop after cancellation")
	}
}

func TestRejectInvalidSnapshots(t *testing.T) {
	root, child := id(), id()
	for name, nodes := range map[string][]Node{
		"duplicate":      {{ID: root}, {ID: root}},
		"missing parent": {{ID: child, ParentID: &root}},
		"parent cycle":   {{ID: root, ParentID: &child}, {ID: child, ParentID: &root}},
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

func TestRecoveryEnforcesAttemptLimit(t *testing.T) {
	for _, limits := range []struct{ previous, maximum int32 }{{0, 1}, {1, 1}, {1, 2}} {
		t.Run(fmt.Sprintf("%d_of_%d", limits.previous, limits.maximum), func(t *testing.T) {
			root := Node{ID: id(), Attempt: limits.previous, MaxAttempts: limits.maximum}
			calls := 0
			e := New([]Node{root}, Options{Slots: 1, Runner: runnerFunc(func(context.Context, Attempt) ([]byte, error) {
				calls++
				return nil, errors.New("retryable failure")
			})})
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			runErr := e.Run(ctx)
			node, err := e.Get(ctx, root.ID)
			if err != nil || node.State != Failed || node.Attempt != limits.maximum || calls != int(limits.maximum-limits.previous) || runErr == nil || !node.NextAttemptAt.IsZero() {
				t.Fatalf("node=%+v, calls=%d, get error=%v, run error=%v", node, calls, err, runErr)
			}
			if limits.previous == limits.maximum && node.Error != "maximum attempts exhausted" {
				t.Fatalf("error=%q", node.Error)
			}
			if err := e.opts.SlotPool.Acquire(ctx); err != nil {
				t.Fatal(err)
			}
			e.opts.SlotPool.Release()
			if err := e.opts.ProcessPool.Acquire(ctx); err != nil {
				t.Fatal(err)
			}
			e.opts.ProcessPool.Release()
		})
	}
}

func TestRecoveryReusesCompletedChild(t *testing.T) {
	root, child := id(), id()
	var e *Coordinator
	e = New([]Node{{ID: root, Attempt: 4, MaxAttempts: 5}, {ID: child, ParentID: &root, State: Done, Output: []byte("stored")}}, Options{Slots: 1, Runner: runnerFunc(func(ctx context.Context, attempt Attempt) ([]byte, error) {
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
	if node.Attempt != 5 || string(node.Output) != "stored" {
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
	select {
	case <-started:
	case <-time.After(time.Second):
		cancel()
		t.Fatal("runner did not start")
	}
	cancel()
	var err error
	select {
	case err = <-done:
	case <-time.After(time.Second):
		t.Fatal("run did not stop after worker loss")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("shutdown: %v", err)
	}
	node, _ := e.Get(context.Background(), root)
	if node.State != Running || node.Attempt != 1 {
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
	var e *Coordinator
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

func TestFirstChildWaitReturnsWhileSecondIsRunning(t *testing.T) {
	root, first, second := id(), id(), id()
	firstReturned := make(chan struct{})
	releaseSecond := make(chan struct{})
	var executor *Coordinator
	executor = New([]Node{{ID: root}}, Options{Slots: 1, Images: imageFunc(func(ctx context.Context, _ []byte) error {
		select {
		case <-releaseSecond:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}), Runner: runnerFunc(func(ctx context.Context, attempt Attempt) ([]byte, error) {
		switch attempt.NodeID {
		case first:
			return []byte("first"), nil
		case second:
			return []byte("second"), nil
		}
		for _, child := range []uuid.UUID{first, second} {
			node := Node{ID: child}
			if child == second {
				node.ImageKey = []byte{1}
			}
			if err := executor.Add(ctx, root, attempt.Number, node); err != nil {
				return nil, err
			}
		}
		secondResult := make(chan error, 1)
		go func() { _, err := executor.Wait(ctx, root, attempt.Number, second); secondResult <- err }()
		result, err := executor.Wait(ctx, root, attempt.Number, first)
		if err != nil || string(result.Output) != "first" {
			return nil, errors.New("first child result missing")
		}
		close(firstReturned)
		if err := <-secondResult; err != nil {
			return nil, err
		}
		return []byte("done"), nil
	})})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- executor.Run(ctx) }()
	select {
	case <-firstReturned:
	case <-ctx.Done():
		t.Fatal("first child waited for the second")
	}
	close(releaseSecond)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestRestoreRetryDeadline(t *testing.T) {
	clock := newTestClock()
	started := make(chan struct{})
	root := Node{ID: id(), Attempt: 2, MaxAttempts: 3, NextAttemptAt: clock.Now().Add(10 * time.Second)}
	e := New([]Node{root}, Options{Clock: clock, Runner: runnerFunc(func(context.Context, Attempt) ([]byte, error) {
		close(started)
		return nil, nil
	})})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- e.Run(ctx) }()
	select {
	case <-clock.timerCreated:
	case <-ctx.Done():
		t.Fatal("retry timer not restored")
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
