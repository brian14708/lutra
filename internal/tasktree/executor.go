// Package tasktree coordinates one run's task tree without database reads.
package tasktree

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
)

type State string

const (
	Pending  State = "pending"
	Ready    State = "ready"
	Building State = "building"
	Running  State = "running"
	Waiting  State = "waiting"
	Done     State = "done"
	Failed   State = "failed"
	Canceled State = "canceled"
)

func (s State) Terminal() bool { return s == Done || s == Failed || s == Canceled }

type WaitReason string

const (
	WaitingForImage    WaitReason = "image"
	WaitingForChildren WaitReason = "children"
	WaitingForRetry    WaitReason = "retry"
)

type Node struct {
	ID                             uuid.UUID
	ParentID                       *uuid.UUID
	Attempt, Failures, MaxAttempts int32
	State                          State
	WaitingOn                      WaitReason
	ImageKey, Output               []byte
	CacheKey                       []byte
	CacheHit                       bool
	Error                          string
	NextAttemptAt                  time.Time
}

type Attempt struct {
	RunID, ClaimToken, NodeID uuid.UUID
	Number                    int32
}

type Transition struct {
	RunID, ClaimToken, NodeID          uuid.UUID
	From, State                        State
	WaitingOn                          WaitReason
	Attempt, Failures, ExpectedAttempt int32
	Output                             []byte
	Error                              string
	At, NextAttemptAt                  time.Time
	CacheHit                           bool
}

type Store interface {
	Transition(context.Context, Transition) error
}
type Runner interface {
	Run(context.Context, Attempt) ([]byte, error)
}
type Images interface {
	Ensure(context.Context, []byte) error
}
type Cache interface {
	Acquire(context.Context, []byte) ([]byte, error, CacheLease, error)
}
type CacheLease interface {
	Context() context.Context
	Finish(context.Context, []byte, error) error
	Release(context.Context) error
}
type Clock interface {
	Now() time.Time
	NewTimer(time.Duration) Timer
}
type Timer interface {
	C() <-chan time.Time
	Reset(time.Duration)
	Stop()
}
type Slots interface {
	Acquire(context.Context) error
	Release()
}

var ErrProcessCapacity = errors.New("live process capacity exhausted")

// ProcessPool counts live task processes, including parents waiting for children.
type ProcessPool struct {
	mu              sync.Mutex
	target, ceiling int
	live, waiting   int
	changed         chan struct{}
}

func NewProcessPool(target, ceiling int) *ProcessPool {
	target = max(target, 1)
	return &ProcessPool{target: target, ceiling: max(ceiling, target), changed: make(chan struct{})}
}

func (p *ProcessPool) signal() {
	close(p.changed)
	p.changed = make(chan struct{})
}

func (p *ProcessPool) Acquire(ctx context.Context) error {
	for {
		p.mu.Lock()
		if p.live < p.target || (p.waiting > 0 && p.live < p.ceiling) {
			p.live++
			p.mu.Unlock()
			return nil
		}
		if p.live >= p.ceiling {
			p.mu.Unlock()
			return ErrProcessCapacity
		}
		changed := p.changed
		p.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
}

func (p *ProcessPool) Release() {
	p.mu.Lock()
	p.live--
	p.signal()
	p.mu.Unlock()
}

func (p *ProcessPool) Waiting(delta int) {
	p.mu.Lock()
	p.waiting += delta
	p.signal()
	p.mu.Unlock()
}

type slotPool struct{ ch chan struct{} }

func NewSlots(n int) Slots { return &slotPool{ch: make(chan struct{}, max(n, 1))} }
func (s *slotPool) Acquire(ctx context.Context) error {
	select {
	case s.ch <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (s *slotPool) Release() { <-s.ch }

type realClock struct{}

func (realClock) Now() time.Time                 { return time.Now() }
func (realClock) NewTimer(d time.Duration) Timer { return &wallTimer{time.NewTimer(d)} }

type wallTimer struct{ *time.Timer }

func (t *wallTimer) C() <-chan time.Time   { return t.Timer.C }
func (t *wallTimer) Reset(d time.Duration) { t.Timer.Reset(d) }
func (t *wallTimer) Stop()                 { t.Timer.Stop() }

type Options struct {
	RunID, ClaimToken uuid.UUID
	Slots             int
	ReadyQueue        int
	SlotPool          Slots
	ProcessPool       *ProcessPool
	Clock             Clock
	Store             Store
	Runner            Runner
	Images            Images
	Cache             Cache
	RetryBackoff      func(int32) time.Duration
}

type active struct {
	ctx      context.Context
	cancel   context.CancelFunc
	held     bool
	waiters  int
	resumeMu sync.Mutex
}

type Coordinator struct {
	mu         sync.Mutex
	committing map[uuid.UUID]chan struct{}
	nodes      map[uuid.UUID]*Node
	active     map[uuid.UUID]*active
	changed    chan struct{}
	wake       chan struct{}
	opts       Options
	ctx        context.Context
	cancel     context.CancelFunc
	started    bool
	err        error
	wg         sync.WaitGroup
	queued     int
}

func New(snapshot []Node, opts Options) *Coordinator {
	if opts.Clock == nil {
		opts.Clock = realClock{}
	}
	if opts.SlotPool == nil {
		opts.SlotPool = NewSlots(opts.Slots)
	}
	if opts.ProcessPool == nil {
		opts.ProcessPool = NewProcessPool(max(opts.Slots, 4), max(opts.Slots, 4)*4)
	}
	if opts.RetryBackoff == nil {
		opts.RetryBackoff = func(n int32) time.Duration { return time.Second * time.Duration(1<<min(max(n-1, 0), 6)) }
	}
	if opts.ReadyQueue < 1 {
		opts.ReadyQueue = max(4*opts.Slots, 32)
	}
	e := &Coordinator{nodes: make(map[uuid.UUID]*Node), active: make(map[uuid.UUID]*active), committing: make(map[uuid.UUID]chan struct{}), changed: make(chan struct{}), wake: make(chan struct{}, 1), opts: opts}
	for _, n := range snapshot {
		if n.State == "" {
			n.State = Pending
		}
		copy := clone(n)
		if e.nodes[n.ID] != nil {
			e.err = errors.New("task tree has duplicate action IDs")
		}
		e.nodes[n.ID] = &copy
	}
	return e
}

func clone(n Node) Node {
	n.Output = append([]byte(nil), n.Output...)
	n.ImageKey = append([]byte(nil), n.ImageKey...)
	n.CacheKey = append([]byte(nil), n.CacheKey...)
	if n.ParentID != nil {
		p := *n.ParentID
		n.ParentID = &p
	}
	return n
}

func (e *Coordinator) notifyLocked() {
	close(e.changed)
	e.changed = make(chan struct{})
	select {
	case e.wake <- struct{}{}:
	default:
	}
}

func (e *Coordinator) waitStableLocked(ids ...uuid.UUID) {
	for {
		var committed chan struct{}
		if len(ids) == 0 {
			for _, pending := range e.committing {
				committed = pending
				break
			}
		} else {
			for _, id := range ids {
				if pending := e.committing[id]; pending != nil {
					committed = pending
					break
				}
			}
		}
		if committed == nil {
			return
		}
		e.mu.Unlock()
		<-committed
		e.mu.Lock()
	}
}

// Only committed transitions become visible to dispatchers and callbacks.
func (e *Coordinator) commitLocked(ctx context.Context, next Node) error {
	e.waitStableLocked(next.ID)
	old := e.nodes[next.ID]
	if e.err != nil {
		return e.err
	}
	if e.opts.Store != nil {
		committed := make(chan struct{})
		e.committing[next.ID] = committed
		e.mu.Unlock()
		err := e.opts.Store.Transition(ctx, Transition{RunID: e.opts.RunID, ClaimToken: e.opts.ClaimToken, NodeID: next.ID, From: old.State, State: next.State, WaitingOn: next.WaitingOn, Attempt: next.Attempt, Failures: next.Failures, ExpectedAttempt: old.Attempt, Output: next.Output, Error: next.Error, At: e.opts.Clock.Now(), NextAttemptAt: next.NextAttemptAt, CacheHit: next.CacheHit})
		e.mu.Lock()
		delete(e.committing, next.ID)
		close(committed)
		if err != nil {
			e.err = err
			if e.cancel != nil {
				e.cancel()
			}
			e.notifyLocked()
			return err
		}
	}
	copy := clone(next)
	e.nodes[next.ID] = &copy
	e.notifyLocked()
	return nil
}

func (e *Coordinator) validateLocked() error {
	if e.err != nil {
		return e.err
	}
	for id := range e.nodes {
		ancestors := make(map[uuid.UUID]bool)
		for current := e.nodes[id]; current.ParentID != nil; current = e.nodes[*current.ParentID] {
			if e.nodes[*current.ParentID] == nil {
				return errors.New("task tree references a missing parent")
			}
			if ancestors[current.ID] {
				return errors.New("task tree contains a parent cycle")
			}
			ancestors[current.ID] = true
		}
	}
	return nil
}

func (e *Coordinator) Run(ctx context.Context) error {
	e.mu.Lock()
	e.waitStableLocked()
	if e.started {
		e.mu.Unlock()
		return errors.New("coordinator already started")
	}
	e.started = true
	e.ctx, e.cancel = context.WithCancel(ctx)
	if err := e.validateLocked(); err != nil {
		e.mu.Unlock()
		e.cancel()
		return err
	}
	root := uuid.Nil
	for id, n := range e.nodes {
		if n.ParentID == nil {
			if root != uuid.Nil {
				e.mu.Unlock()
				e.cancel()
				return errors.New("task tree has multiple roots")
			}
			root = id
		}
	}
	if len(e.nodes) > 0 && root == uuid.Nil {
		e.mu.Unlock()
		e.cancel()
		return errors.New("task tree has no root")
	}
	// Recovery can observe a parent completion before descendant cleanup.
	for id, node := range e.nodes {
		if node.State.Terminal() {
			if err := e.closeLocked(e.ctx, id, "ancestor completed"); err != nil {
				e.mu.Unlock()
				e.cancel()
				return err
			}
		}
	}
	e.mu.Unlock()
	var retryTimer Timer
	defer func() {
		if retryTimer != nil {
			retryTimer.Stop()
		}
		e.cancel()
		e.wg.Wait()
	}()
	for {
		e.mu.Lock()
		e.waitStableLocked(root)
		if e.err != nil {
			err := e.err
			e.mu.Unlock()
			return err
		}
		if e.ctx.Err() != nil {
			err := e.ctx.Err()
			e.mu.Unlock()
			return err
		}
		if root == uuid.Nil || e.nodes[root].State.Terminal() {
			var err error
			if root != uuid.Nil && e.nodes[root].State != Done {
				err = errors.New(e.nodes[root].Error)
			}
			if closeErr := e.closeLocked(context.WithoutCancel(ctx), uuid.Nil, "root action completed"); closeErr != nil {
				err = closeErr
			}
			e.mu.Unlock()
			return err
		}
		e.dispatchLocked()
		retry := e.nextRetryLocked()
		e.mu.Unlock()
		var deadline <-chan time.Time
		if retry.IsZero() {
			if retryTimer != nil {
				retryTimer.Stop()
			}
		} else {
			duration := max(retry.Sub(e.opts.Clock.Now()), 0)
			if retryTimer == nil {
				retryTimer = e.opts.Clock.NewTimer(duration)
			} else {
				retryTimer.Stop()
				retryTimer.Reset(duration)
			}
			deadline = retryTimer.C()
		}
		select {
		case <-e.ctx.Done():
		case <-e.wake:
		case <-deadline:
		}
	}
}

func (e *Coordinator) nextRetryLocked() time.Time {
	var next time.Time
	for _, n := range e.nodes {
		if n.State == Pending && n.NextAttemptAt.After(e.opts.Clock.Now()) && (next.IsZero() || n.NextAttemptAt.Before(next)) {
			next = n.NextAttemptAt
		}
	}
	return next
}

func (e *Coordinator) dispatchLocked() {
	for id, current := range e.nodes {
		if e.committing[id] != nil {
			continue
		}
		if e.queued >= e.opts.ReadyQueue {
			return
		}
		if current.State != Pending || e.opts.Clock.Now().Before(current.NextAttemptAt) {
			continue
		}
		if e.terminalAncestorLocked(id) {
			continue
		}
		e.waitStableLocked(id)
		current = e.nodes[id]
		if current.State != Pending || e.ctx.Err() != nil {
			continue
		}
		next := clone(*current)
		next.State = Ready
		next.WaitingOn = ""
		if e.commitLocked(e.ctx, next) != nil {
			return
		}
		e.wg.Add(1)
		e.queued++
		release := e.admissionRelease()
		go func(id uuid.UUID) {
			defer e.wg.Done()
			defer release()
			e.execute(id, release)
		}(id)
	}
}

func (e *Coordinator) terminalAncestorLocked(id uuid.UUID) bool {
	for node := e.nodes[id]; node.ParentID != nil; {
		e.waitStableLocked(*node.ParentID)
		node = e.nodes[*node.ParentID]
		if node.State.Terminal() {
			return true
		}
	}
	return false
}

func (e *Coordinator) admissionRelease() func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			e.mu.Lock()
			e.queued--
			e.notifyLocked()
			e.mu.Unlock()
		})
	}
}

func (e *Coordinator) acquireAdmission(ctx context.Context) (func(), error) {
	for {
		e.mu.Lock()
		if err := ctx.Err(); err != nil {
			e.mu.Unlock()
			return nil, err
		}
		if e.queued < e.opts.ReadyQueue {
			e.queued++
			e.mu.Unlock()
			return e.admissionRelease(), nil
		}
		changed := e.changed
		e.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-changed:
		}
	}
}

func (e *Coordinator) execute(id uuid.UUID, releaseAdmission func()) {
	var cacheLease CacheLease
	workCtx := e.ctx
	e.mu.Lock()
	e.waitStableLocked(id)
	n := clone(*e.nodes[id])
	if n.State != Ready || e.ctx.Err() != nil {
		e.mu.Unlock()
		return
	}
	if len(n.CacheKey) > 0 && e.opts.Cache != nil {
		e.mu.Unlock()
		// A cache owner may need a child in this queue to produce its result.
		releaseAdmission()
		output, taskErr, lease, acquireErr := e.opts.Cache.Acquire(e.ctx, n.CacheKey)
		if lease != nil {
			cacheLease = lease
			workCtx = lease.Context()
			defer func() {
				if cacheLease != nil {
					_ = cacheLease.Release(context.WithoutCancel(e.ctx))
				}
			}()
			if acquireErr == nil {
				releaseAdmission, acquireErr = e.acquireAdmission(workCtx)
				if acquireErr == nil {
					defer releaseAdmission()
				}
			}
		}
		e.mu.Lock()
		e.waitStableLocked(id)
		n = clone(*e.nodes[id])
		if n.State.Terminal() || e.ctx.Err() != nil {
			e.mu.Unlock()
			return
		}
		if workCtx.Err() != nil {
			e.mu.Unlock()
			e.abortOwner(errors.New("cache lease lost"))
			return
		}
		if acquireErr != nil {
			e.mu.Unlock()
			e.abortOwner(acquireErr)
			return
		}
		if lease == nil {
			n.CacheHit = true
			n.Output = output
			if taskErr != nil {
				n.State, n.Error = Failed, taskErr.Error()
			} else {
				n.State, n.Output, n.Error = Done, output, ""
			}
			if e.commitLocked(e.ctx, n) == nil {
				_ = e.closeLocked(e.ctx, id, "parent action completed")
			}
			e.mu.Unlock()
			return
		}
	}
	if len(n.ImageKey) > 0 && e.opts.Images != nil {
		n.State = Building
		n.WaitingOn = WaitingForImage
		if e.commitLocked(workCtx, n) != nil {
			e.mu.Unlock()
			return
		}
		e.mu.Unlock()
		err := e.opts.Images.Ensure(workCtx, n.ImageKey)
		e.mu.Lock()
		e.waitStableLocked(id)
		n = clone(*e.nodes[id])
		if n.State.Terminal() || e.ctx.Err() != nil {
			e.mu.Unlock()
			return
		}
		if workCtx.Err() != nil {
			e.mu.Unlock()
			e.abortOwner(errors.New("cache lease lost"))
			return
		}
		n.WaitingOn = ""
		if err != nil {
			n.State = Failed
			n.Error = err.Error()
			if e.commitLocked(e.ctx, n) == nil {
				_ = e.closeLocked(e.ctx, id, "parent action failed")
			}
			e.mu.Unlock()
			return
		}
		n.State = Ready
		if e.commitLocked(workCtx, n) != nil {
			e.mu.Unlock()
			return
		}
	}
	e.mu.Unlock()
	if err := e.opts.SlotPool.Acquire(workCtx); err != nil {
		if e.ctx.Err() == nil {
			e.failBeforeRun(id, err)
		}
		return
	}
	if err := e.opts.ProcessPool.Acquire(workCtx); err != nil {
		e.opts.SlotPool.Release()
		if e.ctx.Err() == nil {
			e.failBeforeRun(id, err)
		}
		return
	}
	processHeld := true
	defer func() {
		if processHeld {
			e.opts.ProcessPool.Release()
		}
	}()
	taskCtx, cancel := context.WithCancel(workCtx)
	a := &active{ctx: taskCtx, cancel: cancel, held: true}
	e.mu.Lock()
	e.waitStableLocked(id)
	n = clone(*e.nodes[id])
	if n.State != Ready || e.ctx.Err() != nil {
		e.mu.Unlock()
		cancel()
		e.opts.SlotPool.Release()
		return
	}
	if e.terminalAncestorLocked(id) {
		_ = e.closeLocked(e.ctx, id, "ancestor completed")
		e.mu.Unlock()
		cancel()
		e.opts.SlotPool.Release()
		return
	}
	e.waitStableLocked(id)
	n = clone(*e.nodes[id])
	if n.State != Ready || e.ctx.Err() != nil {
		e.mu.Unlock()
		cancel()
		e.opts.SlotPool.Release()
		return
	}
	if workCtx.Err() != nil {
		e.mu.Unlock()
		cancel()
		e.opts.SlotPool.Release()
		e.abortOwner(errors.New("cache lease lost"))
		return
	}
	n.State = Running
	n.Attempt++
	n.NextAttemptAt = time.Time{}
	if e.commitLocked(taskCtx, n) != nil {
		e.mu.Unlock()
		cancel()
		e.opts.SlotPool.Release()
		return
	}
	e.active[id] = a
	attempt := Attempt{RunID: e.opts.RunID, ClaimToken: e.opts.ClaimToken, NodeID: id, Number: n.Attempt}
	e.mu.Unlock()
	// Ready admission bounds preparation, not the lifetime of a running task.
	releaseAdmission()
	var out []byte
	var err error
	if e.opts.Runner == nil {
		err = errors.New("runner unavailable")
	} else {
		out, err = e.opts.Runner.Run(taskCtx, attempt)
	}
	e.opts.ProcessPool.Release()
	processHeld = false
	if workCtx.Err() != nil && e.ctx.Err() == nil {
		e.abortOwner(errors.New("cache lease lost"))
		a.resumeMu.Lock()
		e.mu.Lock()
		e.waitStableLocked(id)
		if a.held {
			e.opts.SlotPool.Release()
			a.held = false
		}
		delete(e.active, id)
		cancel()
		e.mu.Unlock()
		a.resumeMu.Unlock()
		return
	}
	if cacheLease != nil && taskCtx.Err() == nil {
		if cacheErr := cacheLease.Finish(taskCtx, out, err); cacheErr != nil {
			_ = cacheLease.Release(context.WithoutCancel(e.ctx))
		}
		cacheLease = nil
	}
	// Release capacity and remove the attempt before a retry can be dispatched.
	a.resumeMu.Lock()
	e.mu.Lock()
	e.waitStableLocked(id)
	if a.held {
		e.opts.SlotPool.Release()
		a.held = false
	}
	delete(e.active, id)
	cancel()
	if taskCtx.Err() != nil && e.ctx.Err() != nil {
		e.mu.Unlock()
		a.resumeMu.Unlock()
		return
	}
	n = clone(*e.nodes[id])
	if n.State.Terminal() || n.Attempt != attempt.Number {
		e.mu.Unlock()
		a.resumeMu.Unlock()
		return
	}
	n.Output = append([]byte(nil), out...)
	n.WaitingOn = ""
	if err == nil {
		n.State = Done
		n.Error = ""
	} else {
		n.Failures++
		n.Error = err.Error()
		if !isCacheable(err) && n.Failures < max(n.MaxAttempts, 1) {
			n.State = Pending
			n.WaitingOn = WaitingForRetry
			n.NextAttemptAt = e.opts.Clock.Now().Add(e.opts.RetryBackoff(n.Failures))
		} else {
			n.State = Failed
		}
	}
	if e.commitLocked(e.ctx, n) == nil {
		if n.State != Pending {
			_ = e.closeLocked(e.ctx, id, "parent action completed")
		}
	}
	e.mu.Unlock()
	a.resumeMu.Unlock()
}

func (e *Coordinator) abortOwner(err error) {
	e.mu.Lock()
	if e.err == nil {
		e.err = err
		e.cancel()
		e.notifyLocked()
	}
	e.mu.Unlock()
}

func (e *Coordinator) failBeforeRun(id uuid.UUID, err error) {
	e.mu.Lock()
	e.waitStableLocked(id)
	defer e.mu.Unlock()
	if e.ctx.Err() != nil {
		return
	}
	n := clone(*e.nodes[id])
	if n.State.Terminal() {
		return
	}
	n.State, n.Error = Failed, err.Error()
	if e.commitLocked(e.ctx, n) == nil {
		_ = e.closeLocked(e.ctx, id, "parent action failed")
	}
}

type cacheable interface{ Cacheable() bool }

func isCacheable(err error) bool {
	var value cacheable
	return errors.As(err, &value) && value.Cacheable()
}

func (e *Coordinator) Add(ctx context.Context, parent uuid.UUID, attempt int32, node Node) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	e.mu.Lock()
	e.waitStableLocked(parent, node.ID)
	defer e.mu.Unlock()
	p := e.nodes[parent]
	if e.err != nil {
		return e.err
	}
	if p == nil || p.Attempt != attempt || e.active[parent] == nil || p.State.Terminal() {
		return errors.New("parent attempt is not active")
	}
	if old := e.nodes[node.ID]; old != nil {
		if old.ParentID == nil || *old.ParentID != parent {
			return errors.New("node belongs to another parent")
		}
		return nil
	}
	if node.ID == parent {
		return errors.New("node cannot parent itself")
	}
	node.ParentID = &parent
	if node.State == "" {
		node.State = Pending
	}
	copy := clone(node)
	e.nodes[node.ID] = &copy
	e.notifyLocked()
	return nil
}

func (e *Coordinator) Get(ctx context.Context, id uuid.UUID) (Node, error) {
	e.mu.Lock()
	e.waitStableLocked(id)
	defer e.mu.Unlock()
	n := e.nodes[id]
	if n == nil {
		return Node{}, errors.New("node not found")
	}
	return clone(*n), ctx.Err()
}

func (e *Coordinator) Wait(ctx context.Context, parent uuid.UUID, attempt int32, child uuid.UUID) (Node, error) {
	e.mu.Lock()
	e.waitStableLocked(parent, child)
	p, n, a := e.nodes[parent], e.nodes[child], e.active[parent]
	if p == nil || a == nil || p.Attempt != attempt || p.State.Terminal() {
		e.mu.Unlock()
		return Node{}, errors.New("parent attempt is not active")
	}
	if n == nil || n.ParentID == nil || *n.ParentID != parent {
		e.mu.Unlock()
		return Node{}, errors.New("node is not a child of this parent")
	}
	if n.State.Terminal() {
		copy := clone(*n)
		resume := a.waiters == 0
		e.mu.Unlock()
		if resume {
			if err := e.resume(parent, attempt, a); err != nil {
				return Node{}, err
			}
		}
		return result(copy)
	}
	if a.held {
		next := clone(*p)
		next.State = Waiting
		next.WaitingOn = WaitingForChildren
		if err := e.commitLocked(ctx, next); err != nil {
			e.mu.Unlock()
			return Node{}, err
		}
		a.held = false
		e.opts.SlotPool.Release()
	}
	a.waiters++
	e.opts.ProcessPool.Waiting(1)
	e.mu.Unlock()
	defer e.opts.ProcessPool.Waiting(-1)
	waitCtx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(a.ctx, cancel)
	defer stop()
	defer cancel()
	for {
		e.mu.Lock()
		e.waitStableLocked(child)
		n = e.nodes[child]
		copy := clone(*n)
		changed := e.changed
		e.mu.Unlock()
		if copy.State.Terminal() || waitCtx.Err() != nil {
			e.mu.Lock()
			e.waitStableLocked(parent)
			a.waiters--
			resume := a.waiters == 0
			e.notifyLocked()
			e.mu.Unlock()
			if resume && a.ctx.Err() == nil {
				if err := e.resume(parent, attempt, a); err != nil {
					return Node{}, err
				}
			}
			if waitCtx.Err() != nil {
				return Node{}, waitCtx.Err()
			}
			return result(copy)
		}
		select {
		case <-waitCtx.Done():
		case <-changed:
		}
	}
}

func (e *Coordinator) resume(parent uuid.UUID, attempt int32, a *active) error {
	a.resumeMu.Lock()
	defer a.resumeMu.Unlock()
	e.mu.Lock()
	e.waitStableLocked(parent)
	p := e.nodes[parent]
	if e.err != nil {
		err := e.err
		e.mu.Unlock()
		return err
	}
	if e.active[parent] != a || p.Attempt != attempt || p.State.Terminal() || a.ctx.Err() != nil {
		e.mu.Unlock()
		return errors.New("parent attempt is not active")
	}
	if a.held {
		e.mu.Unlock()
		return nil
	}
	if a.waiters > 0 {
		e.mu.Unlock()
		return nil
	}
	e.mu.Unlock()
	if err := e.opts.SlotPool.Acquire(a.ctx); err != nil {
		return err
	}
	e.mu.Lock()
	e.waitStableLocked(parent)
	defer e.mu.Unlock()
	p = e.nodes[parent]
	if e.active[parent] != a || p.State.Terminal() || a.ctx.Err() != nil || a.waiters > 0 {
		e.opts.SlotPool.Release()
		if a.waiters > 0 && a.ctx.Err() == nil {
			return nil
		}
		return errors.New("parent attempt is not active")
	}
	a.held = true
	next := clone(*p)
	next.State = Running
	next.WaitingOn = ""
	return e.commitLocked(a.ctx, next)
}

func result(n Node) (Node, error) {
	if n.State == Done {
		return n, nil
	}
	return n, errors.New(n.Error)
}

func (e *Coordinator) closeLocked(ctx context.Context, parent uuid.UUID, reason string) error {
	ids := make(map[uuid.UUID]bool)
	queue := make([]uuid.UUID, 0, len(e.nodes))
	if parent == uuid.Nil {
		for id := range e.nodes {
			queue = append(queue, id)
		}
	} else {
		queue = append(queue, parent)
	}
	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		if ids[id] {
			continue
		}
		ids[id] = true
		e.waitStableLocked(id)
		for child, n := range e.nodes {
			if n.ParentID != nil && *n.ParentID == id && !ids[child] {
				queue = append(queue, child)
			}
		}
		n := clone(*e.nodes[id])
		if n.State.Terminal() {
			continue
		}
		if a := e.active[id]; a != nil {
			a.cancel()
		}
		n.State = Canceled
		n.WaitingOn = ""
		n.Error = reason
		if err := e.commitLocked(ctx, n); err != nil {
			return err
		}
	}
	return nil
}

func (e *Coordinator) Cancel(ctx context.Context) {
	e.mu.Lock()
	e.waitStableLocked()
	defer e.mu.Unlock()
	_ = e.closeLocked(context.WithoutCancel(ctx), uuid.Nil, "run canceled")
	if e.cancel != nil {
		e.cancel()
	}
}

func (e *Coordinator) CancelSubtree(ctx context.Context, id uuid.UUID) error {
	e.mu.Lock()
	e.waitStableLocked(id)
	defer e.mu.Unlock()
	n := e.nodes[id]
	if n == nil {
		return fmt.Errorf("node %s not found", id)
	}
	if !n.State.Terminal() {
		next := clone(*n)
		next.State = Canceled
		next.Error = "action canceled"
		if a := e.active[id]; a != nil {
			a.cancel()
		}
		if err := e.commitLocked(ctx, next); err != nil {
			return err
		}
	}
	return e.closeLocked(ctx, id, "ancestor canceled")
}
