// Package graphexec owns one run's graph and schedules it without database reads.
package graphexec

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
	Prerequisites                  []uuid.UUID
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
	Lookup(context.Context, uuid.UUID, []byte) ([]byte, error, bool)
	Store(context.Context, uuid.UUID, []byte, []byte, error) error
}
type Clock interface {
	Now() time.Time
	After(time.Duration) <-chan time.Time
}
type Slots interface {
	Acquire(context.Context) error
	Release()
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

func (realClock) Now() time.Time                         { return time.Now() }
func (realClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

type Options struct {
	RunID, ClaimToken uuid.UUID
	Slots             int
	SlotPool          Slots
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

type Executor struct {
	mu      sync.Mutex
	nodes   map[uuid.UUID]*Node
	active  map[uuid.UUID]*active
	changed chan struct{}
	wake    chan struct{}
	opts    Options
	ctx     context.Context
	cancel  context.CancelFunc
	started bool
	err     error
	wg      sync.WaitGroup
}

func New(snapshot []Node, opts Options) *Executor {
	if opts.Clock == nil {
		opts.Clock = realClock{}
	}
	if opts.SlotPool == nil {
		opts.SlotPool = NewSlots(opts.Slots)
	}
	if opts.RetryBackoff == nil {
		opts.RetryBackoff = func(n int32) time.Duration { return time.Second * time.Duration(1<<min(max(n-1, 0), 6)) }
	}
	e := &Executor{nodes: make(map[uuid.UUID]*Node), active: make(map[uuid.UUID]*active), changed: make(chan struct{}), wake: make(chan struct{}, 1), opts: opts}
	for _, n := range snapshot {
		if n.State == "" {
			n.State = Pending
		}
		copy := clone(n)
		if e.nodes[n.ID] != nil {
			e.err = errors.New("graph has duplicate node IDs")
		}
		e.nodes[n.ID] = &copy
	}
	return e
}

func clone(n Node) Node {
	n.Output = append([]byte(nil), n.Output...)
	n.ImageKey = append([]byte(nil), n.ImageKey...)
	n.CacheKey = append([]byte(nil), n.CacheKey...)
	n.Prerequisites = append([]uuid.UUID(nil), n.Prerequisites...)
	if n.ParentID != nil {
		p := *n.ParentID
		n.ParentID = &p
	}
	return n
}

func (e *Executor) notifyLocked() {
	close(e.changed)
	e.changed = make(chan struct{})
	select {
	case e.wake <- struct{}{}:
	default:
	}
}

// Holding the graph lock through the write makes committed state the only
// state visible to dispatchers and callbacks, including terminal outputs.
func (e *Executor) commitLocked(ctx context.Context, next Node) error {
	old := e.nodes[next.ID]
	if e.err != nil {
		return e.err
	}
	if e.opts.Store != nil {
		err := e.opts.Store.Transition(ctx, Transition{RunID: e.opts.RunID, ClaimToken: e.opts.ClaimToken, NodeID: next.ID, From: old.State, State: next.State, WaitingOn: next.WaitingOn, Attempt: next.Attempt, Failures: next.Failures, ExpectedAttempt: old.Attempt, Output: next.Output, Error: next.Error, At: e.opts.Clock.Now(), NextAttemptAt: next.NextAttemptAt, CacheHit: next.CacheHit})
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

func (e *Executor) validateLocked() error {
	if e.err != nil {
		return e.err
	}
	visiting, visited := make(map[uuid.UUID]bool), make(map[uuid.UUID]bool)
	var visit func(uuid.UUID) error
	visit = func(id uuid.UUID) error {
		if visiting[id] {
			return errors.New("graph contains a cycle")
		}
		if visited[id] {
			return nil
		}
		n := e.nodes[id]
		if n == nil {
			return errors.New("graph references a missing node")
		}
		visiting[id] = true
		for _, prerequisite := range n.Prerequisites {
			if err := visit(prerequisite); err != nil {
				return err
			}
		}
		visiting[id], visited[id] = false, true
		return nil
	}
	for id := range e.nodes {
		ancestors := make(map[uuid.UUID]bool)
		for current := e.nodes[id]; current.ParentID != nil; current = e.nodes[*current.ParentID] {
			if e.nodes[*current.ParentID] == nil {
				return errors.New("graph references a missing parent")
			}
			if ancestors[current.ID] {
				return errors.New("graph contains a parent cycle")
			}
			ancestors[current.ID] = true
		}
		if err := visit(id); err != nil {
			return err
		}
	}
	return nil
}

func (e *Executor) Run(ctx context.Context) error {
	e.mu.Lock()
	if e.started {
		e.mu.Unlock()
		return errors.New("executor already started")
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
				return errors.New("graph has multiple roots")
			}
			root = id
		}
	}
	if len(e.nodes) > 0 && root == uuid.Nil {
		e.mu.Unlock()
		e.cancel()
		return errors.New("graph has no root")
	}
	for id, n := range e.nodes {
		if n.State == Pending && e.opts.Clock.Now().Before(n.NextAttemptAt) {
			e.timerLocked(id, n.Attempt, n.NextAttemptAt)
		}
	}
	e.mu.Unlock()
	defer func() { e.cancel(); e.wg.Wait() }()
	for {
		e.mu.Lock()
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
		e.mu.Unlock()
		select {
		case <-e.ctx.Done():
		case <-e.wake:
		}
	}
}

func (e *Executor) dispatchLocked() {
	for {
		propagated := false
		for id, current := range e.nodes {
			if current.State != Pending || e.opts.Clock.Now().Before(current.NextAttemptAt) {
				continue
			}
			ready := true
			for _, p := range current.Prerequisites {
				prereq := e.nodes[p]
				if prereq == nil || prereq.State != Done {
					ready = false
					if prereq == nil || prereq.State.Terminal() {
						next := clone(*current)
						next.State = Failed
						next.Error = "prerequisite failed"
						if e.commitLocked(e.ctx, next) != nil {
							return
						}
						propagated = true
					}
				}
			}
			if !ready {
				continue
			}
			next := clone(*current)
			next.State = Ready
			next.WaitingOn = ""
			if e.commitLocked(e.ctx, next) != nil {
				return
			}
			e.wg.Add(1)
			go func(id uuid.UUID) { defer e.wg.Done(); e.execute(id) }(id)
		}
		if !propagated {
			return
		}
	}
}

func (e *Executor) execute(id uuid.UUID) {
	var cacheOwner uuid.UUID
	e.mu.Lock()
	n := clone(*e.nodes[id])
	if n.State != Ready || e.ctx.Err() != nil {
		e.mu.Unlock()
		return
	}
	if len(n.CacheKey) > 0 && e.opts.Cache != nil {
		owner := uuid.New()
		e.mu.Unlock()
		output, err, hit := e.opts.Cache.Lookup(e.ctx, owner, n.CacheKey)
		if !hit {
			defer func() {
				cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				_ = e.opts.Cache.Store(cleanup, owner, n.CacheKey, nil, context.Canceled)
			}()
		}
		e.mu.Lock()
		n = clone(*e.nodes[id])
		if n.State.Terminal() || e.ctx.Err() != nil {
			e.mu.Unlock()
			return
		}
		if hit {
			n.CacheHit = err == nil || isCacheable(err)
			n.Output = output
			if err != nil {
				n.State, n.Error = Failed, err.Error()
			} else {
				n.State, n.Output, n.Error = Done, output, ""
			}
			if e.commitLocked(e.ctx, n) == nil {
				_ = e.closeLocked(e.ctx, id, "parent action completed")
			}
			e.mu.Unlock()
			return
		}
		cacheOwner = owner
	}
	if len(n.ImageKey) > 0 && e.opts.Images != nil {
		n.State = Waiting
		n.WaitingOn = WaitingForImage
		if e.commitLocked(e.ctx, n) != nil {
			e.mu.Unlock()
			return
		}
		e.mu.Unlock()
		err := e.opts.Images.Ensure(e.ctx, n.ImageKey)
		e.mu.Lock()
		n = clone(*e.nodes[id])
		if n.State.Terminal() || e.ctx.Err() != nil {
			e.mu.Unlock()
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
		if e.commitLocked(e.ctx, n) != nil {
			e.mu.Unlock()
			return
		}
	}
	e.mu.Unlock()
	if e.opts.SlotPool.Acquire(e.ctx) != nil {
		return
	}
	taskCtx, cancel := context.WithCancel(e.ctx)
	a := &active{ctx: taskCtx, cancel: cancel, held: true}
	e.mu.Lock()
	n = clone(*e.nodes[id])
	if n.State != Ready || e.ctx.Err() != nil {
		e.mu.Unlock()
		cancel()
		e.opts.SlotPool.Release()
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
	var out []byte
	var err error
	if e.opts.Runner == nil {
		err = errors.New("runner unavailable")
	} else {
		out, err = e.opts.Runner.Run(taskCtx, attempt)
	}
	if len(n.CacheKey) > 0 && e.opts.Cache != nil && taskCtx.Err() == nil {
		if cacheErr := e.opts.Cache.Store(taskCtx, cacheOwner, n.CacheKey, out, err); cacheErr != nil {
			err = cacheErr
		}
	}
	// Release capacity and remove the attempt before a retry can be dispatched.
	a.resumeMu.Lock()
	e.mu.Lock()
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
		if n.State == Pending {
			e.timerLocked(id, n.Attempt, n.NextAttemptAt)
		} else {
			_ = e.closeLocked(e.ctx, id, "parent action completed")
		}
	}
	e.mu.Unlock()
	a.resumeMu.Unlock()
}

type cacheable interface{ Cacheable() bool }

func isCacheable(err error) bool {
	var value cacheable
	return errors.As(err, &value) && value.Cacheable()
}

func (e *Executor) timerLocked(id uuid.UUID, attempt int32, at time.Time) {
	timer := e.opts.Clock.After(max(at.Sub(e.opts.Clock.Now()), 0))
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		select {
		case <-timer:
			e.mu.Lock()
			if n := e.nodes[id]; n != nil && n.Attempt == attempt && n.State == Pending {
				e.notifyLocked()
			}
			e.mu.Unlock()
		case <-e.ctx.Done():
		}
	}()
}

func (e *Executor) Add(ctx context.Context, parent uuid.UUID, attempt int32, node Node) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	e.mu.Lock()
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
	if node.State == Pending && e.opts.Clock.Now().Before(node.NextAttemptAt) {
		e.timerLocked(node.ID, node.Attempt, node.NextAttemptAt)
	}
	e.notifyLocked()
	return nil
}

func (e *Executor) Get(ctx context.Context, id uuid.UUID) (Node, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	n := e.nodes[id]
	if n == nil {
		return Node{}, errors.New("node not found")
	}
	return clone(*n), ctx.Err()
}

func (e *Executor) Wait(ctx context.Context, parent uuid.UUID, attempt int32, child uuid.UUID) (Node, error) {
	e.mu.Lock()
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
		e.mu.Unlock()
		if err := e.resume(parent, attempt, a); err != nil {
			return Node{}, err
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
	e.mu.Unlock()
	waitCtx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(a.ctx, cancel)
	defer stop()
	defer cancel()
	for {
		e.mu.Lock()
		n = e.nodes[child]
		copy := clone(*n)
		changed := e.changed
		e.mu.Unlock()
		if copy.State.Terminal() || waitCtx.Err() != nil {
			e.mu.Lock()
			a.waiters--
			e.notifyLocked()
			e.mu.Unlock()
			if err := e.resume(parent, attempt, a); err != nil {
				return Node{}, err
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

func (e *Executor) resume(parent uuid.UUID, attempt int32, a *active) error {
	a.resumeMu.Lock()
	defer a.resumeMu.Unlock()
	for {
		e.mu.Lock()
		if a.waiters == 0 || a.ctx.Err() != nil {
			e.mu.Unlock()
			break
		}
		changed := e.changed
		e.mu.Unlock()
		select {
		case <-changed:
		case <-a.ctx.Done():
		}
	}
	e.mu.Lock()
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
	e.mu.Unlock()
	if err := e.opts.SlotPool.Acquire(a.ctx); err != nil {
		return err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	p = e.nodes[parent]
	if e.active[parent] != a || p.State.Terminal() || a.ctx.Err() != nil {
		e.opts.SlotPool.Release()
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

func (e *Executor) closeLocked(ctx context.Context, parent uuid.UUID, reason string) error {
	ids := make(map[uuid.UUID]bool)
	if parent == uuid.Nil {
		for id := range e.nodes {
			ids[id] = true
		}
	} else {
		queue := []uuid.UUID{parent}
		for len(queue) > 0 {
			id := queue[0]
			queue = queue[1:]
			for child, n := range e.nodes {
				if n.ParentID != nil && *n.ParentID == id && !ids[child] {
					ids[child] = true
					queue = append(queue, child)
				}
			}
		}
	}
	for id := range ids {
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

func (e *Executor) Cancel(ctx context.Context) {
	e.mu.Lock()
	defer e.mu.Unlock()
	_ = e.closeLocked(context.WithoutCancel(ctx), uuid.Nil, "run canceled")
	if e.cancel != nil {
		e.cancel()
	}
}

func (e *Executor) CancelSubtree(ctx context.Context, id uuid.UUID) error {
	e.mu.Lock()
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
