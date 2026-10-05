// Package cache owns reusable task results.
package cache

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/brian14708/lutra/internal/db"
	"github.com/brian14708/lutra/internal/result"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Result struct {
	ResultCBOR []byte
}

var (
	ErrLeaseLost = errors.New("cache lease lost")
	ErrBusy      = errors.New("cache entry is being built")
)

type Service struct {
	pool       *pgxpool.Pool
	renewEvery time.Duration
}

func New(pool *pgxpool.Pool) *Service { return &Service{pool: pool, renewEvery: 10 * time.Second} }

func storedDigest(key [32]byte) [32]byte {
	// Preserve the task-result namespace used by existing cache entries.
	var source [33]byte
	source[0] = 1
	copy(source[1:], key[:])
	return sha256.Sum256(source[:])
}

func validateCBOR(encoded []byte) error {
	failure, tagged, err := result.Validate(encoded)
	if err != nil {
		return err
	}
	if tagged && !failure.Cacheable {
		return errors.New("invalid cached failure")
	}
	return nil
}

// Acquire returns a cached result, claims an absent entry, or reports a busy entry.
func (s *Service) Acquire(ctx context.Context, key [32]byte) (Result, *Lease, error) {
	stored := storedDigest(key)
	q := db.New(s.pool)
	for {
		if err := ctx.Err(); err != nil {
			return Result{}, nil, err
		}
		entry, err := q.ActiveCacheEntry(ctx, stored[:])
		if errors.Is(err, pgx.ErrNoRows) {
			id, idErr := uuid.NewV7()
			if idErr != nil {
				return Result{}, nil, idErr
			}
			token := uuid.New()
			_, err = q.ClaimCacheEntry(ctx, db.ClaimCacheEntryParams{ID: id, Key: stored[:], ClaimToken: token})
			if err == nil {
				return Result{}, newLease(ctx, q, id, token, s.renewEvery), nil
			}
			if errors.Is(err, pgx.ErrNoRows) {
				continue
			}
		}
		if err != nil {
			return Result{}, nil, err
		}
		switch entry.Status {
		case db.LutraCacheStatusReady:
			if err := validateCBOR(entry.ResultCbor); err != nil {
				return Result{}, nil, err
			}
			return Result{ResultCBOR: entry.ResultCbor}, nil, nil
		case db.LutraCacheStatusBuilding:
			// Wait for the owner to complete or release its claim, sweeping
			// the entry only once its lease has actually lapsed.
			if entry.LeaseUntil.Valid && !entry.LeaseUntil.Time.After(time.Now()) {
				if err := q.DeleteExpiredTaskClaim(ctx, stored[:]); err != nil {
					return Result{}, nil, err
				}
				continue
			}
			return Result{}, nil, ErrBusy
		default:
			return Result{}, nil, fmt.Errorf("invalid cache status %q", entry.Status)
		}
	}
}

type Lease struct {
	q      *db.Queries
	id     uuid.UUID
	token  uuid.UUID
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
	stop   chan struct{}
	every  time.Duration
	mu     sync.Mutex
	ended  bool
}

func newLease(parent context.Context, q *db.Queries, id, token uuid.UUID, every time.Duration) *Lease {
	ctx, cancel := context.WithCancel(parent)
	l := &Lease{q: q, id: id, token: token, ctx: ctx, cancel: cancel, done: make(chan struct{}), stop: make(chan struct{}), every: every}
	go l.renew()
	return l
}

func (l *Lease) Context() context.Context { return l.ctx }

func (l *Lease) renew() {
	defer close(l.done)
	ticker := time.NewTicker(l.every)
	defer ticker.Stop()
	for {
		select {
		case <-l.stop:
			return
		case <-l.ctx.Done():
			return
		case <-ticker.C:
			renewCtx, cancel := context.WithTimeout(l.ctx, 5*time.Second)
			rows, err := l.q.RenewCacheClaim(renewCtx, db.RenewCacheClaimParams{ID: l.id, ClaimToken: l.token})
			cancel()
			if err != nil || rows != 1 {
				l.cancel()
				return
			}
		}
	}
}

func (l *Lease) finish(ctx context.Context, apply func(context.Context) (int64, error)) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.ended {
		return ErrLeaseLost
	}
	l.ended = true
	close(l.stop)
	<-l.done
	defer l.cancel()
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	rows, err := apply(cleanup)
	if err != nil {
		return err
	}
	if rows != 1 {
		return ErrLeaseLost
	}
	return nil
}

func (l *Lease) Complete(ctx context.Context, value Result) error {
	if value.ResultCBOR == nil {
		return errors.New("invalid task cache result")
	}
	encoded := value.ResultCBOR
	if err := validateCBOR(encoded); err != nil {
		return err
	}
	return l.finish(ctx, func(cleanup context.Context) (int64, error) {
		return l.q.CompleteCacheEntry(cleanup, db.CompleteCacheEntryParams{
			ID: l.id, ClaimToken: l.token, ResultCbor: encoded,
		})
	})
}

func (l *Lease) Release(ctx context.Context) error {
	return l.finish(ctx, func(cleanup context.Context) (int64, error) {
		return l.q.ReleaseCacheClaim(cleanup, db.ReleaseCacheClaimParams{ID: l.id, ClaimToken: l.token})
	})
}
