// Package cache owns reusable task results and image build generations.
package cache

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/brian14708/lutra/internal/db"
	"github.com/fxamacker/cbor/v2"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Tag 60000 is the cache exception CBOR profile v1: [message, details].
// The SDK value profile accepts only tag 32, so task output cannot use it.
const exceptionTag = 60000

const (
	maxResultCBOR  = 1 << 20
	maxImageURI    = 4096
	maxFailureText = 64 << 10
)

var exceptionPrefix = []byte{0xd9, 0xea, 0x60}

type Kind uint8

const (
	KindTaskResult Kind = iota + 1
	KindImage
)

type Key struct {
	Kind   Kind
	Digest [32]byte
}

type Result interface{ cacheResult() }

type TaskResult struct {
	OutputCBOR       []byte
	ErrorCode        string
	ErrorDetailsCBOR []byte
}

func (TaskResult) cacheResult() {}

type ImageResult struct{ ArtifactURI string }

func (ImageResult) cacheResult() {}

var ErrLeaseLost = errors.New("cache lease lost")

type Service struct {
	pool       *pgxpool.Pool
	renewEvery time.Duration
}

func New(pool *pgxpool.Pool) *Service { return &Service{pool: pool, renewEvery: 10 * time.Second} }

func (k Key) storedDigest() ([32]byte, error) {
	switch k.Kind {
	case KindTaskResult, KindImage:
		var source [33]byte
		source[0] = byte(k.Kind)
		copy(source[1:], k.Digest[:])
		return sha256.Sum256(source[:]), nil
	default:
		return [32]byte{}, errors.New("invalid cache kind")
	}
}

func encodeException(message string, details []byte) ([]byte, error) {
	mode, err := cbor.CanonicalEncOptions().EncMode()
	if err != nil {
		return nil, err
	}
	var content any
	if details != nil {
		content = cbor.RawMessage(details)
	}
	return mode.Marshal(cbor.Tag{Number: exceptionTag, Content: []any{message, content}})
}

func decodeException(encoded []byte) (string, []byte, bool, error) {
	if !bytes.HasPrefix(encoded, exceptionPrefix) {
		return "", nil, false, nil
	}
	var tag cbor.RawTag
	if err := tag.UnmarshalCBOR(encoded); err != nil || tag.Number != exceptionTag {
		return "", nil, true, errors.New("invalid cached exception")
	}
	var parts []cbor.RawMessage
	if err := cbor.Unmarshal(tag.Content, &parts); err != nil || len(parts) != 2 {
		return "", nil, true, errors.New("invalid cached exception")
	}
	var message string
	if err := cbor.Unmarshal(parts[0], &message); err != nil || message == "" {
		return "", nil, true, errors.New("invalid cached exception")
	}
	return message, parts[1], true, nil
}

func validateCBOR(encoded []byte) error {
	if len(encoded) == 0 || len(encoded) > maxResultCBOR {
		return errors.New("cache CBOR size is out of bounds")
	}
	var value any
	if err := cbor.Unmarshal(encoded, &value); err != nil {
		return fmt.Errorf("invalid cache CBOR: %w", err)
	}
	return nil
}

// Acquire pins a waiter to the generation it first observes. A failed image
// generation is visible to those waiters while a later call may start anew.
func (s *Service) Acquire(ctx context.Context, key Key) (Result, *Lease, error) {
	stored, err := key.storedDigest()
	if err != nil {
		return nil, nil, err
	}
	q := db.New(s.pool)
	var observed uuid.UUID
	for {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		var entry db.LutraCacheEntry
		if observed != uuid.Nil {
			entry, err = q.CacheGeneration(ctx, observed)
			if errors.Is(err, pgx.ErrNoRows) {
				observed = uuid.Nil
				continue
			}
		} else {
			if key.Kind == KindImage {
				err = q.ExpireCacheClaim(ctx, stored[:])
			} else {
				err = q.DeleteExpiredTaskClaim(ctx, stored[:])
			}
			if err != nil {
				return nil, nil, err
			}
			entry, err = q.ActiveCacheEntry(ctx, stored[:])
			if errors.Is(err, pgx.ErrNoRows) {
				id, idErr := uuid.NewV7()
				if idErr != nil {
					return nil, nil, idErr
				}
				token := uuid.New()
				entry, err = q.ClaimCacheEntry(ctx, db.ClaimCacheEntryParams{ID: id, Key: stored[:], ClaimToken: token})
				if err == nil {
					return nil, newLease(ctx, q, key.Kind, id, token, s.renewEvery), nil
				}
				if errors.Is(err, pgx.ErrNoRows) {
					continue
				}
			}
		}
		if err != nil {
			return nil, nil, err
		}
		switch entry.Status {
		case db.LutraCacheStatusReady:
			if err := validateCBOR(entry.ResultCbor); err != nil {
				return nil, nil, err
			}
			if key.Kind == KindImage {
				var uri string
				if err := cbor.Unmarshal(entry.ResultCbor, &uri); err != nil || uri == "" {
					return nil, nil, errors.New("invalid cached image artifact")
				}
				return ImageResult{ArtifactURI: uri}, nil, nil
			}
			if entry.ResultCbor == nil {
				return nil, nil, errors.New("invalid task cache row")
			}
			code, details, tagged, err := decodeException(entry.ResultCbor)
			if err != nil {
				return nil, nil, err
			}
			if tagged {
				return TaskResult{ErrorCode: code, ErrorDetailsCBOR: details}, nil, nil
			}
			return TaskResult{OutputCBOR: entry.ResultCbor}, nil, nil
		case db.LutraCacheStatusFailed:
			if err := validateCBOR(entry.ResultCbor); err != nil {
				return nil, nil, err
			}
			if key.Kind == KindImage && !bytes.Equal(entry.ResultCbor, []byte{0xf6}) {
				message, _, tagged, err := decodeException(entry.ResultCbor)
				if err != nil || !tagged {
					return nil, nil, errors.New("invalid cached image failure")
				}
				return nil, nil, errors.New(message)
			}
			observed = uuid.Nil
		case db.LutraCacheStatusBuilding:
			observed = entry.ID
			if !entry.LeaseUntil.Time.After(time.Now()) {
				observed = uuid.Nil
			}
		default:
			return nil, nil, fmt.Errorf("invalid cache status %q", entry.Status)
		}
		select {
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

type Lease struct {
	q      *db.Queries
	kind   Kind
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

func newLease(parent context.Context, q *db.Queries, kind Kind, id, token uuid.UUID, every time.Duration) *Lease {
	ctx, cancel := context.WithCancel(parent)
	l := &Lease{q: q, kind: kind, id: id, token: token, ctx: ctx, cancel: cancel, done: make(chan struct{}), stop: make(chan struct{}), every: every}
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

func (l *Lease) Complete(ctx context.Context, result Result) error {
	switch value := result.(type) {
	case TaskResult:
		if l.kind != KindTaskResult || (value.OutputCBOR == nil) != (value.ErrorCode != "") ||
			(value.ErrorCode != "" && value.ErrorDetailsCBOR == nil) ||
			(value.ErrorCode == "" && value.ErrorDetailsCBOR != nil) {
			return errors.New("invalid task cache result")
		}
		encoded := value.OutputCBOR
		if value.ErrorDetailsCBOR != nil {
			if len(value.ErrorDetailsCBOR) > 64<<10 {
				return errors.New("cache error details exceed 64 KiB")
			}
			if err := validateCBOR(value.ErrorDetailsCBOR); err != nil {
				return err
			}
		}
		if value.ErrorCode != "" {
			var err error
			encoded, err = encodeException(value.ErrorCode, value.ErrorDetailsCBOR)
			if err != nil {
				return err
			}
		} else if bytes.HasPrefix(encoded, exceptionPrefix) {
			return errors.New("task output uses reserved cache exception tag")
		}
		if err := validateCBOR(encoded); err != nil {
			return err
		}
		return l.finish(ctx, func(cleanup context.Context) (int64, error) {
			return l.q.CompleteCacheEntry(cleanup, db.CompleteCacheEntryParams{
				ID: l.id, ClaimToken: l.token, ResultCbor: encoded,
			})
		})
	case ImageResult:
		if l.kind != KindImage || value.ArtifactURI == "" || len(value.ArtifactURI) > maxImageURI {
			return errors.New("invalid image cache result")
		}
		mode, err := cbor.CanonicalEncOptions().EncMode()
		if err != nil {
			return err
		}
		encoded, err := mode.Marshal(value.ArtifactURI)
		if err != nil {
			return err
		}
		return l.finish(ctx, func(cleanup context.Context) (int64, error) {
			return l.q.CompleteCacheEntry(cleanup, db.CompleteCacheEntryParams{ID: l.id, ClaimToken: l.token, ResultCbor: encoded})
		})
	default:
		return errors.New("invalid cache result")
	}
}

func (l *Lease) Fail(ctx context.Context, message string) error {
	if l.kind != KindImage || message == "" {
		return errors.New("only image builds may publish failure")
	}
	if len(message) > maxFailureText {
		message = strings.ToValidUTF8(message[:maxFailureText], "\uFFFD")
	}
	encoded, err := encodeException(message, nil)
	if err != nil {
		return err
	}
	return l.finish(ctx, func(cleanup context.Context) (int64, error) {
		return l.q.FailImageCache(cleanup, db.FailImageCacheParams{ID: l.id, ClaimToken: l.token, ResultCbor: encoded})
	})
}

func (l *Lease) Release(ctx context.Context) error {
	return l.finish(ctx, func(cleanup context.Context) (int64, error) {
		return l.q.ReleaseCacheClaim(cleanup, db.ReleaseCacheClaimParams{ID: l.id, ClaimToken: l.token})
	})
}
