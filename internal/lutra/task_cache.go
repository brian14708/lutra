package lutra

import (
	"context"
	"errors"
	"time"

	"github.com/brian14708/lutra/internal/db"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const taskCacheLease = 30

type taskCache struct{ worker *RunWorker }

func (c taskCache) Lookup(ctx context.Context, owner uuid.UUID, key []byte) ([]byte, error, bool) {
	key = append([]byte(nil), key...)
	q := db.New(c.worker.DB)
	for {
		row, err := q.GetTaskCache(ctx, key)
		if errors.Is(err, pgx.ErrNoRows) {
			claim, claimErr := q.ClaimTaskCache(ctx, db.ClaimTaskCacheParams{CacheKey: key, ClaimToken: owner, Column3: taskCacheLease})
			if claimErr == nil && len(claim.CacheKey) == 32 {
				c.worker.wg.Add(1)
				go func() {
					defer c.worker.wg.Done()
					ticker := time.NewTicker(10 * time.Second)
					defer ticker.Stop()
					for {
						select {
						case <-ctx.Done():
							return
						case <-ticker.C:
							rows, err := q.RenewTaskCache(ctx, db.RenewTaskCacheParams{CacheKey: key, ClaimToken: owner})
							if err != nil || rows != 1 {
								return
							}
						}
					}
				}()
				return nil, nil, false
			}
			if claimErr != nil && !errors.Is(claimErr, pgx.ErrNoRows) {
				return nil, claimErr, true
			}
			continue
		}
		if err != nil {
			return nil, err, true
		}
		if row.Status == db.LutraTaskCacheStatusReady {
			if row.ErrorCode != "" {
				failure := &CacheableError{Code: row.ErrorCode, Details: row.ErrorDetails}
				output, err := failure.output()
				if err != nil {
					return nil, err, true
				}
				return output, failure, true
			}
			return row.OutputCbor, nil, true
		}
		if !row.LeaseUntil.Time.After(time.Now()) {
			if err := q.ExpireTaskCache(ctx); err != nil {
				return nil, err, true
			}
			continue
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err(), true
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func (c taskCache) Store(ctx context.Context, owner uuid.UUID, key, output []byte, err error) error {
	var failure *CacheableError
	if err != nil && !errors.As(err, &failure) {
		return db.New(c.worker.DB).ReleaseTaskCache(ctx, db.ReleaseTaskCacheParams{CacheKey: key, ClaimToken: owner})
	}
	code, details := "", []byte(nil)
	if failure != nil {
		code, details = failure.Code, failure.Details
	}
	rows, err := db.New(c.worker.DB).FinishTaskCache(ctx, db.FinishTaskCacheParams{CacheKey: key, OutputCbor: output, ErrorCode: code, ErrorDetails: details, ClaimToken: owner})
	if err == nil && rows != 1 {
		return ErrLeaseLost
	}
	return err
}
