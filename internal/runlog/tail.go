package runlog

import (
	"context"
	"errors"
	"time"

	lutrav1 "github.com/brian14708/lutra/gen/lutra/v1"
	"github.com/brian14708/lutra/internal/db"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Cursor is specific to one stream and prefix. InspectSeq also includes excluded records.
type Cursor struct {
	Stream     string
	Prefix     []byte
	InspectSeq int64
	UntilSeq   *int64
}

// PrefixSuccessor returns the exclusive upper bound, or nil for an unbounded range.
func PrefixSuccessor(prefix []byte) []byte {
	result := append([]byte(nil), prefix...)
	for i := len(result) - 1; i >= 0; i-- {
		if result[i] != 255 {
			result[i]++
			return result[:i+1]
		}
	}
	return nil
}

// Subscription owns a listener connection. It keeps at most one metadata batch in memory.
type Subscription struct {
	service  Service
	runID    uuid.UUID
	listener *pgx.Conn
}

// Subscribe registers LISTEN before callers take a snapshot or cursor boundary.
func (s Service) Subscribe(ctx context.Context, id uuid.UUID) (*Subscription, error) {
	sub := &Subscription{service: s, runID: id}
	if err := sub.listen(ctx); err != nil {
		return nil, err
	}
	return sub, nil
}

func (s *Subscription) listen(ctx context.Context) error {
	// Long-lived LISTEN sessions must not consume connections needed for queries and writes.
	conn, err := pgx.ConnectConfig(ctx, s.service.DB.Config().ConnConfig.Copy())
	if err != nil {
		return err
	}
	if _, err := conn.Exec(ctx, "LISTEN lutra_logs"); err != nil {
		_ = conn.Close(ctx)
		return err
	}
	s.listener = conn
	return nil
}

func (s *Subscription) release() {
	if s.listener != nil {
		conn := s.listener
		s.listener = nil
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = conn.Close(ctx)
	}
}

func (s *Subscription) Close() {
	s.release()
}

func (s *Subscription) reconnect(ctx context.Context) error {
	s.release()
	delay := s.service.ReconnectDelay
	if delay <= 0 {
		delay = 100 * time.Millisecond
	}
	for {
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		if err := s.listen(ctx); err == nil {
			return nil
		}
	}
}

// ErrStop closes a subscription successfully after the consumer receives a terminal event.
var ErrStop = errors.New("subscription complete")

func (s *Subscription) batchSize() int32 {
	size := s.service.TailBatchSize
	if size <= 0 {
		size = 256
	}
	buffer := s.service.MaxResponseBuffer
	if buffer <= 0 {
		buffer = 256
	}
	if size > buffer {
		size = buffer
	}
	if size > 10000 {
		size = 10000
	}
	return int32(size)
}

// Run sends synchronously with backpressure and visits each stream once per batch round.
func (s *Subscription) Run(ctx context.Context, cursors []Cursor, send func(*lutrav1.TailResponse) error) error {
	for {
		progress := false
		for i := range cursors {
			if err := ctx.Err(); err != nil {
				return err
			}
			c := &cursors[i]
			if c.UntilSeq != nil && c.InspectSeq >= *c.UntilSeq {
				continue
			}
			lower := c.Prefix
			if len(lower) == 0 {
				lower = nil
			}
			rows, err := db.New(s.service.DB).ReadFilteredLogRecords(ctx, db.ReadFilteredLogRecordsParams{
				RunID: s.runID, Stream: c.Stream, AfterSeq: c.InspectSeq, LowerKey: lower,
				UpperKey: PrefixSuccessor(c.Prefix), BatchSize: s.batchSize(),
			})
			if err != nil {
				return err
			}
			inspect := c.InspectSeq
			for _, row := range rows {
				if c.UntilSeq != nil && row.Seq.Valid && row.Seq.Int64 > *c.UntilSeq {
					inspect = *c.UntilSeq
					break
				}
				if row.Seq.Valid {
					err = send(&lutrav1.TailResponse{Stream: c.Stream, Seq: row.Seq.Int64, Key: row.Key, ValueCbor: row.ValueCbor, CreatedUnixNanos: row.CreatedAt.Time.UnixNano()})
					if err != nil && !errors.Is(err, ErrStop) {
						return err
					}
					c.InspectSeq = row.Seq.Int64
					if errors.Is(err, ErrStop) {
						return nil
					}
				}
				if row.InspectSeq > inspect {
					inspect = row.InspectSeq
					progress = true
				}
			}
			c.InspectSeq = inspect
		}
		complete := true
		for _, c := range cursors {
			if c.UntilSeq == nil || c.InspectSeq < *c.UntilSeq {
				complete = false
			}
		}
		if complete {
			return nil
		}
		if progress {
			continue
		}
		for {
			notification, err := s.listener.WaitForNotification(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				if err := s.reconnect(ctx); err != nil {
					return err
				}
				break
			}
			if matchingNotification(notification, s.runID, cursors) {
				// pgx drains notifications from a read into its internal queue. Consume that
				// queue once so a burst causes one replay round rather than one query per wakeup.
				for {
					drainCtx, cancel := context.WithCancel(ctx)
					cancel()
					_, err := s.listener.WaitForNotification(drainCtx)
					if err != nil {
						break
					}
				}
				break
			}
		}
	}
}

func matchingNotification(n *pgconn.Notification, id uuid.UUID, cursors []Cursor) bool {
	for _, c := range cursors {
		if n.Payload == id.String()+":"+c.Stream {
			return true
		}
	}
	return false
}
