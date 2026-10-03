// Package runlog implements keyed append and listener-backed log subscriptions.
package runlog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"regexp"
	"time"

	"connectrpc.com/connect"
	lutrav1 "github.com/brian14708/lutra/gen/lutra/v1"
	"github.com/brian14708/lutra/internal/blob"
	"github.com/brian14708/lutra/internal/db"
	"github.com/fxamacker/cbor/v2"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"
)

const (
	defaultInlineLimit = 64 << 10
	defaultMaxRecord   = 64 << 20
	defaultMaxBatch    = 128 << 20
	maxKeySize         = 1024
	defaultReadLimit   = 100
	maxReadLimit       = 1000
	logValueMIME       = "application/cbor"
)

var streamPattern = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_.-]{0,127}$`)

// Service implements LogService. Store is optional; when it is unavailable,
// values remain inline regardless of InlineLimit so the database-only service
// remains useful in development and tests.
type Service struct {
	DB                *pgxpool.Pool
	Store             *minio.Core
	Bucket            string
	InlineLimit       int
	MaxRecord         int
	MaxBatch          int
	TailBatchSize     int
	MaxResponseBuffer int
	ReconnectDelay    time.Duration
	Retention         time.Duration
	MaxCursorAge      time.Duration
}

func (s Service) limits() (inline, record, batch int) {
	inline, record, batch = s.InlineLimit, s.MaxRecord, s.MaxBatch
	if inline <= 0 {
		inline = defaultInlineLimit
	}
	if record <= 0 {
		record = defaultMaxRecord
	}
	if batch <= 0 {
		batch = defaultMaxBatch
	}
	return
}

func invalid(message string) error {
	return connect.NewError(connect.CodeInvalidArgument, errors.New(message))
}

func internal(err error) error {
	return connect.NewError(connect.CodeInternal, err)
}

func unavailable(err error) error {
	return connect.NewError(connect.CodeUnavailable, err)
}

func (s Service) validateRun(ctx context.Context, id string) (uuid.UUID, error) {
	runID, err := uuid.Parse(id)
	if err != nil {
		return uuid.Nil, invalid("invalid run id")
	}
	if s.DB == nil {
		return uuid.Nil, unavailable(errors.New("log database unavailable"))
	}
	exists, err := db.New(s.DB).RunExists(ctx, runID)
	if err != nil {
		return uuid.Nil, internal(err)
	}
	if !exists {
		return uuid.Nil, connect.NewError(connect.CodeNotFound, errors.New("run not found"))
	}
	return runID, nil
}

func validateStream(stream string) error {
	if !streamPattern.MatchString(stream) {
		return invalid("invalid stream")
	}
	return nil
}

func batchDigest(entries []*lutrav1.LogEntry) []byte {
	h := sha256.New()
	var length [8]byte
	for _, entry := range entries {
		for _, value := range [][]byte{entry.GetKey(), entry.GetValueCbor()} {
			binary.BigEndian.PutUint64(length[:], uint64(len(value)))
			h.Write(length[:])
			h.Write(value)
		}
	}
	return h.Sum(nil)
}

func (s Service) Append(ctx context.Context, req *connect.Request[lutrav1.AppendRequest]) (*connect.Response[lutrav1.AppendResponse], error) {
	msg := req.Msg
	runID, err := s.validateRun(ctx, msg.GetRunId())
	if err != nil {
		return nil, err
	}
	result, err := s.AppendEntries(ctx, runID, msg.GetStream(), msg.GetAppendId(), msg.GetEntries())
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(result), nil
}

// AppendEntries commits a batch and its notification together.
func (s Service) AppendEntries(ctx context.Context, runID uuid.UUID, stream, appendID string, entries []*lutrav1.LogEntry) (*lutrav1.AppendResponse, error) {
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return nil, unavailable(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	result, err := s.AppendTx(ctx, tx, runID, stream, appendID, entries)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, unavailable(err)
	}
	return result, nil
}

// AppendTx appends in the caller's transaction; PostgreSQL delivers notifications on commit.
func (s Service) AppendTx(ctx context.Context, tx pgx.Tx, runID uuid.UUID, stream, appendID string, entries []*lutrav1.LogEntry) (*lutrav1.AppendResponse, error) {
	if err := validateStream(stream); err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		return nil, invalid("append batch is empty")
	}
	_, maxRecord, maxBatch := s.limits()
	total := 0
	for _, entry := range entries {
		value := entry.GetValueCbor()
		if len(entry.GetKey()) > maxKeySize {
			return nil, connect.NewError(connect.CodeResourceExhausted, errors.New("key exceeds 1024 bytes"))
		}
		if len(value) == 0 || len(value)+len(entry.GetKey()) > maxRecord {
			return nil, connect.NewError(connect.CodeResourceExhausted, errors.New("record size exceeded"))
		}
		if err := cbor.Wellformed(value); err != nil {
			return nil, invalid("value is not valid CBOR")
		}
		total += len(value) + len(entry.GetKey())
		if total > maxBatch {
			return nil, connect.NewError(connect.CodeResourceExhausted, errors.New("batch size exceeded"))
		}
	}
	if len(appendID) > 200 {
		return nil, invalid("append id is too long")
	}
	digest := batchDigest(entries)
	queries := db.New(tx)
	streamRow, err := queries.LockLogStream(ctx, db.LockLogStreamParams{RunID: runID, Stream: stream})
	if errors.Is(err, pgx.ErrNoRows) {
		if err := queries.CreateLogStream(ctx, db.CreateLogStreamParams{RunID: runID, Stream: stream, NextSeq: 1}); err != nil {
			return nil, internal(err)
		}
		streamRow, err = queries.LockLogStream(ctx, db.LockLogStreamParams{RunID: runID, Stream: stream})
	}
	if err != nil {
		return nil, internal(err)
	}
	// The stream row lock serializes concurrent retries. Recheck the append
	// record after acquiring it so a racing request returns the original range.
	if appendID != "" {
		previous, lookupErr := queries.GetLogAppend(ctx, db.GetLogAppendParams{RunID: runID, Stream: stream, AppendID: appendID})
		if lookupErr == nil {
			if !bytes.Equal(previous.BatchDigest, digest) {
				return nil, connect.NewError(connect.CodeAlreadyExists, errors.New("append id already belongs to different values"))
			}
			return &lutrav1.AppendResponse{FirstSeq: previous.FirstSeq, LastSeq: previous.LastSeq}, nil
		}
		if !errors.Is(lookupErr, pgx.ErrNoRows) {
			return nil, internal(lookupErr)
		}
	}
	first := streamRow.NextSeq
	last := first + int64(len(entries)) - 1
	if last < first || last == int64(^uint64(0)>>1) {
		return nil, invalid("sequence exhausted")
	}
	inlineLimit, _, _ := s.limits()
	for i, entry := range entries {
		value := entry.GetValueCbor()
		key := entry.GetKey()
		if key == nil {
			key = []byte{}
		}
		valueCbor := value
		if len(value) > inlineLimit && s.Store != nil && s.Bucket != "" {
			uri, uploadErr := s.storeValue(ctx, tx, value)
			if uploadErr != nil {
				return nil, uploadErr
			}
			var err error
			valueCbor, err = cbor.Marshal(cbor.Tag{Number: 32, Content: uri})
			if err != nil {
				return nil, internal(err)
			}
		}
		if err := queries.InsertLogRecord(ctx, db.InsertLogRecordParams{RunID: runID, Stream: stream, Seq: first + int64(i), Key: key, ValueCbor: valueCbor}); err != nil {
			return nil, internal(err)
		}
	}
	if err := queries.UpdateLogStreamNextSeq(ctx, db.UpdateLogStreamNextSeqParams{RunID: runID, Stream: stream, NextSeq: last + 1}); err != nil {
		return nil, internal(err)
	}
	if appendID != "" {
		if err := queries.InsertLogAppend(ctx, db.InsertLogAppendParams{RunID: runID, Stream: stream, AppendID: appendID, BatchDigest: digest, FirstSeq: first, LastSeq: last}); err != nil {
			return nil, internal(err)
		}
	}
	if err := queries.NotifyLogStream(ctx, runID.String()+":"+stream); err != nil {
		return nil, internal(err)
	}
	return &lutrav1.AppendResponse{FirstSeq: first, LastSeq: last}, nil
}

func (s Service) storeValue(ctx context.Context, tx pgx.Tx, value []byte) (string, error) {
	digest := sha256.Sum256(value)
	queries := db.New(tx)
	if _, err := queries.GetBlobBySHA256(ctx, digest[:]); err == nil {
		return blobURI(digest[:]), nil
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return "", internal(err)
	} else {
		objectID := uuid.New()
		_, err := s.Store.PutObject(ctx, s.Bucket, blob.ObjectKey(objectID), bytes.NewReader(value), int64(len(value)), "", "", minio.PutObjectOptions{ContentType: logValueMIME})
		if err != nil {
			return "", unavailable(err)
		}
		if _, err := queries.InsertBlobIfAbsent(ctx, db.InsertBlobIfAbsentParams{Sha256: digest[:], ObjectKey: objectID}); err != nil {
			return "", internal(err)
		}
	}
	return blobURI(digest[:]), nil
}

func blobURI(digest []byte) string {
	return "blob:" + logValueMIME + ";resolve," + base64.StdEncoding.EncodeToString(digest)
}

func (s Service) Read(ctx context.Context, req *connect.Request[lutrav1.ReadRequest]) (*connect.Response[lutrav1.ReadResponse], error) {
	runID, err := s.validateRun(ctx, req.Msg.GetRunId())
	if err != nil {
		return nil, err
	}
	if err := validateStream(req.Msg.GetStream()); err != nil {
		return nil, err
	}
	after := req.Msg.GetAfterSeq()
	if after < 0 {
		return nil, invalid("after_seq must be nonnegative")
	}
	limit := int(req.Msg.GetLimit())
	if limit == 0 {
		limit = defaultReadLimit
	}
	if limit < 1 || limit > maxReadLimit {
		return nil, invalid("limit is out of range")
	}
	rows, err := db.New(s.DB).ReadLogRecords(ctx, db.ReadLogRecordsParams{RunID: runID, Stream: req.Msg.GetStream(), Seq: after, Key: req.Msg.Key, Limit: int32(limit + 1)})
	if err != nil {
		return nil, internal(err)
	}
	truncated := len(rows) > limit
	if truncated {
		rows = rows[:limit]
	}
	records := make([]*lutrav1.LogRecord, 0, len(rows))
	for _, row := range rows {
		records = append(records, &lutrav1.LogRecord{Stream: row.Stream, Seq: row.Seq, Key: row.Key, ValueCbor: row.ValueCbor, CreatedUnixNanos: row.CreatedAt.Time.UnixNano()})
	}
	return connect.NewResponse(&lutrav1.ReadResponse{Records: records, Truncated: truncated}), nil
}

func (s Service) Tail(ctx context.Context, req *connect.Request[lutrav1.TailRequest], stream *connect.ServerStream[lutrav1.TailResponse]) error {
	runID, err := s.validateRun(ctx, req.Msg.GetRunId())
	if err != nil {
		return err
	}
	if len(req.Msg.GetStreams()) == 0 {
		return invalid("at least one stream cursor is required")
	}
	cursors := make([]Cursor, 0, len(req.Msg.GetStreams()))
	seen := make(map[string]struct{}, len(req.Msg.GetStreams()))
	for _, item := range req.Msg.GetStreams() {
		if err := validateStream(item.GetStream()); err != nil {
			return err
		}
		if item.GetAfterSeq() < 0 {
			return invalid("after_seq must be nonnegative")
		}
		if _, ok := seen[item.GetStream()]; ok {
			return invalid("duplicate stream cursor")
		}
		seen[item.GetStream()] = struct{}{}
		cursors = append(cursors, Cursor{Stream: item.GetStream(), InspectSeq: item.GetAfterSeq(), Prefix: item.GetKeyPrefix()})
	}
	sub, err := s.Subscribe(ctx, runID)
	if err != nil {
		return err
	}
	defer sub.Close()
	return sub.Run(ctx, cursors, stream.Send)
}
