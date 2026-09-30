// Package logstore implements the durable per-run CBOR log service.
package logstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"io"
	"regexp"
	"time"

	"connectrpc.com/connect"
	lutrav1 "github.com/brian14708/lutra/gen/lutra/v1"
	"github.com/brian14708/lutra/internal/blob"
	"github.com/brian14708/lutra/internal/db"
	"github.com/fxamacker/cbor/v2"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"
)

const (
	defaultInlineLimit = 64 << 10
	defaultMaxRecord   = 64 << 20
	defaultMaxBatch    = 128 << 20
	defaultReadLimit   = 100
	maxReadLimit       = 1000
	logValueMIME       = "application/cbor"
)

var streamPattern = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_.-]{0,127}$`)

// Service implements LogService. Store is optional; when it is unavailable,
// values remain inline regardless of InlineLimit so the database-only service
// remains useful in development and tests.
type Service struct {
	DB          *pgxpool.Pool
	Store       *minio.Core
	Bucket      string
	InlineLimit int
	MaxRecord   int
	MaxBatch    int
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
	if s.DB == nil {
		return uuid.Nil, unavailable(errors.New("log database unavailable"))
	}
	runID, err := uuid.Parse(id)
	if err != nil {
		return uuid.Nil, invalid("invalid run id")
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

func batchDigest(values [][]byte) []byte {
	h := sha256.New()
	var length [8]byte
	for _, value := range values {
		for i := range length {
			length[i] = byte(len(value) >> (8 * (7 - i)))
		}
		h.Write(length[:])
		h.Write(value)
	}
	return h.Sum(nil)
}

func (s Service) Append(ctx context.Context, req *connect.Request[lutrav1.AppendRequest]) (*connect.Response[lutrav1.AppendResponse], error) {
	msg := req.Msg
	runID, err := s.validateRun(ctx, msg.GetRunId())
	if err != nil {
		return nil, err
	}
	if err := validateStream(msg.GetStream()); err != nil {
		return nil, err
	}
	values := msg.GetValuesCbor()
	if len(values) == 0 {
		return nil, invalid("append batch is empty")
	}
	_, maxRecord, maxBatch := s.limits()
	total := 0
	for _, value := range values {
		if len(value) == 0 || len(value) > maxRecord {
			return nil, connect.NewError(connect.CodeResourceExhausted, errors.New("record size exceeded"))
		}
		if err := cbor.Wellformed(value); err != nil {
			return nil, invalid("value is not valid CBOR")
		}
		total += len(value)
		if total > maxBatch {
			return nil, connect.NewError(connect.CodeResourceExhausted, errors.New("batch size exceeded"))
		}
	}
	appendID := msg.GetAppendId()
	if len(appendID) > 200 {
		return nil, invalid("append id is too long")
	}
	digest := batchDigest(values)
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return nil, unavailable(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	queries := db.New(tx)
	streamRow, err := queries.LockLogStream(ctx, db.LockLogStreamParams{RunID: runID, Stream: msg.GetStream()})
	if errors.Is(err, pgx.ErrNoRows) {
		if err := queries.CreateLogStream(ctx, db.CreateLogStreamParams{RunID: runID, Stream: msg.GetStream(), NextSeq: 1}); err != nil {
			return nil, internal(err)
		}
		streamRow, err = queries.LockLogStream(ctx, db.LockLogStreamParams{RunID: runID, Stream: msg.GetStream()})
	}
	if err != nil {
		return nil, internal(err)
	}
	// The stream row lock serializes concurrent retries. Recheck the append
	// record after acquiring it so a racing request returns the original range.
	if appendID != "" {
		previous, lookupErr := queries.GetLogAppend(ctx, db.GetLogAppendParams{RunID: runID, Stream: msg.GetStream(), AppendID: appendID})
		if lookupErr == nil {
			if !bytes.Equal(previous.BatchDigest, digest) {
				return nil, connect.NewError(connect.CodeAlreadyExists, errors.New("append id already belongs to different values"))
			}
			if err := tx.Commit(ctx); err != nil {
				return nil, unavailable(err)
			}
			return connect.NewResponse(&lutrav1.AppendResponse{FirstSeq: previous.FirstSeq, LastSeq: previous.LastSeq}), nil
		}
		if !errors.Is(lookupErr, pgx.ErrNoRows) {
			return nil, internal(lookupErr)
		}
	}
	first := streamRow.NextSeq
	last := first + int64(len(values)) - 1
	inlineLimit, _, _ := s.limits()
	for i, value := range values {
		valueCbor := value
		valueURI := pgtype.Text{}
		if len(value) > inlineLimit && s.Store != nil && s.Bucket != "" {
			uri, uploadErr := s.storeValue(ctx, tx, value)
			if uploadErr != nil {
				return nil, uploadErr
			}
			valueCbor = nil
			valueURI = pgtype.Text{String: uri, Valid: true}
		}
		if err := queries.InsertLogRecord(ctx, db.InsertLogRecordParams{RunID: runID, Stream: msg.GetStream(), Seq: first + int64(i), ValueCbor: valueCbor, ValueUri: valueURI, PayloadSize: int64(len(value))}); err != nil {
			return nil, internal(err)
		}
	}
	if err := queries.UpdateLogStreamNextSeq(ctx, db.UpdateLogStreamNextSeqParams{RunID: runID, Stream: msg.GetStream(), NextSeq: last + 1}); err != nil {
		return nil, internal(err)
	}
	if appendID != "" {
		if err := queries.InsertLogAppend(ctx, db.InsertLogAppendParams{RunID: runID, Stream: msg.GetStream(), AppendID: appendID, BatchDigest: digest, FirstSeq: first, LastSeq: last}); err != nil {
			return nil, internal(err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, unavailable(err)
	}
	return connect.NewResponse(&lutrav1.AppendResponse{FirstSeq: first, LastSeq: last}), nil
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
	return "blob:" + logValueMIME + "," + base64.StdEncoding.EncodeToString(digest)
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
	rows, err := db.New(s.DB).ReadLogRecords(ctx, db.ReadLogRecordsParams{RunID: runID, Stream: req.Msg.GetStream(), Seq: after, Limit: int32(limit + 1)})
	if err != nil {
		return nil, internal(err)
	}
	truncated := len(rows) > limit
	if truncated {
		rows = rows[:limit]
	}
	records := make([]*lutrav1.LogRecord, 0, len(rows))
	for _, row := range rows {
		value, err := s.rowValue(ctx, row.ValueCbor, row.ValueUri)
		if err != nil {
			return nil, err
		}
		records = append(records, &lutrav1.LogRecord{Stream: row.Stream, Seq: row.Seq, ValueCbor: value, CreatedUnixNanos: row.CreatedAt.Time.UnixNano()})
	}
	return connect.NewResponse(&lutrav1.ReadResponse{Records: records, Truncated: truncated}), nil
}

func (s Service) rowValue(ctx context.Context, inline []byte, uri pgtype.Text) ([]byte, error) {
	if len(inline) != 0 {
		return inline, nil
	}
	if !uri.Valid || s.Store == nil || s.Bucket == "" {
		return nil, unavailable(errors.New("log blob store unavailable"))
	}
	digest, mimeType, err := blob.ParseURI(uri.String)
	if err != nil || mimeType != logValueMIME {
		return nil, internal(errors.New("invalid log blob URI"))
	}
	entry, err := db.New(s.DB).GetBlobBySHA256(ctx, digest)
	if err != nil {
		return nil, unavailable(err)
	}
	reader, _, _, err := s.Store.GetObject(ctx, s.Bucket, blob.ObjectKey(entry.ObjectKey), minio.GetObjectOptions{})
	if err != nil {
		return nil, unavailable(err)
	}
	defer func() { _ = reader.Close() }()
	value, err := io.ReadAll(io.LimitReader(reader, int64(s.limitsMaxRecord())+1))
	if err != nil {
		return nil, unavailable(err)
	}
	if len(value) > s.limitsMaxRecord() {
		return nil, connect.NewError(connect.CodeResourceExhausted, errors.New("log blob exceeds record limit"))
	}
	return value, nil
}

func (s Service) limitsMaxRecord() int { _, record, _ := s.limits(); return record }

type cursor struct {
	stream string
	after  int64
}

func (s Service) Tail(ctx context.Context, req *connect.Request[lutrav1.TailRequest], stream *connect.ServerStream[lutrav1.TailResponse]) error {
	runID, err := s.validateRun(ctx, req.Msg.GetRunId())
	if err != nil {
		return err
	}
	if len(req.Msg.GetStreams()) == 0 {
		return invalid("at least one stream cursor is required")
	}
	cursors := make([]cursor, 0, len(req.Msg.GetStreams()))
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
		cursors = append(cursors, cursor{stream: item.GetStream(), after: item.GetAfterSeq()})
	}
	for {
		progress := false
		for i := range cursors {
			// One record per cursor per pass keeps a hot stream from starving
			// quieter streams and lets RPC flow control apply between records.
			rows, readErr := db.New(s.DB).ReadLogRecords(ctx, db.ReadLogRecordsParams{RunID: runID, Stream: cursors[i].stream, Seq: cursors[i].after, Limit: 1})
			if readErr != nil {
				return internal(readErr)
			}
			for _, row := range rows {
				value, valueErr := s.rowValue(ctx, row.ValueCbor, row.ValueUri)
				if valueErr != nil {
					return valueErr
				}
				if sendErr := stream.Send(&lutrav1.TailResponse{Stream: row.Stream, Seq: row.Seq, ValueCbor: value, CreatedUnixNanos: row.CreatedAt.Time.UnixNano()}); sendErr != nil {
					return sendErr
				}
				cursors[i].after = row.Seq
				progress = true
			}
		}
		if progress {
			continue
		}
		timer := time.NewTimer(200 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
