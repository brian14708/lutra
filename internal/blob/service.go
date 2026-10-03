package blob

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"connectrpc.com/connect"
	lutrav1 "github.com/brian14708/lutra/gen/lutra/v1"
	"github.com/brian14708/lutra/internal/db"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"
)

const (
	partSize            = 8 << 20
	urlLifetime         = 15 * time.Minute
	downloadURLLifetime = 24 * time.Hour
)

func uploadMetadata(values map[string]string) (map[string]string, []byte, error) {
	if len(values) > 32 {
		return nil, nil, errors.New("too many blob metadata entries")
	}
	clean := make(map[string]string, len(values))
	for key, value := range values {
		if len(key) == 0 || len(key) > 64 || len(value) > 1024 || strings.ToLower(key) != key {
			return nil, nil, errors.New("invalid blob metadata")
		}
		clean[key] = value
	}
	data, err := json.Marshal(clean)
	if err != nil || len(data) > 16<<10 {
		return nil, nil, errors.New("invalid blob metadata")
	}
	return clean, data, nil
}

// Service implements the blob ConnectRPC API.
type Service struct {
	DB     *pgxpool.Pool
	Store  *minio.Core
	Signer *minio.Client
	Bucket string
}

// ObjectKey returns the storage key of an existing blob.
func ObjectKey(id uuid.UUID) string { return "blobs/" + id.String() }

// URI returns the canonical blob URI for a content digest and MIME type.
func URI(mimeType string, digest []byte) string {
	return "blob:" + mimeType + "," + base64.StdEncoding.EncodeToString(digest)
}

// Put stores data as a content-addressed blob, deduplicating by digest, and
// returns its canonical URI.
func Put(ctx context.Context, store *minio.Core, bucket string, queries *db.Queries, mimeType string, data []byte) (string, error) {
	hash := sha256.Sum256(data)
	uri := URI(mimeType, hash[:])
	if _, err := queries.GetBlobBySHA256(ctx, hash[:]); err == nil {
		return uri, nil
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return "", err
	}
	id := uuid.New()
	if _, err := store.Client.PutObject(ctx, bucket, ObjectKey(id), bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{ContentType: mimeType}); err != nil {
		return "", err
	}
	rows, err := queries.InsertBlobIfAbsent(ctx, db.InsertBlobIfAbsentParams{Sha256: hash[:], ObjectKey: id})
	if err != nil {
		return "", err
	}
	if rows == 0 {
		// Another upload owns the content; drop the duplicate object.
		_ = store.RemoveObject(ctx, bucket, ObjectKey(id), minio.RemoveObjectOptions{})
	}
	return uri, nil
}

func partCount(size int64) int32 {
	return int32((size + partSize - 1) / partSize)
}

// ParseURI returns the content digest and MIME type in a blob URI.
func ParseURI(uri string) ([]byte, string, error) {
	if !strings.HasPrefix(uri, "blob:") {
		return nil, "", errors.New("blob URI must start with blob")
	}
	head, digestName, ok := strings.Cut(uri[5:], ",")
	if !ok || strings.TrimSpace(head) == "" {
		return nil, "", errors.New("invalid blob URI")
	}
	head = strings.TrimSuffix(strings.TrimSuffix(head, ";resolve"), ";resolve=true")
	digest, err := base64.StdEncoding.DecodeString(digestName)
	if err != nil || len(digest) != 32 || base64.StdEncoding.EncodeToString(digest) != digestName {
		return nil, "", errors.New("invalid blob URI digest")
	}
	if _, _, err := mime.ParseMediaType(head); err != nil {
		return nil, "", errors.New("invalid blob URI MIME type")
	}
	return digest, head, nil
}

func objectMissing(err error) bool {
	if err == nil {
		return false
	}
	code := minio.ToErrorResponse(err).Code
	return code == "NoSuchKey" || code == "NoSuchObject" || code == "NotFound"
}

func (s Service) ready() error {
	if s.DB == nil || s.Store == nil || s.Signer == nil || s.Bucket == "" {
		return connect.NewError(connect.CodeUnavailable, errors.New("blob store unavailable"))
	}
	return nil
}

func (s Service) CreateUpload(ctx context.Context, req *connect.Request[lutrav1.CreateUploadRequest]) (*connect.Response[lutrav1.CreateUploadResponse], error) {
	if err := s.ready(); err != nil {
		return nil, err
	}
	msg := req.Msg
	digest := msg.GetContentSha256()
	mimeType := msg.GetMimeType()
	size := msg.GetSize()
	if len(digest) != 32 || size < 0 || size > 83886080000 || strings.TrimSpace(mimeType) == "" || len(mimeType) > 255 {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("invalid upload metadata"))
	}
	if _, _, err := mime.ParseMediaType(mimeType); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("invalid MIME type"))
	}
	metadata, metadataJSON, err := uploadMetadata(msg.GetMetadata())
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	queries := db.New(s.DB)
	_, err = queries.GetBlobBySHA256(ctx, digest)
	if err == nil {
		return connect.NewResponse(&lutrav1.CreateUploadResponse{AlreadyExists: true}), nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	id, err := uuid.NewV7()
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	objectID, err := uuid.NewRandom()
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	key := ObjectKey(objectID)
	parts := int32(1)
	multipart := pgtype.Text{}
	if size > partSize {
		parts = partCount(size)
		uploadID, err := s.Store.NewMultipartUpload(ctx, s.Bucket, key, minio.PutObjectOptions{ContentType: mimeType, UserMetadata: metadata})
		if err != nil {
			return nil, connect.NewError(connect.CodeUnavailable, err)
		}
		multipart = pgtype.Text{String: uploadID, Valid: true}
	}
	err = queries.CreateBlobUpload(ctx, db.CreateBlobUploadParams{
		SessionID: id, Sha256: digest, ObjectKey: objectID,
		Size: size, MimeType: mimeType, Metadata: metadataJSON, MultipartID: multipart,
	})
	if err != nil {
		if multipart.Valid {
			_ = s.Store.AbortMultipartUpload(ctx, s.Bucket, key, multipart.String)
		}
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&lutrav1.CreateUploadResponse{SessionId: id.String(), PartSize: partSize, PartCount: parts}), nil
}

func (s Service) session(ctx context.Context, id string) (db.LutraBlobUpload, error) {
	sessionID, err := uuid.Parse(id)
	if err != nil {
		return db.LutraBlobUpload{}, connect.NewError(connect.CodeInvalidArgument, errors.New("invalid session ID"))
	}
	row, err := db.New(s.DB).GetBlobUpload(ctx, sessionID)
	if errors.Is(err, pgx.ErrNoRows) {
		return db.LutraBlobUpload{}, connect.NewError(connect.CodeNotFound, errors.New("upload session not found"))
	}
	if err != nil {
		return db.LutraBlobUpload{}, connect.NewError(connect.CodeInternal, err)
	}
	return row, nil
}

func (s Service) PresignPart(ctx context.Context, req *connect.Request[lutrav1.PresignPartRequest]) (*connect.Response[lutrav1.PresignPartResponse], error) {
	if err := s.ready(); err != nil {
		return nil, err
	}
	session, err := s.session(ctx, req.Msg.GetSessionId())
	if err != nil {
		return nil, err
	}
	count := int32(1)
	if session.MultipartID.Valid {
		count = partCount(session.Size)
	}
	number := req.Msg.GetPartNumber()
	if number < 1 || number > count {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("invalid part number"))
	}
	var signed *url.URL
	var headers map[string]string
	if session.MultipartID.Valid {
		params := url.Values{"partNumber": {strconv.Itoa(int(number))}, "uploadId": {session.MultipartID.String}}
		signed, err = s.Signer.Presign(ctx, http.MethodPut, s.Bucket, ObjectKey(session.ObjectKey), urlLifetime, params)
	} else {
		var metadata map[string]string
		if err := json.Unmarshal(session.Metadata, &metadata); err != nil {
			return nil, connect.NewError(connect.CodeInternal, errors.New("invalid stored blob metadata"))
		}
		header := http.Header{
			"If-None-Match":         {"*"},
			"Content-Type":          {session.MimeType},
			"x-amz-checksum-sha256": {base64.StdEncoding.EncodeToString(session.Sha256)},
		}
		for key, value := range metadata {
			header.Set("X-Amz-Meta-"+key, value)
		}
		headers = make(map[string]string, len(header))
		for key, values := range header {
			headers[key] = values[0]
		}
		signed, err = s.Signer.PresignHeader(ctx, http.MethodPut, s.Bucket, ObjectKey(session.ObjectKey), urlLifetime, nil, header)
	}
	if err != nil {
		return nil, connect.NewError(connect.CodeUnavailable, err)
	}
	return connect.NewResponse(&lutrav1.PresignPartResponse{Url: signed.String(), Headers: headers}), nil
}

func (s Service) CompleteUpload(ctx context.Context, req *connect.Request[lutrav1.CompleteUploadRequest]) (*connect.Response[lutrav1.CompleteUploadResponse], error) {
	if err := s.ready(); err != nil {
		return nil, err
	}
	sessionID, err := uuid.Parse(req.Msg.GetSessionId())
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("invalid session ID"))
	}
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	queries := db.New(s.DB).WithTx(tx)
	session, err := queries.LockBlobUpload(ctx, sessionID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("upload session not found"))
	}
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	parts := req.Msg.GetParts()
	key := ObjectKey(session.ObjectKey)
	if !session.MultipartID.Valid {
		if len(parts) != 0 {
			return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("single PUT requires no parts"))
		}
		object, statErr := s.Store.StatObject(ctx, s.Bucket, key, minio.StatObjectOptions{Checksum: true})
		if objectMissing(statErr) {
			return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("blob was not uploaded"))
		}
		if statErr != nil {
			return nil, connect.NewError(connect.CodeUnavailable, statErr)
		}
		if object.Size != session.Size {
			return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("blob size mismatch"))
		}
		if object.ChecksumSHA256 != base64.StdEncoding.EncodeToString(session.Sha256) {
			return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("blob checksum unavailable or mismatched"))
		}
	} else {
		count := int(partCount(session.Size))
		if len(parts) != count {
			return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("incorrect part count"))
		}
		complete := make([]minio.CompletePart, count)
		for index, part := range parts {
			if part.GetNumber() != int32(index+1) || strings.TrimSpace(part.GetEtag()) == "" {
				return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("invalid part list"))
			}
			complete[index] = minio.CompletePart{PartNumber: index + 1, ETag: part.GetEtag()}
		}
		opts := minio.PutObjectOptions{}
		opts.SetMatchETagExcept("*")
		_, err = s.Store.CompleteMultipartUpload(ctx, s.Bucket, key, session.MultipartID.String, complete, opts)
		// A complete that errors may still have materialized the object (e.g. a
		// retried request); a single stat disambiguates and feeds the size check.
		object, statErr := s.Store.StatObject(ctx, s.Bucket, key, minio.StatObjectOptions{})
		if err != nil && statErr != nil {
			return nil, connect.NewError(connect.CodeUnavailable, err)
		}
		if statErr != nil {
			return nil, connect.NewError(connect.CodeUnavailable, statErr)
		}
		if object.Size != session.Size {
			return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("blob size mismatch"))
		}
		reader, _, _, err := s.Store.GetObject(ctx, s.Bucket, key, minio.GetObjectOptions{})
		if err != nil {
			return nil, connect.NewError(connect.CodeUnavailable, err)
		}
		hash := sha256.New()
		size, readErr := io.Copy(hash, io.LimitReader(reader, session.Size+1))
		closeErr := reader.Close()
		if err := errors.Join(readErr, closeErr); err != nil {
			return nil, connect.NewError(connect.CodeUnavailable, err)
		}
		if size != session.Size || !bytes.Equal(hash.Sum(nil), session.Sha256) {
			return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("blob content digest mismatch"))
		}
	}
	rows, err := queries.InsertBlobIfAbsent(ctx, db.InsertBlobIfAbsentParams{Sha256: session.Sha256, ObjectKey: session.ObjectKey})
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	if err := queries.DeleteBlobUpload(ctx, session.SessionID); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	if rows == 0 {
		// Another upload owns the content; drop the duplicate object.
		_ = s.Store.RemoveObject(ctx, s.Bucket, key, minio.RemoveObjectOptions{})
	}
	return connect.NewResponse(&lutrav1.CompleteUploadResponse{Uri: URI(session.MimeType, session.Sha256)}), nil
}

func (s Service) AbortUpload(ctx context.Context, req *connect.Request[lutrav1.AbortUploadRequest]) (*connect.Response[lutrav1.AbortUploadResponse], error) {
	if err := s.ready(); err != nil {
		return nil, err
	}
	session, err := s.session(ctx, req.Msg.GetSessionId())
	if err != nil {
		return nil, err
	}
	if err := s.abortSession(ctx, session); err != nil {
		return nil, connect.NewError(connect.CodeUnavailable, err)
	}
	return connect.NewResponse(&lutrav1.AbortUploadResponse{}), nil
}

func (s Service) abortSession(ctx context.Context, session db.LutraBlobUpload) error {
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	queries := db.New(s.DB).WithTx(tx)
	row, err := queries.LockBlobUpload(ctx, session.SessionID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	session = row
	key := ObjectKey(session.ObjectKey)
	if session.MultipartID.Valid {
		_ = s.Store.AbortMultipartUpload(ctx, s.Bucket, key, session.MultipartID.String)
	}
	if err := s.Store.RemoveObject(ctx, s.Bucket, key, minio.RemoveObjectOptions{}); err != nil {
		return err
	}
	if err := queries.DeleteBlobUpload(ctx, session.SessionID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// CleanupExpiredUploads removes stale multipart sessions and staged objects.
func (s Service) CleanupExpiredUploads(ctx context.Context) {
	rows, err := db.New(s.DB).ListExpiredBlobUploads(ctx)
	if err != nil {
		return
	}
	for _, row := range rows {
		_ = s.abortSession(ctx, row)
	}
}

// RunCleanup sweeps expired upload sessions once at startup and then on the
// given interval until ctx is canceled.
func (s Service) RunCleanup(ctx context.Context, interval time.Duration) {
	s.CleanupExpiredUploads(ctx)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.CleanupExpiredUploads(ctx)
		case <-ctx.Done():
			return
		}
	}
}

func (s Service) GetDownload(ctx context.Context, req *connect.Request[lutrav1.GetDownloadRequest]) (*connect.Response[lutrav1.GetDownloadResponse], error) {
	if err := s.ready(); err != nil {
		return nil, err
	}
	digest, _, parseErr := ParseURI(req.Msg.GetUri())
	if parseErr != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, parseErr)
	}
	record, err := db.New(s.DB).GetBlobBySHA256(ctx, digest)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("blob not found"))
	}
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	key := ObjectKey(record.ObjectKey)
	_, err = s.Store.StatObject(ctx, s.Bucket, key, minio.StatObjectOptions{})
	if objectMissing(err) {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("blob not found"))
	}
	if err != nil {
		return nil, connect.NewError(connect.CodeUnavailable, err)
	}
	signed, err := s.Signer.PresignedGetObject(ctx, s.Bucket, key, downloadURLLifetime, nil)
	if err != nil {
		return nil, connect.NewError(connect.CodeUnavailable, err)
	}
	return connect.NewResponse(&lutrav1.GetDownloadResponse{Url: signed.String()}), nil
}
