// Package lutra contains Lutra workflow and artifact services.
package lutra

import (
	"context"
	"encoding/base64"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"time"
	"uuid"

	"connectrpc.com/connect"
	lutrav1 "github.com/brian14708/lutra/gen/lutra/v1"
	lutrav1connect "github.com/brian14708/lutra/gen/lutra/v1/lutrav1connect"
	"github.com/brian14708/lutra/internal/auth"
	"github.com/brian14708/lutra/internal/db"
	"github.com/brian14708/lutra/internal/digest"
	"github.com/brian14708/lutra/internal/rpcutil"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/minio/minio-go/v7"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// presignTTL bounds how long an upload or download URL stays valid.
const presignTTL = 30 * time.Minute

// artifactObjectKey is the single definition of the object storage layout;
// every writer and reader of artifact payloads must agree on it.
func artifactObjectKey(projectID, artifactID string, chunkIndex int32, uploadID string) string {
	return "artifacts/" + projectID + "/" + artifactID + "/" + strconv.Itoa(int(chunkIndex)) + "/" + uploadID
}

// checksumHeader maps a digest to the S3 checksum header that makes the object
// store validate the upload and retain the checksum for StatObject. Algorithms
// without an S3 checksum equivalent report ok=false; those uploads fall back
// to server-side verification at completion.
func checksumHeader(value string) (name, encoded string, ok bool) {
	algorithm, raw, err := digest.Parse(value)
	if err != nil {
		return "", "", false
	}
	switch algorithm {
	case "sha1":
		return "X-Amz-Checksum-SHA1", base64.StdEncoding.EncodeToString(raw), true
	case "sha256":
		return "X-Amz-Checksum-SHA256", base64.StdEncoding.EncodeToString(raw), true
	}
	return "", "", false
}

// ArtifactService implements artifact upload and download operations.
type ArtifactService struct {
	lutrav1connect.UnimplementedArtifactServiceHandler
	store  *auth.Store
	object *minio.Client
	bucket string
}

// NewArtifactService creates an artifact service backed by the given store.
func NewArtifactService(store *auth.Store, object *minio.Client, bucket string) *ArtifactService {
	return &ArtifactService{store: store, object: object, bucket: bucket}
}

// CreateArtifact creates an empty artifact in a project.
func (s *ArtifactService) CreateArtifact(ctx context.Context, req *connect.Request[lutrav1.CreateArtifactRequest]) (*connect.Response[lutrav1.CreateArtifactResponse], error) {
	if err := s.store.Require(ctx, req.Msg.GetProjectId(), "artifact", "register"); err != nil {
		return nil, err
	}
	a, err := s.store.Queries().CreateArtifact(ctx, db.CreateArtifactParams{ArtifactID: uuid.New(), ProjectID: rpcutil.UUID(req.Msg.GetProjectId()), MimeType: req.Msg.GetMimeType()})
	if err != nil {
		return nil, rpcutil.Conflict(err)
	}
	return connect.NewResponse(&lutrav1.CreateArtifactResponse{Artifact: artifactProto(a)}), nil
}

// AllocateChunk reserves an artifact chunk and returns its upload location.
func (s *ArtifactService) AllocateChunk(ctx context.Context, req *connect.Request[lutrav1.AllocateChunkRequest]) (*connect.Response[lutrav1.AllocateChunkResponse], error) {
	if err := s.store.Require(ctx, req.Msg.GetProjectId(), "artifact", "register"); err != nil {
		return nil, err
	}
	a, err := s.store.Queries().GetArtifact(ctx, db.GetArtifactParams{ArtifactID: rpcutil.UUID(req.Msg.GetArtifactId()), ProjectID: rpcutil.UUID(req.Msg.GetProjectId())})
	if err != nil {
		return nil, rpcutil.DB(err)
	}
	if a.SealTime.Valid {
		return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("artifact is sealed"))
	}
	if req.Msg.GetDigest() == "" || req.Msg.GetSizeBytes() == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("chunk size and digest are required"))
	}
	if err := digest.Validate(req.Msg.GetDigest()); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	uploadID := uuid.New()
	objectKey := artifactObjectKey(a.ProjectID.String(), a.ArtifactID.String(), int32(req.Msg.GetChunkIndex()), uploadID.String())
	chunk, err := s.store.Queries().UpsertArtifactChunk(ctx, db.UpsertArtifactChunkParams{ArtifactID: a.ArtifactID, ChunkIndex: int32(req.Msg.GetChunkIndex()), SizeBytes: int64(req.Msg.GetSizeBytes()), Digest: req.Msg.GetDigest(), UploadID: uploadID})
	if err != nil {
		return nil, rpcutil.Internal(err)
	}
	if s.object == nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("object store is not configured"))
	}
	// When the digest has an S3 checksum equivalent, sign it into the
	// presigned PUT so the object store itself rejects a mismatched upload.
	headers := map[string]string{"Content-Length": strconv.FormatUint(req.Msg.GetSizeBytes(), 10)}
	var putURL *url.URL
	if name, value, ok := checksumHeader(req.Msg.GetDigest()); ok {
		headers[name] = value
		putURL, err = s.object.PresignHeader(ctx, http.MethodPut, s.bucket, objectKey, presignTTL, url.Values{}, http.Header{name: {value}})
	} else {
		putURL, err = s.object.PresignedPutObject(ctx, s.bucket, objectKey, presignTTL)
	}
	if err != nil {
		return nil, rpcutil.Internal(err)
	}
	return connect.NewResponse(&lutrav1.AllocateChunkResponse{Chunk: chunkProto(chunk), UploadUrl: putURL.String(), UploadHeaders: headers, UploadExpiresAt: timestamppb.New(time.Now().Add(presignTTL))}), nil
}

// CompleteChunk verifies and commits an uploaded artifact chunk.
func (s *ArtifactService) CompleteChunk(ctx context.Context, req *connect.Request[lutrav1.CompleteChunkRequest]) (*connect.Response[lutrav1.CompleteChunkResponse], error) {
	if err := s.store.Require(ctx, req.Msg.GetProjectId(), "artifact", "register"); err != nil {
		return nil, err
	}
	a, err := s.store.Queries().GetArtifact(ctx, db.GetArtifactParams{ArtifactID: rpcutil.UUID(req.Msg.GetArtifactId()), ProjectID: rpcutil.UUID(req.Msg.GetProjectId())})
	if err != nil {
		return nil, rpcutil.DB(err)
	}
	allocated, err := s.store.Queries().GetArtifactChunk(ctx, db.GetArtifactChunkParams{ArtifactID: a.ArtifactID, ChunkIndex: int32(req.Msg.GetChunkIndex())})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("chunk allocation not found"))
		}
		return nil, rpcutil.Internal(err)
	}
	if allocated.UploadID.String() != req.Msg.GetUploadId() {
		return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("chunk upload ID is stale"))
	}
	if s.object == nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("object store is not configured"))
	}
	objectKey := artifactObjectKey(a.ProjectID.String(), a.ArtifactID.String(), allocated.ChunkIndex, allocated.UploadID.String())
	if err := s.verifyObject(ctx, objectKey, allocated.SizeBytes, allocated.Digest); err != nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition, err)
	}
	chunk, err := s.store.Queries().CompleteArtifactChunk(ctx, db.CompleteArtifactChunkParams{ArtifactID: a.ArtifactID, ChunkIndex: int32(req.Msg.GetChunkIndex()), UploadID: rpcutil.UUID(req.Msg.GetUploadId())})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("chunk allocation not found or upload ID is stale"))
		}
		return nil, rpcutil.Internal(err)
	}
	return connect.NewResponse(&lutrav1.CompleteChunkResponse{Chunk: chunkProto(chunk)}), nil
}

// SealArtifact seals an artifact after all chunks have been completed.
func (s *ArtifactService) SealArtifact(ctx context.Context, req *connect.Request[lutrav1.SealArtifactRequest]) (*connect.Response[lutrav1.SealArtifactResponse], error) {
	if err := s.store.Require(ctx, req.Msg.GetProjectId(), "artifact", "register"); err != nil {
		return nil, err
	}
	tx, err := s.store.Begin(ctx)
	if err != nil {
		return nil, rpcutil.Internal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	q := s.store.Queries().WithTx(tx)
	a, err := q.LockArtifact(ctx, db.LockArtifactParams{ArtifactID: rpcutil.UUID(req.Msg.GetArtifactId()), ProjectID: rpcutil.UUID(req.Msg.GetProjectId())})
	if err != nil {
		return nil, rpcutil.DB(err)
	}
	if a.SealTime.Valid {
		return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("artifact is sealed"))
	}
	chunks, err := q.ListArtifactChunks(ctx, a.ArtifactID)
	if err != nil {
		return nil, rpcutil.Internal(err)
	}
	if len(chunks) != int(req.Msg.GetChunkCount()) {
		return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("all chunk indexes must be allocated"))
	}
	// Chunk payloads were verified against their digests at completion, so the
	// sealed digest is derived from the chunk digests without re-reading the
	// object store.
	var size int64
	chunkDigests := make([]string, 0, len(chunks))
	for i, chunk := range chunks {
		if chunk.ChunkIndex != int32(i) || !chunk.CompleteTime.Valid {
			return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("chunks must be contiguous and completed"))
		}
		size += chunk.SizeBytes
		chunkDigests = append(chunkDigests, chunk.Digest)
	}
	sealedDigest, err := digest.Combine(chunkDigests)
	if err != nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition, err)
	}
	sealed, err := q.SealArtifact(ctx, db.SealArtifactParams{ArtifactID: a.ArtifactID, ProjectID: a.ProjectID, SizeBytes: size, Digest: pgtype.Text{String: sealedDigest, Valid: true}})
	if err != nil {
		return nil, rpcutil.DB(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, rpcutil.Internal(err)
	}
	return connect.NewResponse(&lutrav1.SealArtifactResponse{Artifact: artifactProto(sealed)}), nil
}

// verifyObject confirms an uploaded chunk matches its allocation. The S3
// checksum recorded at upload time answers this without downloading the
// payload; stores or algorithms without checksum support fall back to hashing
// the payload directly.
func (s *ArtifactService) verifyObject(ctx context.Context, objectKey string, expectedSize int64, expectedDigest string) error {
	info, err := s.object.StatObject(ctx, s.bucket, objectKey, minio.StatObjectOptions{})
	if err != nil {
		return err
	}
	if info.Size != expectedSize {
		return errors.New("uploaded chunk size does not match allocation")
	}
	algorithm, raw, err := digest.Parse(expectedDigest)
	if err != nil {
		return err
	}
	var checksum string
	switch algorithm {
	case "sha1":
		checksum = info.ChecksumSHA1
	case "sha256":
		checksum = info.ChecksumSHA256
	}
	if checksum != "" {
		if checksum != base64.StdEncoding.EncodeToString(raw) {
			return errors.New("uploaded chunk digest does not match allocation")
		}
		return nil
	}
	object, err := s.object.GetObject(ctx, s.bucket, objectKey, minio.GetObjectOptions{})
	if err != nil {
		return err
	}
	defer func() { _ = object.Close() }()
	written, err := digest.VerifyReader(expectedDigest, object)
	if err != nil {
		return err
	}
	if written != expectedSize {
		return errors.New("uploaded chunk size does not match allocation")
	}
	return nil
}

// DownloadArtifact returns the bytes of a sealed artifact.
func (s *ArtifactService) DownloadArtifact(ctx context.Context, req *connect.Request[lutrav1.DownloadArtifactRequest]) (*connect.Response[lutrav1.DownloadArtifactResponse], error) {
	if err := s.store.Require(ctx, req.Msg.GetProjectId(), "artifact", "read"); err != nil {
		return nil, err
	}
	a, err := s.store.Queries().GetArtifact(ctx, db.GetArtifactParams{ArtifactID: rpcutil.UUID(req.Msg.GetArtifactId()), ProjectID: rpcutil.UUID(req.Msg.GetProjectId())})
	if err != nil {
		return nil, rpcutil.DB(err)
	}
	if !a.SealTime.Valid {
		return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("artifact is not sealed"))
	}
	chunks, err := s.store.Queries().ListArtifactChunks(ctx, a.ArtifactID)
	if err != nil {
		return nil, rpcutil.Internal(err)
	}
	response := &lutrav1.DownloadArtifactResponse{Artifact: artifactProto(a), Chunks: make([]*lutrav1.ArtifactDownloadChunk, 0, len(chunks))}
	expires := timestamppb.New(time.Now().Add(presignTTL))
	for _, c := range chunks {
		value := &lutrav1.ArtifactDownloadChunk{ChunkIndex: uint32(c.ChunkIndex), SizeBytes: uint64(c.SizeBytes), Digest: c.Digest}
		if s.object != nil {
			objectKey := artifactObjectKey(a.ProjectID.String(), a.ArtifactID.String(), c.ChunkIndex, c.UploadID.String())
			getURL, getErr := s.object.PresignedGetObject(ctx, s.bucket, objectKey, presignTTL, url.Values{})
			if getErr != nil {
				return nil, rpcutil.Internal(getErr)
			}
			value.DownloadUrl = getURL.String()
			value.DownloadExpiresAt = expires
		}
		response.Chunks = append(response.Chunks, value)
	}
	return connect.NewResponse(response), nil
}

func artifactProto(a db.LutraArtifact) *lutrav1.Artifact {
	state := lutrav1.ArtifactState_ARTIFACT_STATE_ALLOCATED
	if a.SealTime.Valid {
		state = lutrav1.ArtifactState_ARTIFACT_STATE_SEALED
	}
	return &lutrav1.Artifact{ProjectId: a.ProjectID.String(), ArtifactId: a.ArtifactID.String(), State: state, MimeType: a.MimeType, Digest: a.Digest.String, CreateTime: rpcutil.Timestamp(a.CreateTime), SealTime: rpcutil.Timestamp(a.SealTime)}
}

func chunkProto(c db.LutraArtifactChunk) *lutrav1.ArtifactChunk {
	state := lutrav1.ArtifactChunkState_ARTIFACT_CHUNK_STATE_ALLOCATED
	if c.CompleteTime.Valid {
		state = lutrav1.ArtifactChunkState_ARTIFACT_CHUNK_STATE_COMPLETED
	}
	return &lutrav1.ArtifactChunk{ArtifactId: c.ArtifactID.String(), ChunkIndex: uint32(c.ChunkIndex), SizeBytes: uint64(c.SizeBytes), Digest: c.Digest, State: state, UploadId: c.UploadID.String(), CompleteTime: rpcutil.Timestamp(c.CompleteTime)}
}
