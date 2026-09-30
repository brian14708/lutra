package lutra

import (
	"context"
	"errors"
	"time"

	"github.com/brian14708/lutra/internal/blob"
	"github.com/brian14708/lutra/internal/db"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const maxImageSize = 2 << 30

// storeArtifact persists a built image archive as a blob.
func (w *Worker) storeArtifact(ctx context.Context, data []byte) (string, error) {
	if w.Store == nil || w.Bucket == "" {
		return "", errors.New("image artifact store unavailable")
	}
	return blob.Put(ctx, w.Store, w.Bucket, w.queries, archiveMIME, data)
}

// loadArtifact resolves an image artifact URI and verifies its content digest.
func (w *Worker) loadArtifact(ctx context.Context, uri string) ([]byte, error) {
	digest, mimeType, err := blob.ParseURI(uri)
	if err != nil || mimeType != archiveMIME || uri != blob.URI(archiveMIME, digest) {
		return nil, errors.New("invalid image artifact blob URI")
	}
	if w.Store == nil || w.Bucket == "" {
		return nil, errors.New("image artifact unavailable")
	}
	record, err := w.queries.GetBlobBySHA256(ctx, digest)
	if err != nil {
		return nil, err
	}
	return w.readVerified(ctx, record.ObjectKey, digest, maxImageSize)
}

// ensureImage returns a ready image for the key, building it when no ready or
// in-flight build exists.
func (w *Worker) ensureImage(ctx context.Context, imageKey []byte, executor Executor, req *EnvironmentExecution) (*Image, error) {
	for {
		build, owned, err := w.claimImageBuild(ctx, imageKey)
		if err != nil {
			return nil, err
		}
		if build.Status == db.LutraImageBuildStatusReady {
			return &Image{ArtifactURI: build.ArtifactUri}, nil
		}
		if owned {
			return w.buildImage(ctx, build, executor, req)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func (w *Worker) claimImageBuild(ctx context.Context, imageKey []byte) (db.LutraImageBuild, bool, error) {
	tx, err := w.DB.Begin(ctx)
	if err != nil {
		return db.LutraImageBuild{}, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	q := db.New(tx)
	if err := q.ExpireImageBuilds(ctx, imageKey); err != nil {
		return db.LutraImageBuild{}, false, err
	}
	current, err := q.LatestImageBuild(ctx, imageKey)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return current, false, err
	}
	if err == nil && (current.Status == db.LutraImageBuildStatusReady || current.Status == db.LutraImageBuildStatusBuilding) {
		return current, false, tx.Commit(ctx)
	}
	current, err = q.InsertImageBuild(ctx, db.InsertImageBuildParams{ID: uuid.New(), ImageKey: imageKey, ClaimToken: uuid.New()})
	if errors.Is(err, pgx.ErrNoRows) {
		return current, false, tx.Commit(ctx)
	}
	if err != nil {
		return current, false, err
	}
	// A competing build may have finished while the unique-index insert waited.
	previous, err := q.ReadyImageBuild(ctx, imageKey)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return current, false, err
	}
	if err == nil {
		if err := q.DeleteUnneededImageBuild(ctx, current.ID); err != nil {
			return current, false, err
		}
		return previous, false, tx.Commit(ctx)
	}
	return current, true, tx.Commit(ctx)
}

func (w *Worker) buildImage(ctx context.Context, build db.LutraImageBuild, executor Executor, req *EnvironmentExecution) (*Image, error) {
	buildCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	renewDone := make(chan struct{})
	go func() {
		defer close(renewDone)
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-buildCtx.Done():
				return
			case <-ticker.C:
				rows, err := w.queries.RenewImageBuild(buildCtx, db.RenewImageBuildParams{ID: build.ID, ClaimToken: build.ClaimToken})
				if err != nil || rows != 1 {
					cancel()
					return
				}
			}
		}
	}()
	image, buildErr := executor.Build(buildCtx, req)
	cancel()
	<-renewDone
	status, errorText, artifactURI := db.LutraImageBuildStatusReady, "", ""
	if buildErr != nil {
		status = db.LutraImageBuildStatusFailed
		errorText = buildErr.Error()
	} else {
		artifactURI = image.ArtifactURI
	}
	finishCtx, finishCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer finishCancel()
	rows, err := w.queries.FinishImageBuild(finishCtx, db.FinishImageBuildParams{ID: build.ID, ClaimToken: build.ClaimToken, Status: status, ArtifactUri: artifactURI, Error: errorText})
	if err != nil {
		return nil, err
	}
	if rows != 1 {
		return nil, errors.New("image build lease lost")
	}
	if buildErr != nil {
		return nil, buildErr
	}
	return image, nil
}
