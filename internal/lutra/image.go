package lutra

import (
	"context"
	"errors"

	"github.com/brian14708/lutra/internal/blob"
	"github.com/brian14708/lutra/internal/cache"
)

const maxImageSize = 2 << 30

func (w *Worker) storeArtifact(ctx context.Context, data []byte) (string, error) {
	if w.Store == nil || w.Bucket == "" {
		return "", errors.New("image artifact store unavailable")
	}
	return blob.Put(ctx, w.Store, w.Bucket, w.queries, archiveMIME, data)
}

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

func (w *Worker) ensureImage(ctx context.Context, imageKey []byte, executor Executor, req *EnvironmentExecution) (*Image, error) {
	var digest [32]byte
	if len(imageKey) != len(digest) {
		return nil, errors.New("invalid image key")
	}
	copy(digest[:], imageKey)
	result, lease, err := w.Cache.Acquire(ctx, cache.Key{Kind: cache.KindImage, Digest: digest})
	if err != nil {
		return nil, err
	}
	if lease == nil {
		return &Image{ArtifactURI: result.(cache.ImageResult).ArtifactURI}, nil
	}
	image, buildErr := executor.Build(lease.Context(), req)
	if buildErr != nil {
		if err := lease.Fail(ctx, buildErr.Error()); err != nil {
			return nil, err
		}
		return nil, buildErr
	}
	if err := lease.Complete(ctx, cache.ImageResult{ArtifactURI: image.ArtifactURI}); err != nil {
		return nil, err
	}
	return image, nil
}
