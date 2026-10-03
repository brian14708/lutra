package runlog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"

	"github.com/brian14708/lutra/internal/blob"
	"github.com/brian14708/lutra/internal/db"
	"github.com/fxamacker/cbor/v2"
	"github.com/minio/minio-go/v7"
)

func (s Service) resolveValue(ctx context.Context, value []byte) ([]byte, error) {
	if len(value) == 0 || value[0]>>5 != 6 {
		return value, nil
	}
	var tag cbor.RawTag
	if err := cbor.Unmarshal(value, &tag); err != nil || tag.Number != 32 {
		return nil, errors.New("invalid stored log reference")
	}
	var uri string
	if err := cbor.Unmarshal(tag.Content, &uri); err != nil {
		return nil, err
	}
	digest, mimeType, err := blob.ParseURI(uri)
	if err != nil || mimeType != logValueMIME || uri != blobURI(digest) {
		return nil, errors.New("invalid stored log URI")
	}
	if s.Store == nil || s.Bucket == "" {
		return nil, errors.New("log blob store unavailable")
	}
	record, err := db.New(s.DB).GetBlobBySHA256(ctx, digest)
	if err != nil {
		return nil, err
	}
	reader, _, _, err := s.Store.GetObject(ctx, s.Bucket, blob.ObjectKey(record.ObjectKey), minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	_, maxRecord, _ := s.limits()
	value, readErr := io.ReadAll(io.LimitReader(reader, int64(maxRecord)+1))
	if err := errors.Join(readErr, reader.Close()); err != nil {
		return nil, err
	}
	if len(value) > maxRecord {
		return nil, errors.New("stored log value exceeds record size limit")
	}
	hash := sha256.Sum256(value)
	if !bytes.Equal(hash[:], digest) {
		return nil, errors.New("stored log value checksum mismatch")
	}
	return value, nil
}
