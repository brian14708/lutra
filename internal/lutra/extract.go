package lutra

import (
	"archive/tar"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/klauspost/compress/zstd"
)

const archiveMIME = "application/x-tar+zstd"

func extractBundle(archive io.Reader, destination string) error {
	decoder, err := zstd.NewReader(archive)
	if err != nil {
		return err
	}
	defer decoder.Close()
	reader := tar.NewReader(decoder)
	const maxEntries = 10_000
	const maxExtractedBytes = 512 << 20
	var entries int
	var extractedBytes int64
	for {
		member, err := reader.Next()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		entries++
		if entries > maxEntries {
			return errors.New("bundle has too many entries")
		}
		name := strings.TrimSuffix(member.Name, "/")
		if name == "" || filepath.IsAbs(name) || strings.Contains(name, "\\") || filepath.Clean(name) != name || name == ".." || strings.HasPrefix(name, "../") {
			return fmt.Errorf("unsafe bundle path %q", member.Name)
		}
		if name == ".venv" || strings.HasPrefix(name, ".venv/") || strings.Contains("/"+name+"/", "/.venv/") {
			return errors.New("source bundle cannot replace the built image")
		}
		target := filepath.Join(destination, name)
		switch member.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0o700); err != nil {
				return err
			}
		case tar.TypeReg:
			if member.Size < 0 || member.Size > 128<<20 {
				return errors.New("bundle entry too large")
			}
			extractedBytes += member.Size
			if extractedBytes > maxExtractedBytes {
				return errors.New("bundle is too large when extracted")
			}
			if err := os.MkdirAll(filepath.Dir(target), 0o700); err != nil {
				return err
			}
			mode := os.FileMode(0o600)
			if member.Mode&0o100 != 0 {
				mode = 0o700
			}
			output, err := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY, mode)
			if err != nil {
				return err
			}
			_, copyErr := io.CopyN(output, reader, member.Size)
			closeErr := output.Close()
			if copyErr != nil {
				return copyErr
			}
			if closeErr != nil {
				return closeErr
			}
		default:
			return fmt.Errorf("unsupported bundle entry %q", member.Name)
		}
	}
}
