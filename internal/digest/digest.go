// Package digest parses and computes algorithm-qualified content digests.
//
// Digests are stored as <algorithm>:<lowercase hexadecimal bytes>. Keeping the
// algorithm in the value lets the storage and API evolve without changing
// every schema and message when a different hash is introduced.
package digest

import (
	"bytes"
	"crypto/md5"  // #nosec G501 -- MD5 is supported only for compatibility with provider checksums.
	"crypto/sha1" // #nosec G505 -- SHA-1 is supported only for compatibility with legacy objects.
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"strings"
)

var errMalformed = errors.New("digest must be algorithm:hex")

// DefaultAlgorithm is the digest used when a caller does not select one.
// The algorithm remains part of every stored value (for example sha256:<hex>).
const DefaultAlgorithm = "sha256"

// Parse validates a digest and returns its algorithm and expected bytes.
func Parse(value string) (string, []byte, error) {
	algorithm, encoded, ok := strings.Cut(strings.ToLower(strings.TrimSpace(value)), ":")
	if !ok || algorithm == "" || encoded == "" || len(encoded)%2 != 0 {
		return "", nil, errMalformed
	}
	for _, r := range algorithm {
		if (r < 'a' || r > 'z') && (r < '0' || r > '9') && r != '-' {
			return "", nil, errMalformed
		}
	}
	decoded, err := hex.DecodeString(encoded)
	if err != nil {
		return "", nil, fmt.Errorf("%w: %w", errMalformed, err)
	}
	return algorithm, decoded, nil
}

// NewHash returns a hash implementation for an algorithm understood by the
// local verifier. Unknown algorithms are rejected rather than silently
// accepting an unverifiable object.
func NewHash(algorithm string) (hash.Hash, error) {
	switch strings.ToLower(algorithm) {
	case "md5":
		return md5.New(), nil
	case "sha1":
		return sha1.New(), nil
	case "sha256":
		return sha256.New(), nil
	case "sha512":
		return sha512.New(), nil
	default:
		return nil, fmt.Errorf("unsupported digest algorithm %q", algorithm)
	}
}

// Format returns the algorithm-qualified form of a raw sum.
func Format(algorithm string, sum []byte) string {
	return strings.ToLower(algorithm) + ":" + hex.EncodeToString(sum)
}

// Sum returns an algorithm-qualified digest for data.
func Sum(algorithm string, data []byte) (string, error) {
	h, err := NewHash(algorithm)
	if err != nil {
		return "", err
	}
	_, _ = h.Write(data)
	return Format(algorithm, h.Sum(nil)), nil
}

// Combine returns the digest of an ordered sequence of chunk digests: the
// hash of their decoded bytes concatenated in order. An artifact sealed from
// chunks that were already verified at upload time can be identified this way
// without re-reading the payload. All chunk digests must use one algorithm.
func Combine(values []string) (string, error) {
	var algorithm string
	var h hash.Hash
	for _, value := range values {
		a, raw, err := Parse(value)
		if err != nil {
			return "", err
		}
		if h == nil {
			h, err = NewHash(a)
			if err != nil {
				return "", err
			}
			algorithm = a
		} else if a != algorithm {
			return "", errors.New("chunk digests use mixed algorithms")
		}
		_, _ = h.Write(raw)
	}
	if h == nil {
		var err error
		h, err = NewHash(DefaultAlgorithm)
		if err != nil {
			return "", err
		}
		algorithm = DefaultAlgorithm
	}
	return Format(algorithm, h.Sum(nil)), nil
}

// VerifyBytes validates value against data.
func VerifyBytes(value string, data []byte) error {
	algorithm, expected, err := Parse(value)
	if err != nil {
		return err
	}
	h, err := NewHash(algorithm)
	if err != nil {
		return err
	}
	_, _ = h.Write(data)
	if !bytes.Equal(h.Sum(nil), expected) {
		return fmt.Errorf("digest mismatch for %s", algorithm)
	}
	return nil
}

// VerifyReader validates value against the bytes read from r and returns how
// many bytes were read.
func VerifyReader(value string, r io.Reader) (int64, error) {
	algorithm, expected, err := Parse(value)
	if err != nil {
		return 0, err
	}
	h, err := NewHash(algorithm)
	if err != nil {
		return 0, err
	}
	written, err := io.Copy(h, r)
	if err != nil {
		return written, err
	}
	if !bytes.Equal(h.Sum(nil), expected) {
		return written, fmt.Errorf("digest mismatch for %s", algorithm)
	}
	return written, nil
}

// Validate checks syntax, algorithm support, and the digest length expected by
// the selected algorithm without requiring the content itself.
func Validate(value string) error {
	algorithm, expected, err := Parse(value)
	if err != nil {
		return err
	}
	h, err := NewHash(algorithm)
	if err != nil {
		return err
	}
	if len(expected) != h.Size() {
		return fmt.Errorf("digest length does not match %s", algorithm)
	}
	return nil
}
