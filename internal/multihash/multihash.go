// Package multihash hashes ordered byte sequences into a single SHA-256 digest.
package multihash

import (
	"crypto/sha256"
	"encoding/binary"
)

// Sum hashes each value prefixed by its byte length as an unsigned 64-bit
// big-endian integer. Boundaries and order matter; nil and empty values are
// equivalent, but omitting a value is different from including an empty one.
func Sum(values ...[]byte) [sha256.Size]byte {
	h := sha256.New()
	var length [8]byte
	for _, value := range values {
		binary.BigEndian.PutUint64(length[:], uint64(len(value)))
		h.Write(length[:])
		h.Write(value)
	}
	var digest [sha256.Size]byte
	h.Sum(digest[:0])
	return digest
}
