package blob

import (
	"crypto/sha256"
	"encoding/base64"
	"testing"
)

func TestParseBlobURI(t *testing.T) {
	digest := sha256.Sum256([]byte("hello"))
	name := base64.StdEncoding.EncodeToString(digest[:])
	for _, tc := range []struct {
		uri   string
		mime  string
		valid bool
	}{
		{"blob:application/cbor," + name, "application/cbor", true},
		{"blob:text/plain; charset=utf-8;resolve," + name, "text/plain; charset=utf-8", true},
		{"blob:application/cbor,invalid", "", false},
		{"data:text/plain," + name, "", false},
		{"blob:application/cbor," + name + "x", "", false},
	} {
		got, mime, err := ParseURI(tc.uri)
		if (err == nil) != tc.valid {
			t.Errorf("parseBlobURI(%q) error = %v", tc.uri, err)
		}
		if tc.valid && (string(got) != string(digest[:]) || mime != tc.mime) {
			t.Errorf("parseBlobURI(%q) = %x, %q", tc.uri, got, mime)
		}
	}
}
