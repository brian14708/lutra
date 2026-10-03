package multihash

import (
	"encoding/hex"
	"testing"
)

func TestSumVectors(t *testing.T) {
	binaryValue := make([]byte, 256)
	for i := range binaryValue {
		binaryValue[i] = byte(i)
	}
	for _, test := range []struct {
		name   string
		values [][]byte
		want   string
	}{
		{"no values", nil, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"},
		{"empty value", [][]byte{{}}, "af5570f5a1810b7af78caf4bc70a660f0df51e42baf91d4de5b2328de0e83dfc"},
		{"nil value", [][]byte{nil}, "af5570f5a1810b7af78caf4bc70a660f0df51e42baf91d4de5b2328de0e83dfc"},
		{"two values", [][]byte{[]byte("ab"), []byte("c")}, "601d5476e2ccfe2c87a2bba7a322659734a05749d5b5aa781f513e4912db0d5f"},
		{"different boundaries", [][]byte{[]byte("a"), []byte("bc")}, "3fafa1cf2f19a7c1129beb20cf0983f73a489a221fc0dd2f16d1be292d089205"},
		{"binary values", [][]byte{{0, 255}, binaryValue}, "28fdf31fc4687841f64cbaa4dbcedd8baa734bdbe4756186dad79eddaf864728"},
	} {
		t.Run(test.name, func(t *testing.T) {
			digest := Sum(test.values...)
			if got := hex.EncodeToString(digest[:]); got != test.want {
				t.Fatalf("Sum() = %s, want %s", got, test.want)
			}
		})
	}
}

func TestSumPreservesOrderAndEmptyValues(t *testing.T) {
	a, b := []byte("a"), []byte("b")
	seen := make(map[[32]byte]bool)
	for _, values := range [][][]byte{
		{a, b}, {b, a}, {a, nil, b}, {nil, a, b}, {a, b, nil},
	} {
		digest := Sum(values...)
		if seen[digest] {
			t.Fatalf("distinct sequence %q reused a digest", values)
		}
		seen[digest] = true
	}
	if string(a) != "a" || string(b) != "b" {
		t.Fatal("Sum mutated its inputs")
	}
}
