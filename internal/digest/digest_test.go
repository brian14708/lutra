package digest

import "testing"

func TestAlgorithmQualifiedDigest(t *testing.T) {
	value, err := Sum("sha256", []byte("lutra"))
	if err != nil {
		t.Fatal(err)
	}
	if err := Validate(value); err != nil {
		t.Fatal(err)
	}
	if err := VerifyBytes(value, []byte("lutra")); err != nil {
		t.Fatal(err)
	}
	if err := VerifyBytes(value, []byte("changed")); err == nil {
		t.Fatal("expected a digest mismatch")
	}
}

func TestCombineIsOrderSensitiveAndSingleAlgorithm(t *testing.T) {
	first, err := Sum("sha256", []byte("first"))
	if err != nil {
		t.Fatal(err)
	}
	second, err := Sum("sha256", []byte("second"))
	if err != nil {
		t.Fatal(err)
	}
	combined, err := Combine([]string{first, second})
	if err != nil {
		t.Fatal(err)
	}
	if err := Validate(combined); err != nil {
		t.Fatal(err)
	}
	reversed, err := Combine([]string{second, first})
	if err != nil {
		t.Fatal(err)
	}
	if combined == reversed {
		t.Fatal("combined digest ignored chunk order")
	}
	other, err := Sum("sha512", []byte("other"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Combine([]string{first, other}); err == nil {
		t.Fatal("expected mixed algorithms to be rejected")
	}
}

func TestDigestSupportsFutureAlgorithms(t *testing.T) {
	value, err := Sum("sha512", []byte("lutra"))
	if err != nil {
		t.Fatal(err)
	}
	if err := Validate(value); err != nil {
		t.Fatal(err)
	}
}
