package lutra

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestImageArchiveRoundTrip(t *testing.T) {
	source := t.TempDir()
	bin := filepath.Join(source, ".venv", "bin")
	if err := os.MkdirAll(bin, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(bin, "python3"), []byte("image executable"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("python3", filepath.Join(bin, "python")); err != nil {
		t.Fatal(err)
	}
	archive, err := archiveImage(t.Context(), source)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.HasPrefix(archive, []byte{0x28, 0xb5, 0x2f, 0xfd}) {
		t.Fatal("image is not zstd-compressed")
	}
	destination := t.TempDir()
	if err := extractImage(bytes.NewReader(archive), destination); err != nil {
		t.Fatal(err)
	}
	python := filepath.Join(destination, ".venv", "bin", "python")
	data, err := os.ReadFile(python)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "image executable" {
		t.Fatalf("restored executable = %q", data)
	}
	link, err := os.Readlink(python)
	if err != nil {
		t.Fatal(err)
	}
	if link != "python3" {
		t.Fatalf("restored symlink = %q", link)
	}
	info, err := os.Stat(python)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm()&0o100 == 0 {
		t.Fatal("restored executable is not executable")
	}
}
