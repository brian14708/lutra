package lutra

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	lutrav1 "github.com/brian14708/lutra/gen/lutra/v1"
	"github.com/klauspost/compress/zstd"
)

func TestLocalExecutorBuildsNestedUvProject(t *testing.T) {
	if _, err := exec.LookPath("bwrap"); err != nil {
		t.Skip("bwrap unavailable")
	}
	if _, err := exec.LookPath("uv"); err != nil {
		t.Skip("uv unavailable")
	}
	project := t.TempDir()
	manifest := []byte("[project]\nname = \"nested-task\"\nversion = \"0.1.0\"\nrequires-python = \">=3.11\"\n")
	if err := os.WriteFile(filepath.Join(project, "pyproject.toml"), manifest, 0o644); err != nil {
		t.Fatal(err)
	}
	if output, err := exec.Command("uv", "lock", "--offline", "--directory", project).CombinedOutput(); err != nil {
		t.Fatalf("uv lock: %v: %s", err, output)
	}
	lock, err := os.ReadFile(filepath.Join(project, "uv.lock"))
	if err != nil {
		t.Fatal(err)
	}
	compressed := testBundleArchive(t, []struct {
		name string
		data []byte
	}{
		{"project/pyproject.toml", manifest}, {"project/uv.lock", lock},
	})
	digest := sha256.Sum256(compressed)
	script := []byte(`import helper
import json
import sys

assert helper.VALUE == 42

for line in sys.stdin:
    frame = json.loads(line)
    if frame["type"] != "half_close":
        continue
    call = frame["id"]
    print(json.dumps({"id": call, "type": "headers", "status": 200, "headers": {"Content-Type": ["application/json"]}}), flush=True)
    print(json.dumps({"id": call, "type": "message", "value": {"contentType": "application/cbor", "output": "GCo="}}), flush=True)
    print(json.dumps({"id": call, "type": "end"}), flush=True)
    break
`)
	source := testBundleArchive(t, []struct {
		name string
		data []byte
	}{{"project/task.py", script}, {"library/helper.py", []byte("VALUE = 42\n")}})
	sourceDigest := sha256.Sum256(source)
	var artifact []byte
	executor := &LocalExecutor{
		OpenBundle: func(_ context.Context, actual []byte) (io.ReadCloser, error) {
			if bytes.Equal(actual, digest[:]) {
				return io.NopCloser(bytes.NewReader(compressed)), nil
			}
			if bytes.Equal(actual, sourceDigest[:]) {
				return io.NopCloser(bytes.NewReader(source)), nil
			}
			t.Fatal("unexpected bundle digest")
			return nil, nil
		},
		StoreArtifact: func(_ context.Context, data []byte) (string, error) {
			artifact = data
			return "artifact", nil
		},
		LoadArtifact: func(_ context.Context, uri string) ([]byte, error) {
			if uri != "artifact" {
				t.Fatal("unexpected image artifact URI")
			}
			return artifact, nil
		},
	}
	var output bytes.Buffer
	req := &EnvironmentExecution{
		Environment: &lutrav1.EnvironmentIdentifier{Name: "test"},
		Spec: &lutrav1.EnvironmentSpec{Image: &lutrav1.ImageSpec{
			Name: "local-python", BuildContextUri: sourceURI(digest[:]), Workdir: "project", PythonRequires: ">=3.11",
			BuildCommand: &lutrav1.StartupCommand{Args: []string{"uv", "sync", "--frozen", "--active", "--all-packages", "--no-dev", "--no-install-local"}},
		}},
		Stderr: &output,
	}
	image, err := executor.Build(t.Context(), req)
	if err != nil {
		t.Fatalf("%v: %s", err, output.String())
	}
	if image.ArtifactURI != "artifact" {
		t.Fatalf("artifact URI = %q", image.ArtifactURI)
	}
	destination := t.TempDir()
	if err := extractImage(bytes.NewReader(artifact), destination); err != nil {
		t.Fatal(err)
	}
	python := filepath.Join(destination, ".venv", "bin", "python")
	if output, err := exec.Command(python, "-c", "print('ready')").CombinedOutput(); err != nil || string(output) != "ready\n" {
		t.Fatalf("image Python: output=%q error=%v", output, err)
	}
	req.Spec.SourceUri = sourceURI(sourceDigest[:])
	req.Spec.ImportRoots = []string{"library"}
	req.Spec.Entrypoints = []*lutrav1.Entrypoint{{Command: &lutrav1.StartupCommand{Args: []string{"./.venv/bin/python", "task.py"}}}}
	req.EntrypointID = 1
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	job, err := executor.Run(ctx, image, req)
	if err != nil {
		t.Fatalf("run nested task: %v: %s", err, output.String())
	}
	result, err := job.Wait(ctx)
	if err != nil || !bytes.Equal(result, []byte{0x18, 0x2a}) {
		t.Fatalf("nested task result=%x error=%v: %s", result, err, output.String())
	}
}

func testBundleArchive(t *testing.T, files []struct {
	name string
	data []byte
},
) []byte {
	t.Helper()
	var raw bytes.Buffer
	writer := tar.NewWriter(&raw)
	for _, file := range files {
		if err := writer.WriteHeader(&tar.Header{Name: file.name, Mode: 0o644, Size: int64(len(file.data))}); err != nil {
			t.Fatal(err)
		}
		if _, err := writer.Write(file.data); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	var compressed bytes.Buffer
	encoder, err := zstd.NewWriter(&compressed)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(encoder, &raw); err != nil {
		t.Fatal(err)
	}
	if err := encoder.Close(); err != nil {
		t.Fatal(err)
	}
	return compressed.Bytes()
}

func TestSourceBundleRejectsNestedVirtualEnvironment(t *testing.T) {
	archive := testBundleArchive(t, []struct {
		name string
		data []byte
	}{{"project/.venv/bin/python", []byte("replacement")}})
	if err := extractBundle(bytes.NewReader(archive), t.TempDir()); err == nil {
		t.Fatal("nested virtual environment replaced the built image")
	}
}

func TestLocalImageKeySeparatesSourceFromDependencies(t *testing.T) {
	executor := &LocalExecutor{}
	spec := &lutrav1.EnvironmentSpec{
		SourceUri: "source-a",
		Image: &lutrav1.ImageSpec{
			BuildContextUri: "build-a",
			BuildCommand:    &lutrav1.StartupCommand{Args: []string{"uv", "pip", "sync"}},
			Workdir:         ".",
		},
	}
	initial, err := executor.ImageKey(spec)
	if err != nil {
		t.Fatal(err)
	}
	spec.SourceUri = "source-b"
	changedSource, err := executor.ImageKey(spec)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(initial, changedSource) {
		t.Fatal("source-only change rebuilt the image")
	}
	spec.Image.BuildContextUri = "build-b"
	changedLock, err := executor.ImageKey(spec)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(initial, changedLock) {
		t.Fatal("dependency change reused the image")
	}
	spec.Image.BuildContextUri = "build-a"
	executor.RuntimeVersion = "new-runtime"
	changedRuntime, err := executor.ImageKey(spec)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(initial, changedRuntime) {
		t.Fatal("runtime change reused the image")
	}
	spec.Image.PythonRequires = ">=99"
	if _, err := executor.ImageKey(spec); err == nil {
		t.Fatal("unsupported Python requirement was accepted")
	}
}

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
