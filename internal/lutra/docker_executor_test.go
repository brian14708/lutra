package lutra

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	lutrav1 "github.com/brian14708/lutra/gen/lutra/v1"
	"github.com/google/uuid"
	"github.com/klauspost/compress/zstd"
)

func dockerTestBundle(t *testing.T, files map[string]string) ([]byte, string) {
	t.Helper()
	var raw bytes.Buffer
	writer := tar.NewWriter(&raw)
	for name, body := range files {
		if err := writer.WriteHeader(&tar.Header{Name: name, Mode: 0o644, Size: int64(len(body))}); err != nil {
			t.Fatal(err)
		}
		if _, err := io.WriteString(writer, body); err != nil {
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
	digest := sha256.Sum256(compressed.Bytes())
	return compressed.Bytes(), sourceURI(digest[:])
}

func TestDockerImageKeyAndRegistration(t *testing.T) {
	uri := sourceURI(bytes.Repeat([]byte{1}, 32))
	spec := &lutrav1.EnvironmentSpec{
		NamespaceId: uuid.NewString(), Name: "docker", SourceUri: uri,
		Image:       &lutrav1.ImageSpec{Name: "docker", FromImage: "python:3.12-slim", BuildContextUri: uri, Workdir: ".", PythonRequires: ">=3.11", BuildCommand: &lutrav1.StartupCommand{Args: []string{"uv", "--version"}}},
		Entrypoints: []*lutrav1.Entrypoint{{Command: &lutrav1.StartupCommand{Args: []string{"./.venv/bin/python", "task.py"}}, MaxAttempts: 1}},
		ImportRoots: []string{"."},
	}
	if _, err := normalizeEnvironment(spec); err != nil {
		t.Fatal(err)
	}
	executor := &DockerExecutor{}
	initial, err := executor.ImageKey(spec)
	if err != nil {
		t.Fatal(err)
	}
	spec.SourceUri = sourceURI(bytes.Repeat([]byte{2}, 32))
	sameImage, err := executor.ImageKey(spec)
	if err != nil || !bytes.Equal(initial, sameImage) {
		t.Fatal("source-only change altered Docker image key")
	}
	spec.Image.FromImage = "python:3.13-slim"
	newImage, err := executor.ImageKey(spec)
	if err != nil || bytes.Equal(initial, newImage) {
		t.Fatal("base image change did not alter Docker image key")
	}
	spec.Image.FromImage = "python:3.12-slim\nRUN echo unsafe"
	if _, err := normalizeEnvironment(spec); err == nil {
		t.Fatal("Dockerfile injection in base image was accepted")
	}
	spec.Image.FromImage = "python:3.12-slim"
	spec.Image.Workdir = "${HOME}"
	if _, err := normalizeEnvironment(spec); err == nil {
		t.Fatal("Dockerfile variable in workdir was accepted")
	}
}

func TestDockerExecutorBuildAndRun(t *testing.T) {
	base := os.Getenv("LUTRA_TEST_DOCKER_BASE")
	if base == "" {
		t.Skip("set LUTRA_TEST_DOCKER_BASE to an image containing Python and uv")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("Docker CLI unavailable")
	}
	build, buildURI := dockerTestBundle(t, map[string]string{"task.py": "raise RuntimeError('source was not copied')\n"})
	source, taskURI := dockerTestBundle(t, map[string]string{"task.py": `import json, sys
task_id = None
for line in sys.stdin:
    frame = json.loads(line)
    if frame.get("type") == "half_close":
        task_id = frame["id"]
        print(json.dumps({"id": "p1", "type": "request", "path": "/test.Callback/Check", "headers": {"Content-Type": ["application/json"]}, "value": {}}), flush=True)
    elif frame.get("id") == "p1" and frame.get("type") == "response":
        if frame.get("status") != 200:
            raise RuntimeError("callback failed")
        for response in (
            {"id": task_id, "type": "headers", "status": 200, "headers": {"Content-Type": ["application/json"]}},
            {"id": task_id, "type": "message", "value": {"contentType": "application/cbor", "output": "AQ=="}},
            {"id": task_id, "type": "end"},
        ):
            print(json.dumps(response), flush=True)
`})
	bundles := map[string][]byte{buildURI: build, taskURI: source}
	var callbackCalled atomic.Bool
	executor := &DockerExecutor{OpenBundle: func(_ context.Context, digest []byte) (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(bundles[sourceURI(digest)])), nil
	}}
	request := &EnvironmentExecution{
		Environment: &lutrav1.EnvironmentIdentifier{NamespaceId: "test", Name: "docker", Version: "test"},
		Spec: &lutrav1.EnvironmentSpec{
			SourceUri:   taskURI,
			Image:       &lutrav1.ImageSpec{Name: "docker", FromImage: base, BuildContextUri: buildURI, Workdir: ".", PythonRequires: ">=3.11", BuildCommand: &lutrav1.StartupCommand{Args: []string{"uv", "--version"}}, Resources: &lutrav1.Resources{CpuMillis: 500, MemoryBytes: 256 << 20}},
			Entrypoints: []*lutrav1.Entrypoint{{Command: &lutrav1.StartupCommand{Args: []string{"./.venv/bin/python", "-u", "task.py"}}}},
		},
		EntrypointID: 1, RunID: "test-run", ActionID: "test-action", Attempt: 1,
		TaskAPIHandler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/test.Callback/Check" {
				callbackCalled.Store(true)
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, "{}")
		}),
		Stderr: &bytes.Buffer{},
	}
	image, err := executor.Build(t.Context(), request)
	if err != nil {
		t.Fatalf("build: %v: %s", err, request.Stderr.(*bytes.Buffer).String())
	}
	job, err := executor.Run(t.Context(), image, request)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	limits, err := exec.Command("docker", "inspect", "--format", "{{.HostConfig.NanoCpus}} {{.HostConfig.Memory}}", job.ID()).Output()
	if err != nil || strings.TrimSpace(string(limits)) != "500000000 268435456" {
		t.Fatalf("container limits = %q, error = %v", limits, err)
	}
	output, err := job.Wait(t.Context())
	if err != nil || !bytes.Equal(output, []byte{1}) {
		t.Fatalf("task output = %x, error = %v, stderr = %s", output, err, request.Stderr.(*bytes.Buffer).String())
	}
	if !callbackCalled.Load() {
		t.Fatal("task callback was not handled")
	}
	if output, err := exec.Command("docker", "container", "inspect", job.ID()).CombinedOutput(); err == nil {
		t.Fatalf("container was not removed: %v: %s", err, output)
	}
	hangingSource, hangingURI := dockerTestBundle(t, map[string]string{"task.py": "import sys\nfor line in sys.stdin: pass\n"})
	bundles[hangingURI] = hangingSource
	request.Spec.SourceUri = hangingURI
	hangingJob, err := executor.Run(t.Context(), image, request)
	if err != nil {
		t.Fatal(err)
	}
	waitCtx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if _, err := hangingJob.Wait(waitCtx); err == nil {
		t.Fatal("canceled task completed successfully")
	}
	if err := hangingJob.Kill(context.Background()); err != nil {
		t.Fatal(err)
	}
	if output, err := exec.Command("docker", "container", "inspect", hangingJob.ID()).CombinedOutput(); err == nil {
		t.Fatalf("canceled container was not removed: %v: %s", err, output)
	}
}
