package lutra

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	lutrav1 "github.com/brian14708/lutra/gen/lutra/v1"
	"github.com/google/uuid"
	"github.com/klauspost/compress/zstd"
)

func containerTestBundle(t *testing.T, files map[string]string) ([]byte, string) {
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

func TestSourceBundleRejectsNestedVirtualEnvironment(t *testing.T) {
	archive, _ := containerTestBundle(t, map[string]string{"project/.venv/bin/python": "replacement"})
	if err := extractBundle(bytes.NewReader(archive), t.TempDir()); err == nil {
		t.Fatal("nested virtual environment replaced the built image")
	}
}

func TestContainerRuntimeSetting(t *testing.T) {
	for _, test := range []struct {
		value, want string
		valid       bool
	}{
		{"", "docker", true},
		{"docker", "docker", true},
		{"podman", "podman", true},
		{"containerd", "", false},
	} {
		t.Run(test.value, func(t *testing.T) {
			t.Setenv("LUTRA_CONTAINER_RUNTIME", test.value)
			got, err := containerRuntime()
			if (err == nil) != test.valid || got != test.want {
				t.Fatalf("runtime = %q, error = %v", got, err)
			}
		})
	}
}

func TestContainerBuildStreamsEngineOutput(t *testing.T) {
	for _, runtime := range []string{"docker", "podman"} {
		t.Run(runtime, func(t *testing.T) {
			dir := t.TempDir()
			engine := filepath.Join(dir, runtime)
			script := "#!/bin/sh\ncase \"$1\" in\n" +
				"image) exit 1 ;;\n" +
				"pull) echo 'pulling layer'; echo 'pull complete' >&2 ;;\n" +
				"build) cat >/dev/null; echo 'building image'; echo 'build complete' >&2 ;;\n" +
				"esac\n"
			if err := os.WriteFile(engine, []byte(script), 0o700); err != nil {
				t.Fatal(err)
			}
			bundle, uri := containerTestBundle(t, map[string]string{"task.py": "pass\n"})
			executor := &ContainerExecutor{Runtime: engine, OpenBundle: func(context.Context, []byte) (io.ReadCloser, error) {
				return io.NopCloser(bytes.NewReader(bundle)), nil
			}}
			var logs bytes.Buffer
			var pullLogs bytes.Buffer
			image, err := executor.Build(t.Context(), &EnvironmentExecution{
				Spec: &lutrav1.EnvironmentSpec{Image: &lutrav1.ImageSpec{
					FromImage: "python:3.12-slim", BuildContextUri: uri,
				}},
				Stderr:     &logs,
				PullOutput: &pullLogs,
			})
			if err != nil || image == nil {
				t.Fatalf("build = %v, error = %v", image, err)
			}
			if got, want := logs.String(), "building image\nbuild complete\n"; got != want {
				t.Fatalf("logs = %q, want %q", got, want)
			}
			if got, want := pullLogs.String(), "pulling layer\npull complete\n"; got != want {
				t.Fatalf("pull logs = %q, want %q", got, want)
			}
		})
	}
}

func TestCanceledContainerCreateReturnsAndCleansUp(t *testing.T) {
	dir := t.TempDir()
	started := filepath.Join(dir, "started")
	release := filepath.Join(dir, "release")
	removed := filepath.Join(dir, "removed")
	runtime := filepath.Join(dir, "engine")
	script := "#!/bin/sh\ncase \"$1 $2\" in\n" +
		"  'image inspect') exit 0 ;;\n" +
		"esac\n" +
		"case \"$1\" in\n" +
		"  create) touch '" + started + "'; while [ ! -e '" + release + "' ]; do sleep 0.01; done; printf '%064d\\n' 0 ;;\n" +
		"  rm) touch '" + removed + "' ;;\n" +
		"esac\n"
	if err := os.WriteFile(runtime, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	bundle, uri := containerTestBundle(t, map[string]string{"task.py": "pass\n"})
	executor := &ContainerExecutor{Runtime: runtime, OpenBundle: func(context.Context, []byte) (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(bundle)), nil
	}}
	req := &EnvironmentExecution{
		Environment: &lutrav1.EnvironmentIdentifier{NamespaceId: "test", Name: "test", Version: "test"},
		Spec: &lutrav1.EnvironmentSpec{
			SourceUri: uri, Image: &lutrav1.ImageSpec{Resources: &lutrav1.Resources{CpuMillis: 500, MemoryBytes: 256 << 20}},
			Entrypoints: []*lutrav1.Entrypoint{{Command: &lutrav1.StartupCommand{Args: []string{"python", "task.py"}}}},
		},
		EntrypointID: 1,
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		_, err := executor.Run(ctx, &Image{ArtifactURI: "container://lutra:sha-" + strings.Repeat("0", 64)}, req)
		done <- err
	}()
	deadline := time.After(5 * time.Second)
	for {
		if _, err := os.Stat(started); err == nil {
			break
		}
		select {
		case <-deadline:
			t.Fatal("container create did not start")
		case <-time.After(10 * time.Millisecond):
		}
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled run: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled run waited for container create")
	}
	if err := os.WriteFile(release, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	deadline = time.After(5 * time.Second)
	for {
		if _, err := os.Stat(removed); err == nil {
			break
		}
		select {
		case <-deadline:
			t.Fatal("created container was not removed")
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestContainerImageKeyAndRegistration(t *testing.T) {
	uri := sourceURI(bytes.Repeat([]byte{1}, 32))
	spec := &lutrav1.EnvironmentSpec{
		NamespaceId: uuid.NewString(), Name: "container", SourceUri: uri,
		Image:       &lutrav1.ImageSpec{Name: "container", FromImage: "python:3.12-slim", BuildContextUri: uri, Workdir: ".", PythonRequires: ">=3.11", BuildCommand: &lutrav1.StartupCommand{Args: []string{"uv", "--version"}}},
		Entrypoints: []*lutrav1.Entrypoint{{Command: &lutrav1.StartupCommand{Args: []string{"./.venv/bin/python", "task.py"}}, MaxAttempts: 1}},
		ImportRoots: []string{"."},
	}
	if _, err := normalizeEnvironment(spec); err != nil {
		t.Fatal(err)
	}
	executor := &ContainerExecutor{Runtime: "docker"}
	initial, err := executor.ImageKey(spec)
	if err != nil {
		t.Fatal(err)
	}
	otherRuntime, err := (&ContainerExecutor{Runtime: "podman"}).ImageKey(spec)
	if err != nil || !bytes.Equal(initial, otherRuntime) {
		t.Fatal("container runtime changed the environment image key")
	}
	spec.SourceUri = sourceURI(bytes.Repeat([]byte{2}, 32))
	sameImage, err := executor.ImageKey(spec)
	if err != nil || !bytes.Equal(initial, sameImage) {
		t.Fatal("source-only change altered container image key")
	}
	spec.Image.FromImage = "python:3.13-slim"
	newImage, err := executor.ImageKey(spec)
	if err != nil || bytes.Equal(initial, newImage) {
		t.Fatal("base image change did not alter container image key")
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

func TestContainerExecutorBuildAndRun(t *testing.T) {
	base := os.Getenv("LUTRA_TEST_CONTAINER_BASE")
	if base == "" {
		t.Skip("set LUTRA_TEST_CONTAINER_BASE to an image containing Python and uv")
	}
	for _, runtime := range []string{"docker", "podman"} {
		t.Run(runtime, func(t *testing.T) {
			if _, err := exec.LookPath(runtime); err != nil {
				t.Skipf("%s CLI unavailable", runtime)
			}
			testContainerBuildAndRun(t, runtime, base)
		})
	}
}

func testContainerBuildAndRun(t *testing.T, runtime, base string) {
	build, buildURI := containerTestBundle(t, map[string]string{"task.py": "raise RuntimeError('source was not copied')\n"})
	source, taskURI := containerTestBundle(t, map[string]string{"task.py": `import json, sys
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
	executor := &ContainerExecutor{Runtime: runtime, OpenBundle: func(_ context.Context, digest []byte) (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(bundles[sourceURI(digest)])), nil
	}}
	request := &EnvironmentExecution{
		Environment: &lutrav1.EnvironmentIdentifier{NamespaceId: "test", Name: "container", Version: "test"},
		Spec: &lutrav1.EnvironmentSpec{
			SourceUri:   taskURI,
			Image:       &lutrav1.ImageSpec{Name: "container", FromImage: base, BuildContextUri: buildURI, Workdir: ".", PythonRequires: ">=3.11", BuildCommand: &lutrav1.StartupCommand{Args: []string{"uv", "--version"}}, Resources: &lutrav1.Resources{CpuMillis: 500, MemoryBytes: 256 << 20}},
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
	limits, err := exec.Command(runtime, "inspect", "--format", "{{.HostConfig.NanoCpus}} {{.HostConfig.Memory}}", job.ID()).Output()
	if err != nil || strings.TrimSpace(string(limits)) != "500000000 268435456" {
		t.Fatalf("container limits = %q, error = %v", limits, err)
	}
	output, err := job.Wait(t.Context())
	if err != nil || !bytes.Equal(output, []byte{1}) {
		t.Fatalf("task output = %x, error = %v, stderr = %s", output, err, request.Stderr.(*bytes.Buffer).String())
	}
	if !strings.Contains(request.Stderr.(*bytes.Buffer).String(), "container "+job.ID()+" stopped\n") {
		t.Fatalf("missing container stop log: %s", request.Stderr.(*bytes.Buffer).String())
	}
	if !callbackCalled.Load() {
		t.Fatal("task callback was not handled")
	}
	if output, err := exec.Command(runtime, "container", "inspect", job.ID()).CombinedOutput(); err == nil {
		t.Fatalf("container was not removed: %v: %s", err, output)
	}
	tag := strings.TrimPrefix(image.ArtifactURI, "container://")
	if output, err := exec.Command(runtime, "image", "rm", tag).CombinedOutput(); err != nil {
		t.Fatalf("remove cached image: %v: %s", err, output)
	}
	rebuilt, err := executor.Build(t.Context(), request)
	if err != nil || rebuilt == nil || rebuilt.ArtifactURI != image.ArtifactURI {
		t.Fatalf("rebuild missing image: %v, image = %v", err, rebuilt)
	}
	openBundle := executor.OpenBundle
	executor.OpenBundle = func(context.Context, []byte) (io.ReadCloser, error) {
		return nil, errors.New("cached image fetched its build context")
	}
	cached, err := executor.Build(t.Context(), request)
	executor.OpenBundle = openBundle
	if err != nil || cached == nil || cached.ArtifactURI != image.ArtifactURI {
		t.Fatalf("reuse cached image: %v, image = %v", err, cached)
	}
	hangingSource, hangingURI := containerTestBundle(t, map[string]string{"task.py": "import sys\nfor line in sys.stdin: pass\n"})
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
	if output, err := exec.Command(runtime, "container", "inspect", hangingJob.ID()).CombinedOutput(); err == nil {
		t.Fatalf("canceled container was not removed: %v: %s", err, output)
	}
}
