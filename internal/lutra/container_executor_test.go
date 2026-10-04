package lutra

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
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
	"github.com/fxamacker/cbor/v2"
	"github.com/google/uuid"
	"github.com/klauspost/compress/zstd"
	"google.golang.org/protobuf/proto"
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
			Entrypoints: []*lutrav1.Entrypoint{{Command: &lutrav1.Command{Args: []string{"python", "task.py"}}}},
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
		NamespaceId: uuid.NewString(), Name: "container", SourceUri: uri, Workdir: ".",
		Image:       &lutrav1.ImageSpec{Name: "container", FromImage: "python:3.12-slim", BuildContextUri: uri, PythonRequires: ">=3.11"},
		Entrypoints: []*lutrav1.Entrypoint{{Command: &lutrav1.Command{Args: []string{"/opt/lutra/venv/bin/python", "task.py"}}, MaxAttempts: 1}},
		PythonPaths: []string{"."},
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
	spec.Workdir = "../outside"
	if _, err := normalizeEnvironment(spec); err == nil {
		t.Fatal("workdir traversal was accepted")
	}
}

func TestContainerExecutorBuildAndRun(t *testing.T) {
	base := os.Getenv("LUTRA_TEST_CONTAINER_BASE")
	if base == "" {
		t.Skip("set LUTRA_TEST_CONTAINER_BASE to an image containing Python and uv")
	}
	for _, runtime := range []string{"docker", "podman"} {
		t.Run(runtime, func(t *testing.T) {
			if err := exec.Command(runtime, "info").Run(); err != nil {
				t.Skipf("%s unavailable: %v", runtime, err)
			}
			testContainerBuildAndRun(t, runtime, base)
		})
	}
}

func testContainerBuildAndRun(t *testing.T, runtime, base string) {
	build, buildURI := containerTestBundle(t, map[string]string{"sync.sh": "uv sync --frozen --active --no-config --script /opt/lutra/dependencies/environment.py\n", "environment.py": "# /// script\n# requires-python = \">=3.11\"\n# dependencies = []\n# ///\n", "environment.py.lock": "version = 1\nrevision = 3\nrequires-python = \">=3.11\"\n[manifest]\nrequirements = []\n"})
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
			Image:       &lutrav1.ImageSpec{Name: "container", FromImage: base, BuildContextUri: buildURI, PythonRequires: ">=3.11", Resources: &lutrav1.Resources{CpuMillis: 500, MemoryBytes: 256 << 20}},
			Entrypoints: []*lutrav1.Entrypoint{{Command: &lutrav1.Command{Args: []string{"/opt/lutra/venv/bin/python", "-u", "task.py"}}}},
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
	failedSource, failedURI := containerTestBundle(t, map[string]string{"lutra-runtime.py": "# /// script\n# requires-python = \">=3.11\"\n# dependencies = []\n# ///\n", "lutra-runtime.py.lock": `version = 1
revision = 3
requires-python = ">=3.11"
[manifest]
requirements = [{name="missing"}]
[[package]]
name = "missing"
version = "0.1"
source = {editable = "missing"}
`})
	bundles[failedURI] = failedSource
	request.Spec.SourceUri = failedURI
	failedJob, err := executor.Run(t.Context(), image, request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := failedJob.Wait(t.Context()); err == nil {
		t.Fatal("editable setup failure was not propagated")
	}
	if err := exec.Command(runtime, "container", "inspect", failedJob.ID()).Run(); err == nil {
		t.Fatal("container with failed setup was not removed")
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

// This exercises SDK preparation and the real task host, including the example's
// ordinary local dependency on the SDK. Both submissions share one dependency image.
func TestContainerSourceOverlay(t *testing.T) {
	base := os.Getenv("LUTRA_TEST_CONTAINER_BASE")
	if base == "" {
		t.Skip("set LUTRA_TEST_CONTAINER_BASE to run container integration tests")
	}
	for _, runtime := range []string{"docker", "podman"} {
		t.Run(runtime, func(t *testing.T) {
			if err := exec.Command(runtime, "info").Run(); err != nil {
				t.Skipf("%s unavailable: %v", runtime, err)
			}
			root, err := filepath.Abs("../..")
			if err != nil {
				t.Fatal(err)
			}
			temporary := t.TempDir()
			prepare := `import json, runpy, shutil, sys
from pathlib import Path
from lutra.client import _prepare_bundle
root, target = map(Path, sys.argv[1:])
shutil.copytree(root / "sdk", target / "sdk", ignore=shutil.ignore_patterns("*.lock", "__pycache__"))
script = target / "sdk/examples/hello.py"
cache_write = '    Path("/opt/lutra/cache/lutra-cache-test").touch()'
cache_check = '    assert Path("/opt/lutra/cache/lutra-cache-test").is_file()'
script.write_text(script.read_text().replace('    return f"Hello, {name}!"', '    from pathlib import Path\n' + cache_write + '\n    return f"Hello, {name}!"'))
for revision in ("first", "second"):
    if revision == "second":
        script.write_text(script.read_text().replace("Hello, {name}!", "Updated, {name}!").replace(cache_write, cache_check))
    environment = runpy.run_path(str(script))["environment"]
    inputs = _prepare_bundle(environment)
    (target / (revision + ".source")).write_bytes(inputs.source)
    (target / (revision + ".build")).write_bytes(inputs.build_context)
    (target / (revision + ".json")).write_text(json.dumps({"workdir": inputs.workdir, "python_paths": inputs.python_paths, "python_requires": inputs.python_requires, "command": inputs.entrypoints[0]}))
`
			command := exec.CommandContext(t.Context(), filepath.Join(root, ".venv/bin/python"), "-c", prepare, root, temporary)
			if output, err := command.CombinedOutput(); err != nil {
				t.Fatalf("prepare: %v: %s", err, output)
			}
			bundles := map[string][]byte{}
			load := func(name string) string {
				contents, err := os.ReadFile(filepath.Join(temporary, name))
				if err != nil {
					t.Fatal(err)
				}
				digest := sha256.Sum256(contents)
				uri := sourceURI(digest[:])
				bundles[uri] = contents
				return uri
			}
			buildURI := load("first.build")
			if load("second.build") != buildURI {
				t.Fatal("source edit changed dependency image inputs")
			}
			metadata, err := os.ReadFile(filepath.Join(temporary, "first.json"))
			if err != nil {
				t.Fatal(err)
			}
			var inputs struct {
				Workdir        string   `json:"workdir"`
				PythonPaths    []string `json:"python_paths"`
				PythonRequires string   `json:"python_requires"`
				Command        []string `json:"command"`
			}
			if err := json.Unmarshal(metadata, &inputs); err != nil {
				t.Fatal(err)
			}
			executor := &ContainerExecutor{Runtime: runtime, OpenBundle: func(_ context.Context, digest []byte) (io.ReadCloser, error) {
				return io.NopCloser(bytes.NewReader(bundles[sourceURI(digest)])), nil
			}}
			input, err := cbor.Marshal([]any{[]any{"Lutra"}, map[string]any{}})
			if err != nil {
				t.Fatal(err)
			}
			var logs bytes.Buffer
			req := &EnvironmentExecution{
				Environment: &lutrav1.EnvironmentIdentifier{NamespaceId: uuid.NewString(), Name: "greetings", Version: strings.Repeat("1", 64)},
				Spec: &lutrav1.EnvironmentSpec{
					SourceUri: load("first.source"), Workdir: inputs.Workdir, PythonPaths: inputs.PythonPaths,
					Image:       &lutrav1.ImageSpec{Name: "container", FromImage: base, BuildContextUri: buildURI, PythonRequires: inputs.PythonRequires, Resources: &lutrav1.Resources{CpuMillis: 1000, MemoryBytes: 512 << 20}},
					Entrypoints: []*lutrav1.Entrypoint{{Command: &lutrav1.Command{Args: inputs.Command}}},
				},
				Input:        input,
				EntrypointID: 1, RunID: uuid.NewString(), ActionID: uuid.NewString(), Attempt: 1, Stderr: &logs,
			}
			image, err := executor.Build(t.Context(), req)
			if err != nil {
				t.Fatalf("build: %v: %s", err, logs.String())
			}
			for index, want := range []string{"Hello, Lutra!", "Updated, Lutra!"} {
				if index == 1 {
					req.Spec.SourceUri = load("second.source")
					open := executor.OpenBundle
					executor.OpenBundle = func(context.Context, []byte) (io.ReadCloser, error) {
						return nil, errors.New("source edit rebuilt dependency image")
					}
					cached, err := executor.Build(t.Context(), req)
					executor.OpenBundle = open
					if err != nil || cached.ArtifactURI != image.ArtifactURI {
						t.Fatalf("reuse image: %v", err)
					}
				}
				job, err := executor.Run(t.Context(), image, req)
				if err != nil {
					t.Fatal(err)
				}
				output, err := job.Wait(t.Context())
				if err != nil {
					t.Fatalf("run: %v: %s", err, logs.String())
				}
				if strings.Contains(logs.String(), "experimental") {
					t.Fatalf("unexpected experimental install: %s", logs.String())
				}

				var result string
				if err := cbor.Unmarshal(output, &result); err != nil || result != want {
					t.Fatalf("result = %q, want %q: %v", result, want, err)
				}
			}
		})
	}
}

func TestImageKeySeparatesBuildAndRuntimeInputs(t *testing.T) {
	uri := sourceURI(bytes.Repeat([]byte{3}, 32))
	spec := &lutrav1.EnvironmentSpec{SourceUri: uri, Workdir: ".", Image: &lutrav1.ImageSpec{
		FromImage: "python:3.12-slim", BuildContextUri: uri, PythonRequires: ">=3.11", Platform: "linux/amd64",
	}}
	executor := &ContainerExecutor{}
	original, err := executor.ImageKey(spec)
	if err != nil {
		t.Fatal(err)
	}
	spec.Workdir = "nested directory"
	spec.PythonPaths = []string{"scripts"}
	spec.Image.Env = map[string]*lutrav1.EnvValue{"SETTING": {Source: &lutrav1.EnvValue_StaticValue{StaticValue: "changed"}}}
	spec.Image.Resources = &lutrav1.Resources{CpuMillis: 500}
	unchanged, err := executor.ImageKey(spec)
	if err != nil || !bytes.Equal(original, unchanged) {
		t.Fatal("runtime inputs changed image identity")
	}
	for _, mutate := range []func(*lutrav1.ImageSpec){
		func(image *lutrav1.ImageSpec) { image.BuildContextUri = sourceURI(bytes.Repeat([]byte{4}, 32)) },
		func(image *lutrav1.ImageSpec) { image.Platform = "linux/arm64" },
		func(image *lutrav1.ImageSpec) { image.FromImage = "python:3.13-slim" },
		func(image *lutrav1.ImageSpec) { image.PythonRequires = ">=3.13" },
	} {
		copy := proto.Clone(spec).(*lutrav1.EnvironmentSpec)
		mutate(copy.Image)
		changed, err := executor.ImageKey(copy)
		if err != nil || bytes.Equal(original, changed) {
			t.Fatal("build input did not change image identity")
		}
	}
	file, err := dockerfile(spec.Image)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(file, uvBuildImage) || !strings.Contains(file, "/opt/lutra/venv") || strings.Contains(file, "pylock") {
		t.Fatalf("unexpected dependency recipe: %s", file)
	}
}
