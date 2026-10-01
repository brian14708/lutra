package lutra

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"connectrpc.com/connect"
	"github.com/klauspost/compress/zstd"

	taskv1 "github.com/brian14708/lutra/gen/lutra/task/v1"
	"github.com/brian14708/lutra/internal/blob"
	"github.com/brian14708/lutra/internal/taskstdio"
	"github.com/fxamacker/cbor/v2"

	lutrav1 "github.com/brian14708/lutra/gen/lutra/v1"
)

// EnvironmentExecution is the executor-neutral request. Resolved artifact
// builds are deliberately absent; image builds choose those separately.
type EnvironmentExecution struct {
	Environment  *lutrav1.EnvironmentIdentifier
	Provider     string
	Spec         *lutrav1.EnvironmentSpec
	EntrypointID uint32
	Input        []byte
	Environments []*lutrav1.EnvironmentIdentifier
	RunID        string
	ActionID     string
	Attempt      int32
	// Stderr receives build and task output. TaskAPIHandler answers the
	// task's callbacks; remote providers may inject direct API access instead.
	Stderr         io.Writer
	TaskAPIHandler http.Handler
}

type CacheableError struct {
	Code    string
	Details []byte
}

var cacheErrorCodePattern = regexp.MustCompile(`^[a-z][a-z0-9_.-]{0,127}$`)

func (e *CacheableError) Error() string { return e.Code }
func (*CacheableError) Cacheable() bool { return true }

func (e *CacheableError) output() ([]byte, error) {
	mode, err := cbor.CanonicalEncOptions().EncMode()
	if err != nil {
		return nil, err
	}
	return mode.Marshal([]any{"lutra.cacheable-error.v1", e.Code, cbor.RawMessage(e.Details)})
}

// Image references a built image artifact. The provider that built it
// resolves the URI at run time: a blob URI for local builds, a registry
// digest for container providers.
type Image struct {
	ArtifactURI string
}

// Executor is an image provider, consuming the environment spec in two
// phases: Build constructs the image and Run starts the task on top of it.
// ImageKey derives the provider's content key from the spec, folding in only
// what affects its image content: a provider
// that bakes env vars into the image includes them, one that sets them at run
// time does not.
type Executor interface {
	ImageKey(spec *lutrav1.EnvironmentSpec) ([]byte, error)
	Build(context.Context, *EnvironmentExecution) (*Image, error)
	Run(context.Context, *Image, *EnvironmentExecution) (Job, error)
}

// Job is a started task execution. The provider owns its lifetime: Wait
// blocks until the task finishes, releasing resources, and Kill terminates it
// early, e.g. on cancellation. Recovery replays tasks in fresh sandboxes.
type Job interface {
	ID() string
	Wait(context.Context) ([]byte, error)
	Kill(context.Context) error
}

// LocalExecutor builds locked dependencies and launches each task in a fresh
// sandbox. It sets env vars at run time and does not enforce resources, so
// neither affects its image identity.
type LocalExecutor struct {
	// StoreArtifact persists a built image archive and returns its artifact
	// URI; LoadArtifact resolves and verifies that URI.
	StoreArtifact  func(context.Context, []byte) (string, error)
	LoadArtifact   func(context.Context, string) ([]byte, error)
	OpenBundle     func(context.Context, []byte) (io.ReadCloser, error)
	RuntimeVersion string
}

func (e *LocalExecutor) ImageKey(spec *lutrav1.EnvironmentSpec) ([]byte, error) {
	runtimeVersion := e.RuntimeVersion
	if runtimeVersion == "" {
		runtimeVersion = "python3-default"
	}
	mode, err := cbor.CanonicalEncOptions().EncMode()
	if err != nil {
		return nil, err
	}
	value, err := mode.Marshal(map[string]any{
		"profile": "lutra.image.local-python.v1", "runtime_version": runtimeVersion, "build_context_uri": spec.GetImage().GetBuildContextUri(), "build_command": spec.GetImage().GetBuildCommand().GetArgs(), "workdir": spec.GetImage().GetWorkdir(),
	})
	if err != nil {
		return nil, err
	}
	hash := sha256.Sum256(value)
	return hash[:], nil
}

func (e *LocalExecutor) Build(ctx context.Context, req *EnvironmentExecution) (*Image, error) {
	if req == nil || req.Environment == nil {
		return nil, errors.New("environment is required")
	}
	dir, err := os.MkdirTemp("", "lutra-build-")
	if err != nil {
		return nil, err
	}
	defer func() { _ = os.RemoveAll(dir) }()
	workdir := filepath.Join("/workspace", req.Spec.GetImage().GetWorkdir())
	hostWorkdir := filepath.Join(dir, req.Spec.GetImage().GetWorkdir())
	if err := os.MkdirAll(hostWorkdir, 0o700); err != nil {
		return nil, err
	}
	if err := e.unpackSource(ctx, req.Spec.GetImage().GetBuildContextUri(), hostWorkdir); err != nil {
		return nil, err
	}
	python, err := exec.LookPath("python3")
	if err != nil {
		return nil, err
	}
	python, err = filepath.EvalSymlinks(python)
	if err != nil {
		return nil, err
	}
	bwrap, err := exec.LookPath("bwrap")
	if err != nil {
		return nil, err
	}
	args := sandboxArgs(dir, workdir)
	args = append(args, "--setenv", "UV_PROJECT_ENVIRONMENT", workdir+"/.venv", "--setenv", "UV_PYTHON", python, "--")
	args = append(args, req.Spec.GetImage().GetBuildCommand().GetArgs()...)
	command := exec.CommandContext(ctx, bwrap, args...)
	command.Env = []string{"PATH=" + os.Getenv("PATH")}
	command.Stderr = req.Stderr
	command.Stdout = req.Stderr
	if err := command.Run(); err != nil {
		return nil, fmt.Errorf("build uv environment: %w", err)
	}
	archive, err := archiveImage(ctx, hostWorkdir)
	if err != nil {
		return nil, err
	}
	if e.StoreArtifact == nil {
		return nil, errors.New("image artifact store unavailable")
	}
	uri, err := e.StoreArtifact(ctx, archive)
	if err != nil {
		return nil, err
	}
	return &Image{ArtifactURI: uri}, nil
}

func (e *LocalExecutor) Run(ctx context.Context, image *Image, req *EnvironmentExecution) (Job, error) {
	if image == nil || req == nil {
		return nil, errors.New("image and entrypoint are required")
	}
	if req.EntrypointID == 0 || req.EntrypointID > uint32(len(req.Spec.GetEntrypoints())) {
		return nil, errors.New("entrypoint is not registered")
	}
	startup := req.Spec.GetEntrypoints()[req.EntrypointID-1].GetCommand()
	if e.LoadArtifact == nil {
		return nil, errors.New("image artifact store unavailable")
	}
	archive, err := e.LoadArtifact(ctx, image.ArtifactURI)
	if err != nil {
		return nil, err
	}
	dir, err := os.MkdirTemp("", "lutra-run-")
	if err != nil {
		return nil, err
	}
	job := &localJob{req: req, cleanup: func() { _ = os.RemoveAll(dir) }}
	started := false
	defer func() {
		if !started {
			job.cleanup()
		}
	}()
	workdir := filepath.Join("/workspace", req.Spec.GetImage().GetWorkdir())
	hostWorkdir := filepath.Join(dir, req.Spec.GetImage().GetWorkdir())
	if err := os.MkdirAll(hostWorkdir, 0o700); err != nil {
		return nil, err
	}
	if err := extractImage(bytes.NewReader(archive), hostWorkdir); err != nil {
		return nil, err
	}
	if err := e.unpackSource(ctx, req.Spec.GetSourceUri(), hostWorkdir); err != nil {
		return nil, err
	}
	environments := req.Environments
	if environments == nil {
		environments = []*lutrav1.EnvironmentIdentifier{}
	}
	environmentsJSON, err := json.Marshal(environments)
	if err != nil {
		return nil, err
	}
	python := filepath.Join(hostWorkdir, ".venv", "bin", "python")
	if _, err := os.Stat(python); err != nil {
		return nil, fmt.Errorf("image python unavailable: %w", err)
	}
	bwrap, err := exec.LookPath("bwrap")
	if err != nil {
		return nil, err
	}
	checkArgs := append(sandboxArgs(dir, workdir), "--", "uv", "sync", "--check", "--offline", "--locked", "--no-dev", "--no-install-workspace", "--inexact")
	check := exec.CommandContext(ctx, bwrap, checkArgs...)
	check.Env = []string{"PATH=" + os.Getenv("PATH")}
	check.Stdout = req.Stderr
	check.Stderr = req.Stderr
	if err := check.Run(); err != nil {
		return nil, fmt.Errorf("check uv environment: %w", err)
	}
	args := sandboxArgs(dir, workdir)
	envVars := req.Spec.GetImage().GetEnvVars()
	keys := make([]string, 0, len(envVars))
	for key := range envVars {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		args = append(args, "--setenv", key, envVars[key])
	}
	values := map[string]string{"LUTRA_TASK_NAMESPACE": req.Environment.NamespaceId, "LUTRA_TASK_VERSION": req.Environment.Version, "LUTRA_ENVIRONMENT_NAME": req.Environment.Name, "LUTRA_ENVIRONMENTS_JSON": string(environmentsJSON), "LUTRA_ATTEMPT": fmt.Sprint(req.Attempt), "LUTRA_TASK_RUN_ID": req.RunID, "LUTRA_TASK_ACTION_ID": req.ActionID, "PYTHONPATH": workdir + ":" + workdir + "/src:" + workdir + "/sdk/src"}
	for key, value := range values {
		args = append(args, "--setenv", key, value)
	}
	args = append(args, "--")
	args = append(args, startup.Args...)
	jobCtx, stop := context.WithCancel(ctx)
	job.stop = stop
	command := exec.CommandContext(jobCtx, bwrap, args...)
	command.Env = []string{"PATH=" + os.Getenv("PATH")}
	command.Stderr = req.Stderr
	process, err := taskstdio.Start(command)
	if err != nil {
		stop()
		return nil, err
	}
	process.Transport.SetHandler(req.TaskAPIHandler)
	job.process = process
	started = true
	return job, nil
}

type localJob struct {
	req     *EnvironmentExecution
	process *taskstdio.Process
	stop    context.CancelFunc
	cleanup func()
}

func (j *localJob) ID() string {
	return strconv.Itoa(j.process.Command.Process.Pid)
}

func (j *localJob) Wait(ctx context.Context) ([]byte, error) {
	defer j.stop()
	defer j.cleanup()
	req := j.req
	result, callErr := j.process.Client().Execute(ctx, connect.NewRequest(&taskv1.ExecuteRequest{InvocationId: req.ActionID, RunId: req.RunID, ActionId: req.ActionID, Attempt: req.Attempt, ContentType: "application/cbor", Input: req.Input}))
	closeErr := j.process.Close()
	if callErr != nil {
		return nil, callErr
	}
	if closeErr != nil {
		return nil, closeErr
	}
	if result.Msg.GetErrorCode() != "" {
		if !cacheErrorCodePattern.MatchString(result.Msg.GetErrorCode()) || len(result.Msg.GetErrorDetails()) > 64<<10 || len(result.Msg.GetOutput()) != 0 {
			return nil, errors.New("task returned invalid cacheable error")
		}
		var details any
		if err := cbor.Unmarshal(result.Msg.GetErrorDetails(), &details); err != nil {
			return nil, errors.New("task returned invalid cacheable error details")
		}
		failure := &CacheableError{Code: result.Msg.GetErrorCode(), Details: result.Msg.GetErrorDetails()}
		output, err := failure.output()
		if err != nil {
			return nil, err
		}
		return output, failure
	}
	if result.Msg.GetContentType() != "application/cbor" {
		return nil, errors.New("task returned unsupported content type")
	}
	return result.Msg.GetOutput(), nil
}

func (j *localJob) Kill(context.Context) error {
	j.stop()
	return nil
}

func (e *LocalExecutor) unpackSource(ctx context.Context, uri, dir string) error {
	digest, mimeType, err := blob.ParseURI(uri)
	if err != nil || mimeType != archiveMIME || uri != sourceURI(digest) {
		return errors.New("invalid source bundle URI")
	}
	if e.OpenBundle == nil {
		return errors.New("source blob store unavailable")
	}
	input, err := e.OpenBundle(ctx, digest)
	if err != nil {
		return err
	}
	defer func() { _ = input.Close() }()
	limited := &io.LimitedReader{R: input, N: maxSourceSize + 1}
	hash := sha256.New()
	verified := io.TeeReader(limited, hash)
	if err := extractBundle(verified, dir); err != nil {
		return err
	}
	if _, err := io.Copy(io.Discard, verified); err != nil {
		return err
	}
	if limited.N == 0 {
		return errors.New("source bundle exceeds size limit")
	}
	if !bytes.Equal(hash.Sum(nil), digest) {
		return errors.New("source bundle checksum mismatch")
	}
	return nil
}

func archiveImage(ctx context.Context, dir string) ([]byte, error) {
	archive, err := os.CreateTemp("", "lutra-image-*.tar.zst")
	if err != nil {
		return nil, err
	}
	path := archive.Name()
	if err := archive.Close(); err != nil {
		_ = os.Remove(path)
		return nil, err
	}
	defer func() { _ = os.Remove(path) }()
	command := exec.CommandContext(ctx, "uv", "run", "--no-sync", "--package", "lutra", "python", "-m", "lutra.archive", "create", "--prefix", ".venv", filepath.Join(dir, ".venv"), path)
	if output, err := command.CombinedOutput(); err != nil {
		return nil, fmt.Errorf("archive image: %w: %s", err, output)
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()
	data, err := io.ReadAll(io.LimitReader(file, maxImageSize+1))
	if err != nil {
		return nil, err
	}
	if len(data) > maxImageSize {
		return nil, errors.New("image archive exceeds limit")
	}
	return data, nil
}

func extractImage(input io.Reader, dir string) error {
	decoder, err := zstd.NewReader(input)
	if err != nil {
		return err
	}
	defer decoder.Close()
	reader := tar.NewReader(decoder)
	var size int64
	entries := 0
	for {
		h, err := reader.Next()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		entries++
		name := strings.TrimSuffix(h.Name, "/")
		if entries > 100000 || filepath.IsAbs(name) || filepath.Clean(name) != name || (name != ".venv" && !strings.HasPrefix(name, ".venv/")) || strings.Contains(name, "\\") {
			return errors.New("unsafe image archive path")
		}
		target := filepath.Join(dir, name)
		for parent := filepath.Dir(target); parent != dir; parent = filepath.Dir(parent) {
			info, err := os.Lstat(parent)
			if err == nil && info.Mode()&os.ModeSymlink != 0 {
				return errors.New("image archive traverses symlink")
			}
			if err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
		}
		if err := os.MkdirAll(filepath.Dir(target), 0o700); err != nil {
			return err
		}
		switch h.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0o700); err != nil {
				return err
			}
		case tar.TypeReg:
			size += h.Size
			if h.Size < 0 || size > 2<<30 {
				return errors.New("image archive exceeds limit")
			}
			file, err := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY, os.FileMode(h.Mode)&0o777)
			if err != nil {
				return err
			}
			_, copyErr := io.CopyN(file, reader, h.Size)
			closeErr := file.Close()
			if copyErr != nil {
				return copyErr
			}
			if closeErr != nil {
				return closeErr
			}
		case tar.TypeSymlink:
			resolved := filepath.Clean(filepath.Join(filepath.Dir(name), h.Linkname))
			if filepath.IsAbs(h.Linkname) {
				link := filepath.Clean(h.Linkname)
				if !strings.HasPrefix(link, "/nix/store/") && !strings.HasPrefix(link, "/usr/") {
					return errors.New("unsafe image symlink")
				}
			} else if resolved != ".venv" && !strings.HasPrefix(resolved, ".venv/") {
				return errors.New("unsafe image symlink")
			}
			if err := os.Symlink(h.Linkname, target); err != nil {
				return err
			}
		default:
			return errors.New("unsupported image archive entry")
		}
	}
}

func sandboxArgs(dir, workdir string) []string {
	return []string{
		"--die-with-parent", "--new-session", "--unshare-pid", "--clearenv",
		"--ro-bind-try", "/nix/store", "/nix/store", "--ro-bind", "/usr", "/usr",
		"--ro-bind-try", "/etc/ssl", "/etc/ssl", "--ro-bind-try", "/etc/static/ssl", "/etc/static/ssl",
		"--ro-bind-try", "/etc/resolv.conf", "/etc/resolv.conf", "--ro-bind-try", "/etc/hosts", "/etc/hosts",
		"--ro-bind-try", "/etc/nsswitch.conf", "/etc/nsswitch.conf", "--ro-bind-try", "/etc/passwd", "/etc/passwd", "--ro-bind-try", "/etc/group", "/etc/group",
		"--ro-bind-try", "/bin", "/bin", "--ro-bind-try", "/lib", "/lib", "--ro-bind-try", "/lib64", "/lib64", "--tmpfs", "/tmp",
		"--bind", dir, "/workspace", "--dev", "/dev", "--proc", "/proc", "--setenv", "PATH", os.Getenv("PATH"),
		"--setenv", "HOME", workdir, "--setenv", "UV_CACHE_DIR", "/workspace/.uv-cache", "--chdir", workdir,
	}
}
