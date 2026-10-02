package lutra

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	lutrav1 "github.com/brian14708/lutra/gen/lutra/v1"
	"github.com/brian14708/lutra/internal/taskstdio"
	"github.com/fxamacker/cbor/v2"
	"github.com/google/uuid"
)

var (
	dockerImageID     = regexp.MustCompile(`^sha256:[a-f0-9]{64}$`)
	dockerContainerID = regexp.MustCompile(`^[a-f0-9]{64}$`)
)

// DockerExecutor builds and runs images on the worker's persistent Docker daemon.
type DockerExecutor struct {
	OpenBundle func(context.Context, []byte) (io.ReadCloser, error)
}

func (e *DockerExecutor) ImageKey(spec *lutrav1.EnvironmentSpec) ([]byte, error) {
	image := spec.GetImage()
	mode, err := cbor.CanonicalEncOptions().EncMode()
	if err != nil {
		return nil, err
	}
	value, err := mode.Marshal(map[string]any{
		"profile": "lutra.image.docker-python.v2", "from_image": image.GetFromImage(),
		"build_context_uri": image.GetBuildContextUri(), "build_command": image.GetBuildCommand().GetArgs(),
		"workdir": image.GetWorkdir(), "python_requires": image.GetPythonRequires(),
	})
	if err != nil {
		return nil, err
	}
	hash := sha256.Sum256(value)
	return hash[:], nil
}

func dockerWorkdir(workdir string) string { return path.Join("/workspace", workdir) }

func dockerfile(image *lutrav1.ImageSpec) (string, error) {
	venv, err := json.Marshal([]string{"uv", "venv", "--no-python-downloads", "--python", image.GetPythonRequires(), ".venv"})
	if err != nil {
		return "", err
	}
	build, err := json.Marshal(image.GetBuildCommand().GetArgs())
	if err != nil {
		return "", err
	}
	workdir := dockerWorkdir(image.GetWorkdir())
	return fmt.Sprintf("FROM %s\nUSER 0:0\nWORKDIR %s\nENV UV_PROJECT_ENVIRONMENT=%s\nENV VIRTUAL_ENV=%s\nCOPY . /workspace/\nRUN %s\nRUN %s\n", image.GetFromImage(), workdir, workdir+"/.venv", workdir+"/.venv", venv, build), nil
}

func (e *DockerExecutor) Build(ctx context.Context, req *EnvironmentExecution) (*Image, error) {
	buildDir, err := os.MkdirTemp("", "lutra-docker-build-")
	if err != nil {
		return nil, err
	}
	defer func() { _ = os.RemoveAll(buildDir) }()
	contextDir := filepath.Join(buildDir, "context")
	if err := os.Mkdir(contextDir, 0o700); err != nil {
		return nil, err
	}
	image := req.Spec.GetImage()
	if err := unpackSource(ctx, e.OpenBundle, image.GetBuildContextUri(), contextDir); err != nil {
		return nil, err
	}
	file, err := dockerfile(image)
	if err != nil {
		return nil, err
	}
	if output, pullErr := exec.CommandContext(ctx, "docker", "pull", image.GetFromImage()).CombinedOutput(); pullErr != nil {
		if _, inspectErr := exec.CommandContext(ctx, "docker", "image", "inspect", image.GetFromImage()).CombinedOutput(); inspectErr != nil {
			return nil, fmt.Errorf("pull Docker base image: %w: %s", pullErr, strings.TrimSpace(string(output)))
		}
	}
	iidfile := filepath.Join(buildDir, "image-id")
	command := exec.CommandContext(ctx, "docker", "build", "--iidfile", iidfile, "--file", "-", contextDir)
	command.Stdin = strings.NewReader(file)
	command.Stdout, command.Stderr = req.Stderr, req.Stderr
	if err := command.Run(); err != nil {
		return nil, fmt.Errorf("build Docker image: %w", err)
	}
	id, err := os.ReadFile(iidfile)
	if err != nil {
		return nil, err
	}
	value := strings.TrimSpace(string(id))
	if !dockerImageID.MatchString(value) {
		return nil, errors.New("docker build returned an invalid image ID")
	}
	return &Image{ArtifactURI: "docker://" + value}, nil
}

func dockerTaskEnv(req *EnvironmentExecution) ([]string, error) {
	environments := req.Environments
	if environments == nil {
		environments = []*lutrav1.EnvironmentIdentifier{}
	}
	environmentsJSON, err := json.Marshal(environments)
	if err != nil {
		return nil, err
	}
	workdir := dockerWorkdir(req.Spec.GetImage().GetWorkdir())
	importPaths := make([]string, 0, len(req.Spec.GetImportRoots()))
	for _, root := range req.Spec.GetImportRoots() {
		importPaths = append(importPaths, path.Join("/workspace", root))
	}
	values := map[string]string{
		"HOME": workdir, "PYTHONPATH": strings.Join(importPaths, ":"),
		"LUTRA_TASK_NAMESPACE": req.Environment.NamespaceId, "LUTRA_TASK_VERSION": req.Environment.Version,
		"LUTRA_ENVIRONMENT_NAME": req.Environment.Name, "LUTRA_ENVIRONMENTS_JSON": string(environmentsJSON),
		"LUTRA_ATTEMPT": strconv.FormatInt(int64(req.Attempt), 10), "LUTRA_TASK_RUN_ID": req.RunID,
		"LUTRA_TASK_ACTION_ID": req.ActionID, "LUTRA_BUNDLE_ROOT": "/workspace",
	}
	for key, value := range req.Spec.GetImage().GetEnvVars() {
		values[key] = value
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	args := make([]string, 0, len(keys)*2)
	for _, key := range keys {
		args = append(args, "--env", key+"="+values[key])
	}
	return args, nil
}

func (e *DockerExecutor) Run(ctx context.Context, image *Image, req *EnvironmentExecution) (Job, error) {
	id := strings.TrimPrefix(image.ArtifactURI, "docker://")
	if !dockerImageID.MatchString(id) || image.ArtifactURI != "docker://"+id {
		return nil, errors.New("invalid Docker image artifact")
	}
	if output, err := exec.CommandContext(ctx, "docker", "image", "inspect", id).CombinedOutput(); err != nil {
		return nil, fmt.Errorf("cached Docker image %s is unavailable on the shared daemon: %w: %s", id, err, strings.TrimSpace(string(output)))
	}
	if req.EntrypointID == 0 || req.EntrypointID > uint32(len(req.Spec.GetEntrypoints())) {
		return nil, errors.New("entrypoint is not registered")
	}
	sourceDir, err := os.MkdirTemp("", "lutra-docker-source-")
	if err != nil {
		return nil, err
	}
	started := false
	defer func() {
		if !started {
			_ = os.RemoveAll(sourceDir)
		}
	}()
	if err := unpackSource(ctx, e.OpenBundle, req.Spec.GetSourceUri(), sourceDir); err != nil {
		return nil, err
	}
	env, err := dockerTaskEnv(req)
	if err != nil {
		return nil, err
	}
	imageSpec := req.Spec.GetImage()
	startup := req.Spec.GetEntrypoints()[req.EntrypointID-1].GetCommand().GetArgs()
	workdir := dockerWorkdir(imageSpec.GetWorkdir())
	containerName := "lutra-" + uuid.NewString()
	remove := func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := removeDockerContainer(cleanupCtx, containerName); err != nil {
			slog.Error("remove Docker task container", "container", containerName, "error", err)
		}
	}
	args := []string{"create", "--rm", "--interactive", "--name", containerName, "--user", "0:0", "--cpus", strconv.FormatFloat(float64(imageSpec.GetResources().GetCpuMillis())/1000, 'f', 3, 64), "--memory", strconv.FormatUint(imageSpec.GetResources().GetMemoryBytes(), 10), "--memory-swap", strconv.FormatUint(imageSpec.GetResources().GetMemoryBytes(), 10), "--workdir", workdir, "--entrypoint", startup[0], "--label", "lutra.run=" + req.RunID, "--label", "lutra.action=" + req.ActionID}
	args = append(args, env...)
	args = append(args, id)
	args = append(args, startup[1:]...)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// Let create finish after cancellation so the daemon cannot create a
	// container after cleanup has already checked its name.
	output, err := exec.Command("docker", args...).CombinedOutput()
	if err != nil {
		remove()
		return nil, fmt.Errorf("create Docker container: %w: %s", err, strings.TrimSpace(string(output)))
	}
	containerID := strings.TrimSpace(string(output))
	if !dockerContainerID.MatchString(containerID) {
		remove()
		return nil, errors.New("docker create returned an invalid container ID")
	}
	cleanup := func() {
		remove()
		_ = os.RemoveAll(sourceDir)
	}
	defer func() {
		if !started {
			cleanup()
		}
	}()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	output, err = exec.CommandContext(ctx, "docker", "cp", sourceDir+"/.", containerID+":/workspace").CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("copy task source into Docker container: %w: %s", err, strings.TrimSpace(string(output)))
	}
	jobCtx, stop := context.WithCancel(ctx)
	command := exec.CommandContext(jobCtx, "docker", "start", "--attach", "--interactive", containerID)
	command.Stderr = req.Stderr
	process, err := taskstdio.Start(command)
	if err != nil {
		stop()
		return nil, err
	}
	process.Transport.SetHandler(req.TaskAPIHandler)
	started = true
	return &dockerJob{id: containerID, req: req, process: process, stop: stop, cleanup: cleanup}, nil
}

func removeDockerContainer(ctx context.Context, id string) error {
	output, err := exec.CommandContext(ctx, "docker", "rm", "--force", id).CombinedOutput()
	if err != nil && !strings.Contains(string(output), "No such container") {
		return fmt.Errorf("remove Docker container: %w: %s", err, strings.TrimSpace(string(output)))
	}
	return nil
}

type dockerJob struct {
	id      string
	req     *EnvironmentExecution
	process *taskstdio.Process
	stop    context.CancelFunc
	cleanup func()
}

func (j *dockerJob) ID() string { return j.id }

func (j *dockerJob) Wait(ctx context.Context) ([]byte, error) {
	defer j.stop()
	defer j.cleanup()
	return executeProcess(ctx, j.process, j.req)
}

func (j *dockerJob) Kill(ctx context.Context) error {
	j.stop()
	return removeDockerContainer(ctx, j.id)
}
