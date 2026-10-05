package lutra

import (
	"bytes"
	"context"
	"encoding/hex"
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
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	lutrav1 "github.com/brian14708/lutra/gen/lutra/v1"
	"github.com/brian14708/lutra/internal/multihash"
	"github.com/brian14708/lutra/internal/taskstdio"
	"github.com/google/uuid"
)

var (
	containerArtifact  = regexp.MustCompile(`^container://lutra:sha-[a-f0-9]{64}$`)
	containerIDPattern = regexp.MustCompile(`^[a-f0-9]{64}$`)
)

// ContainerExecutor builds and runs images in the worker's selected local engine.
type ContainerExecutor struct {
	OpenBundle func(context.Context, []byte) (io.ReadCloser, error)
	Runtime    string
}

func containerRuntime() (string, error) {
	runtime := os.Getenv("LUTRA_CONTAINER_RUNTIME")
	if runtime == "" {
		runtime = "docker"
	}
	if runtime != "docker" && runtime != "podman" {
		return "", fmt.Errorf("invalid LUTRA_CONTAINER_RUNTIME %q", runtime)
	}
	return runtime, nil
}

func (e *ContainerExecutor) ImageKey(spec *lutrav1.EnvironmentSpec) ([]byte, error) {
	image := spec.GetImage()
	recipe, err := dockerfile(image)
	if err != nil {
		return nil, err
	}
	hash := multihash.Sum([]byte(recipe), []byte(image.GetBuildContextUri()), []byte(imagePlatform(image)))
	return hash[:], nil
}

func containerWorkdir(workdir string) string { return path.Join("/workspace", workdir) }

func imagePlatform(image *lutrav1.ImageSpec) string {
	if image.GetPlatform() != "" {
		return image.GetPlatform()
	}
	return "linux/" + runtime.GOARCH
}

const containerBootstrap = `#!/bin/sh
set -eu
sh -c "$1"
shift
exec "$@"
`

func dockerfile(image *lutrav1.ImageSpec) (string, error) {
	if err := validateImageInputs(image); err != nil {
		return "", err
	}
	bootstrap, err := json.Marshal([]string{"/bin/sh", "-c", "printf '%s' '" + containerBootstrap + "' > /opt/lutra/bootstrap && chmod 755 /opt/lutra/bootstrap"})
	if err != nil {
		return "", err
	}
	var b strings.Builder
	stages := make(map[string]string)
	for _, copy := range image.GetOciCopies() {
		if _, ok := stages[copy.GetImage()]; !ok {
			stage := fmt.Sprintf("lutra_oci_%d", len(stages))
			stages[copy.GetImage()] = stage
			fmt.Fprintf(&b, "FROM --platform=%s %s AS %s\n", imagePlatform(image), copy.GetImage(), stage)
		}
	}
	fmt.Fprintf(&b, "FROM %s\nUSER 0:0\nWORKDIR /opt/lutra\n", image.GetFromImage())
	keys := make([]string, 0, len(image.GetBuildEnv()))
	for key := range image.GetBuildEnv() {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		// ENV uses Dockerfile quoting rather than the JSON form of COPY and RUN.
		value := strings.NewReplacer("\\", "\\\\", "\"", "\\\"", "$", "\\$").Replace(image.GetBuildEnv()[key])
		fmt.Fprintf(&b, "ENV %s=\"%s\"\n", key, value)
	}
	fmt.Fprint(&b, "COPY [\".\", \"/opt/lutra/dependencies/\"]\n")
	for _, copy := range deduplicateCopies(image.GetOciCopies()) {
		paths, err := json.Marshal([]string{copy.GetSource(), copy.GetDestination()})
		if err != nil {
			return "", err
		}
		fmt.Fprintf(&b, "COPY --from=%s %s\n", stages[copy.GetImage()], paths)
	}
	for _, command := range image.GetBuildCommands() {
		args, err := json.Marshal(command.GetArgs())
		if err != nil {
			return "", err
		}
		fmt.Fprintf(&b, "RUN %s\n", args)
	}
	fmt.Fprintf(&b, "RUN %s\nWORKDIR /workspace\n", bootstrap)
	return b.String(), nil
}

// imageTag names the locally built image for an environment key.
func imageTag(key []byte) string { return "lutra:sha-" + hex.EncodeToString(key) }

func (e *ContainerExecutor) Build(ctx context.Context, req *EnvironmentExecution) (*Image, error) {
	key, err := e.ImageKey(req.Spec)
	if err != nil {
		return nil, err
	}
	tag := imageTag(key)
	if err := exec.CommandContext(ctx, e.Runtime, "image", "inspect", tag).Run(); err == nil {
		return &Image{ArtifactURI: "container://" + tag}, nil
	}
	buildDir, err := os.MkdirTemp("", "lutra-container-build-")
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
	logWriter := req.Stderr
	if logWriter == nil {
		logWriter = io.Discard
	}
	var pullOutput bytes.Buffer
	pullCommand := exec.CommandContext(ctx, e.Runtime, "pull", "--platform", imagePlatform(image), image.GetFromImage())
	pullLog := req.PullOutput
	if pullLog == nil {
		pullLog = logWriter
	}
	pullWriter := io.MultiWriter(pullLog, &pullOutput)
	pullCommand.Stdout, pullCommand.Stderr = pullWriter, pullWriter
	if pullErr := pullCommand.Run(); pullErr != nil {
		if _, inspectErr := exec.CommandContext(ctx, e.Runtime, "image", "inspect", image.GetFromImage()).CombinedOutput(); inspectErr != nil {
			return nil, fmt.Errorf("pull container base image: %w: %s", pullErr, strings.TrimSpace(pullOutput.String()))
		}
	}
	command := exec.CommandContext(ctx, e.Runtime, "build", "--platform", imagePlatform(image), "--tag", tag, "--file", "-", contextDir)
	command.Stdin = strings.NewReader(file)
	command.Stdout, command.Stderr = logWriter, logWriter
	if err := command.Run(); err != nil {
		return nil, fmt.Errorf("build container image: %w", err)
	}
	return &Image{ArtifactURI: "container://" + tag}, nil
}

func containerTaskEnv(req *EnvironmentExecution) ([]string, error) {
	environments := req.Environments
	if environments == nil {
		environments = []*lutrav1.EnvironmentIdentifier{}
	}
	environmentsJSON, err := json.Marshal(environments)
	if err != nil {
		return nil, err
	}
	workdir := containerWorkdir(req.Spec.GetWorkdir())
	values := map[string]string{
		"HOME":                 workdir,
		"LUTRA_TASK_NAMESPACE": req.Environment.NamespaceId, "LUTRA_TASK_VERSION": req.Environment.Version,
		"LUTRA_ENVIRONMENT_NAME": req.Environment.Name, "LUTRA_ENVIRONMENTS_JSON": string(environmentsJSON),
		"LUTRA_ATTEMPT": strconv.FormatInt(int64(req.Attempt), 10), "LUTRA_TASK_RUN_ID": req.RunID,
		"LUTRA_TASK_ACTION_ID": req.ActionID, "LUTRA_BUNDLE_ROOT": "/workspace",
	}
	resolved, payload, err := executionConfig(req)
	if err != nil {
		return nil, err
	}
	for key, value := range resolved {
		values[key] = value
	}
	values["LUTRA_CONFIG_CBOR"] = payload
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

func (e *ContainerExecutor) Run(ctx context.Context, image *Image, req *EnvironmentExecution) (Job, error) {
	if image == nil || !containerArtifact.MatchString(image.ArtifactURI) {
		return nil, errors.New("invalid container image artifact")
	}
	tag := strings.TrimPrefix(image.ArtifactURI, "container://")
	if output, err := exec.CommandContext(ctx, e.Runtime, "image", "inspect", tag).CombinedOutput(); err != nil {
		return nil, fmt.Errorf("container image %s is unavailable: %w: %s", tag, err, strings.TrimSpace(string(output)))
	}
	if req.EntrypointID == 0 || req.EntrypointID > uint32(len(req.Spec.GetEntrypoints())) {
		return nil, errors.New("entrypoint is not registered")
	}
	sourceDir, err := os.MkdirTemp("", "lutra-container-source-")
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
	env, err := containerTaskEnv(req)
	if err != nil {
		return nil, err
	}
	imageSpec := req.Spec.GetImage()
	startup := req.Spec.GetEntrypoints()[req.EntrypointID-1].GetCommand().GetArgs()
	prepare := req.Spec.GetPrepareCommand().GetArgs()
	workdir := containerWorkdir(req.Spec.GetWorkdir())
	containerName := "lutra-" + uuid.NewString()
	remove := func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := removeContainer(cleanupCtx, e.Runtime, containerName); err != nil {
			slog.Error("remove task container", "container", containerName, "error", err)
		}
	}
	args := []string{"create", "--rm", "--interactive", "--name", containerName, "--user", "0:0", "--cpus", strconv.FormatFloat(float64(imageSpec.GetResources().GetCpuMillis())/1000, 'f', 3, 64), "--memory", strconv.FormatUint(imageSpec.GetResources().GetMemoryBytes(), 10), "--memory-swap", strconv.FormatUint(imageSpec.GetResources().GetMemoryBytes(), 10), "--workdir", workdir, "--entrypoint", "/opt/lutra/bootstrap", "--platform", imagePlatform(imageSpec), "--label", "lutra.run=" + req.RunID, "--label", "lutra.action=" + req.ActionID}
	args = append(args, "--network", "host")
	args = append(args, env...)
	args = append(args, tag)
	quoted := make([]string, len(prepare))
	for i, arg := range prepare {
		quoted[i] = "'" + strings.ReplaceAll(arg, "'", "'\"'\"'") + "'"
	}
	args = append(args, strings.Join(quoted, " "))
	args = append(args, startup...)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// Cleanup must follow create, since the engine may create the container
	// after the worker stops waiting for its CLI.
	type createResult struct {
		output []byte
		err    error
	}
	created := make(chan createResult, 1)
	go func() {
		output, err := exec.Command(e.Runtime, args...).CombinedOutput()
		created <- createResult{output: output, err: err}
	}()
	var result createResult
	select {
	case result = <-created:
	case <-ctx.Done():
		go func() {
			<-created
			remove()
		}()
		return nil, ctx.Err()
	}
	output, err := result.output, result.err
	if err != nil {
		remove()
		return nil, fmt.Errorf("create container: %w: %s", err, strings.TrimSpace(string(output)))
	}
	containerID := strings.TrimSpace(string(output))
	if !containerIDPattern.MatchString(containerID) {
		remove()
		return nil, errors.New("container create returned an invalid container ID")
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
	output, err = exec.CommandContext(ctx, e.Runtime, "cp", sourceDir+"/.", containerID+":/workspace").CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("copy task source into container: %w: %s", err, strings.TrimSpace(string(output)))
	}
	jobCtx, stop := context.WithCancel(ctx)
	command := exec.CommandContext(jobCtx, e.Runtime, "start", "--attach", "--interactive", containerID)
	command.Stderr = req.Stderr
	process, err := taskstdio.Start(command)
	if err != nil {
		stop()
		return nil, err
	}
	process.Transport.SetHandler(req.TaskAPIHandler)
	started = true
	if req.Stderr != nil {
		_, _ = fmt.Fprintf(req.Stderr, "container %s start command launched; stdio attached\n", containerID)
	}
	return &containerJob{id: containerID, runtime: e.Runtime, req: req, process: process, stop: stop, cleanup: cleanup}, nil
}

func removeContainer(ctx context.Context, runtime, id string) error {
	output, err := exec.CommandContext(ctx, runtime, "rm", "--force", id).CombinedOutput()
	if err != nil && !strings.Contains(string(output), "No such container") {
		return fmt.Errorf("remove container: %w: %s", err, strings.TrimSpace(string(output)))
	}
	return nil
}

type containerJob struct {
	id      string
	runtime string
	req     *EnvironmentExecution
	process *taskstdio.Process
	stop    context.CancelFunc
	cleanup func()
}

func (j *containerJob) ID() string { return j.id }

func (j *containerJob) Wait(ctx context.Context) ([]byte, error) {
	defer j.stop()
	defer j.cleanup()
	output, err := executeProcess(ctx, j.process, j.req)
	if j.req.Stderr != nil {
		_, _ = fmt.Fprintf(j.req.Stderr, "container %s stopped\n", j.id)
	}
	return output, err
}

func (j *containerJob) Kill(ctx context.Context) error {
	j.stop()
	return removeContainer(ctx, j.runtime, j.id)
}
