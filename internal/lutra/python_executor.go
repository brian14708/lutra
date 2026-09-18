package lutra

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"uuid"

	lutrav1 "github.com/brian14708/lutra/gen/lutra/v1"
	"github.com/brian14708/lutra/internal/auth"
	"github.com/brian14708/lutra/internal/db"
	"github.com/brian14708/lutra/internal/digest"
	"github.com/brian14708/lutra/internal/rpcutil"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/minio/minio-go/v7"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

// pythonExecutor owns task bundle execution and the artifact I/O needed by a
// task process. Keeping it outside WorkerEngine makes queue and lease logic
// independent from the Python runtime.
type pythonExecutor struct {
	store  *auth.Store
	object *minio.Client
	bucket string
}

type workerContext struct {
	ProjectID      string            `json:"project_id"`
	RunID          string            `json:"run_id"`
	ParentActionID string            `json:"parent_action_id"`
	Attempt        int32             `json:"attempt"`
	Endpoint       string            `json:"endpoint"`
	Token          string            `json:"token"`
	TokenExpiresAt time.Time         `json:"token_expires_at"`
	FencingToken   string            `json:"fencing_token"`
	TaskIDs        map[string]string `json:"task_ids"`
	TraceParent    string            `json:"traceparent,omitempty"`
	TraceState     string            `json:"tracestate,omitempty"`
}

func newPythonExecutor(store *auth.Store) *pythonExecutor {
	return &pythonExecutor{store: store}
}

// SetArtifactStore enables the local Python subprocess executor.
func (w *WorkerEngine) SetArtifactStore(object *minio.Client, bucket string) {
	w.executor.setArtifactStore(object, bucket)
}

func (e *pythonExecutor) setArtifactStore(object *minio.Client, bucket string) {
	e.object, e.bucket = object, bucket
}

// execute runs the action's task bundle in a subprocess and returns the
// failure message, final action state, and the stored output bindings (nil
// unless the action succeeded).
func (e *pythonExecutor) execute(ctx context.Context, action db.LutraAction, token string, tokenExpires time.Time, fencing string) (failureMessage string, state db.LutraActionState, outputs []byte) {
	if e.object == nil {
		return "artifact object store is not configured", db.LutraActionStateFailed, nil
	}
	task, err := e.store.Queries().GetTask(ctx, db.GetTaskParams{TaskID: action.TaskID, ProjectID: action.ProjectID})
	if err != nil {
		return err.Error(), db.LutraActionStateFailed, nil
	}
	artifact, err := e.store.Queries().GetArtifact(ctx, db.GetArtifactParams{ArtifactID: task.CodeBundleArtifactID, ProjectID: action.ProjectID})
	if err != nil {
		return err.Error(), db.LutraActionStateFailed, nil
	}
	if err := checkBundleSize(artifact); err != nil {
		return err.Error(), db.LutraActionStateFailed, nil
	}
	bundledTasks, err := e.store.Queries().ListTasks(ctx, action.ProjectID)
	if err != nil {
		return err.Error(), db.LutraActionStateFailed, nil
	}
	taskIDs := make(map[string]string, len(bundledTasks))
	for _, bundledTask := range bundledTasks {
		if bundledTask.CodeBundleArtifactID == artifact.ArtifactID {
			taskIDs[bundledTask.Name+"\x00"+bundledTask.Version] = bundledTask.TaskID.String()
		}
	}
	chunks, err := e.store.Queries().ListArtifactChunks(ctx, artifact.ArtifactID)
	if err != nil {
		return err.Error(), db.LutraActionStateFailed, nil
	}
	root, err := os.MkdirTemp("", "lutra-action-")
	if err != nil {
		return err.Error(), db.LutraActionStateFailed, nil
	}
	defer func() { _ = os.RemoveAll(root) }()
	archiveBytes, err := fetchArtifactBytes(ctx, e.object, e.bucket, artifact, chunks)
	if err != nil {
		return err.Error(), db.LutraActionStateFailed, nil
	}
	reader, err := zip.NewReader(bytes.NewReader(archiveBytes), int64(len(archiveBytes)))
	if err != nil {
		return err.Error(), db.LutraActionStateFailed, nil
	}
	for _, file := range reader.File {
		target, pathErr := filepath.Abs(filepath.Join(root, file.Name))
		if pathErr != nil || !strings.HasPrefix(target, filepath.Clean(root)+string(os.PathSeparator)) {
			return "unsafe bundle path", db.LutraActionStateFailed, nil
		}
		if file.FileInfo().IsDir() {
			if mkErr := os.MkdirAll(target, 0o755); mkErr != nil {
				return mkErr.Error(), db.LutraActionStateFailed, nil
			}
			continue
		}
		if mkErr := os.MkdirAll(filepath.Dir(target), 0o755); mkErr != nil {
			return mkErr.Error(), db.LutraActionStateFailed, nil
		}
		source, openErr := file.Open()
		if openErr != nil {
			return openErr.Error(), db.LutraActionStateFailed, nil
		}
		data, readErr := io.ReadAll(source)
		_ = source.Close()
		if readErr != nil {
			return readErr.Error(), db.LutraActionStateFailed, nil
		}
		if writeErr := os.WriteFile(target, data, 0o644); writeErr != nil {
			return writeErr.Error(), db.LutraActionStateFailed, nil
		}
	}
	var argv []string
	if json.Unmarshal(task.EntrypointArgv, &argv) != nil || len(argv) == 0 {
		return "invalid task entrypoint", db.LutraActionStateFailed, nil
	}
	if python := os.Getenv("LUTRA_PYTHON"); python != "" {
		argv[0] = python
	} else if _, statErr := os.Stat(filepath.Join(".venv", "bin", "python")); statErr == nil {
		argv[0], _ = filepath.Abs(filepath.Join(".venv", "bin", "python"))
	}
	inputs := unmarshalBindings(action.Inputs)
	encoded := make([]map[string]string, 0, len(inputs))
	for index, input := range inputs {
		value := input.GetInlineBytes()
		if artifactID := input.GetArtifactId(); artifactID != "" {
			value, err = e.readArtifact(ctx, action.ProjectID.String(), artifactID)
			if err != nil {
				return err.Error(), db.LutraActionStateFailed, nil
			}
		}
		slot := input.GetSlotName()
		if slot == "" {
			slot = strconv.Itoa(index)
		}
		encoded = append(encoded, map[string]string{"slot": slot, "value": base64.StdEncoding.EncodeToString(value)})
	}
	inputPath, outputPath := filepath.Join(root, "inputs.json"), filepath.Join(root, "output.bin")
	inputData, _ := json.Marshal(encoded)
	if err := os.WriteFile(inputPath, inputData, 0o600); err != nil {
		return err.Error(), db.LutraActionStateFailed, nil
	}
	commandArgs := make([]string, 0, len(argv)+6)
	commandArgs = append(commandArgs, argv[1:]...)
	commandArgs = append(commandArgs, "--bundle", root, "--inputs", inputPath, "--output", outputPath)
	taskCtx, cancelTask := context.WithTimeout(ctx, time.Duration(configuredLimit("LUTRA_TASK_TIMEOUT_SECONDS", 3600))*time.Second)
	defer cancelTask()
	command := exec.CommandContext(taskCtx, argv[0], commandArgs...)
	command.Dir = root
	pythonPath := os.Getenv("LUTRA_SDK_PATH")
	if pythonPath == "" {
		pythonPath = filepath.Join("sdk", "src")
	}
	if absolute, pathErr := filepath.Abs(pythonPath); pathErr == nil {
		pythonPath = absolute
	}
	workerEndpoint := os.Getenv("LUTRA_WORKER_ENDPOINT")
	if workerEndpoint == "" {
		workerEndpoint = "http://127.0.0.1:8080/worker-api"
	} else if !strings.Contains(workerEndpoint, "://") {
		workerEndpoint = "http://" + workerEndpoint
	}
	contextValue := workerContext{
		ProjectID: action.ProjectID.String(), RunID: action.RunID.String(), ParentActionID: action.ActionID.String(),
		Attempt: action.AttemptCount, Endpoint: workerEndpoint, TaskIDs: taskIDs,
		Token: token, TokenExpiresAt: tokenExpires, FencingToken: fencing,
	}
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	contextValue.TraceParent = carrier.Get("traceparent")
	contextValue.TraceState = carrier.Get("tracestate")
	contextJSON, err := json.Marshal(contextValue)
	if err != nil {
		return err.Error(), db.LutraActionStateFailed, nil
	}
	env := workerEnvironment()
	env = append(env,
		"LUTRA_WORKER_CONTEXT="+string(contextJSON),
		"PYTHONPATH="+pythonPath,
	)
	command.Env = env
	if output, runErr := command.CombinedOutput(); runErr != nil {
		if errors.Is(taskCtx.Err(), context.DeadlineExceeded) {
			return "task timed out", db.LutraActionStateTimedOut, nil
		}
		return string(output), db.LutraActionStateFailed, nil
	}
	output, err := os.ReadFile(outputPath)
	if err != nil {
		return err.Error(), db.LutraActionStateFailed, nil
	}
	if maxOutput := configuredLimit("LUTRA_MAX_OUTPUT_BYTES", 16<<20); int64(len(output)) > maxOutput {
		return "task output exceeds the configured size limit", db.LutraActionStateFailed, nil
	}
	encodedOutputs, err := e.storeOutputArtifact(ctx, action, output)
	if err != nil {
		return err.Error(), db.LutraActionStateFailed, nil
	}
	return "", db.LutraActionStateSucceeded, encodedOutputs
}

// workerEnvironment deliberately passes only process settings needed to find
// and run Python. In particular, database, object-store, API-key, and JWT
// secrets from the server environment must never be visible to user code.
func workerEnvironment() []string {
	allowed := map[string]bool{
		"HOME":          true,
		"LANG":          true,
		"PATH":          true,
		"TMP":           true,
		"TEMP":          true,
		"TMPDIR":        true,
		"VIRTUAL_ENV":   true,
		"PYTHONHOME":    true,
		"SSL_CERT_FILE": true,
		"SSL_CERT_DIR":  true,
	}
	env := make([]string, 0, len(allowed))
	for _, entry := range os.Environ() {
		key, _, ok := strings.Cut(entry, "=")
		if ok && (allowed[key] || strings.HasPrefix(key, "LC_") || workerTelemetryEnv(key)) {
			env = append(env, entry)
		}
	}
	return env
}

func workerTelemetryEnv(key string) bool {
	if !strings.HasPrefix(key, "OTEL_") {
		return false
	}
	return !strings.Contains(key, "HEADERS") && !strings.Contains(key, "CERTIFICATE") && !strings.Contains(key, "CLIENT_KEY")
}

func configuredLimit(name string, fallback int64) int64 {
	value, err := strconv.ParseInt(os.Getenv(name), 10, 64)
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}

// checkBundleSize bounds the code bundles that are assembled in memory by
// fetchArtifactBytes. The sealed size is client controlled, so without this
// guard a single large upload would drive the server's allocation.
func checkBundleSize(bundle db.LutraArtifact) error {
	if limit := configuredLimit("LUTRA_MAX_BUNDLE_BYTES", 256<<20); bundle.SizeBytes > limit {
		return fmt.Errorf("code bundle exceeds the configured size limit of %d bytes", limit)
	}
	return nil
}

func (e *pythonExecutor) storeOutputArtifact(ctx context.Context, action db.LutraAction, output []byte) ([]byte, error) {
	if e.object == nil {
		return nil, errors.New("artifact object store is not configured")
	}
	value, err := digest.Sum(digest.DefaultAlgorithm, output)
	if err != nil {
		return nil, err
	}
	sealed, err := digest.Combine([]string{value})
	if err != nil {
		return nil, err
	}
	artifactID, uploadID := uuid.New(), uuid.New()
	objectKey := artifactObjectKey(action.ProjectID.String(), artifactID.String(), 0, uploadID.String())
	if _, err := e.store.Queries().CreateArtifact(ctx, db.CreateArtifactParams{ArtifactID: artifactID, ProjectID: action.ProjectID, MimeType: "application/octet-stream"}); err != nil {
		return nil, err
	}
	if _, err := e.store.Queries().UpsertArtifactChunk(ctx, db.UpsertArtifactChunkParams{ArtifactID: artifactID, ChunkIndex: 0, SizeBytes: int64(len(output)), Digest: value, UploadID: uploadID}); err != nil {
		return nil, err
	}
	if _, err := e.object.PutObject(ctx, e.bucket, objectKey, bytes.NewReader(output), int64(len(output)), minio.PutObjectOptions{ContentType: "application/octet-stream"}); err != nil {
		return nil, err
	}
	if _, err := e.store.Queries().CompleteArtifactChunk(ctx, db.CompleteArtifactChunkParams{ArtifactID: artifactID, ChunkIndex: 0, UploadID: uploadID}); err != nil {
		return nil, err
	}
	if _, err := e.store.Queries().SealArtifact(ctx, db.SealArtifactParams{ArtifactID: artifactID, ProjectID: action.ProjectID, SizeBytes: int64(len(output)), Digest: pgtype.Text{String: sealed, Valid: true}}); err != nil {
		return nil, err
	}
	return marshalBindings([]*lutrav1.InputBinding{{SlotName: "result", Value: &lutrav1.InputBinding_ArtifactId{ArtifactId: artifactID.String()}}})
}

func (e *pythonExecutor) readArtifact(ctx context.Context, projectID, artifactID string) ([]byte, error) {
	if e.object == nil {
		return nil, errors.New("artifact object store is not configured")
	}
	artifact, err := e.store.Queries().GetArtifact(ctx, db.GetArtifactParams{ArtifactID: rpcutil.UUID(artifactID), ProjectID: rpcutil.UUID(projectID)})
	if err != nil {
		return nil, err
	}
	if !artifact.SealTime.Valid || artifact.SizeBytes > configuredLimit("LUTRA_MAX_OUTPUT_BYTES", 16<<20) {
		return nil, errors.New("artifact is unavailable or exceeds the output limit")
	}
	chunks, err := e.store.Queries().ListArtifactChunks(ctx, artifact.ArtifactID)
	if err != nil {
		return nil, err
	}
	return fetchArtifactBytes(ctx, e.object, e.bucket, artifact, chunks)
}

// fetchArtifactBytes assembles an artifact from its object-store chunks,
// verifying chunk contiguity, completion, per-chunk digests, and the total
// size. Per-chunk digests are the integrity check for the whole artifact: the
// sealed digest is derived from them at seal time.
func fetchArtifactBytes(ctx context.Context, client *minio.Client, bucket string, artifact db.LutraArtifact, chunks []db.LutraArtifactChunk) ([]byte, error) {
	data := bytes.NewBuffer(make([]byte, 0, artifact.SizeBytes))
	for index, chunk := range chunks {
		if chunk.ChunkIndex != int32(index) || !chunk.CompleteTime.Valid {
			return nil, errors.New("artifact chunks are incomplete")
		}
		object, err := client.GetObject(ctx, bucket, artifactObjectKey(artifact.ProjectID.String(), artifact.ArtifactID.String(), chunk.ChunkIndex, chunk.UploadID.String()), minio.GetObjectOptions{})
		if err != nil {
			return nil, err
		}
		chunkData, readErr := io.ReadAll(io.LimitReader(object, chunk.SizeBytes+1))
		closeErr := object.Close()
		if readErr != nil {
			return nil, readErr
		}
		if closeErr != nil {
			return nil, closeErr
		}
		if int64(len(chunkData)) != chunk.SizeBytes {
			return nil, errors.New("artifact chunk size mismatch")
		}
		if err := digest.VerifyBytes(chunk.Digest, chunkData); err != nil {
			return nil, err
		}
		_, _ = data.Write(chunkData)
	}
	if int64(data.Len()) != artifact.SizeBytes {
		return nil, errors.New("artifact size mismatch")
	}
	return data.Bytes(), nil
}
