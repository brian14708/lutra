package lutra

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"uuid"

	"connectrpc.com/connect"
	lutrav1 "github.com/brian14708/lutra/gen/lutra/v1"
	lutrav1connect "github.com/brian14708/lutra/gen/lutra/v1/lutrav1connect"
	"github.com/brian14708/lutra/internal/auth"
	"github.com/brian14708/lutra/internal/db"
	"github.com/brian14708/lutra/internal/rpcutil"
	"github.com/minio/minio-go/v7"
)

// TaskService implements task registration and lookup operations.
type TaskService struct {
	lutrav1connect.UnimplementedTaskServiceHandler
	store  *auth.Store
	object *minio.Client
	bucket string
}

// NewTaskService creates a task service backed by store and object storage.
func NewTaskService(store *auth.Store, object *minio.Client, bucket string) *TaskService {
	return &TaskService{store: store, object: object, bucket: bucket}
}

// CreateTask registers an immutable task definition.
func (s *TaskService) CreateTask(ctx context.Context, req *connect.Request[lutrav1.CreateTaskRequest]) (*connect.Response[lutrav1.CreateTaskResponse], error) {
	projectID := req.Msg.GetProjectId()
	if err := s.store.Require(ctx, projectID, "task", "register"); err != nil {
		return nil, err
	}
	if len(req.Msg.GetEntrypointArgv()) == 0 || req.Msg.GetCodeBundleArtifactId() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("entrypoint and code bundle are required"))
	}
	bundle, err := s.store.Queries().GetArtifact(ctx, db.GetArtifactParams{ArtifactID: rpcutil.UUID(req.Msg.GetCodeBundleArtifactId()), ProjectID: rpcutil.UUID(projectID)})
	if err != nil {
		return nil, rpcutil.DB(err)
	}
	if !bundle.SealTime.Valid {
		return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("code bundle artifact is not sealed"))
	}
	if err := s.validateBundleManifest(ctx, bundle, req.Msg.GetName(), req.Msg.GetVersion()); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	entry, _ := json.Marshal(req.Msg.GetEntrypointArgv())
	inputs, _ := json.Marshal(req.Msg.GetInputSlots())
	outputs, _ := json.Marshal(req.Msg.GetOutputSlots())
	task, err := s.store.Queries().CreateTask(ctx, db.CreateTaskParams{TaskID: uuid.New(), ProjectID: rpcutil.UUID(projectID), Name: req.Msg.GetName(), Version: req.Msg.GetVersion(), EntrypointArgv: entry, CodeBundleArtifactID: rpcutil.UUID(req.Msg.GetCodeBundleArtifactId()), InputSlots: inputs, OutputSlots: outputs})
	if err != nil {
		// Registration is idempotent for an immutable task identity. This is
		// needed when a client submits the same bundle for more than one run.
		if existing, lookupErr := s.store.Queries().GetTaskByIdentity(ctx, db.GetTaskByIdentityParams{ProjectID: rpcutil.UUID(projectID), Name: req.Msg.GetName(), Version: req.Msg.GetVersion()}); lookupErr == nil {
			sameBundle := existing.CodeBundleArtifactID == rpcutil.UUID(req.Msg.GetCodeBundleArtifactId())
			if !sameBundle {
				oldArtifact, oldErr := s.store.Queries().GetArtifact(ctx, db.GetArtifactParams{ArtifactID: existing.CodeBundleArtifactID, ProjectID: rpcutil.UUID(projectID)})
				newArtifact, newErr := s.store.Queries().GetArtifact(ctx, db.GetArtifactParams{ArtifactID: rpcutil.UUID(req.Msg.GetCodeBundleArtifactId()), ProjectID: rpcutil.UUID(projectID)})
				sameBundle = oldErr == nil && newErr == nil && oldArtifact.Digest.Valid && newArtifact.Digest.Valid && oldArtifact.Digest.String == newArtifact.Digest.String
			}
			if sameBundle && equalJSON(existing.EntrypointArgv, entry) && equalJSON(existing.InputSlots, inputs) && equalJSON(existing.OutputSlots, outputs) {
				return connect.NewResponse(&lutrav1.CreateTaskResponse{Task: taskProto(existing)}), nil
			}
		}
		return nil, rpcutil.Conflict(err)
	}
	return connect.NewResponse(&lutrav1.CreateTaskResponse{Task: taskProto(task)}), nil
}

func (s *TaskService) validateBundleManifest(ctx context.Context, bundle db.LutraArtifact, name, version string) error {
	if s.object == nil {
		return errors.New("artifact object store is not configured")
	}
	if err := checkBundleSize(bundle); err != nil {
		return err
	}
	chunks, err := s.store.Queries().ListArtifactChunks(ctx, bundle.ArtifactID)
	if err != nil {
		return err
	}
	archiveBytes, err := fetchArtifactBytes(ctx, s.object, s.bucket, bundle, chunks)
	if err != nil {
		return err
	}
	reader, err := zip.NewReader(bytes.NewReader(archiveBytes), int64(len(archiveBytes)))
	if err != nil {
		return fmt.Errorf("invalid code bundle: %w", err)
	}
	var manifest struct {
		Tasks []struct {
			Name       string `json:"name"`
			Version    string `json:"version"`
			Entrypoint string `json:"entrypoint"`
		} `json:"tasks"`
	}
	for _, file := range reader.File {
		if file.Name != "lutra-manifest.json" {
			continue
		}
		stream, openErr := file.Open()
		if openErr != nil {
			return openErr
		}
		decodeErr := json.NewDecoder(stream).Decode(&manifest)
		_ = stream.Close()
		if decodeErr != nil {
			return fmt.Errorf("invalid bundle manifest: %w", decodeErr)
		}
		found := false
		for _, task := range manifest.Tasks {
			if task.Name == "" || task.Version == "" || task.Entrypoint == "" {
				return errors.New("bundle manifest contains an incomplete task")
			}
			if task.Name == name && task.Version == version {
				found = true
			}
		}
		if !found {
			return fmt.Errorf("task %s@%s is not declared by the bundle manifest", name, version)
		}
		return nil
	}
	return errors.New("code bundle manifest is missing")
}

// GetTask returns a task definition.
func (s *TaskService) GetTask(ctx context.Context, req *connect.Request[lutrav1.GetTaskRequest]) (*connect.Response[lutrav1.GetTaskResponse], error) {
	if err := s.store.Require(ctx, req.Msg.GetProjectId(), "task", "read"); err != nil {
		return nil, err
	}
	task, err := s.store.Queries().GetTask(ctx, db.GetTaskParams{TaskID: rpcutil.UUID(req.Msg.GetTaskId()), ProjectID: rpcutil.UUID(req.Msg.GetProjectId())})
	if err != nil {
		return nil, rpcutil.DB(err)
	}
	return connect.NewResponse(&lutrav1.GetTaskResponse{Task: taskProto(task)}), nil
}

// ListTasks returns the task definitions visible in a project.
func (s *TaskService) ListTasks(ctx context.Context, req *connect.Request[lutrav1.ListTasksRequest]) (*connect.Response[lutrav1.ListTasksResponse], error) {
	if err := s.store.Require(ctx, req.Msg.GetProjectId(), "task", "read"); err != nil {
		return nil, err
	}
	tasks, err := s.store.Queries().ListTasks(ctx, rpcutil.UUID(req.Msg.GetProjectId()))
	if err != nil {
		return nil, rpcutil.Internal(err)
	}
	result := make([]*lutrav1.Task, 0, len(tasks))
	for _, task := range tasks {
		result = append(result, taskProto(task))
	}
	return connect.NewResponse(&lutrav1.ListTasksResponse{Tasks: result}), nil
}

// RunService implements workflow run and action operations.
