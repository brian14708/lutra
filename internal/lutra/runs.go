package lutra

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"regexp"
	"time"

	"connectrpc.com/connect"
	lutrav1 "github.com/brian14708/lutra/gen/lutra/v1"
	"github.com/brian14708/lutra/internal/blob"
	"github.com/brian14708/lutra/internal/db"
	"github.com/fxamacker/cbor/v2"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

var (
	taskNamePattern  = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z_0-9]*(\.[a-zA-Z_][a-zA-Z_0-9]*)*$`)
	namespacePattern = regexp.MustCompile(`^[a-z][a-z0-9-]{0,62}$`)
	versionPattern   = regexp.MustCompile(`^[a-f0-9]{64}$`)
)

const (
	localTaskImage   = "local-python"
	sourceBundleMIME = "application/vnd.lutra.source-bundle+zstd"
)

func sourceURI(digest []byte) string {
	return "blob:" + sourceBundleMIME + "," + base64.StdEncoding.EncodeToString(digest)
}

func sourceDigest(task *lutrav1.TaskSpec) ([]byte, error) {
	digest, mimeType, err := blob.ParseURI(task.GetSource().GetUri())
	if err != nil || mimeType != sourceBundleMIME || task.GetSource().GetUri() != sourceURI(digest) {
		return nil, invalidTask("invalid task source")
	}
	return digest, nil
}

func invalidTask(message string) error {
	return connect.NewError(connect.CodeInvalidArgument, errors.New(message))
}

func (s Service) register(ctx context.Context, task *lutrav1.TaskSpec, digest []byte) error {
	if s.Worker == nil {
		return connect.NewError(connect.CodeUnavailable, errors.New("worker unavailable"))
	}
	if task == nil || !namespacePattern.MatchString(task.GetProject()) || !namespacePattern.MatchString(task.GetDomain()) || !taskNamePattern.MatchString(task.GetName()) || !taskNamePattern.MatchString(task.GetModule()) || !taskNamePattern.MatchString(task.GetQualname()) || !versionPattern.MatchString(task.GetVersion()) || task.GetImage().GetName() != localTaskImage {
		return invalidTask("invalid task spec")
	}
	tx, err := s.Worker.DB.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	queries := db.New(tx)
	_, err = queries.GetBlobBySHA256(ctx, digest)
	if errors.Is(err, pgx.ErrNoRows) {
		return invalidTask("task source blob is not uploaded")
	}
	if err != nil {
		return err
	}
	if err := queries.InsertTaskSpec(ctx, db.InsertTaskSpecParams{
		Project: task.Project, Domain: task.Domain, Name: task.Name, Version: task.Version,
		SourceSha256: digest, Image: task.GetImage().GetName(),
		Module: task.Module, Qualname: task.Qualname,
	}); err != nil {
		return err
	}
	entry, err := queries.GetTaskSpec(ctx, db.GetTaskSpecParams{
		Project: task.Project, Domain: task.Domain, Name: task.Name, Version: task.Version,
	})
	if err != nil {
		return err
	}
	if entry.Module != task.Module || entry.Qualname != task.Qualname || !bytes.Equal(entry.SourceSha256, digest) || entry.Image != task.GetImage().GetName() {
		return connect.NewError(connect.CodeAlreadyExists, errors.New("task version has a different spec"))
	}
	return tx.Commit(ctx)
}

type parentKey struct{}

func (s Service) CreateRun(ctx context.Context, req *connect.Request[lutrav1.CreateRunRequest]) (*connect.Response[lutrav1.CreateRunResponse], error) {
	if s.Worker == nil {
		return nil, connect.NewError(connect.CodeUnavailable, errors.New("worker unavailable"))
	}
	queries := db.New(s.Worker.DB)
	task := req.Msg.GetSpec()
	if task == nil {
		return nil, invalidTask("task spec is required")
	}
	digest, err := sourceDigest(task)
	if err != nil {
		return nil, err
	}
	parent := req.Msg.GetParentId()
	var parentID *uuid.UUID
	if parent != "" {
		parsed, err := uuid.Parse(parent)
		if err != nil {
			return nil, invalidTask("invalid parent id")
		}
		parentID = &parsed
	}
	if reverseParent, ok := ctx.Value(parentKey{}).(string); ok && parent != reverseParent {
		return nil, invalidTask("task submissions must belong to the active parent")
	}
	if parent != "" {
		if ctx.Value(parentKey{}) != parent {
			return nil, invalidTask("parent run is not active")
		}
		parentDigest, err := queries.GetParentSourceDigest(ctx, *parentID)
		if err != nil || !bytes.Equal(parentDigest, digest) {
			return nil, invalidTask("child source differs from parent")
		}
	}
	if err := s.register(ctx, task, digest); err != nil {
		return nil, err
	}
	var arguments []cbor.RawMessage
	if err := cbor.Unmarshal(req.Msg.GetInputCbor(), &arguments); err != nil || len(arguments) != 2 {
		return nil, invalidTask("input must contain positional and keyword arguments")
	}
	var positional []cbor.RawMessage
	var keyword map[string]cbor.RawMessage
	if err := cbor.Unmarshal(arguments[0], &positional); err != nil {
		return nil, invalidTask("invalid positional arguments")
	}
	if err := cbor.Unmarshal(arguments[1], &keyword); err != nil {
		return nil, invalidTask("invalid keyword arguments")
	}
	if len(req.Msg.GetIdempotencyKey()) > 200 {
		return nil, invalidTask("idempotency key is too long")
	}
	key := pgtype.Text{String: req.Msg.GetIdempotencyKey(), Valid: req.Msg.GetIdempotencyKey() != ""}
	id, err := queries.InsertRun(ctx, db.InsertRunParams{
		ID: uuid.New(), ParentID: parentID, IdempotencyKey: key, Project: task.Project,
		Domain: task.Domain, Name: task.Name, Version: task.Version, InputCbor: req.Msg.InputCbor,
	})
	if errors.Is(err, pgx.ErrNoRows) && key.Valid {
		var existing db.GetRunByIdempotencyKeyRow
		existing, err = queries.GetRunByIdempotencyKey(ctx, db.GetRunByIdempotencyKeyParams{
			Project: task.Project, Domain: task.Domain, IdempotencyKey: key,
		})
		if err == nil && (existing.Name != task.Name || existing.Version != task.Version || !bytes.Equal(existing.InputCbor, req.Msg.InputCbor) || !sameParent(existing.ParentID, parentID)) {
			return nil, invalidTask("idempotency key already belongs to a different invocation")
		}
		id = existing.ID
	}
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	run, err := s.readRun(ctx, id)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&lutrav1.CreateRunResponse{Run: run}), nil
}

func sameParent(existing, requested *uuid.UUID) bool {
	if existing == nil || requested == nil {
		return existing == nil && requested == nil
	}
	return *existing == *requested
}

func (s Service) readRun(ctx context.Context, id uuid.UUID) (*lutrav1.Run, error) {
	row, err := db.New(s.Worker.DB).ReadRun(ctx, id)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("run not found"))
	}
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	run := &lutrav1.Run{
		Id: id.String(),
		Spec: &lutrav1.TaskSpec{
			Project: row.Project, Domain: row.Domain, Name: row.Name, Version: row.Version,
			Module: row.Module, Qualname: row.Qualname,
			Source: &lutrav1.SourceBundle{Uri: sourceURI(row.SourceSha256)},
			Image:  &lutrav1.TaskImage{Name: row.Image},
		},
		Status: row.Status, Attempts: row.Attempts, OutputCbor: row.OutputCbor, Error: row.Error,
		CreatedAt: row.CreatedAt.Time.UTC().Format(time.RFC3339Nano),
		UpdatedAt: row.UpdatedAt.Time.UTC().Format(time.RFC3339Nano),
	}
	if row.ParentID != nil {
		run.ParentId = row.ParentID.String()
	}
	return run, nil
}

func (s Service) GetRun(ctx context.Context, req *connect.Request[lutrav1.GetRunRequest]) (*connect.Response[lutrav1.GetRunResponse], error) {
	id, err := uuid.Parse(req.Msg.GetId())
	if err != nil {
		return nil, invalidTask("invalid run id")
	}
	if parent, ok := ctx.Value(parentKey{}).(string); ok && !req.Msg.GetWait() {
		actual, err := db.New(s.Worker.DB).GetChildParentID(ctx, id)
		if err != nil || actual == nil || actual.String() != parent {
			return nil, invalidTask("run is not a child of this task")
		}
	}
	if req.Msg.GetWait() {
		parent, ok := ctx.Value(parentKey{}).(string)
		if !ok {
			return nil, invalidTask("waiting is only available inside a parent task")
		}
		if err := s.Worker.waitChild(ctx, parent, id); err != nil {
			return nil, err
		}
	}
	run, err := s.readRun(ctx, id)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&lutrav1.GetRunResponse{Run: run}), nil
}

func (s Service) CancelRun(ctx context.Context, req *connect.Request[lutrav1.CancelRunRequest]) (*connect.Response[lutrav1.CancelRunResponse], error) {
	id, err := uuid.Parse(req.Msg.GetId())
	if err != nil {
		return nil, invalidTask("invalid run id")
	}
	_, err = db.New(s.Worker.DB).CancelRun(ctx, id)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	s.Worker.cancel(id)
	run, err := s.readRun(ctx, id)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&lutrav1.CancelRunResponse{Run: run}), nil
}

func (s Service) WatchRun(ctx context.Context, req *connect.Request[lutrav1.WatchRunRequest], stream *connect.ServerStream[lutrav1.WatchRunResponse]) error {
	id, err := uuid.Parse(req.Msg.GetId())
	if err != nil {
		return invalidTask("invalid run id")
	}
	last := ""
	for {
		run, err := s.readRun(ctx, id)
		if err != nil {
			return err
		}
		key := fmt.Sprintf("%s:%d:%s", run.Status, run.Attempts, run.UpdatedAt)
		if key != last {
			if err := stream.Send(&lutrav1.WatchRunResponse{Run: run}); err != nil {
				return err
			}
			last = key
		}
		if terminal(run.Status) {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func terminal(status string) bool {
	return status == "succeeded" || status == "failed" || status == "canceled"
}
