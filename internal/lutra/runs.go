package lutra

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"regexp"
	"strings"
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

type taskContextKey struct{}

type taskContext struct {
	runID    uuid.UUID
	actionID uuid.UUID
	token    uuid.UUID
}

func sourceURI(digest []byte) string {
	return "blob:" + sourceBundleMIME + "," + base64.StdEncoding.EncodeToString(digest)
}

func sourceDigest(task *lutrav1.TaskSpec) ([]byte, error) {
	if task == nil || task.GetSource() == nil {
		return nil, invalidTask("task source is required")
	}
	digest, mimeType, err := blob.ParseURI(task.GetSource().GetUri())
	if err != nil || mimeType != sourceBundleMIME || task.GetSource().GetUri() != sourceURI(digest) {
		return nil, invalidTask("invalid task source")
	}
	return digest, nil
}

func invalidTask(message string) error {
	return connect.NewError(connect.CodeInvalidArgument, errors.New(message))
}

func validateTask(task *lutrav1.TaskSpec) error {
	if task == nil || !namespacePattern.MatchString(task.GetProject()) || !namespacePattern.MatchString(task.GetDomain()) ||
		!taskNamePattern.MatchString(task.GetName()) || !taskNamePattern.MatchString(task.GetModule()) ||
		!taskNamePattern.MatchString(task.GetQualname()) || !versionPattern.MatchString(task.GetVersion()) ||
		task.GetImage().GetName() != localTaskImage {
		return invalidTask("invalid task spec")
	}
	return nil
}

func (s Service) registerTx(ctx context.Context, q *db.Queries, task *lutrav1.TaskSpec, digest []byte) error {
	if s.Worker == nil {
		return connect.NewError(connect.CodeUnavailable, errors.New("worker unavailable"))
	}
	if err := validateTask(task); err != nil {
		return err
	}
	if _, err := q.GetBlobBySHA256(ctx, digest); errors.Is(err, pgx.ErrNoRows) {
		return invalidTask("task source blob is not uploaded")
	} else if err != nil {
		return err
	}
	if err := q.InsertTaskSpec(ctx, db.InsertTaskSpecParams{
		Project: task.Project, Domain: task.Domain, Name: task.Name, Version: task.Version,
		SourceSha256: digest, Image: task.GetImage().GetName(), Module: task.Module, Qualname: task.Qualname,
	}); err != nil {
		return err
	}
	entry, err := q.GetTaskSpec(ctx, db.GetTaskSpecParams{Project: task.Project, Domain: task.Domain, Name: task.Name, Version: task.Version})
	if err != nil {
		return err
	}
	if entry.Module != task.Module || entry.Qualname != task.Qualname || !bytes.Equal(entry.SourceSha256, digest) || entry.Image != task.GetImage().GetName() {
		return connect.NewError(connect.CodeAlreadyExists, errors.New("task version has a different spec"))
	}
	return nil
}

func validateInput(input []byte) error {
	var arguments []cbor.RawMessage
	if err := cbor.Unmarshal(input, &arguments); err != nil || len(arguments) != 2 {
		return invalidTask("input must contain positional and keyword arguments")
	}
	var positional []cbor.RawMessage
	var keyword map[string]cbor.RawMessage
	if err := cbor.Unmarshal(arguments[0], &positional); err != nil {
		return invalidTask("invalid positional arguments")
	}
	if err := cbor.Unmarshal(arguments[1], &keyword); err != nil {
		return invalidTask("invalid keyword arguments")
	}
	return nil
}

func validateIdempotency(key string, required bool) error {
	if required && key == "" {
		return invalidTask("idempotency key is required")
	}
	if len(key) > 200 {
		return invalidTask("idempotency key is too long")
	}
	return nil
}

func (s Service) CreateRun(ctx context.Context, req *connect.Request[lutrav1.CreateRunRequest]) (*connect.Response[lutrav1.CreateRunResponse], error) {
	if s.Worker == nil {
		return nil, connect.NewError(connect.CodeUnavailable, errors.New("worker unavailable"))
	}
	task := req.Msg.GetSpec()
	digest, err := sourceDigest(task)
	if err != nil {
		return nil, err
	}
	if err := validateInput(req.Msg.GetInputCbor()); err != nil {
		return nil, err
	}
	if err := validateIdempotency(req.Msg.GetIdempotencyKey(), false); err != nil {
		return nil, err
	}
	tx, err := s.Worker.DB.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	q := db.New(tx)
	if err := s.registerTx(ctx, q, task, digest); err != nil {
		return nil, err
	}
	key := pgtype.Text{String: req.Msg.GetIdempotencyKey(), Valid: req.Msg.GetIdempotencyKey() != ""}
	runID := uuid.New()
	inserted, err := q.InsertRun(ctx, db.InsertRunParams{ID: runID, Project: task.Project, Domain: task.Domain, RootIdempotencyKey: key})
	if errors.Is(err, pgx.ErrNoRows) && key.Valid {
		existing, lookupErr := q.GetRunByIdempotencyKey(ctx, db.GetRunByIdempotencyKeyParams{Project: task.Project, Domain: task.Domain, RootIdempotencyKey: key})
		if lookupErr != nil {
			return nil, lookupErr
		}
		if existing.RootActionID == nil {
			return nil, connect.NewError(connect.CodeInternal, errors.New("idempotent run has no root action"))
		}
		action, lookupErr := q.ReadTaskAction(ctx, *existing.RootActionID)
		if lookupErr != nil {
			return nil, lookupErr
		}
		if action.Name != task.Name || action.Version != task.Version || !bytes.Equal(action.InputCbor, req.Msg.GetInputCbor()) {
			return nil, invalidTask("idempotency key already belongs to a different invocation")
		}
		runID = existing.ID
	} else if err != nil {
		return nil, err
	} else {
		rootID, err := q.InsertRootAction(ctx, db.InsertRootActionParams{ID: uuid.New(), RunID: inserted, Project: task.Project, Domain: task.Domain, Name: task.Name, Version: task.Version, InputCbor: req.Msg.GetInputCbor()})
		if err != nil {
			return nil, err
		}
		if err := q.UpdateRunRootAction(ctx, db.UpdateRunRootActionParams{RootActionID: &rootID, ID: inserted}); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	run, err := s.readRun(ctx, runID)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&lutrav1.CreateRunResponse{Run: run}), nil
}

func (s Service) CreateTaskAction(ctx context.Context, req *connect.Request[lutrav1.CreateTaskActionRequest]) (*connect.Response[lutrav1.CreateTaskActionResponse], error) {
	active, ok := ctx.Value(taskContextKey{}).(taskContext)
	if !ok {
		return nil, connect.NewError(connect.CodePermissionDenied, errors.New("task actions are only available inside a task"))
	}
	task := req.Msg.GetSpec()
	digest, err := sourceDigest(task)
	if err != nil {
		return nil, err
	}
	if err := validateInput(req.Msg.GetInputCbor()); err != nil {
		return nil, err
	}
	if err := validateIdempotency(req.Msg.GetIdempotencyKey(), true); err != nil {
		return nil, err
	}
	tx, err := s.Worker.DB.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	q := db.New(tx)
	locked, err := q.LockRun(ctx, active.runID)
	if err != nil || locked.RootActionID == nil {
		return nil, invalidTask("run is not active")
	}
	if task.GetProject() != locked.Project || task.GetDomain() != locked.Domain {
		return nil, invalidTask("child task belongs to a different project or domain")
	}
	rootStatus, err := q.GetRootStatus(ctx, *locked.RootActionID)
	if err != nil || terminal(string(rootStatus)) {
		return nil, invalidTask("run is not active")
	}
	caller, err := q.GetActiveCaller(ctx, active.actionID)
	if err != nil || caller.RunID != active.runID || caller.ClaimToken == nil || *caller.ClaimToken != active.token {
		return nil, invalidTask("caller action is not active")
	}
	if err := s.registerTx(ctx, q, task, digest); err != nil {
		return nil, err
	}
	if !bytes.Equal(caller.SourceSha256, digest) {
		return nil, invalidTask("child source differs from caller")
	}
	key := pgtype.Text{String: req.Msg.GetIdempotencyKey(), Valid: true}
	existing, lookupErr := q.GetTaskActionByIdempotency(ctx, db.GetTaskActionByIdempotencyParams{RunID: active.runID, IdempotencyKey: key})
	var actionID uuid.UUID
	if lookupErr == nil {
		if existing.CallerActionID == nil || *existing.CallerActionID != active.actionID || existing.Name != task.Name || existing.Version != task.Version || !bytes.Equal(existing.InputCbor, req.Msg.GetInputCbor()) {
			return nil, invalidTask("idempotency key already belongs to a different action")
		}
		actionID = existing.ID
	} else if !errors.Is(lookupErr, pgx.ErrNoRows) {
		return nil, lookupErr
	} else {
		actionID, err = q.InsertTaskAction(ctx, db.InsertTaskActionParams{ID: uuid.New(), RunID: active.runID, CallerActionID: &active.actionID, Project: task.Project, Domain: task.Domain, Name: task.Name, Version: task.Version, InputCbor: req.Msg.GetInputCbor(), IdempotencyKey: key})
		if errors.Is(err, pgx.ErrNoRows) {
			existing, lookupErr = q.GetTaskActionByIdempotency(ctx, db.GetTaskActionByIdempotencyParams{RunID: active.runID, IdempotencyKey: key})
			if lookupErr != nil || existing.CallerActionID == nil || *existing.CallerActionID != active.actionID || existing.Name != task.Name || existing.Version != task.Version || !bytes.Equal(existing.InputCbor, req.Msg.GetInputCbor()) {
				return nil, invalidTask("idempotency key already belongs to a different action")
			}
			actionID = existing.ID
		} else if err != nil {
			return nil, err
		}
	}
	if err := q.InsertTaskActionEdge(ctx, db.InsertTaskActionEdgeParams{RunID: active.runID, SourceActionID: actionID, DependentActionID: active.actionID}); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	action, err := s.readTaskAction(ctx, actionID)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&lutrav1.CreateTaskActionResponse{Action: action}), nil
}

func (s Service) readRun(ctx context.Context, id uuid.UUID) (*lutrav1.Run, error) {
	row, err := db.New(s.Worker.DB).ReadRun(ctx, id)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("run not found"))
	}
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	rootID := ""
	if row.RootActionID != nil {
		rootID = row.RootActionID.String()
	}
	return &lutrav1.Run{Id: id.String(), RootActionId: rootID, Spec: specFromParts(row.Project, row.Domain, row.Name, row.Version, row.Module, row.Qualname, row.SourceSha256, row.Image), Status: string(row.Status), OutputCbor: row.OutputCbor, Error: row.Error, CreatedAt: formatTime(row.RunCreatedAt), UpdatedAt: formatTime(row.UpdatedAt)}, nil
}

func specFromParts(project, domain, name, version, module, qualname string, digest []byte, image string) *lutrav1.TaskSpec {
	return &lutrav1.TaskSpec{Project: project, Domain: domain, Name: name, Version: version, Module: module, Qualname: qualname, Source: &lutrav1.SourceBundle{Uri: sourceURI(digest)}, Image: &lutrav1.TaskImage{Name: image}}
}

func formatTime(value pgtype.Timestamptz) string { return value.Time.UTC().Format(time.RFC3339Nano) }

func (s Service) readTaskAction(ctx context.Context, id uuid.UUID) (*lutrav1.TaskAction, error) {
	row, err := db.New(s.Worker.DB).ReadTaskAction(ctx, id)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("task action not found"))
	}
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	upstream, err := db.New(s.Worker.DB).ListActionUpstreams(ctx, db.ListActionUpstreamsParams{RunID: row.RunID, ActionIds: []uuid.UUID{row.ID}})
	if err != nil {
		return nil, err
	}
	return actionFromRow(actionRowFromRead(row), upstream), nil
}

type actionRow struct {
	ID                                                                     uuid.UUID
	RunID                                                                  uuid.UUID
	CallerActionID                                                         *uuid.UUID
	Project, Domain, Name, Version, Module, Qualname, Image, Status, Error string
	SourceSha256, InputCbor, OutputCbor                                    []byte
	Attempts                                                               int32
	CreatedAt, UpdatedAt                                                   pgtype.Timestamptz
}

func actionFromRow(row actionRow, upstream []db.ListActionUpstreamsRow) *lutrav1.TaskAction {
	action := &lutrav1.TaskAction{Id: row.ID.String(), RunId: row.RunID.String(), Spec: specFromParts(row.Project, row.Domain, row.Name, row.Version, row.Module, row.Qualname, row.SourceSha256, row.Image), InputCbor: row.InputCbor, OutputCbor: row.OutputCbor, Status: row.Status, Error: row.Error, Attempts: row.Attempts, CreatedAt: formatTime(row.CreatedAt), UpdatedAt: formatTime(row.UpdatedAt)}
	if row.CallerActionID != nil {
		action.CallerActionId = row.CallerActionID.String()
	}
	for _, edge := range upstream {
		if edge.DependentActionID == row.ID {
			action.UpstreamActionIds = append(action.UpstreamActionIds, edge.SourceActionID.String())
		}
	}
	return action
}

func actionRowFromRead(row db.ReadTaskActionRow) actionRow {
	return actionRow{ID: row.ID, RunID: row.RunID, CallerActionID: row.CallerActionID, Project: row.Project, Domain: row.Domain, Name: row.Name, Version: row.Version, Module: row.Module, Qualname: row.Qualname, SourceSha256: row.SourceSha256, Image: row.Image, InputCbor: row.InputCbor, OutputCbor: row.OutputCbor, Status: string(row.Status), Error: row.Error, Attempts: row.Attempts, CreatedAt: row.CreatedAt, UpdatedAt: row.UpdatedAt}
}

func actionRowFromList(row db.ListTaskActionsRow) actionRow {
	return actionRow{ID: row.ID, RunID: row.RunID, CallerActionID: row.CallerActionID, Project: row.Project, Domain: row.Domain, Name: row.Name, Version: row.Version, Module: row.Module, Qualname: row.Qualname, SourceSha256: row.SourceSha256, Image: row.Image, InputCbor: row.InputCbor, OutputCbor: row.OutputCbor, Status: string(row.Status), Error: row.Error, Attempts: row.Attempts, CreatedAt: row.CreatedAt, UpdatedAt: row.UpdatedAt}
}

func (s Service) GetRun(ctx context.Context, req *connect.Request[lutrav1.GetRunRequest]) (*connect.Response[lutrav1.GetRunResponse], error) {
	id, err := uuid.Parse(req.Msg.GetId())
	if err != nil {
		return nil, invalidTask("invalid run id")
	}
	run, err := s.readRun(ctx, id)
	if err != nil {
		return nil, err
	}
	if req.Msg.GetWait() {
		for !terminal(run.Status) {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(100 * time.Millisecond):
			}
			run, err = s.readRun(ctx, id)
			if err != nil {
				return nil, err
			}
		}
	}
	return connect.NewResponse(&lutrav1.GetRunResponse{Run: run}), nil
}

func (s Service) GetTaskAction(ctx context.Context, req *connect.Request[lutrav1.GetTaskActionRequest]) (*connect.Response[lutrav1.GetTaskActionResponse], error) {
	id, err := uuid.Parse(req.Msg.GetId())
	if err != nil {
		return nil, invalidTask("invalid task action id")
	}
	if active, ok := ctx.Value(taskContextKey{}).(taskContext); ok {
		caller, lookupErr := db.New(s.Worker.DB).GetTaskActionCaller(ctx, id)
		if lookupErr != nil || caller.RunID != active.runID || caller.CallerActionID == nil || *caller.CallerActionID != active.actionID {
			return nil, invalidTask("task action is not a child of this task")
		}
	}
	if req.Msg.GetWait() {
		active, ok := ctx.Value(taskContextKey{}).(taskContext)
		if !ok {
			return nil, invalidTask("waiting is only available inside a task")
		}
		if err := s.Worker.waitAction(ctx, active, id); err != nil {
			return nil, err
		}
	}
	action, err := s.readTaskAction(ctx, id)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&lutrav1.GetTaskActionResponse{Action: action}), nil
}

func (s Service) ListTaskActions(ctx context.Context, req *connect.Request[lutrav1.ListTaskActionsRequest]) (*connect.Response[lutrav1.ListTaskActionsResponse], error) {
	runID, err := uuid.Parse(req.Msg.GetRunId())
	if err != nil {
		return nil, invalidTask("invalid run id")
	}
	pageSize := req.Msg.GetPageSize()
	if pageSize <= 0 || pageSize > 1000 {
		pageSize = 100
	}
	q := db.New(s.Worker.DB)
	var rows []actionRow
	if req.Msg.GetPageToken() == "" {
		listRows, queryErr := q.ListTaskActions(ctx, db.ListTaskActionsParams{RunID: runID, CursorTime: pgtype.Timestamptz{Time: time.Unix(0, 0), Valid: true}, CursorID: uuid.Nil, PageSize: pageSize + 1})
		err = queryErr
		for _, row := range listRows {
			rows = append(rows, actionRowFromList(row))
		}
	} else {
		decoded, decodeErr := base64.RawURLEncoding.DecodeString(req.Msg.GetPageToken())
		if decodeErr != nil {
			return nil, invalidTask("invalid page token")
		}
		parts := strings.Split(string(decoded), "|")
		if len(parts) != 2 {
			return nil, invalidTask("invalid page token")
		}
		cursorTime, parseErr := time.Parse(time.RFC3339Nano, parts[0])
		cursorID, uuidErr := uuid.Parse(parts[1])
		if parseErr != nil || uuidErr != nil {
			return nil, invalidTask("invalid page token")
		}
		listRows, queryErr := q.ListTaskActions(ctx, db.ListTaskActionsParams{RunID: runID, CursorTime: pgtype.Timestamptz{Time: cursorTime, Valid: true}, CursorID: cursorID, PageSize: pageSize + 1})
		err = queryErr
		for _, row := range listRows {
			rows = append(rows, actionRowFromList(row))
		}
	}
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	more := len(rows) > int(pageSize)
	if more {
		rows = rows[:pageSize]
	}
	response := &lutrav1.ListTaskActionsResponse{}
	ids := make([]uuid.UUID, 0, len(rows))
	for _, row := range rows {
		ids = append(ids, row.ID)
	}
	upstream, err := q.ListActionUpstreams(ctx, db.ListActionUpstreamsParams{RunID: runID, ActionIds: ids})
	if err != nil {
		return nil, err
	}
	for _, row := range rows {
		response.Actions = append(response.Actions, actionFromRow(row, upstream))
	}
	if more && len(rows) > 0 {
		last := rows[len(rows)-1]
		response.NextPageToken = base64.RawURLEncoding.EncodeToString([]byte(fmt.Sprintf("%s|%s", formatTime(last.CreatedAt), last.ID)))
	}
	return connect.NewResponse(response), nil
}

func (s Service) CancelRun(ctx context.Context, req *connect.Request[lutrav1.CancelRunRequest]) (*connect.Response[lutrav1.CancelRunResponse], error) {
	id, err := uuid.Parse(req.Msg.GetId())
	if err != nil {
		return nil, invalidTask("invalid run id")
	}
	tx, err := s.Worker.DB.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	q := db.New(tx)
	if _, err := q.LockRun(ctx, id); errors.Is(err, pgx.ErrNoRows) {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("run not found"))
	} else if err != nil {
		return nil, err
	}
	if _, err := q.CancelRunIfActive(ctx, id); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	s.Worker.cancelRun(id)
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
		run, readErr := s.readRun(ctx, id)
		if readErr != nil {
			return readErr
		}
		key := run.Status + ":" + run.UpdatedAt
		if key != last {
			if sendErr := stream.Send(&lutrav1.WatchRunResponse{Run: run}); sendErr != nil {
				return sendErr
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
