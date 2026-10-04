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

	"github.com/brian14708/lutra/internal/result"

	"connectrpc.com/connect"
	lutrav1 "github.com/brian14708/lutra/gen/lutra/v1"
	"github.com/brian14708/lutra/internal/blob"
	"github.com/brian14708/lutra/internal/db"
	"github.com/brian14708/lutra/internal/runlog"
	"github.com/fxamacker/cbor/v2"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"google.golang.org/protobuf/proto"
)

var (
	versionPattern         = regexp.MustCompile(`^[a-f0-9]{64}$`)
	semanticVersionPattern = regexp.MustCompile(`^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-(0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*)(\.(0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*))*)?(\+[0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?$`)
)

const containerTaskImage = "container"

func sourceURI(digest []byte) string {
	return blob.URI(archiveMIME, digest)
}

func validateInput(input []byte) error {
	var arguments []cbor.RawMessage
	if err := cbor.Unmarshal(input, &arguments); err != nil || len(arguments) != 2 {
		return invalid("input must contain positional and keyword arguments")
	}
	var positional []cbor.RawMessage
	var keyword map[string]cbor.RawMessage
	if err := cbor.Unmarshal(arguments[0], &positional); err != nil {
		return invalid("invalid positional arguments")
	}
	if err := cbor.Unmarshal(arguments[1], &keyword); err != nil {
		return invalid("invalid keyword arguments")
	}
	return nil
}

func validateActionCache(spec *lutrav1.ActionSpec) error {
	if !spec.GetCache() {
		if spec.GetTaskVersion() != "" {
			return invalid("uncached actions cannot include cache fields")
		}
		return nil
	}
	if len(spec.GetTaskVersion()) > 200 || (spec.GetTaskVersion() != "" && !semanticVersionPattern.MatchString(spec.GetTaskVersion())) {
		return invalid("cached actions require a semantic version or source-derived version")
	}
	return nil
}

func validateIdempotency(key string, required bool) error {
	if required && key == "" {
		return invalid("idempotency key is required")
	}
	if len(key) > 200 {
		return invalid("idempotency key is too long")
	}
	return nil
}

func (s Service) CreateRun(ctx context.Context, req *connect.Request[lutrav1.CreateRunRequest]) (*connect.Response[lutrav1.CreateRunResponse], error) {
	if s.DB == nil {
		return nil, connect.NewError(connect.CodeUnavailable, errors.New("worker unavailable"))
	}
	actionSpec := req.Msg.GetActionSpec()
	if actionSpec == nil {
		return nil, invalid("action spec is required")
	}
	if err := validateInput(actionSpec.GetInputCbor()); err != nil {
		return nil, err
	}
	if err := validateActionCache(actionSpec); err != nil {
		return nil, err
	}
	if err := validateIdempotency(req.Msg.GetIdempotencyKey(), false); err != nil {
		return nil, err
	}
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	q := db.New(tx)
	environment, entrypoint, err := lookupTask(ctx, q, req.Msg.GetEnvironment(), req.Msg.GetEntrypointId())
	if err != nil {
		return nil, err
	}
	storedSpec, err := resolvedActionSpec(entrypoint, actionSpec)
	if err != nil {
		return nil, err
	}
	key := pgtype.Text{String: req.Msg.GetIdempotencyKey(), Valid: req.Msg.GetIdempotencyKey() != ""}
	runID, err := uuid.NewV7()
	if err != nil {
		return nil, err
	}
	inserted, err := q.InsertRun(ctx, db.InsertRunParams{ID: runID, NamespaceID: environment.NamespaceID, RootIdempotencyKey: key})
	if errors.Is(err, pgx.ErrNoRows) && key.Valid {
		existing, lookupErr := q.GetRunByIdempotencyKey(ctx, db.GetRunByIdempotencyKeyParams{NamespaceID: environment.NamespaceID, RootIdempotencyKey: key})
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
		if action.EnvironmentID != environment.ID || action.EntrypointID != int64(req.Msg.GetEntrypointId()) || !bytes.Equal(action.ActionSpec, storedSpec) {
			return nil, invalid("idempotency key already belongs to a different invocation")
		}
		runID = existing.ID
	} else if err != nil {
		return nil, err
	} else {
		rootID, err := uuid.NewV7()
		if err != nil {
			return nil, err
		}
		rootID, err = q.InsertRootAction(ctx, db.InsertRootActionParams{ID: rootID, RunID: inserted, EnvironmentID: environment.ID, EntrypointID: int64(req.Msg.GetEntrypointId()), ActionSpec: storedSpec})
		if err != nil {
			return nil, err
		}
		if err := q.UpdateRunRootAction(ctx, db.UpdateRunRootActionParams{RootActionID: &rootID, ID: inserted}); err != nil {
			return nil, err
		}
		if err := s.Logs.AppendStatus(ctx, tx, runID); err != nil {
			return nil, err
		}
		if err := snapshotRunConfig(ctx, q, inserted, environment, actionSpec.GetConfigOverridesCbor()); err != nil {
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
	if len(req.Msg.GetActionSpec().GetConfigOverridesCbor()) != 0 {
		return nil, configFailure("config.invalid")
	}
	active, ok := taskContextFromContext(ctx)
	if !ok {
		return nil, connect.NewError(connect.CodePermissionDenied, errors.New("task actions are only available inside a task"))
	}
	actionSpec := req.Msg.GetActionSpec()
	if actionSpec == nil {
		return nil, invalid("action spec is required")
	}
	if err := validateInput(actionSpec.GetInputCbor()); err != nil {
		return nil, err
	}
	if err := validateActionCache(actionSpec); err != nil {
		return nil, err
	}
	if err := validateIdempotency(req.Msg.GetIdempotencyKey(), true); err != nil {
		return nil, err
	}
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	q := db.New(tx)
	environment, entrypoint, err := lookupTask(ctx, q, req.Msg.GetEnvironment(), req.Msg.GetEntrypointId())
	if err != nil {
		return nil, err
	}
	storedSpec, err := resolvedActionSpec(entrypoint, actionSpec)
	if err != nil {
		return nil, err
	}
	locked, err := q.LockRun(ctx, active.RunID)
	if err != nil || locked.RootActionID == nil {
		return nil, invalid("run is not active")
	}
	if environment.NamespaceID != locked.NamespaceID {
		return nil, invalid("child task belongs to a different namespace")
	}
	rootStatus, err := q.GetRootStatus(ctx, *locked.RootActionID)
	if err != nil || terminal(string(rootStatus)) {
		return nil, invalid("run is not active")
	}
	caller, err := q.GetActiveCaller(ctx, active.ActionID)
	if err != nil || caller.RunID != active.RunID || caller.ClaimToken == nil || *caller.ClaimToken != active.ClaimToken || caller.Attempts != active.Attempt {
		return nil, invalid("caller action is not active")
	}
	var callerSpec lutrav1.EnvironmentSpec
	if err := proto.Unmarshal(caller.EnvironmentSpec, &callerSpec); err != nil {
		return nil, err
	}
	allowed := caller.EnvironmentID == environment.ID
	child := req.Msg.GetEnvironment()
	for _, dependency := range callerSpec.Dependencies {
		if dependency.NamespaceId == child.NamespaceId && dependency.Name == child.Name && dependency.Version == child.Version {
			allowed = true
			break
		}
	}
	if !allowed {
		return nil, connect.NewError(connect.CodePermissionDenied, errors.New("child environment is not a declared dependency"))
	}
	key := pgtype.Text{String: req.Msg.GetIdempotencyKey(), Valid: true}
	existing, lookupErr := q.GetTaskActionByIdempotency(ctx, db.GetTaskActionByIdempotencyParams{RunID: active.RunID, CallerActionID: &active.ActionID, IdempotencyKey: key})
	var actionID uuid.UUID
	if lookupErr == nil {
		if existing.CallerActionID == nil || *existing.CallerActionID != active.ActionID || existing.EnvironmentID != environment.ID || existing.EntrypointID != int64(req.Msg.GetEntrypointId()) || !bytes.Equal(existing.ActionSpec, storedSpec) {
			return nil, invalid("idempotency key already belongs to a different action")
		}
		actionID = existing.ID
	} else if !errors.Is(lookupErr, pgx.ErrNoRows) {
		return nil, lookupErr
	} else {
		actionID, err = uuid.NewV7()
		if err != nil {
			return nil, err
		}
		actionID, err = q.InsertTaskAction(ctx, db.InsertTaskActionParams{ID: actionID, RunID: active.RunID, CallerActionID: &active.ActionID, EnvironmentID: environment.ID, EntrypointID: int64(req.Msg.GetEntrypointId()), ActionSpec: storedSpec, IdempotencyKey: key})
		if errors.Is(err, pgx.ErrNoRows) {
			existing, lookupErr = q.GetTaskActionByIdempotency(ctx, db.GetTaskActionByIdempotencyParams{RunID: active.RunID, CallerActionID: &active.ActionID, IdempotencyKey: key})
			if lookupErr != nil || existing.CallerActionID == nil || *existing.CallerActionID != active.ActionID || existing.EnvironmentID != environment.ID || existing.EntrypointID != int64(req.Msg.GetEntrypointId()) || !bytes.Equal(existing.ActionSpec, storedSpec) {
				return nil, invalid("idempotency key already belongs to a different action")
			}
			actionID = existing.ID
		} else if err != nil {
			return nil, err
		}
		if err == nil {
			if err := s.Logs.AppendActionStatus(ctx, tx, actionID, false); err != nil {
				return nil, err
			}
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	if active.add == nil {
		return nil, invalid("run coordinator is unavailable")
	}
	if err := active.add(ctx, actionID); err != nil {
		return nil, err
	}
	action, err := s.readTaskAction(ctx, actionID)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&lutrav1.CreateTaskActionResponse{Action: action}), nil
}

func (s Service) readRun(ctx context.Context, id uuid.UUID) (*lutrav1.Run, error) {
	return readRun(ctx, db.New(s.DB), id)
}

func readRun(ctx context.Context, q *db.Queries, id uuid.UUID) (*lutrav1.Run, error) {
	row, err := q.ReadRun(ctx, id)
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
	return &lutrav1.Run{Id: id.String(), RootActionId: rootID, Environment: environmentIdentifier(row.NamespaceID, row.EnvironmentName, row.Version), EntrypointId: uint32(row.EntrypointID), Status: string(row.Status), ResultCbor: row.ResultCbor, CreatedAt: formatTime(row.RunCreatedAt), UpdatedAt: formatTime(row.UpdatedAt)}, nil
}

func environmentIdentifier(namespaceID uuid.UUID, name, version string) *lutrav1.EnvironmentIdentifier {
	return &lutrav1.EnvironmentIdentifier{NamespaceId: namespaceID.String(), Name: name, Version: version}
}

func formatTime(value pgtype.Timestamptz) string { return value.Time.UTC().Format(time.RFC3339Nano) }

func (s Service) readTaskAction(ctx context.Context, id uuid.UUID) (*lutrav1.TaskAction, error) {
	row, err := db.New(s.DB).ReadTaskAction(ctx, id)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("task action not found"))
	}
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return actionFromRow(actionRowFromRead(row))
}

type actionRow struct {
	NamespaceID                      uuid.UUID
	ID                               uuid.UUID
	RunID                            uuid.UUID
	CallerActionID                   *uuid.UUID
	EnvironmentName, Version, Status string
	EnvironmentID                    uuid.UUID
	EntrypointID                     int64
	ActionSpec, ResultCbor           []byte
	Attempts                         int32
	CreatedAt, UpdatedAt             pgtype.Timestamptz
}

func actionFromRow(row actionRow) (*lutrav1.TaskAction, error) {
	var spec lutrav1.ActionSpec
	if err := proto.Unmarshal(row.ActionSpec, &spec); err != nil {
		return nil, err
	}
	spec.ConfigOverridesCbor = nil
	action := &lutrav1.TaskAction{Id: row.ID.String(), RunId: row.RunID.String(), Environment: environmentIdentifier(row.NamespaceID, row.EnvironmentName, row.Version), EntrypointId: uint32(row.EntrypointID), ActionSpec: &spec, ResultCbor: row.ResultCbor, Status: row.Status, Attempts: row.Attempts, CreatedAt: formatTime(row.CreatedAt), UpdatedAt: formatTime(row.UpdatedAt)}
	if row.CallerActionID != nil {
		action.CallerActionId = row.CallerActionID.String()
	}
	return action, nil
}

func actionRowFromRead(row db.ReadTaskActionRow) actionRow {
	return actionRow{ID: row.ID, RunID: row.RunID, CallerActionID: row.CallerActionID, NamespaceID: row.NamespaceID, EnvironmentName: row.EnvironmentName, Version: row.Version, EntrypointID: row.EntrypointID, EnvironmentID: row.EnvironmentID, ActionSpec: row.ActionSpec, ResultCbor: row.ResultCbor, Status: string(row.Status), Attempts: row.Attempts, CreatedAt: row.CreatedAt, UpdatedAt: row.UpdatedAt}
}

func actionRowFromList(row db.ListTaskActionsRow) actionRow {
	return actionRow{ID: row.ID, RunID: row.RunID, CallerActionID: row.CallerActionID, NamespaceID: row.NamespaceID, EnvironmentName: row.EnvironmentName, Version: row.Version, EntrypointID: row.EntrypointID, EnvironmentID: row.EnvironmentID, ActionSpec: row.ActionSpec, ResultCbor: row.ResultCbor, Status: string(row.Status), Attempts: row.Attempts, CreatedAt: row.CreatedAt, UpdatedAt: row.UpdatedAt}
}

func (s Service) GetRun(ctx context.Context, req *connect.Request[lutrav1.GetRunRequest]) (*connect.Response[lutrav1.GetRunResponse], error) {
	id, err := uuid.Parse(req.Msg.GetId())
	if err != nil {
		return nil, invalid("invalid run id")
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
		return nil, invalid("invalid task action id")
	}
	if active, ok := TaskIdentityFromContext(ctx); ok {
		caller, lookupErr := db.New(s.DB).GetTaskActionCaller(ctx, id)
		if lookupErr != nil || caller.RunID != active.RunID || caller.CallerActionID == nil || *caller.CallerActionID != active.ActionID {
			return nil, invalid("task action is not a child of this task")
		}
	}
	if req.Msg.GetWait() {
		active, ok := taskContextFromContext(ctx)
		if !ok {
			return nil, invalid("waiting is only available inside a task")
		}
		if active.coordinator == nil {
			return nil, invalid("run coordinator is unavailable")
		}
		if _, err := active.coordinator.Wait(ctx, active.ActionID, active.Attempt, id); err != nil {
			node, lookupErr := active.coordinator.Get(ctx, id)
			if lookupErr != nil || !node.State.Terminal() {
				return nil, err
			}
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
		return nil, invalid("invalid run id")
	}
	pageSize := req.Msg.GetPageSize()
	if pageSize <= 0 || pageSize > 1000 {
		pageSize = 100
	}
	q := db.New(s.DB)
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
			return nil, invalid("invalid page token")
		}
		parts := strings.Split(string(decoded), "|")
		if len(parts) != 2 {
			return nil, invalid("invalid page token")
		}
		cursorTime, parseErr := time.Parse(time.RFC3339Nano, parts[0])
		cursorID, uuidErr := uuid.Parse(parts[1])
		if parseErr != nil || uuidErr != nil {
			return nil, invalid("invalid page token")
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
	for _, row := range rows {
		action, err := actionFromRow(row)
		if err != nil {
			return nil, err
		}
		response.Actions = append(response.Actions, action)
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
		return nil, invalid("invalid run id")
	}
	tx, err := s.DB.Begin(ctx)
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
	canceled, err := result.EncodeFailure(result.Failure{Message: "run canceled"})
	if err != nil {
		return nil, err
	}
	if actions, err := q.CancelRunIfActive(ctx, db.CancelRunIfActiveParams{RunID: id, ResultCbor: canceled}); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	} else if len(actions) > 0 {
		for _, action := range actions {
			if err := s.Logs.AppendActionStatus(ctx, tx, action, false); err != nil {
				return nil, err
			}
		}
		if err := s.Logs.AppendStatus(ctx, tx, id); err != nil {
			return nil, err
		}
	}
	if _, err := q.ClearRunClaim(ctx, id); err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, "SELECT pg_notify('lutra_run_cancel', $1)", id.String()); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	run, err := s.readRun(ctx, id)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&lutrav1.CancelRunResponse{Run: run}), nil
}

func (s Service) WatchRun(ctx context.Context, req *connect.Request[lutrav1.WatchRunRequest], stream *connect.ServerStream[lutrav1.WatchRunResponse]) error {
	id, err := uuid.Parse(req.Msg.GetId())
	if err != nil {
		return invalid("invalid run id")
	}
	cursor := req.Msg.GetTaskLogs()
	if cursor != nil && (cursor.GetStream() != runlog.TaskLogStream || cursor.GetAfterSeq() < 0 || len(cursor.GetKeyPrefix()) > 1024) {
		return invalid("invalid task log cursor")
	}
	return s.watchRun(ctx, id, cursor, stream.Send)
}

func (s Service) watchRun(ctx context.Context, id uuid.UUID, taskLogs *lutrav1.StreamCursor, send func(*lutrav1.WatchRunResponse) error) error {
	sub, err := s.Logs.Subscribe(ctx, id)
	if err != nil {
		return err
	}
	defer sub.Close()
	tx, err := s.DB.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	q := db.New(tx)
	run, err := readRun(ctx, q, id)
	if err != nil {
		return err
	}
	end, err := q.LogStreamEnd(ctx, db.LogStreamEndParams{RunID: id, Stream: runlog.StatusStream})
	if err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	if err := send(&lutrav1.WatchRunResponse{Run: run}); err != nil {
		return err
	}
	if terminal(run.Status) && taskLogs == nil {
		return nil
	}
	cursors := []runlog.Cursor{}
	if taskLogs != nil {
		cursors = append(cursors, runlog.Cursor{Stream: runlog.TaskLogStream, Prefix: taskLogs.GetKeyPrefix(), InspectSeq: taskLogs.GetAfterSeq()})
	}
	cursors = append(cursors, runlog.Cursor{Stream: runlog.StatusStream, Prefix: []byte("status"), InspectSeq: end})
	if taskLogs != nil {
		cursors[1].Prefix = nil
		cursors[1].InspectSeq = 0
	}
	sendLog := func(record *lutrav1.TailResponse) error {
		if record.Stream == runlog.StatusStream {
			if bytes.Equal(record.Key, []byte("status")) {
				return nil
			}
			event, err := s.Logs.DecodeActionStatus(ctx, record.ValueCbor)
			if err != nil {
				return connect.NewError(connect.CodeDataLoss, err)
			}
			return send(&lutrav1.WatchRunResponse{ActionStatus: event})
		}
		return send(&lutrav1.WatchRunResponse{Log: &lutrav1.LogRecord{Stream: record.Stream, Seq: record.Seq, Key: record.Key, ValueCbor: record.ValueCbor, CreatedUnixNanos: record.CreatedUnixNanos}})
	}
	if !terminal(run.Status) {
		err = sub.Run(ctx, cursors, func(record *lutrav1.TailResponse) error {
			if record.Stream == runlog.TaskLogStream {
				return sendLog(record)
			}
			if !bytes.Equal(record.GetKey(), []byte("status")) {
				return sendLog(record)
			}
			if record.Seq <= end {
				return nil
			}
			event, err := s.Logs.DecodeStatus(ctx, record.GetValueCbor())
			if err != nil {
				return connect.NewError(connect.CodeDataLoss, err)
			}
			next := &lutrav1.Run{
				Id: run.Id, RootActionId: run.RootActionId, Environment: run.Environment, EntrypointId: run.EntrypointId, CreatedAt: run.CreatedAt,
				Status: event.Status, ResultCbor: event.ResultCBOR, UpdatedAt: event.UpdatedAt,
			}
			if err := send(&lutrav1.WatchRunResponse{Run: next}); err != nil {
				return err
			}
			if terminal(event.Status) {
				return runlog.ErrStop
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	if taskLogs == nil {
		return nil
	}
	logEnd, err := db.New(s.DB).LogStreamEnd(ctx, db.LogStreamEndParams{RunID: id, Stream: runlog.TaskLogStream})
	if err != nil {
		return err
	}
	cursors[0].UntilSeq = &logEnd
	statusEnd, err := db.New(s.DB).LogStreamEnd(ctx, db.LogStreamEndParams{RunID: id, Stream: runlog.StatusStream})
	if err != nil {
		return err
	}
	cursors[1].UntilSeq = &statusEnd
	return sub.Run(ctx, cursors, sendLog)
}

func terminal(status string) bool {
	return status == "succeeded" || status == "failed" || status == "canceled"
}
