package lutra

import (
	"context"
	"errors"
	"os"
	"time"
	"uuid"

	"connectrpc.com/connect"
	lutrav1 "github.com/brian14708/lutra/gen/lutra/v1"
	lutrav1connect "github.com/brian14708/lutra/gen/lutra/v1/lutrav1connect"
	"github.com/brian14708/lutra/internal/auth"
	"github.com/brian14708/lutra/internal/db"
	"github.com/brian14708/lutra/internal/rpcutil"
	"github.com/golang-jwt/jwt/v5"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const attemptTokenAudience = "lutra-worker"

type attemptClaims struct {
	ProjectID      string `json:"project_id"`
	RunID          string `json:"run_id"`
	ParentActionID string `json:"parent_action_id"`
	Attempt        int32  `json:"attempt"`
	FencingToken   string `json:"fencing_token"`
	jwt.RegisteredClaims
}

type attemptCapability struct {
	ProjectID, RunID, ParentActionID string
	Attempt                          int32
	FencingToken                     string
	Expires                          time.Time
}

// matches reports whether the capability authorizes exactly this attempt scope.
func (c attemptCapability) matches(projectID, runID, parentActionID string) bool {
	return c.ProjectID == projectID && c.RunID == runID && c.ParentActionID == parentActionID
}

// leaseActive reports whether an attempt lease is open and unexpired.
func leaseActive(attempt db.LutraActionAttempt) bool {
	return attempt.State == db.LutraAttemptStateOpen && attempt.LeaseExpiresAt.Valid && time.Now().Before(attempt.LeaseExpiresAt.Time)
}

// WorkerService implements worker capability and child-action operations.
type WorkerService struct {
	lutrav1connect.UnimplementedWorkerServiceHandler
	store  *auth.Store
	engine workerServiceRuntime
	jwtKey []byte
}

// NewWorkerService creates a worker service backed by store and engine.
func NewWorkerService(store *auth.Store, engine workerServiceRuntime) *WorkerService {
	return &WorkerService{store: store, engine: engine, jwtKey: []byte(os.Getenv("LUTRA_JWT_KEY"))}
}

// IssueAttemptToken creates the capability handed to the action subprocess.
// It is called only after the worker has durably opened the corresponding
// action attempt.
func (s *WorkerService) IssueAttemptToken(projectID, runID, parentActionID string, attempt int32, fencingToken string) (string, time.Time) {
	if len(s.jwtKey) == 0 || attempt < 1 || fencingToken == "" {
		return "", time.Time{}
	}
	expires := time.Now().Add(5 * time.Minute)
	issued := time.Now()
	claims := attemptClaims{
		ProjectID:      projectID,
		RunID:          runID,
		ParentActionID: parentActionID,
		Attempt:        attempt,
		FencingToken:   fencingToken,
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    "lutra",
			Subject:   parentActionID,
			Audience:  jwt.ClaimStrings{attemptTokenAudience},
			ExpiresAt: jwt.NewNumericDate(expires),
			IssuedAt:  jwt.NewNumericDate(issued),
			NotBefore: jwt.NewNumericDate(issued.Add(-time.Second)),
			ID:        uuid.New().String(),
		},
	}
	token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(s.jwtKey)
	if err != nil {
		return "", time.Time{}
	}
	return token, expires
}

// RefreshAttemptToken renews a worker's attempt-scoped capability token.
func (s *WorkerService) RefreshAttemptToken(ctx context.Context, req *connect.Request[lutrav1.RefreshAttemptTokenRequest]) (*connect.Response[lutrav1.RefreshAttemptTokenResponse], error) {
	if req.Msg.GetProjectId() == "" || req.Msg.GetRunId() == "" || req.Msg.GetParentActionId() == "" || req.Msg.GetFencingToken() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("attempt scope is required"))
	}
	capability, err := s.capability(req)
	if err != nil {
		return nil, err
	}
	if !capability.matches(req.Msg.GetProjectId(), req.Msg.GetRunId(), req.Msg.GetParentActionId()) || capability.Attempt != int32(req.Msg.GetAttempt()) || capability.FencingToken != req.Msg.GetFencingToken() {
		return nil, connect.NewError(connect.CodePermissionDenied, errors.New("attempt scope does not match request"))
	}
	parent, err := s.authorizeAttempt(ctx, capability)
	if err != nil {
		return nil, err
	}
	attempt, err := s.store.Queries().RenewActionAttempt(ctx, db.RenewActionAttemptParams{ActionID: parent.ActionID, Attempt: int32(req.Msg.GetAttempt()), FencingToken: rpcutil.UUID(capability.FencingToken)})
	if err != nil {
		return nil, rpcutil.DB(err)
	}
	if !leaseActive(attempt) {
		return nil, connect.NewError(connect.CodePermissionDenied, errors.New("attempt lease is no longer active"))
	}
	token, expires := s.IssueAttemptToken(req.Msg.GetProjectId(), req.Msg.GetRunId(), req.Msg.GetParentActionId(), int32(req.Msg.GetAttempt()), capability.FencingToken)
	if token == "" {
		return nil, rpcutil.Internal(errors.New("generate attempt token"))
	}
	return connect.NewResponse(&lutrav1.RefreshAttemptTokenResponse{Token: token, ExpiresAt: timestamppb.New(expires)}), nil
}

func (s *WorkerService) capability(req connect.AnyRequest) (attemptCapability, error) {
	value := req.Header().Get("Authorization")
	if len(value) < 8 || value[:7] != "Bearer " {
		return attemptCapability{}, connect.NewError(connect.CodeUnauthenticated, errors.New("attempt token required"))
	}
	if len(s.jwtKey) == 0 {
		return attemptCapability{}, connect.NewError(connect.CodeUnavailable, errors.New("worker JWT signing key is not configured"))
	}
	claims := &attemptClaims{}
	parsed, err := jwt.ParseWithClaims(value[7:], claims, func(token *jwt.Token) (any, error) {
		if token.Method != jwt.SigningMethodHS256 {
			return nil, errors.New("unexpected attempt token signing method")
		}
		return s.jwtKey, nil
	}, jwt.WithAudience(attemptTokenAudience), jwt.WithIssuer("lutra"), jwt.WithValidMethods([]string{jwt.SigningMethodHS256.Alg()}))
	if err != nil || !parsed.Valid {
		return attemptCapability{}, connect.NewError(connect.CodeUnauthenticated, errors.New("invalid attempt token"))
	}
	if claims.ProjectID == "" || claims.RunID == "" || claims.ParentActionID == "" || claims.Attempt < 1 || claims.FencingToken == "" || claims.ExpiresAt == nil {
		return attemptCapability{}, connect.NewError(connect.CodeUnauthenticated, errors.New("invalid attempt token claims"))
	}
	for _, value := range []string{claims.ProjectID, claims.RunID, claims.ParentActionID, claims.FencingToken} {
		if _, err := uuid.Parse(value); err != nil {
			return attemptCapability{}, connect.NewError(connect.CodeUnauthenticated, errors.New("invalid attempt token claims"))
		}
	}
	return attemptCapability{ProjectID: claims.ProjectID, RunID: claims.RunID, ParentActionID: claims.ParentActionID, Attempt: claims.Attempt, FencingToken: claims.FencingToken, Expires: claims.ExpiresAt.Time}, nil
}

// authorizeAttempt rechecks the durable parent action and lease for every
// worker capability operation. JWT expiry alone is insufficient because a
// newer retry may have replaced the lease while an old subprocess is alive.
func (s *WorkerService) authorizeAttempt(ctx context.Context, capability attemptCapability) (db.LutraAction, error) {
	parent, err := s.store.Queries().GetAction(ctx, db.GetActionParams{ActionID: rpcutil.UUID(capability.ParentActionID), ProjectID: rpcutil.UUID(capability.ProjectID), RunID: rpcutil.UUID(capability.RunID)})
	if err != nil {
		return db.LutraAction{}, rpcutil.DB(err)
	}
	if parent.State != db.LutraActionStateRunning || parent.AttemptCount != capability.Attempt {
		return db.LutraAction{}, connect.NewError(connect.CodePermissionDenied, errors.New("attempt is no longer current"))
	}
	attempt, err := s.store.Queries().GetActionAttempt(ctx, db.GetActionAttemptParams{ActionID: parent.ActionID, Attempt: capability.Attempt})
	if err != nil {
		return db.LutraAction{}, rpcutil.DB(err)
	}
	if attempt.FencingToken != rpcutil.UUID(capability.FencingToken) || !leaseActive(attempt) {
		return db.LutraAction{}, connect.NewError(connect.CodePermissionDenied, errors.New("attempt lease is no longer active"))
	}
	return parent, nil
}

// CreateChildAction creates an idempotent child action for a running attempt.
func (s *WorkerService) CreateChildAction(ctx context.Context, req *connect.Request[lutrav1.CreateChildActionRequest]) (*connect.Response[lutrav1.CreateChildActionResponse], error) {
	capability, err := s.capability(req)
	if err != nil {
		return nil, err
	}
	if !capability.matches(req.Msg.GetProjectId(), req.Msg.GetRunId(), req.Msg.GetParentActionId()) {
		return nil, connect.NewError(connect.CodePermissionDenied, errors.New("attempt scope does not match parent"))
	}
	if req.Msg.GetOperationId() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("operation ID is required"))
	}
	parent, err := s.authorizeAttempt(ctx, capability)
	if err != nil {
		return nil, err
	}
	taskID := rpcutil.UUID(req.Msg.GetTaskId())
	if req.Msg.GetTaskId() == "" {
		if req.Msg.GetTaskName() == "" || req.Msg.GetTaskVersion() == "" {
			return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("task ID or task identity is required"))
		}
		task, lookupErr := s.store.Queries().GetTaskByIdentity(ctx, db.GetTaskByIdentityParams{ProjectID: rpcutil.UUID(capability.ProjectID), Name: req.Msg.GetTaskName(), Version: req.Msg.GetTaskVersion()})
		if lookupErr != nil {
			return nil, rpcutil.DB(lookupErr)
		}
		taskID = task.TaskID
	} else if _, err := s.store.Queries().GetTask(ctx, db.GetTaskParams{TaskID: taskID, ProjectID: rpcutil.UUID(capability.ProjectID)}); err != nil {
		return nil, rpcutil.DB(err)
	}
	inputs, err := marshalBindings(req.Msg.GetInputs())
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	if int64(len(inputs)) > configuredLimit("LUTRA_MAX_INPUT_BYTES", 16<<20) {
		return nil, connect.NewError(connect.CodeResourceExhausted, errors.New("child inputs exceed the configured size limit"))
	}
	// A retry of a parent attempt must resolve the same logical child before
	// applying child limits or inserting another queue job.
	parentActionID := parent.ActionID
	parentActionIDPtr := &parentActionID
	operationID := pgtype.Text{String: req.Msg.GetOperationId(), Valid: true}
	if existing, lookupErr := s.store.Queries().GetActionByOperation(ctx, db.GetActionByOperationParams{ProjectID: rpcutil.UUID(capability.ProjectID), RunID: rpcutil.UUID(capability.RunID), ParentActionID: parentActionIDPtr, OperationID: operationID}); lookupErr == nil {
		if existing.TaskID != taskID || !equalJSON(existing.Inputs, inputs) {
			return nil, connect.NewError(connect.CodeAlreadyExists, errors.New("operation ID was used for a different child action"))
		}
		message, loadErr := loadActionProto(ctx, s.store.Queries(), existing)
		if loadErr != nil {
			return nil, rpcutil.Internal(loadErr)
		}
		return connect.NewResponse(&lutrav1.CreateChildActionResponse{Action: message}), nil
	} else if !errors.Is(lookupErr, pgx.ErrNoRows) {
		return nil, rpcutil.DB(lookupErr)
	}
	childCount, err := s.store.Queries().CountChildActions(ctx, parentActionIDPtr)
	if err != nil {
		return nil, rpcutil.Internal(err)
	}
	if childCount >= configuredLimit("LUTRA_MAX_CHILDREN", 1000) {
		return nil, connect.NewError(connect.CodeResourceExhausted, errors.New("parent action has reached the child action limit"))
	}
	tx, err := s.store.Begin(ctx)
	if err != nil {
		return nil, rpcutil.Internal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	q := s.store.Queries().WithTx(tx)
	action, err := q.CreateAction(ctx, db.CreateActionParams{ActionID: uuid.New(), ProjectID: rpcutil.UUID(capability.ProjectID), RunID: rpcutil.UUID(capability.RunID), ParentActionID: parentActionIDPtr, OperationID: pgtype.Text{String: req.Msg.GetOperationId(), Valid: true}, TaskID: taskID, Inputs: inputs})
	if err != nil {
		if !errors.Is(err, pgx.ErrNoRows) {
			return nil, rpcutil.Internal(err)
		}
		// Another retry won the unique operation key between the lookup and
		// insert. Resolve it exactly as the fast path above.
		action, lookupErr := q.GetActionByOperation(ctx, db.GetActionByOperationParams{ProjectID: rpcutil.UUID(capability.ProjectID), RunID: rpcutil.UUID(capability.RunID), ParentActionID: parentActionIDPtr, OperationID: operationID})
		if lookupErr != nil {
			return nil, rpcutil.DB(lookupErr)
		}
		if action.TaskID != taskID || !equalJSON(action.Inputs, inputs) {
			return nil, connect.NewError(connect.CodeAlreadyExists, errors.New("operation ID was used for a different child action"))
		}
		message, loadErr := loadActionProto(ctx, s.store.Queries(), action)
		if loadErr != nil {
			return nil, rpcutil.Internal(loadErr)
		}
		return connect.NewResponse(&lutrav1.CreateChildActionResponse{Action: message}), nil
	}
	if err := s.engine.EnqueueTx(ctx, tx, capability.ProjectID, capability.RunID, action.ActionID.String(), childQueueName); err != nil {
		return nil, rpcutil.Internal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, rpcutil.Internal(err)
	}
	message, err := loadActionProto(ctx, s.store.Queries(), action)
	if err != nil {
		return nil, rpcutil.Internal(err)
	}
	return connect.NewResponse(&lutrav1.CreateChildActionResponse{Action: message}), nil
}

// GetActionOutputs returns the outputs of an accessible child action.
func (s *WorkerService) GetActionOutputs(ctx context.Context, req *connect.Request[lutrav1.GetActionOutputsRequest]) (*connect.Response[lutrav1.GetActionOutputsResponse], error) {
	capability, err := s.capability(req)
	if err != nil {
		return nil, err
	}
	if !capability.matches(req.Msg.GetProjectId(), req.Msg.GetRunId(), req.Msg.GetParentActionId()) {
		return nil, connect.NewError(connect.CodePermissionDenied, errors.New("attempt scope does not match parent"))
	}
	if _, err := s.authorizeAttempt(ctx, capability); err != nil {
		return nil, err
	}
	action, err := s.store.Queries().GetAction(ctx, db.GetActionParams{ActionID: rpcutil.UUID(req.Msg.GetActionId()), ProjectID: rpcutil.UUID(capability.ProjectID), RunID: rpcutil.UUID(capability.RunID)})
	if err != nil {
		return nil, rpcutil.DB(err)
	}
	if action.ParentActionID == nil || action.ParentActionID.String() != capability.ParentActionID {
		return nil, connect.NewError(connect.CodePermissionDenied, errors.New("action is not a child of this attempt"))
	}
	message, err := loadActionProto(ctx, s.store.Queries(), action)
	if err != nil {
		return nil, rpcutil.Internal(err)
	}
	if action.State != db.LutraActionStateSucceeded {
		return connect.NewResponse(&lutrav1.GetActionOutputsResponse{Action: message, Outputs: nil}), nil
	}
	return connect.NewResponse(&lutrav1.GetActionOutputsResponse{Action: message, Outputs: unmarshalBindings(action.Outputs)}), nil
}

// DownloadActionArtifact downloads an artifact produced by an accessible child action.
func (s *WorkerService) DownloadActionArtifact(ctx context.Context, req *connect.Request[lutrav1.DownloadActionArtifactRequest]) (*connect.Response[lutrav1.DownloadActionArtifactResponse], error) {
	capability, err := s.capability(req)
	if err != nil {
		return nil, err
	}
	if !capability.matches(req.Msg.GetProjectId(), req.Msg.GetRunId(), req.Msg.GetParentActionId()) {
		return nil, connect.NewError(connect.CodePermissionDenied, errors.New("attempt scope does not match parent"))
	}
	if _, err := s.authorizeAttempt(ctx, capability); err != nil {
		return nil, err
	}
	action, err := s.store.Queries().GetAction(ctx, db.GetActionParams{ActionID: rpcutil.UUID(req.Msg.GetActionId()), ProjectID: rpcutil.UUID(capability.ProjectID), RunID: rpcutil.UUID(capability.RunID)})
	if err != nil {
		return nil, rpcutil.DB(err)
	}
	if action.ParentActionID == nil || action.ParentActionID.String() != capability.ParentActionID || action.State != db.LutraActionStateSucceeded {
		return nil, connect.NewError(connect.CodePermissionDenied, errors.New("artifact is not an output of an accessible child action"))
	}
	allowed := false
	for _, binding := range unmarshalBindings(action.Outputs) {
		if binding.GetArtifactId() == req.Msg.GetArtifactId() {
			allowed = true
			break
		}
	}
	if !allowed {
		return nil, connect.NewError(connect.CodePermissionDenied, errors.New("artifact is not an output of the child action"))
	}
	data, err := s.engine.readArtifact(ctx, capability.ProjectID, req.Msg.GetArtifactId())
	if err != nil {
		return nil, rpcutil.DB(err)
	}
	return connect.NewResponse(&lutrav1.DownloadActionArtifactResponse{Data: data}), nil
}
