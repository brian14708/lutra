package lutra

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sort"
	"strings"

	"connectrpc.com/connect"
	lutrav1 "github.com/brian14708/lutra/gen/lutra/v1"
	"github.com/brian14708/lutra/internal/db"
	"github.com/google/uuid"
	restate "github.com/restatedev/sdk-go"
)

const awakeableService = "LutraAwakeableV0"

type awakeableResolution struct {
	ID      string
	Outcome *promiseOutcome
}

func validAwakeableID(id string) error {
	if len(id) <= len("sign_") || len(id) > 256 || !strings.HasPrefix(id, "sign_") {
		return invalid("invalid awakeable ID")
	}
	for _, ch := range id {
		if (ch < 'a' || ch > 'z') && (ch < 'A' || ch > 'Z') && (ch < '0' || ch > '9') && ch != '_' && ch != '-' {
			return invalid("invalid awakeable ID")
		}
	}
	return nil
}

func (w *DurableAdapter) resolveAwakeable(ctx restate.ObjectContext, input awakeableResolution) (restate.Void, error) {
	if err := validAwakeableID(input.ID); err != nil {
		return restate.Void{}, restate.ToTerminalError(err)
	}
	if input.ID != restate.Key(ctx) || input.Outcome == nil {
		return restate.Void{}, restate.TerminalErrorf("invalid awakeable resolution")
	}
	outcome, err := validateOutcome(input.Outcome.Value, input.Outcome.Rejected, input.Outcome.Reason)
	if err != nil {
		return restate.Void{}, restate.ToTerminalError(err)
	}
	previous, err := restate.Get[*promiseOutcome](ctx, "outcome")
	if err != nil {
		return restate.Void{}, err
	}
	if previous != nil {
		if !previous.matches(outcome) {
			return restate.Void{}, restate.ToTerminalError(errors.New("awakeable already has a different outcome"), restate.WithErrorCode(409))
		}
		return restate.Void{}, nil
	}
	restate.ResolveAwakeable(ctx, input.ID, outcome)
	restate.Set(ctx, "outcome", outcome)
	return restate.Void{}, nil
}

func (w *DurableAdapter) inspectWorkflow(ctx restate.WorkflowSharedContext, request *lutrav1.WorkflowInspectRequest) (*lutrav1.WorkflowInspectResponse, error) {
	if err := validateWorkflowInspect(request); err != nil {
		return nil, restate.ToTerminalError(err)
	}
	response := &lutrav1.WorkflowInspectResponse{}
	switch request.Operation {
	case lutrav1.WorkflowInspectRequest_OPERATION_STATE_GET:
		value, err := restate.Get[[]byte](ctx, "state:"+request.Key, restate.WithBinary)
		if err != nil {
			return nil, err
		}
		response.Found, response.ValueCbor = value != nil, value
	case lutrav1.WorkflowInspectRequest_OPERATION_STATE_KEYS:
		keys, err := restate.Keys(ctx)
		if err != nil {
			return nil, err
		}
		for _, key := range keys {
			if strings.HasPrefix(key, "state:") {
				response.Keys = append(response.Keys, strings.TrimPrefix(key, "state:"))
			}
		}
		sort.Strings(response.Keys)
	case lutrav1.WorkflowInspectRequest_OPERATION_PROMISE_PEEK:
		value, err := restate.Promise[*promiseOutcome](ctx, request.Key).Peek()
		if err != nil {
			return nil, err
		}
		if value != nil {
			response.Completed, response.ValueCbor, response.Rejected, response.Reason = true, value.Value, value.Rejected, value.Reason
		}
	case lutrav1.WorkflowInspectRequest_OPERATION_PROMISE_RESOLVE, lutrav1.WorkflowInspectRequest_OPERATION_PROMISE_REJECT:
		outcome, err := validateOutcome(request.ValueCbor, request.Operation == lutrav1.WorkflowInspectRequest_OPERATION_PROMISE_REJECT, request.Reason)
		if err != nil {
			return nil, restate.ToTerminalError(err)
		}
		if err := resolvePromise(ctx, request.Key, outcome); err != nil {
			if connect.CodeOf(err) == connect.CodeAlreadyExists {
				return nil, restate.ToTerminalError(err, restate.WithErrorCode(http.StatusConflict))
			}
			return nil, restate.ToTerminalError(err)
		}
		response.Completed, response.ValueCbor, response.Rejected, response.Reason = true, outcome.Value, outcome.Rejected, outcome.Reason
	default:
		return nil, restate.TerminalErrorf("invalid workflow inspection operation")
	}
	return response, nil
}

func (s Service) WorkflowInspect(ctx context.Context, req *connect.Request[lutrav1.WorkflowInspectRequest]) (*connect.Response[lutrav1.WorkflowInspectResponse], error) {
	id, err := uuid.Parse(req.Msg.RunId)
	if err != nil {
		return nil, invalid("invalid run ID")
	}
	if err := validateWorkflowInspect(req.Msg); err != nil {
		return nil, err
	}
	run, err := s.readRun(ctx, id)
	if err != nil {
		return nil, err
	}
	_, entry, err := lookupTask(ctx, db.New(s.DB), run.Environment, run.EntrypointId)
	if err != nil {
		return nil, err
	}
	if !entry.GetWorkflow() {
		return nil, invalid("inspection requires a workflow run")
	}
	if s.Durable == nil {
		return nil, errors.New("execution backend unavailable")
	}
	data, err := s.Durable.Dispatch.request(ctx, http.MethodPost, s.Durable.Dispatch.Ingress+"/"+actionService+"/"+run.RootActionId+"/Inspect", req.Msg, "")
	if err != nil {
		return nil, ingressBoundaryError(err)
	}
	var response lutrav1.WorkflowInspectResponse
	if err := json.Unmarshal(data, &response); err != nil {
		return nil, err
	}
	return connect.NewResponse(&response), nil
}

func validateWorkflowInspect(request *lutrav1.WorkflowInspectRequest) error {
	if request.Operation < lutrav1.WorkflowInspectRequest_OPERATION_STATE_GET || request.Operation > lutrav1.WorkflowInspectRequest_OPERATION_PROMISE_REJECT {
		return invalid("invalid workflow inspection operation")
	}
	if request.Operation == lutrav1.WorkflowInspectRequest_OPERATION_STATE_KEYS {
		if request.Key != "" {
			return invalid("state keys does not accept a key")
		}
	} else if err := validateIdempotency(request.Key, true); err != nil {
		return err
	}
	if request.Operation == lutrav1.WorkflowInspectRequest_OPERATION_PROMISE_RESOLVE || request.Operation == lutrav1.WorkflowInspectRequest_OPERATION_PROMISE_REJECT {
		_, err := validateOutcome(request.ValueCbor, request.Operation == lutrav1.WorkflowInspectRequest_OPERATION_PROMISE_REJECT, request.Reason)
		return err
	}
	if len(request.ValueCbor) != 0 || request.Reason != "" {
		return invalid("workflow inspection reads do not accept an outcome")
	}
	return nil
}

func (s Service) ResolveAwakeable(ctx context.Context, req *connect.Request[lutrav1.ResolveAwakeableRequest]) (*connect.Response[lutrav1.ResolveAwakeableResponse], error) {
	if err := validAwakeableID(req.Msg.Id); err != nil {
		return nil, err
	}
	outcome, err := validateOutcome(req.Msg.ValueCbor, req.Msg.Reject, req.Msg.Reason)
	if err != nil {
		return nil, err
	}
	if s.Durable == nil {
		return nil, errors.New("execution backend unavailable")
	}
	_, err = s.Durable.Dispatch.request(ctx, http.MethodPost, s.Durable.Dispatch.Ingress+"/"+awakeableService+"/"+req.Msg.Id+"/Resolve", awakeableResolution{req.Msg.Id, outcome}, "")
	if err != nil {
		return nil, ingressBoundaryError(err)
	}
	return connect.NewResponse(&lutrav1.ResolveAwakeableResponse{}), nil
}

func ingressBoundaryError(err error) error {
	var failure *restateHTTPError
	if errors.As(err, &failure) {
		if failure.Status == 409 {
			return connect.NewError(connect.CodeAlreadyExists, errors.New(failure.Body))
		}
		if failure.Status >= 400 && failure.Status < 500 {
			return connect.NewError(connect.CodeFailedPrecondition, errors.New(failure.Body))
		}
	}
	return err
}

func workflowOnly() error {
	return connect.NewError(connect.CodePermissionDenied, errors.New("operation is only available inside a workflow"))
}

func (s Service) WorkflowTimer(context.Context, *connect.Request[lutrav1.WorkflowTimerRequest]) (*connect.Response[lutrav1.WorkflowTimerResponse], error) {
	return nil, workflowOnly()
}

func (s Service) WorkflowWait(context.Context, *connect.Request[lutrav1.WorkflowWaitRequest]) (*connect.Response[lutrav1.WorkflowWaitResponse], error) {
	return nil, workflowOnly()
}

func (s Service) WorkflowState(context.Context, *connect.Request[lutrav1.WorkflowStateRequest]) (*connect.Response[lutrav1.WorkflowStateResponse], error) {
	return nil, workflowOnly()
}

func (s Service) WorkflowPromise(context.Context, *connect.Request[lutrav1.WorkflowPromiseRequest]) (*connect.Response[lutrav1.WorkflowPromiseResponse], error) {
	return nil, workflowOnly()
}

func (s Service) WorkflowAwakeable(context.Context, *connect.Request[lutrav1.WorkflowAwakeableRequest]) (*connect.Response[lutrav1.WorkflowAwakeableResponse], error) {
	return nil, workflowOnly()
}

func (s Service) WorkflowEntropy(context.Context, *connect.Request[lutrav1.WorkflowEntropyRequest]) (*connect.Response[lutrav1.WorkflowEntropyResponse], error) {
	return nil, workflowOnly()
}

func (s Service) WorkflowCancel(context.Context, *connect.Request[lutrav1.WorkflowCancelRequest]) (*connect.Response[lutrav1.WorkflowCancelResponse], error) {
	return nil, workflowOnly()
}
