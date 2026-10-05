package lutra

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"connectrpc.com/connect"
	lutrav1 "github.com/brian14708/lutra/gen/lutra/v1"
	lutrav1connect "github.com/brian14708/lutra/gen/lutra/v1/lutrav1connect"
	"github.com/brian14708/lutra/internal/db"
	"github.com/brian14708/lutra/internal/multihash"
	"github.com/fxamacker/cbor/v2"
	"github.com/google/uuid"
	restate "github.com/restatedev/sdk-go"
	"google.golang.org/protobuf/proto"
)

type workflowResponse struct {
	message proto.Message
	err     error
}

type workflowCallback struct {
	request proto.Message
	done    chan workflowResponse
}

type workflowQueue struct {
	lutrav1connect.UnimplementedLutraServiceHandler
	ctx   context.Context
	inbox *workflowInbox
}

func invokeWorkflow[Response any](ctx context.Context, q *workflowQueue, request proto.Message) (*connect.Response[Response], error) {
	call := workflowCallback{request, make(chan workflowResponse, 1)}
	if err := q.inbox.submit(ctx, call); err != nil {
		return nil, err
	}
	select {
	case response := <-call.done:
		if response.err != nil {
			return nil, response.err
		}
		message, ok := any(response.message).(*Response)
		if !ok {
			return nil, errors.New("invalid workflow response type")
		}
		return connect.NewResponse(message), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-q.ctx.Done():
		return nil, q.ctx.Err()
	}
}

func (q *workflowQueue) CreateTaskAction(ctx context.Context, req *connect.Request[lutrav1.CreateTaskActionRequest]) (*connect.Response[lutrav1.CreateTaskActionResponse], error) {
	return invokeWorkflow[lutrav1.CreateTaskActionResponse](ctx, q, req.Msg)
}

func (q *workflowQueue) WorkflowTimer(ctx context.Context, req *connect.Request[lutrav1.WorkflowTimerRequest]) (*connect.Response[lutrav1.WorkflowTimerResponse], error) {
	return invokeWorkflow[lutrav1.WorkflowTimerResponse](ctx, q, req.Msg)
}

func (q *workflowQueue) WorkflowWait(ctx context.Context, req *connect.Request[lutrav1.WorkflowWaitRequest]) (*connect.Response[lutrav1.WorkflowWaitResponse], error) {
	return invokeWorkflow[lutrav1.WorkflowWaitResponse](ctx, q, req.Msg)
}

func (q *workflowQueue) WorkflowState(ctx context.Context, req *connect.Request[lutrav1.WorkflowStateRequest]) (*connect.Response[lutrav1.WorkflowStateResponse], error) {
	return invokeWorkflow[lutrav1.WorkflowStateResponse](ctx, q, req.Msg)
}

func (q *workflowQueue) WorkflowPromise(ctx context.Context, req *connect.Request[lutrav1.WorkflowPromiseRequest]) (*connect.Response[lutrav1.WorkflowPromiseResponse], error) {
	return invokeWorkflow[lutrav1.WorkflowPromiseResponse](ctx, q, req.Msg)
}

func (q *workflowQueue) WorkflowAwakeable(ctx context.Context, req *connect.Request[lutrav1.WorkflowAwakeableRequest]) (*connect.Response[lutrav1.WorkflowAwakeableResponse], error) {
	return invokeWorkflow[lutrav1.WorkflowAwakeableResponse](ctx, q, req.Msg)
}

func (q *workflowQueue) WorkflowEntropy(ctx context.Context, req *connect.Request[lutrav1.WorkflowEntropyRequest]) (*connect.Response[lutrav1.WorkflowEntropyResponse], error) {
	return invokeWorkflow[lutrav1.WorkflowEntropyResponse](ctx, q, req.Msg)
}

func (q *workflowQueue) WorkflowCancel(ctx context.Context, req *connect.Request[lutrav1.WorkflowCancelRequest]) (*connect.Response[lutrav1.WorkflowCancelResponse], error) {
	return invokeWorkflow[lutrav1.WorkflowCancelResponse](ctx, q, req.Msg)
}

type sandboxResult struct {
	output []byte
	err    error
}

type WorkflowRunner struct {
	Adapter *DurableAdapter
	Tasks   *SandboxRuntime
}

func (w *WorkflowRunner) workflow(ctx restate.WorkflowContext, image *Image, task *EnvironmentExecution) ([]byte, error) {
	var err error // restate results are TerminalError; keep err a plain error
	attempt, err := restate.Run(ctx, func(ctx restate.RunContext) (int32, error) {
		return w.Adapter.begin(ctx, uuid.MustParse(task.ActionID))
	}, restate.WithName("Begin workflow attempt"))
	if err != nil {
		return nil, err
	}
	task.Attempt = attempt
	sandboxCtx, cancel := w.Adapter.actionContext(ctx, uuid.MustParse(task.ActionID))
	defer cancel()
	release, err := w.Tasks.acquire(sandboxCtx, LogTask)
	if err != nil {
		return nil, err
	}
	defer func() {
		if release != nil {
			release()
		}
	}()
	queue := &workflowQueue{ctx: sandboxCtx, inbox: newWorkflowInbox(sandboxCtx, w.Adapter.Dispatch)}
	identity := TaskIdentity{RunID: uuid.MustParse(task.RunID), ActionID: uuid.MustParse(task.ActionID), Attempt: attempt}
	_, handler := lutrav1connect.NewLutraServiceHandler(queue, connect.WithReadMaxBytes(32<<20))
	blobHandler := w.Tasks.newTaskAPIHandler(identity)
	// Blob URLs are transient capabilities and must be refreshed on replay.
	sandboxHandler := http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if strings.HasPrefix(request.URL.Path, "/lutra.v1.BlobService/") {
			blobHandler.ServeHTTP(response, request)
			return
		}
		handler.ServeHTTP(response, request)
	})
	finished := make(chan sandboxResult, 1)
	go func() {
		output, err := w.Tasks.executeEnvironment(sandboxCtx, image, identity, task, sandboxHandler)
		finished <- sandboxResult{output, err}
		queue.inbox.finish()
	}()
	received := false
	defer func() {
		cancel()
		if !received {
			<-finished
		}
	}()
	return w.workflowLoop(ctx, sandboxCtx, identity, queue.inbox, finished, &received, &release)
}

// Journal domain results, leaving Connect encoding and HTTP status to the boundary.
type commandResult struct {
	Fingerprint  [32]byte
	Action       *lutrav1.TaskAction
	InvocationID string
	Failure      *commandFailure
}

type commandFailure struct {
	Code    connect.Code
	Message string
}

func commandFingerprint(request proto.Message) ([32]byte, error) {
	encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(request)
	if err != nil {
		return [32]byte{}, err
	}
	name := string(request.ProtoReflect().Descriptor().FullName())
	return multihash.Sum([]byte(name), encoded), nil
}

func journalCommand(ctx restate.WorkflowContext, request proto.Message, execute func(restate.RunContext) (commandResult, error)) (commandResult, error) {
	fingerprint, err := commandFingerprint(request)
	if err != nil {
		return commandResult{}, restate.ToTerminalError(err)
	}
	recorded, err := restate.Run(ctx, func(ctx restate.RunContext) (commandResult, error) {
		result, err := execute(ctx)
		if err != nil {
			var rpcError *connect.Error
			if !errors.As(err, &rpcError) {
				return commandResult{}, err
			}
			switch rpcError.Code() {
			case connect.CodeInternal, connect.CodeUnavailable, connect.CodeUnknown, connect.CodeDeadlineExceeded, connect.CodeCanceled, connect.CodeResourceExhausted:
				return commandResult{}, err
			}
			result.Failure = &commandFailure{rpcError.Code(), rpcError.Message()}
		}
		result.Fingerprint = fingerprint
		return result, nil
	}, restate.WithName(workflowCommandName(request)))
	if err != nil {
		return commandResult{}, err
	}
	if recorded.Fingerprint != fingerprint {
		return commandResult{}, restate.TerminalErrorf("workflow command changed during replay at %s", request.ProtoReflect().Descriptor().FullName())
	}
	return recorded, nil
}

func workflowCommandName(request proto.Message) string {
	name := strings.TrimSuffix(strings.TrimPrefix(string(request.ProtoReflect().Descriptor().Name()), "Workflow"), "Request")
	switch request := request.(type) {
	case *lutrav1.CreateTaskActionRequest:
		name = "Submit child action: " + request.IdempotencyKey
		parts := []string{}
		if len(request.Metadata) > 0 {
			keys := make([]string, 0, len(request.Metadata))
			for key := range request.Metadata {
				keys = append(keys, key)
			}
			sort.Strings(keys)
			metadata := make([]string, 0, len(keys))
			for _, key := range keys {
				metadata = append(metadata, key+"="+request.Metadata[key])
			}
			parts = append(parts, metadata...)
		}
		if len(parts) > 0 {
			name += " (" + strings.Join(parts, ", ") + ")"
		}
		return name
	case *lutrav1.WorkflowStateRequest:
		name = "State " + strings.ToLower(strings.TrimPrefix(request.Operation.String(), "OPERATION_"))
	case *lutrav1.WorkflowPromiseRequest:
		name = "Promise " + strings.ToLower(strings.TrimPrefix(request.Operation.String(), "OPERATION_"))
	case *lutrav1.WorkflowAwakeableRequest:
		name = "Awakeable " + strings.ToLower(strings.TrimPrefix(request.Operation.String(), "OPERATION_"))
	case *lutrav1.WorkflowEntropyRequest:
		name = "Entropy " + strings.ToLower(strings.TrimPrefix(request.Kind.String(), "KIND_"))
	}
	sequence := request.ProtoReflect().Get(request.ProtoReflect().Descriptor().Fields().ByName("sequence")).Uint()
	return fmt.Sprintf("Command %d: %s", sequence, name)
}

func (w *WorkflowRunner) workflowCommand(ctx restate.WorkflowContext, identity TaskIdentity, children map[string]restate.AttachFuture[[]byte], request *lutrav1.CreateTaskActionRequest) (workflowResponse, error) {
	recorded, err := journalCommand(ctx, request, func(ctx restate.RunContext) (commandResult, error) {
		delay := time.Duration(0)
		if request.DelayMillis != nil {
			var err error
			delay, err = workflowDuration(*request.DelayMillis)
			if err != nil {
				return commandResult{}, err
			}
		}
		service := Service{DB: w.Adapter.DB, Logs: w.Adapter.Logs, Durable: w.Adapter}
		response, err := service.CreateTaskAction(withTaskIdentity(ctx, identity), connect.NewRequest(request))
		if err != nil {
			return commandResult{}, err
		}
		action := response.Msg.Action
		id, err := uuid.Parse(action.Id)
		if err != nil {
			return commandResult{}, err
		}
		invocationID, err := w.Adapter.Dispatch.submitDelayed(ctx, id, delay)
		return commandResult{Action: action, InvocationID: invocationID}, err
	})
	if err != nil {
		return workflowResponse{}, err
	}
	if recorded.Failure != nil {
		return workflowResponse{err: connect.NewError(recorded.Failure.Code, errors.New(recorded.Failure.Message))}, nil
	}
	id := recorded.Action.Id
	if _, exists := children[id]; !exists {
		children[id] = restate.AttachInvocation[[]byte](ctx, recorded.InvocationID, restate.WithBinary)
	}
	return workflowResponse{message: &lutrav1.CreateTaskActionResponse{Action: recorded.Action}}, nil
}

func workflowDuration(millis int64) (time.Duration, error) {
	if millis < 0 || millis > int64((1<<63-1)/time.Millisecond) {
		return 0, invalid("invalid workflow duration")
	}
	return time.Duration(millis) * time.Millisecond, nil
}

func (s Service) SignalRun(ctx context.Context, req *connect.Request[lutrav1.SignalRunRequest]) (*connect.Response[lutrav1.SignalRunResponse], error) {
	id, err := uuid.Parse(req.Msg.Id)
	if err != nil {
		return nil, invalid("invalid run id")
	}
	if caller, ok := TaskIdentityFromContext(ctx); ok && id != caller.RunID {
		return nil, connect.NewError(connect.CodePermissionDenied, errors.New("tasks may only signal their own run"))
	}
	if err := validateIdempotency(req.Msg.Name, true); err != nil {
		return nil, err
	}
	if err := validateIdempotency(req.Msg.IdempotencyKey, false); err != nil {
		return nil, err
	}
	if err := validateSignal(req.Msg.ValueCbor); err != nil {
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
		return nil, invalid("signals require a workflow run")
	}
	if s.Durable == nil {
		return nil, errors.New("execution backend unavailable")
	}
	key := req.Msg.IdempotencyKey
	if key == "" {
		key = fmt.Sprintf("%x", multihash.Sum([]byte(req.Msg.Name), req.Msg.ValueCbor))
	}
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	_, err = s.Durable.Dispatch.request(ctx, http.MethodPost, s.Durable.Dispatch.Ingress+"/"+actionService+"/"+run.RootActionId+"/Signal", signalValue{req.Msg.Name, req.Msg.ValueCbor}, key)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&lutrav1.SignalRunResponse{}), nil
}

func (s Service) TaskSignalWorkflow(ctx context.Context, req *connect.Request[lutrav1.TaskSignalWorkflowRequest]) (*connect.Response[lutrav1.TaskSignalWorkflowResponse], error) {
	caller, ok := TaskIdentityFromContext(ctx)
	if !ok {
		return nil, connect.NewError(connect.CodePermissionDenied, errors.New("workflow signals are only available inside a task"))
	}
	if err := validateIdempotency(req.Msg.Name, true); err != nil {
		return nil, err
	}
	if err := validateSignal(req.Msg.ValueCbor); err != nil {
		return nil, err
	}
	action, err := db.New(s.DB).GetActiveCaller(ctx, caller.ActionID)
	if err != nil || action.RunID != caller.RunID || action.Attempts != caller.Attempt {
		return nil, connect.NewError(connect.CodePermissionDenied, errors.New("task identity is not active in this run"))
	}
	// Include the source action, but not the attempt: retries deliver the same event.
	key := fmt.Sprintf("%x", multihash.Sum([]byte(caller.ActionID.String()), []byte(req.Msg.Name), req.Msg.ValueCbor))
	_, err = s.SignalRun(ctx, connect.NewRequest(&lutrav1.SignalRunRequest{
		Id: caller.RunID.String(), Name: req.Msg.Name, ValueCbor: req.Msg.ValueCbor, IdempotencyKey: key,
	}))
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&lutrav1.TaskSignalWorkflowResponse{}), nil
}

func validateSignal(value []byte) error {
	if len(value) == 0 || len(value) > 1<<20 {
		return invalid("signal must contain 1 byte to 1 MiB of canonical CBOR")
	}
	mode, _ := cbor.DecOptions{MaxNestedLevels: 64, MaxArrayElements: 65536, MaxMapPairs: 65536, DupMapKey: cbor.DupMapKeyEnforcedAPF}.DecMode()
	var decoded any
	if err := mode.Unmarshal(value, &decoded); err != nil {
		return invalid("invalid signal CBOR")
	}
	encoder, _ := cbor.CanonicalEncOptions().EncMode()
	canonical, err := canonicalSignalCBOR(value, mode, encoder)
	if err != nil || !bytes.Equal(value, canonical) {
		return invalid("signal must contain one canonical CBOR value")
	}
	return nil
}

// Keep tag contents as CBOR so canonicalization cannot normalize timestamps.
type signalCBOR string

func (v *signalCBOR) UnmarshalCBOR(data []byte) error {
	*v = signalCBOR(data)
	return nil
}

func (v signalCBOR) MarshalCBOR() ([]byte, error) { return []byte(v), nil }

func canonicalSignalCBOR(data []byte, decoder cbor.DecMode, encoder cbor.EncMode) ([]byte, error) {
	var value any
	switch data[0] >> 5 {
	case 4:
		var items []cbor.RawMessage
		if err := decoder.Unmarshal(data, &items); err != nil {
			return nil, err
		}
		for i, item := range items {
			canonical, err := canonicalSignalCBOR(item, decoder, encoder)
			if err != nil {
				return nil, err
			}
			items[i] = canonical
		}
		value = items
	case 5:
		var items map[signalCBOR]cbor.RawMessage
		if err := decoder.Unmarshal(data, &items); err != nil {
			return nil, err
		}
		canonical := make(map[signalCBOR]cbor.RawMessage, len(items))
		for key, item := range items {
			keyBytes, err := canonicalSignalCBOR([]byte(key), decoder, encoder)
			if err != nil {
				return nil, err
			}
			itemBytes, err := canonicalSignalCBOR(item, decoder, encoder)
			if err != nil {
				return nil, err
			}
			canonical[signalCBOR(keyBytes)] = itemBytes
		}
		value = canonical
	case 6:
		var tag cbor.RawTag
		if err := tag.UnmarshalCBOR(data); err != nil {
			return nil, err
		}
		if tag.Number == 2 || tag.Number == 3 {
			if err := decoder.Unmarshal(data, &value); err != nil {
				return nil, err
			}
			break
		}
		content, err := canonicalSignalCBOR(tag.Content, decoder, encoder)
		if err != nil {
			return nil, err
		}
		tag.Content = content
		value = tag
	default:
		if err := decoder.Unmarshal(data, &value); err != nil {
			return nil, err
		}
	}
	return encoder.Marshal(value)
}
