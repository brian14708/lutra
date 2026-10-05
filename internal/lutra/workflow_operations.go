package lutra

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"connectrpc.com/connect"
	lutrav1 "github.com/brian14708/lutra/gen/lutra/v1"
	"github.com/brian14708/lutra/internal/db"
	"github.com/google/uuid"
	restate "github.com/restatedev/sdk-go"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type promiseOutcome struct {
	Value    []byte
	Rejected bool
	Reason   string
}

func (value *promiseOutcome) matches(other *promiseOutcome) bool {
	return value.Rejected == other.Rejected && value.Reason == other.Reason && bytes.Equal(value.Value, other.Value)
}

type workflowOperation struct {
	future     restate.Future
	read       func() (*lutrav1.WorkflowCompletion, error)
	completion *lutrav1.WorkflowCompletion
}

type workflowWait struct {
	call       workflowCallback
	request    *lutrav1.WorkflowWaitRequest
	operations []*workflowOperation
}

type workflowOperations struct {
	children       map[string]restate.AttachFuture[[]byte]
	childKeys      map[string][32]byte
	ops            map[string]*workflowOperation
	timerDurations map[string]int64
	awakeables     map[string]string
	resolved       map[string]*promiseOutcome
	waits          []*workflowWait
}

func operationID(kind lutrav1.WorkflowFuture_Kind, id string) string {
	return fmt.Sprintf("%d:%s", kind, id)
}

func (s *workflowOperations) add(kind lutrav1.WorkflowFuture_Kind, id string, future restate.Future, read func() (*lutrav1.WorkflowCompletion, error)) *workflowOperation {
	op := &workflowOperation{future: future, read: read}
	s.ops[operationID(kind, id)] = op
	return op
}

func outcomeCompletion(value *promiseOutcome, err error) (*lutrav1.WorkflowCompletion, error) {
	if err != nil {
		return &lutrav1.WorkflowCompletion{Failure: lutrav1.WorkflowCompletion_FAILURE_CANCELED, Message: err.Error()}, nil
	}
	if value.Rejected {
		return &lutrav1.WorkflowCompletion{Failure: lutrav1.WorkflowCompletion_FAILURE_REJECTED, Message: value.Reason}, nil
	}
	return &lutrav1.WorkflowCompletion{ValueCbor: value.Value}, nil
}

func (s *workflowOperations) promise(ctx restate.WorkflowContext, name string) *workflowOperation {
	if op := s.ops[operationID(lutrav1.WorkflowFuture_KIND_PROMISE, name)]; op != nil {
		return op
	}
	promise := restate.Promise[*promiseOutcome](ctx, name)
	return s.add(lutrav1.WorkflowFuture_KIND_PROMISE, name, promise, func() (*lutrav1.WorkflowCompletion, error) {
		value, err := promise.Result()
		return outcomeCompletion(value, err)
	})
}

func (w *WorkflowRunner) workflowLoop(ctx restate.WorkflowContext, sandboxCtx context.Context, identity TaskIdentity, inbox *workflowInbox, finished <-chan sandboxResult, received *bool, release *func()) ([]byte, error) {
	var err error
	s := &workflowOperations{children: make(map[string]restate.AttachFuture[[]byte]), childKeys: make(map[string][32]byte), ops: make(map[string]*workflowOperation), timerDurations: make(map[string]int64), awakeables: make(map[string]string), resolved: make(map[string]*promiseOutcome)}
	sequence := uint64(1)
	delivery := uint64(0)
	intake := restate.Awakeable[workflowEvent](ctx)
	inbox.register(sequence, intake.Id())
	respond := func(call workflowCallback, response workflowResponse) error {
		if *release == nil {
			var err error
			*release, err = w.Tasks.acquire(sandboxCtx, LogTask)
			if err != nil {
				return err
			}
			if err := restate.RunVoid(ctx, func(ctx restate.RunContext) error {
				return w.Adapter.project(ctx, identity.ActionID, db.LutraTaskActionStatusRunning, nil, false)
			}, restate.WithName("Mark workflow running")); err != nil {
				return err
			}
		}
		delivery++
		meta := &lutrav1.WorkflowCommandMeta{Delivery: delivery}
		if response.err != nil {
			meta.ErrorCode = connect.CodeOf(response.err).String()
			var boundary *connect.Error
			if errors.As(response.err, &boundary) {
				meta.ErrorMessage = boundary.Message()
			} else {
				meta.ErrorMessage = response.err.Error()
			}
			response.err = nil
		}
		if response.message == nil {
			response.message = emptyWorkflowResponse(call.request)
		}
		field := response.message.ProtoReflect().Descriptor().Fields().ByName("workflow_meta")
		response.message.ProtoReflect().Set(field, protoreflect.ValueOfMessage(meta.ProtoReflect()))
		call.done <- response
		return nil
	}
	for {
		if len(s.waits) > 0 && *release != nil {
			(*release)()
			*release = nil
		}
		operations := make([]*workflowOperation, 0)
		seen := make(map[*workflowOperation]bool)
		for _, wait := range s.waits {
			for _, op := range wait.operations {
				if op.completion == nil && !seen[op] {
					seen[op] = true
					operations = append(operations, op)
				}
			}
		}
		futures := []restate.Future{intake}
		for _, op := range operations {
			futures = append(futures, op.future)
		}
		var first restate.Future
		first, err = restate.WaitFirst(ctx, futures...)
		if err != nil {
			return nil, err
		}
		index := 0
		for i, future := range futures {
			if future == first {
				index = i
				break
			}
		}
		// SDK selection can prefer another already-completed future during replay.
		index, err = restate.Run(ctx, func(restate.RunContext) (int, error) { return index, nil }, restate.WithName("Record workflow event selection"))
		if err != nil {
			return nil, err
		}
		if index < 0 || index >= len(futures) {
			return nil, restate.TerminalErrorf("workflow wait changed during replay")
		}
		if index == 0 {
			var event workflowEvent
			event, err = intake.Result()
			if err != nil {
				return nil, err
			}
			if event.Sequence != sequence {
				return nil, restate.TerminalErrorf("workflow sequence changed during replay")
			}
			call, err := inbox.take(sandboxCtx, event)
			if err != nil {
				return nil, restate.ToTerminalError(err)
			}
			if event.Finished {
				out := <-finished
				*received = true
				var terminalErr *TerminalTaskError
				if errors.As(out.err, &terminalErr) {
					return out.output, nil
				}
				var taskErr *TaskError
				var configErr *ConfigError
				var cacheable *CacheableError
				if errors.As(out.err, &cacheable) {
					return out.output, nil
				}
				if errors.As(out.err, &taskErr) || errors.As(out.err, &configErr) {
					return out.output, restate.ToTerminalError(out.err)
				}
				return out.output, out.err
			}
			response, pending, err := w.operationCommand(ctx, identity, s, call)
			if err != nil {
				return nil, err
			}
			if !pending {
				if err := respond(call, response); err != nil {
					return nil, err
				}
			}
			sequence++
			intake = restate.Awakeable[workflowEvent](ctx)
			inbox.register(sequence, intake.Id())
		} else {
			op := operations[index-1]
			op.completion, err = op.read()
			if err != nil {
				return nil, err
			}
		}
		remaining := s.waits[:0]
		for _, wait := range s.waits {
			completions := make([]*lutrav1.WorkflowCompletion, 0, len(wait.operations))
			for i, op := range wait.operations {
				if op.completion == nil {
					continue
				}
				completion := proto.Clone(op.completion).(*lutrav1.WorkflowCompletion)
				completion.Future = wait.request.Futures[i]
				completions = append(completions, completion)
				if !wait.request.All {
					break
				}
			}
			ready := len(completions) > 0 && (!wait.request.All || len(completions) == len(wait.operations))
			if !ready {
				remaining = append(remaining, wait)
				continue
			}
			response := workflowResponse{message: &lutrav1.WorkflowWaitResponse{Completions: completions}}
			if err := respond(wait.call, response); err != nil {
				return nil, err
			}
		}
		s.waits = remaining
	}
}

func emptyWorkflowResponse(request proto.Message) proto.Message {
	switch request.(type) {
	case *lutrav1.CreateTaskActionRequest:
		return &lutrav1.CreateTaskActionResponse{}
	case *lutrav1.WorkflowTimerRequest:
		return &lutrav1.WorkflowTimerResponse{}
	case *lutrav1.WorkflowWaitRequest:
		return &lutrav1.WorkflowWaitResponse{}
	case *lutrav1.WorkflowStateRequest:
		return &lutrav1.WorkflowStateResponse{}
	case *lutrav1.WorkflowPromiseRequest:
		return &lutrav1.WorkflowPromiseResponse{}
	case *lutrav1.WorkflowAwakeableRequest:
		return &lutrav1.WorkflowAwakeableResponse{}
	case *lutrav1.WorkflowEntropyRequest:
		return &lutrav1.WorkflowEntropyResponse{}
	case *lutrav1.WorkflowCancelRequest:
		return &lutrav1.WorkflowCancelResponse{}
	default:
		panic("unsupported workflow response")
	}
}

func (w *WorkflowRunner) operationCommand(ctx restate.WorkflowContext, identity TaskIdentity, s *workflowOperations, call workflowCallback) (workflowResponse, bool, error) {
	if request, ok := call.request.(*lutrav1.CreateTaskActionRequest); ok {
		if len(request.Metadata) > 32 {
			return workflowResponse{err: invalid("child metadata accepts at most 32 fields")}, false, nil
		}
		for key, value := range request.Metadata {
			if err := validateIdempotency(key, true); err != nil || len(value) > 1024 {
				if err == nil {
					err = invalid("child metadata values must fit 1024 bytes")
				}
				return workflowResponse{err: err}, false, nil
			}
		}
		semantic := proto.Clone(request).(*lutrav1.CreateTaskActionRequest)
		semantic.Sequence = 0
		semantic.Metadata = nil
		fingerprint, err := commandFingerprint(semantic)
		if err != nil {
			return workflowResponse{}, false, restate.ToTerminalError(err)
		}
		if previous, exists := s.childKeys[request.IdempotencyKey]; exists && previous != fingerprint {
			return workflowResponse{err: connect.NewError(connect.CodeAlreadyExists, errors.New("child key already has different arguments"))}, false, nil
		}
		response, err := w.workflowCommand(ctx, identity, s.children, request)
		if err != nil || response.err != nil {
			return response, false, err
		}
		id := response.message.(*lutrav1.CreateTaskActionResponse).Action.Id
		s.childKeys[request.IdempotencyKey] = fingerprint
		if s.ops[operationID(lutrav1.WorkflowFuture_KIND_CHILD, id)] == nil {
			future := s.children[id]
			s.add(lutrav1.WorkflowFuture_KIND_CHILD, id, future, func() (*lutrav1.WorkflowCompletion, error) {
				var err error
				var output []byte
				output, err = future.Response()
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				responseErr := err
				canceled, err := restate.Run(ctx, func(ctx restate.RunContext) (bool, error) {
					status, err := db.New(w.Adapter.DB).GetRootStatus(ctx, uuid.MustParse(id))
					return status == db.LutraTaskActionStatusCanceled, err
				}, restate.WithName("Check child cancellation"))
				if err != nil {
					return nil, err
				}
				return childCompletion(output, canceled, responseErr), nil
			})
		}
		return response, false, nil
	}
	if _, err := journalCommand(ctx, call.request, func(restate.RunContext) (commandResult, error) { return commandResult{}, nil }); err != nil {
		return workflowResponse{}, false, err
	}
	response, pending, err := w.applyOperation(ctx, identity, s, call)
	if err != nil {
		var boundary *connect.Error
		if errors.As(err, &boundary) {
			return workflowResponse{err: err}, false, nil
		}
		return workflowResponse{}, false, err
	}
	if pending {
		if err := restate.RunVoid(ctx, func(ctx restate.RunContext) error {
			return w.Adapter.project(ctx, identity.ActionID, db.LutraTaskActionStatusWaiting, nil, false)
		}, restate.WithName("Mark workflow waiting")); err != nil {
			return workflowResponse{}, false, err
		}
	}
	return response, pending, nil
}

func childCompletion(output []byte, canceled bool, err error) *lutrav1.WorkflowCompletion {
	if canceled {
		return &lutrav1.WorkflowCompletion{Failure: lutrav1.WorkflowCompletion_FAILURE_CANCELED, Message: "child invocation canceled"}
	}
	if err != nil {
		return &lutrav1.WorkflowCompletion{Failure: lutrav1.WorkflowCompletion_FAILURE_CHILD, Message: err.Error()}
	}
	return &lutrav1.WorkflowCompletion{ValueCbor: output}
}

func (s *workflowOperations) timer(ctx restate.WorkflowContext, key string, millis int64) error {
	if err := validateIdempotency(key, true); err != nil {
		return err
	}
	duration, err := workflowDuration(millis)
	if err != nil {
		return err
	}
	if previous, exists := s.timerDurations[key]; exists {
		if previous != millis {
			return invalid("timer key already has a different duration")
		}
		return nil
	}
	future := restate.After(ctx, duration, restate.WithName("Timer: "+key))
	s.timerDurations[key] = millis
	s.add(lutrav1.WorkflowFuture_KIND_TIMER, key, future, func() (*lutrav1.WorkflowCompletion, error) {
		if err := future.Done(); err != nil {
			return &lutrav1.WorkflowCompletion{Failure: lutrav1.WorkflowCompletion_FAILURE_CANCELED, Message: err.Error()}, nil
		}
		return &lutrav1.WorkflowCompletion{ValueCbor: []byte{0xf6}}, nil
	})
	return nil
}

func (s *workflowOperations) wait(ctx restate.WorkflowContext, call workflowCallback, request *lutrav1.WorkflowWaitRequest) (workflowResponse, bool, error) {
	if len(request.Futures) == 0 || len(request.Futures) > 4096 {
		return workflowResponse{}, false, invalid("wait requires 1 to 4096 futures")
	}
	wait := &workflowWait{call: call, request: request}
	keys := make(map[string]bool)
	identities := make(map[string]bool)
	for _, ref := range request.Futures {
		if err := validateIdempotency(ref.Key, true); err != nil {
			return workflowResponse{}, false, err
		}
		if keys[ref.Key] {
			return workflowResponse{}, false, invalid("wait contains duplicate keys")
		}
		keys[ref.Key] = true
		if ref.Kind == lutrav1.WorkflowFuture_KIND_PROMISE {
			if err := validateIdempotency(ref.Id, true); err != nil {
				return workflowResponse{}, false, err
			}
			s.promise(ctx, ref.Id)
		}
		id := ref.Id
		identity := operationID(ref.Kind, id)
		if identities[identity] {
			return workflowResponse{}, false, invalid("wait contains duplicate futures")
		}
		identities[identity] = true
		op := s.ops[identity]
		if op == nil {
			return workflowResponse{}, false, invalid("wait references an operation this workflow did not create")
		}
		wait.operations = append(wait.operations, op)
	}
	s.waits = append(s.waits, wait)
	return workflowResponse{}, true, nil
}

func (w *WorkflowRunner) applyOperation(ctx restate.WorkflowContext, identity TaskIdentity, s *workflowOperations, call workflowCallback) (workflowResponse, bool, error) {
	switch request := call.request.(type) {
	case *lutrav1.WorkflowTimerRequest:
		if err := s.timer(ctx, request.Key, request.DurationMillis); err != nil {
			return workflowResponse{}, false, err
		}
		return workflowResponse{message: &lutrav1.WorkflowTimerResponse{}}, false, nil
	case *lutrav1.WorkflowWaitRequest:
		return s.wait(ctx, call, request)
	case *lutrav1.WorkflowStateRequest:
		response, err := workflowState(ctx, request)
		return workflowResponse{message: response}, false, err
	case *lutrav1.WorkflowPromiseRequest:
		response, err := workflowPromise(ctx, request)
		return workflowResponse{message: response}, false, err
	case *lutrav1.WorkflowAwakeableRequest:
		response, err := workflowAwakeable(ctx, s, request)
		return workflowResponse{message: response}, false, err
	case *lutrav1.WorkflowEntropyRequest:
		if request.Kind != lutrav1.WorkflowEntropyRequest_KIND_TIME && request.Kind != lutrav1.WorkflowEntropyRequest_KIND_SEED && request.Kind != lutrav1.WorkflowEntropyRequest_KIND_UUID_SEED {
			return workflowResponse{}, false, invalid("invalid entropy kind")
		}
		response, err := restate.Run(ctx, func(restate.RunContext) (*lutrav1.WorkflowEntropyResponse, error) {
			switch request.Kind {
			case lutrav1.WorkflowEntropyRequest_KIND_TIME:
				return &lutrav1.WorkflowEntropyResponse{TimeMillis: time.Now().UnixMilli()}, nil
			case lutrav1.WorkflowEntropyRequest_KIND_SEED, lutrav1.WorkflowEntropyRequest_KIND_UUID_SEED:
				seed := make([]byte, 32)
				if _, err := rand.Read(seed); err != nil {
					return nil, err
				}
				return &lutrav1.WorkflowEntropyResponse{Seed: seed}, nil
			default:
				return nil, restate.TerminalErrorf("invalid entropy kind")
			}
		}, restate.WithName("Generate workflow entropy"))
		return workflowResponse{message: response}, false, err
	case *lutrav1.WorkflowCancelRequest:
		if s.children[request.Id] == nil {
			return workflowResponse{}, false, invalid("workflow can only cancel its own child")
		}
		if err := restate.RunVoid(ctx, func(ctx restate.RunContext) error {
			return w.Adapter.project(ctx, uuid.MustParse(request.Id), db.LutraTaskActionStatusCanceled, failureOutput(errors.New("action canceled")), false)
		}, restate.WithName("Cancel child action")); err != nil {
			return workflowResponse{}, false, err
		}
		return workflowResponse{message: &lutrav1.WorkflowCancelResponse{}}, false, nil
	default:
		return workflowResponse{}, false, invalid("unsupported workflow command")
	}
}

func workflowState(ctx restate.WorkflowContext, request *lutrav1.WorkflowStateRequest) (*lutrav1.WorkflowStateResponse, error) {
	if request.Operation != lutrav1.WorkflowStateRequest_OPERATION_SET && len(request.ValueCbor) != 0 {
		return nil, invalid("only state set accepts a value")
	}
	if request.Operation == lutrav1.WorkflowStateRequest_OPERATION_KEYS && request.Key != "" {
		return nil, invalid("state keys does not accept a key")
	}
	if request.Operation != lutrav1.WorkflowStateRequest_OPERATION_KEYS {
		if err := validateIdempotency(request.Key, true); err != nil {
			return nil, err
		}
	}
	key := "state:" + request.Key
	switch request.Operation {
	case lutrav1.WorkflowStateRequest_OPERATION_GET:
		value, err := restate.Get[[]byte](ctx, key, restate.WithBinary)
		return &lutrav1.WorkflowStateResponse{Found: value != nil, ValueCbor: value}, err
	case lutrav1.WorkflowStateRequest_OPERATION_SET:
		if err := validateSignal(request.ValueCbor); err != nil {
			return nil, err
		}
		restate.Set(ctx, key, request.ValueCbor, restate.WithBinary)
	case lutrav1.WorkflowStateRequest_OPERATION_CLEAR:
		restate.Clear(ctx, key)
	case lutrav1.WorkflowStateRequest_OPERATION_KEYS:
		keys, err := restate.Keys(ctx)
		if err != nil {
			return nil, err
		}
		visible := make([]string, 0, len(keys))
		for _, key := range keys {
			if strings.HasPrefix(key, "state:") {
				visible = append(visible, strings.TrimPrefix(key, "state:"))
			}
		}
		sort.Strings(visible)
		return &lutrav1.WorkflowStateResponse{Keys: visible}, nil
	default:
		return nil, invalid("invalid state operation")
	}
	return &lutrav1.WorkflowStateResponse{}, nil
}

func validateOutcome(value []byte, rejected bool, reason string) (*promiseOutcome, error) {
	if rejected {
		if len(reason) == 0 || len(reason) > 4096 {
			return nil, invalid("rejection reason must be 1 to 4096 bytes")
		}
		if len(value) != 0 {
			return nil, invalid("rejection cannot contain a value")
		}
	} else {
		if reason != "" {
			return nil, invalid("resolution cannot contain a rejection reason")
		}
		if err := validateSignal(value); err != nil {
			return nil, err
		}
	}
	return &promiseOutcome{Value: value, Rejected: rejected, Reason: reason}, nil
}

func resolvePromise(ctx restate.WorkflowSharedContext, name string, outcome *promiseOutcome) error {
	promise := restate.Promise[*promiseOutcome](ctx, name)
	previous, err := promise.Peek()
	if err != nil {
		return err
	}
	if previous != nil {
		if previous.matches(outcome) {
			return nil
		}
		return connect.NewError(connect.CodeAlreadyExists, errors.New("promise already has a different outcome"))
	}
	if resolveErr := promise.Resolve(outcome); resolveErr != nil {
		previous, err := promise.Peek()
		if err != nil {
			return err
		}
		if previous == nil {
			return resolveErr
		}
		if previous.matches(outcome) {
			return nil
		}
		return connect.NewError(connect.CodeAlreadyExists, errors.New("promise already has a different outcome"))
	}
	return nil
}

func workflowPromise(ctx restate.WorkflowContext, request *lutrav1.WorkflowPromiseRequest) (*lutrav1.WorkflowPromiseResponse, error) {
	if err := validateIdempotency(request.Name, true); err != nil {
		return nil, err
	}
	switch request.Operation {
	case lutrav1.WorkflowPromiseRequest_OPERATION_PEEK:
		if len(request.ValueCbor) != 0 || request.Reason != "" {
			return nil, invalid("promise peek does not accept an outcome")
		}
		value, err := restate.Promise[*promiseOutcome](ctx, request.Name).Peek()
		if err != nil {
			return nil, err
		}
		if value == nil {
			return &lutrav1.WorkflowPromiseResponse{}, nil
		}
		return &lutrav1.WorkflowPromiseResponse{Completed: true, ValueCbor: value.Value, Rejected: value.Rejected, Reason: value.Reason}, nil
	case lutrav1.WorkflowPromiseRequest_OPERATION_RESOLVE, lutrav1.WorkflowPromiseRequest_OPERATION_REJECT:
		outcome, err := validateOutcome(request.ValueCbor, request.Operation == lutrav1.WorkflowPromiseRequest_OPERATION_REJECT, request.Reason)
		if err != nil {
			return nil, err
		}
		if err := resolvePromise(ctx, request.Name, outcome); err != nil {
			return nil, err
		}
		return &lutrav1.WorkflowPromiseResponse{Completed: true, ValueCbor: outcome.Value, Rejected: outcome.Rejected, Reason: outcome.Reason}, nil
	default:
		return nil, invalid("invalid promise operation")
	}
}

func workflowAwakeable(ctx restate.WorkflowContext, s *workflowOperations, request *lutrav1.WorkflowAwakeableRequest) (*lutrav1.WorkflowAwakeableResponse, error) {
	switch request.Operation {
	case lutrav1.WorkflowAwakeableRequest_OPERATION_CREATE:
		if request.Id != "" || len(request.ValueCbor) != 0 || request.Reason != "" {
			return nil, invalid("awakeable creation does not accept an ID or outcome")
		}
		if err := validateIdempotency(request.Key, true); err != nil {
			return nil, err
		}
		if id := s.awakeables[request.Key]; id != "" {
			return &lutrav1.WorkflowAwakeableResponse{Id: id}, nil
		}
		future := restate.Awakeable[*promiseOutcome](ctx)
		id := future.Id()
		s.awakeables[request.Key] = id
		s.add(lutrav1.WorkflowFuture_KIND_AWAKEABLE, id, future, func() (*lutrav1.WorkflowCompletion, error) {
			value, err := future.Result()
			return outcomeCompletion(value, err)
		})
		return &lutrav1.WorkflowAwakeableResponse{Id: id}, nil
	case lutrav1.WorkflowAwakeableRequest_OPERATION_RESOLVE, lutrav1.WorkflowAwakeableRequest_OPERATION_REJECT:
		if request.Key != "" {
			return nil, invalid("awakeable completion does not accept a key")
		}
		if err := validAwakeableID(request.Id); err != nil {
			return nil, err
		}
		outcome, err := validateOutcome(request.ValueCbor, request.Operation == lutrav1.WorkflowAwakeableRequest_OPERATION_REJECT, request.Reason)
		if err != nil {
			return nil, err
		}
		if previous := s.resolved[request.Id]; previous != nil {
			if !previous.matches(outcome) {
				return nil, connect.NewError(connect.CodeAlreadyExists, errors.New("awakeable already has a different outcome"))
			}
			return &lutrav1.WorkflowAwakeableResponse{Id: request.Id}, nil
		}
		if _, err := restate.Object[restate.Void](ctx, awakeableService, request.Id, "Resolve").Request(awakeableResolution{request.Id, outcome}); err != nil {
			if err.Code() == 409 {
				return nil, connect.NewError(connect.CodeAlreadyExists, errors.New(err.Error()))
			}
			return nil, err
		}
		s.resolved[request.Id] = outcome
		return &lutrav1.WorkflowAwakeableResponse{Id: request.Id}, nil
	default:
		return nil, invalid("invalid awakeable operation")
	}
}
