package lutra

import (
	"bytes"
	"context"
	"encoding/json"
	"reflect"
	"uuid"

	lutrav1 "github.com/brian14708/lutra/gen/lutra/v1"
	"github.com/brian14708/lutra/internal/db"
	"github.com/brian14708/lutra/internal/rpcutil"
	"google.golang.org/protobuf/encoding/protojson"
)

func marshalBindings(bindings []*lutrav1.InputBinding) ([]byte, error) {
	raw := make([]json.RawMessage, 0, len(bindings))
	for _, b := range bindings {
		data, err := protojson.Marshal(b)
		if err != nil {
			return nil, err
		}
		raw = append(raw, data)
	}
	return json.Marshal(raw)
}

// equalJSON compares JSON values without depending on formatting chosen by the
// database driver.
func equalJSON(left, right []byte) bool {
	var leftValue, rightValue any
	if err := json.Unmarshal(left, &leftValue); err != nil {
		return bytes.Equal(left, right)
	}
	if err := json.Unmarshal(right, &rightValue); err != nil {
		return bytes.Equal(left, right)
	}
	return reflect.DeepEqual(leftValue, rightValue)
}

func unmarshalBindings(raw []byte) []*lutrav1.InputBinding {
	var values []json.RawMessage
	if json.Unmarshal(raw, &values) != nil {
		return nil
	}
	result := make([]*lutrav1.InputBinding, 0, len(values))
	for _, value := range values {
		var b lutrav1.InputBinding
		if protojson.Unmarshal(value, &b) == nil {
			result = append(result, &b)
		}
	}
	return result
}

func taskProto(task db.LutraTask) *lutrav1.Task {
	var argv []string
	_ = json.Unmarshal(task.EntrypointArgv, &argv)
	var inputs []*lutrav1.TaskSlot
	var outputs []*lutrav1.TaskSlot
	_ = json.Unmarshal(task.InputSlots, &inputs)
	_ = json.Unmarshal(task.OutputSlots, &outputs)
	return &lutrav1.Task{ProjectId: task.ProjectID.String(), TaskId: task.TaskID.String(), Name: task.Name, Version: task.Version, EntrypointArgv: argv, CodeBundleArtifactId: task.CodeBundleArtifactID.String(), InputSlots: inputs, OutputSlots: outputs, CreateTime: rpcutil.Timestamp(task.CreateTime)}
}

func runProto(run db.LutraRun, rootTaskID uuid.UUID) *lutrav1.Run {
	return &lutrav1.Run{ProjectId: run.ProjectID.String(), RunId: run.RunID.String(), RootTaskId: rootTaskID.String(), State: runState(run.State), CreateTime: rpcutil.Timestamp(run.CreateTime), UpdateTime: rpcutil.Timestamp(run.UpdateTime)}
}

func actionProto(action db.LutraAction) *lutrav1.Action {
	status := &lutrav1.ActionStatus{State: actionState(action.State), AttemptCount: uint32(action.AttemptCount), FailureMessage: action.FailureMessage}
	if action.StartTime.Valid {
		status.StartTime = rpcutil.Timestamp(action.StartTime)
	}
	if action.EndTime.Valid {
		status.EndTime = rpcutil.Timestamp(action.EndTime)
		if action.StartTime.Valid {
			status.DurationMs = uint64(action.EndTime.Time.Sub(action.StartTime.Time).Milliseconds())
		}
	}
	operationID := ""
	if action.OperationID.Valid {
		operationID = action.OperationID.String
	}
	taskID := action.TaskID.String()
	result := &lutrav1.Action{ProjectId: action.ProjectID.String(), RunId: action.RunID.String(), ActionId: action.ActionID.String(), TaskId: taskID, OperationId: operationID, Status: status, CreateTime: rpcutil.Timestamp(action.CreateTime), UpdateTime: rpcutil.Timestamp(action.UpdateTime), Inputs: unmarshalBindings(action.Inputs), Outputs: unmarshalBindings(action.Outputs)}
	if action.ParentActionID != nil {
		parentActionID := action.ParentActionID.String()
		result.ParentActionId = &parentActionID
	}
	return result
}

func loadActionProto(ctx context.Context, queries *db.Queries, action db.LutraAction) (*lutrav1.Action, error) {
	attempts, err := queries.ListActionAttempts(ctx, action.ActionID)
	if err != nil {
		return nil, err
	}
	return actionProtoWithAttempts(action, attempts), nil
}

func actionProtoWithAttempts(action db.LutraAction, attempts []db.LutraActionAttempt) *lutrav1.Action {
	result := actionProto(action)
	result.Attempts = make([]*lutrav1.ActionAttempt, 0, len(attempts))
	for _, attempt := range attempts {
		result.Attempts = append(result.Attempts, &lutrav1.ActionAttempt{
			Attempt:        uint32(attempt.Attempt),
			State:          attemptState(attempt.State),
			StartTime:      rpcutil.Timestamp(attempt.StartTime),
			EndTime:        rpcutil.Timestamp(attempt.EndTime),
			FailureMessage: attempt.FailureMessage,
		})
	}
	return result
}

var runStates = map[db.LutraRunState]lutrav1.RunState{
	db.LutraRunStateQueued:    lutrav1.RunState_RUN_STATE_QUEUED,
	db.LutraRunStateRunning:   lutrav1.RunState_RUN_STATE_RUNNING,
	db.LutraRunStateSucceeded: lutrav1.RunState_RUN_STATE_SUCCEEDED,
	db.LutraRunStateFailed:    lutrav1.RunState_RUN_STATE_FAILED,
	db.LutraRunStateCancelled: lutrav1.RunState_RUN_STATE_CANCELLED,
}

var actionStates = map[db.LutraActionState]lutrav1.ActionState{
	db.LutraActionStateReady:     lutrav1.ActionState_ACTION_STATE_READY,
	db.LutraActionStateRunning:   lutrav1.ActionState_ACTION_STATE_RUNNING,
	db.LutraActionStateSucceeded: lutrav1.ActionState_ACTION_STATE_SUCCEEDED,
	db.LutraActionStateFailed:    lutrav1.ActionState_ACTION_STATE_FAILED,
	db.LutraActionStateCanceled:  lutrav1.ActionState_ACTION_STATE_CANCELED,
	db.LutraActionStateTimedOut:  lutrav1.ActionState_ACTION_STATE_TIMED_OUT,
}

var attemptStates = map[db.LutraAttemptState]lutrav1.AttemptState{
	db.LutraAttemptStateOpen:      lutrav1.AttemptState_ATTEMPT_STATE_OPEN,
	db.LutraAttemptStateSucceeded: lutrav1.AttemptState_ATTEMPT_STATE_SUCCEEDED,
	db.LutraAttemptStateFailed:    lutrav1.AttemptState_ATTEMPT_STATE_FAILED,
	db.LutraAttemptStateCanceled:  lutrav1.AttemptState_ATTEMPT_STATE_CANCELED,
	db.LutraAttemptStateLost:      lutrav1.AttemptState_ATTEMPT_STATE_LOST,
}

func runState(value db.LutraRunState) lutrav1.RunState { return runStates[value] }

func actionState(value db.LutraActionState) lutrav1.ActionState { return actionStates[value] }

func attemptState(value db.LutraAttemptState) lutrav1.AttemptState { return attemptStates[value] }
