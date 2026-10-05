package lutra

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"connectrpc.com/connect"
	lutrav1 "github.com/brian14708/lutra/gen/lutra/v1"
	"github.com/brian14708/lutra/internal/result"
)

func TestWorkflowInspectionRejectsIrrelevantFields(t *testing.T) {
	for _, request := range []*lutrav1.WorkflowInspectRequest{
		{Operation: lutrav1.WorkflowInspectRequest_OPERATION_STATE_KEYS, Key: "unused"},
		{Operation: lutrav1.WorkflowInspectRequest_OPERATION_STATE_GET, Key: "state", ValueCbor: []byte{0xf6}},
		{Operation: lutrav1.WorkflowInspectRequest_OPERATION_PROMISE_PEEK, Key: "promise", Reason: "unused"},
		{Operation: lutrav1.WorkflowInspectRequest_OPERATION_PROMISE_RESOLVE, Key: "promise", ValueCbor: []byte{0xf6}, Reason: "unused"},
	} {
		if err := validateWorkflowInspect(request); connect.CodeOf(err) != connect.CodeInvalidArgument {
			t.Fatalf("accepted invalid request %v: %v", request, err)
		}
	}
	for _, request := range []*lutrav1.WorkflowInspectRequest{
		{Operation: lutrav1.WorkflowInspectRequest_OPERATION_STATE_KEYS},
		{Operation: lutrav1.WorkflowInspectRequest_OPERATION_STATE_GET, Key: "state"},
		{Operation: lutrav1.WorkflowInspectRequest_OPERATION_PROMISE_RESOLVE, Key: "promise", ValueCbor: []byte{0xf6}},
		{Operation: lutrav1.WorkflowInspectRequest_OPERATION_PROMISE_REJECT, Key: "promise", Reason: "rejected"},
	} {
		if err := validateWorkflowInspect(request); err != nil {
			t.Fatalf("rejected valid request %v: %v", request, err)
		}
	}
}

func TestChildCommandName(t *testing.T) {
	request := &lutrav1.CreateTaskActionRequest{Sequence: 7, IdempotencyKey: "digest-0", Metadata: map[string]string{"order_id": "123", "stage": "review"}}
	if name := workflowCommandName(request); name != "Submit child action: digest-0 (order_id=123, stage=review)" {
		t.Fatalf("name = %q", name)
	}
}

func TestWorkflowInboxOrdersAndChecksReplay(t *testing.T) {
	events := make(chan workflowEvent, 4)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/restate/awakeables/sign_inbox/resolve" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		var event workflowEvent
		if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
			t.Error(err)
		}
		events <- event
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()
	inbox := newWorkflowInbox(t.Context(), &Dispatcher{Ingress: server.URL})
	first := workflowCallback{request: &lutrav1.WorkflowTimerRequest{Sequence: 1, Key: "first", DurationMillis: 3}}
	second := workflowCallback{request: &lutrav1.WorkflowTimerRequest{Sequence: 2, Key: "second", DurationMillis: 8}}
	if err := inbox.submit(t.Context(), second); err != nil {
		t.Fatal(err)
	}
	inbox.register(1, "sign_inbox")
	if err := inbox.submit(t.Context(), first); err != nil {
		t.Fatal(err)
	}
	event := <-events
	if event.Sequence != 1 {
		t.Fatalf("first sequence: %d", event.Sequence)
	}
	call, err := inbox.take(t.Context(), event)
	if err != nil || call.request != first.request {
		t.Fatalf("first call: %v, %v", call, err)
	}
	if err := inbox.submit(t.Context(), first); connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Fatalf("consumed sequence accepted: %v", err)
	}
	inbox.register(2, "sign_inbox")
	event = <-events
	if event.Sequence != 2 {
		t.Fatalf("second sequence: %d", event.Sequence)
	}
	event.Fingerprint[0] ^= 1
	if _, err := inbox.take(t.Context(), event); err == nil || !strings.Contains(err.Error(), "changed during replay") {
		t.Fatalf("changed replay accepted: %v", err)
	}
	inbox.register(3, "sign_inbox")
	inbox.finish()
	event = <-events
	if !event.Finished || event.Sequence != 3 {
		t.Fatalf("finish event: %#v", event)
	}
	if _, err := inbox.take(t.Context(), event); err != nil {
		t.Fatal(err)
	}
}

func TestChildCancellationDoesNotDependOnUserMessage(t *testing.T) {
	output, err := result.EncodeFailure(result.Failure{Terminal: true, Code: 409, Message: "order canceled by customer"})
	if err != nil {
		t.Fatal(err)
	}
	completion := childCompletion(output, false, nil)
	if completion.Failure != lutrav1.WorkflowCompletion_FAILURE_UNSPECIFIED || string(completion.ValueCbor) != string(output) {
		t.Fatalf("user failure treated as cancellation: %v", completion)
	}
	completion = childCompletion(nil, true, errors.New("invocation stopped"))
	if completion.Failure != lutrav1.WorkflowCompletion_FAILURE_CANCELED {
		t.Fatalf("canceled child classification: %v", completion)
	}
}

func TestWorkflowInboxRejectsDuplicateAndMissingSequence(t *testing.T) {
	inbox := newWorkflowInbox(t.Context(), nil)
	call := workflowCallback{request: &lutrav1.WorkflowTimerRequest{Sequence: 1, Key: "clock"}}
	if err := inbox.submit(t.Context(), call); err != nil {
		t.Fatal(err)
	}
	if err := inbox.submit(t.Context(), call); connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Fatalf("duplicate sequence: %v", err)
	}
	call.request = &lutrav1.WorkflowWaitRequest{}
	if err := inbox.submit(t.Context(), call); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("missing sequence: %v", err)
	}
}

func TestWorkflowOutcomeValidationAndMapping(t *testing.T) {
	for _, input := range []struct {
		value  []byte
		reject bool
		reason string
	}{
		{nil, false, ""}, {[]byte{0x18, 1}, false, ""}, {[]byte{1}, false, "bad"}, {nil, true, ""}, {[]byte{1}, true, "bad"}, {nil, true, strings.Repeat("x", 4097)},
	} {
		if _, err := validateOutcome(input.value, input.reject, input.reason); err == nil {
			t.Fatalf("accepted invalid outcome: %#v", input)
		}
	}
	value, err := validateOutcome([]byte{0xf6}, false, "")
	if err != nil || value.Rejected || len(value.Value) != 1 {
		t.Fatalf("valid null outcome: %#v, %v", value, err)
	}
	completion, err := outcomeCompletion(&promiseOutcome{Rejected: true, Reason: "declined"}, nil)
	if err != nil || completion.Failure != lutrav1.WorkflowCompletion_FAILURE_REJECTED || completion.Message != "declined" {
		t.Fatalf("rejection: %#v, %v", completion, err)
	}
	completion, err = outcomeCompletion(nil, errors.New("cancelled"))
	if err != nil || completion.Failure != lutrav1.WorkflowCompletion_FAILURE_CANCELED {
		t.Fatalf("cancellation: %#v, %v", completion, err)
	}
}

func TestWorkflowWaitValidatesOwnershipAndKeys(t *testing.T) {
	s := &workflowOperations{ops: map[string]*workflowOperation{operationID(lutrav1.WorkflowFuture_KIND_CHILD, "owned"): {}}}
	for _, refs := range [][]*lutrav1.WorkflowFuture{
		nil,
		{{Kind: lutrav1.WorkflowFuture_KIND_CHILD, Key: "missing", Id: "other"}},
		{{Kind: lutrav1.WorkflowFuture_KIND_CHILD, Key: "same", Id: "owned"}, {Kind: lutrav1.WorkflowFuture_KIND_CHILD, Key: "same", Id: "owned"}},
		{{Kind: lutrav1.WorkflowFuture_KIND_CHILD, Key: "", Id: "owned"}},
	} {
		if _, _, err := s.wait(nil, workflowCallback{}, &lutrav1.WorkflowWaitRequest{Futures: refs}); connect.CodeOf(err) != connect.CodeInvalidArgument {
			t.Fatalf("invalid wait: %v", err)
		}
	}
	_, pending, err := s.wait(nil, workflowCallback{}, &lutrav1.WorkflowWaitRequest{Futures: []*lutrav1.WorkflowFuture{{Kind: lutrav1.WorkflowFuture_KIND_CHILD, Key: "child", Id: "owned"}}})
	if err != nil || !pending || len(s.waits) != 1 {
		t.Fatalf("valid wait: %v, %v", pending, err)
	}
}

func TestWorkflowAwakeableIDBoundary(t *testing.T) {
	for _, id := range []string{"", "foreign", "sign_../escape", "sign_/a", strings.Repeat("x", 257)} {
		if err := validAwakeableID(id); err == nil {
			t.Fatalf("accepted ID %q", id)
		}
	}
	if err := validAwakeableID("sign_Ab9-_xy"); err != nil {
		t.Fatal(err)
	}
}
