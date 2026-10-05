package lutra

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"connectrpc.com/connect"
	lutrav1 "github.com/brian14708/lutra/gen/lutra/v1"
	lutrav1connect "github.com/brian14708/lutra/gen/lutra/v1/lutrav1connect"
	"github.com/google/uuid"
)

func TestSignalCanonicalValues(t *testing.T) {
	for _, value := range [][]byte{
		{0xf6},
		{0xf5},
		{0x42, 0, 0xff},
		{0x82, 1, 2},
		{0xa1, 0x61, 'a', 1},
		{0xf9, 0x3e, 0},
		{0xc2, 0x49, 1, 0, 0, 0, 0, 0, 0, 0, 0},
	} {
		if err := validateSignal(value); err != nil {
			t.Errorf("valid %x: %v", value, err)
		}
	}
	for _, value := range [][]byte{
		nil,
		{1, 2},
		{0x18, 1},
		{0x9f, 1, 0xff},
		{0xa2, 0x61, 'a', 1, 0x61, 'a', 2},
		{0xc2, 0x41, 1},
		bytes.Repeat([]byte{0}, (1<<20)+1),
	} {
		if err := validateSignal(value); err == nil {
			t.Errorf("accepted invalid CBOR of length %d", len(value))
		}
	}
}

func TestTaskSignalRequiresAuthenticatedIdentity(t *testing.T) {
	_, err := (Service{}).TaskSignalWorkflow(context.Background(), connect.NewRequest(&lutrav1.TaskSignalWorkflowRequest{
		Name: "epoch/0", ValueCbor: []byte{1},
	}))
	if connect.CodeOf(err) != connect.CodePermissionDenied {
		t.Fatalf("unauthenticated signal: %v", err)
	}
}

func TestTaskCannotSignalAnotherRun(t *testing.T) {
	ctx := withTaskIdentity(context.Background(), TaskIdentity{RunID: uuid.New()})
	_, err := (Service{}).SignalRun(ctx, connect.NewRequest(&lutrav1.SignalRunRequest{
		Id: uuid.NewString(), Name: "epoch/0", ValueCbor: []byte{1},
	}))
	if connect.CodeOf(err) != connect.CodePermissionDenied {
		t.Fatalf("cross-run task signal: %v", err)
	}
}

func TestTaskSignalValidatesBeforeDatabaseAccess(t *testing.T) {
	ctx := withTaskIdentity(context.Background(), TaskIdentity{})
	for _, request := range []*lutrav1.TaskSignalWorkflowRequest{
		{Name: "", ValueCbor: []byte{1}},
		{Name: strings.Repeat("x", 201), ValueCbor: []byte{1}},
		{Name: "epoch/0", ValueCbor: []byte{0x18, 1}},
		{Name: "epoch/0", ValueCbor: bytes.Repeat([]byte{0}, (1<<20)+1)},
	} {
		_, err := (Service{}).TaskSignalWorkflow(ctx, connect.NewRequest(request))
		if connect.CodeOf(err) != connect.CodeInvalidArgument {
			t.Fatalf("invalid signal accepted: %v", err)
		}
	}
}

func TestSignalCanonicalTimestamps(t *testing.T) {
	for _, encoded := range []string{
		"c074323032362d31302d30365430303a30303a30305a",
		"c07820323032362d31302d30365430303a30303a30302e3132303030302b30383a3030",
		"c11a68e2fe80",
		"81c074323032362d31302d30365430303a30303a30305a",
		"a16174c074323032362d31302d30365430303a30303a30305a",
		"a1c074323032362d31302d30365430303a30303a30305a01",
	} {
		value, err := hex.DecodeString(encoded)
		if err != nil {
			t.Fatal(err)
		}
		if err := validateSignal(value); err != nil {
			t.Errorf("canonical timestamp %x: %v", value, err)
		}
	}
	for _, encoded := range []string{
		"d80074323032362d31302d30365430303a30303a30305a",
		"c11801",
	} {
		value, err := hex.DecodeString(encoded)
		if err != nil {
			t.Fatal(err)
		}
		if err := validateSignal(value); err == nil {
			t.Errorf("accepted noncanonical timestamp %x", value)
		}
	}
}

func TestWorkflowRejectsTaskPolicies(t *testing.T) {
	entry := &lutrav1.Entrypoint{Workflow: true, MaxAttempts: 1, Command: &lutrav1.Command{Args: []string{"python", "workflow.py"}}}
	if err := validateEntrypoints([]*lutrav1.Entrypoint{entry}); err != nil {
		t.Fatal(err)
	}
	if _, err := resolvedActionSpec(entry, &lutrav1.ActionSpec{MaxAttempts: 2}); err == nil {
		t.Fatal("workflow accepted retry override")
	}
	entry.Cache = true
	if err := validateEntrypoints([]*lutrav1.Entrypoint{entry}); err == nil {
		t.Fatal("workflow accepted caching")
	}
}

func TestCancellationConvergesForFinishedInvocations(t *testing.T) {
	for _, status := range []int{http.StatusOK, http.StatusNotFound, http.StatusConflict, http.StatusServiceUnavailable} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPatch || r.URL.Path != "/invocations/inv_123/cancel" {
					t.Errorf("unexpected cancellation: %s %s", r.Method, r.URL.Path)
				}
				w.WriteHeader(status)
			}))
			defer server.Close()
			engine := &Dispatcher{Admin: server.URL}
			err := engine.cancelInvocation(t.Context(), "inv_123")
			if (err != nil) != (status == http.StatusServiceUnavailable) {
				t.Fatalf("status %d: %v", status, err)
			}
		})
	}
}

func TestWorkflowTypedBoundaryCanonicalizesCommands(t *testing.T) {
	queue := &workflowQueue{ctx: t.Context(), inbox: newWorkflowInbox(t.Context(), nil)}
	_, handler := lutrav1connect.NewLutraServiceHandler(queue)
	server := httptest.NewServer(handler)
	defer server.Close()
	fingerprints := make(chan [32]byte, 2)
	go func() {
		for sequence := uint64(1); sequence <= 2; sequence++ {
			semantic := &lutrav1.WorkflowTimerRequest{Sequence: sequence, Key: "clock", DurationMillis: 25}
			fingerprint, _ := commandFingerprint(semantic)
			call, err := queue.inbox.take(t.Context(), workflowEvent{Sequence: sequence, Fingerprint: fingerprint})
			if err != nil {
				t.Error(err)
				return
			}
			request, ok := call.request.(*lutrav1.WorkflowTimerRequest)
			if !ok || request.DurationMillis != 25 {
				t.Errorf("decoded command = %v", call.request)
			}
			request.Sequence = 0
			fingerprint, err = commandFingerprint(call.request)
			if err != nil {
				t.Error(err)
			}
			fingerprints <- fingerprint
			call.done <- workflowResponse{message: &lutrav1.WorkflowTimerResponse{}}
		}
	}()
	for _, body := range []string{`{"durationMillis":"25","key":"clock","sequence":"1"}`, `{ "duration_millis": 25, "key": "clock", "sequence": 2 }`} {
		request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, server.URL+lutrav1connect.LutraServiceWorkflowTimerProcedure, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Content-Type", "application/json")
		response, err := server.Client().Do(request)
		if err != nil {
			t.Fatal(err)
		}
		result, err := io.ReadAll(response.Body)
		_ = response.Body.Close()
		if err != nil || response.StatusCode != http.StatusOK || string(result) != "{}" {
			t.Fatalf("response = %d %s, error = %v", response.StatusCode, result, err)
		}
	}
	first, second := <-fingerprints, <-fingerprints
	if first != second {
		t.Fatal("equivalent protobuf commands produced different replay fingerprints")
	}
	changed, err := commandFingerprint(&lutrav1.WorkflowTimerRequest{Key: "clock", DurationMillis: 26})
	if err != nil || changed == first {
		t.Fatal("changed command did not change replay fingerprint")
	}
	otherMethod, err := commandFingerprint(&lutrav1.WorkflowWaitRequest{})
	if err != nil || otherMethod == first {
		t.Fatal("different operation did not change replay fingerprint")
	}
}

func TestWorkflowCallbackCancellationAllowsLateCompletion(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	queue := &workflowQueue{ctx: ctx, inbox: newWorkflowInbox(ctx, nil)}
	request := &lutrav1.WorkflowWaitRequest{Sequence: 1, Futures: []*lutrav1.WorkflowFuture{{Kind: lutrav1.WorkflowFuture_KIND_CHILD, Key: "child", Id: "child"}}}
	finished := make(chan error, 1)
	go func() {
		_, err := queue.WorkflowWait(t.Context(), connect.NewRequest(request))
		finished <- err
	}()
	fingerprint, _ := commandFingerprint(request)
	call, err := queue.inbox.take(t.Context(), workflowEvent{Sequence: 1, Fingerprint: fingerprint})
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	if err := <-finished; err != context.Canceled {
		t.Fatalf("canceled callback = %v", err)
	}
	// The handler can complete after cancellation without touching the HTTP writer.
	call.done <- workflowResponse{message: &lutrav1.WorkflowWaitResponse{}}
}
