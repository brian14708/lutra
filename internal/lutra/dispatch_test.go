package lutra

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"connectrpc.com/connect"
	lutrav1 "github.com/brian14708/lutra/gen/lutra/v1"
	"github.com/google/uuid"
)

func TestSubmitUsesWorkflowKey(t *testing.T) {
	id := uuid.New()
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.Method != http.MethodPost || r.URL.Path != "/"+actionService+"/"+id.String()+"/Run/send" || r.Header.Get("Idempotency-Key") != "" {
			t.Errorf("invalid workflow submission: %s %s %v", r.Method, r.URL, r.Header)
		}
		_, _ = w.Write([]byte(`{"invocationId":"inv_same"}`))
	}))
	defer server.Close()
	d := Dispatcher{Ingress: server.URL}
	for range 2 {
		invocation, err := d.submit(t.Context(), id)
		if err != nil || invocation != "inv_same" {
			t.Fatalf("submission = %q, %v", invocation, err)
		}
	}
	if requests != 2 {
		t.Fatalf("requests = %d", requests)
	}
}

func TestRestateResponseLimits(t *testing.T) {
	for _, size := range []int{(1 << 20) * 4 / 3, maxRestateResponseBytes + 1} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write([]byte(strings.Repeat("x", size)))
			}))
			defer server.Close()
			body, err := (&Dispatcher{}).request(t.Context(), http.MethodGet, server.URL, nil, "")
			if size > maxRestateResponseBytes {
				if err == nil || !strings.Contains(err.Error(), "exceeds") {
					t.Fatalf("oversized response: %v", err)
				}
			} else if err != nil || len(body) != size {
				t.Fatalf("valid response: length %d, error %v", len(body), err)
			}
		})
	}
}

func TestDispatchFailureCarriesCommittedRun(t *testing.T) {
	id := uuid.NewString()
	err := dispatchFailure(id, errors.New("offline"))
	var failure *connect.Error
	if !errors.As(err, &failure) || failure.Code() != connect.CodeUnavailable || len(failure.Details()) != 1 {
		t.Fatalf("failure = %v", err)
	}
	value, err := failure.Details()[0].Value()
	if err != nil {
		t.Fatal(err)
	}
	detail, ok := value.(*lutrav1.RunDispatchFailure)
	if !ok || detail.RunId != id {
		t.Fatalf("detail = %v", value)
	}
}
