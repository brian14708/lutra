package lutra

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"connectrpc.com/connect"
	"connectrpc.com/validate"
	lutrav1 "github.com/brian14708/lutra/gen/lutra/v1"
	lutrav1connect "github.com/brian14708/lutra/gen/lutra/v1/lutrav1connect"
)

func TestPingUsesHTTPGet(t *testing.T) {
	_, handler := lutrav1connect.NewLutraServiceHandler(Service{})
	var method string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		method = r.Method
		handler.ServeHTTP(w, r)
	}))
	defer server.Close()

	client := lutrav1connect.NewLutraServiceClient(server.Client(), server.URL, connect.WithHTTPGet())
	response, err := client.Ping(context.Background(), connect.NewRequest(&lutrav1.PingRequest{Message: "ping"}))
	if err != nil {
		t.Fatalf("Ping failed: %v", err)
	}
	if method != http.MethodGet {
		t.Fatalf("expected GET request, got %s", method)
	}
	if response.Msg.GetMessage() != "ping" {
		t.Fatalf("unexpected response: %q", response.Msg.GetMessage())
	}
}

func TestPingRejectsInvalidRequest(t *testing.T) {
	called := false
	call := validate.NewInterceptor().WrapUnary(func(_ context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		called = true
		return connect.NewResponse(&lutrav1.PingResponse{Message: req.Any().(*lutrav1.PingRequest).GetMessage()}), nil
	})

	_, err := call(context.Background(), connect.NewRequest(&lutrav1.PingRequest{
		Message: strings.Repeat("a", 1025),
	}))
	if got := connect.CodeOf(err); got != connect.CodeInvalidArgument {
		t.Fatalf("expected invalid argument, got %s (%v)", got, err)
	}
	if called {
		t.Fatal("invalid request reached the service")
	}

	response, err := call(context.Background(), connect.NewRequest(&lutrav1.PingRequest{
		Message: strings.Repeat("a", 1024),
	}))
	if err != nil {
		t.Fatalf("valid request failed: %v", err)
	}
	responseMessage := response.(*connect.Response[lutrav1.PingResponse]).Msg
	if got := responseMessage.GetMessage(); len(got) != 1024 {
		t.Fatalf("expected 1024-character response, got %d", len(got))
	}
}
