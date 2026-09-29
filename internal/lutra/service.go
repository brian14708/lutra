// Package lutra implements the Lutra service business logic.
package lutra

import (
	"context"
	"strings"
	"unicode/utf8"

	"connectrpc.com/connect"
	lutrav1 "github.com/brian14708/lutra/gen/lutra/v1"
	"github.com/fxamacker/cbor/v2"
)

// Service implements the core Lutra ConnectRPC API.
type Service struct {
	Worker *Worker
}

// Greet serves the console's built-in greeting.
func (s Service) Greet(_ context.Context, req *connect.Request[lutrav1.GreetRequest]) (*connect.Response[lutrav1.GreetResponse], error) {
	var parameters map[string]cbor.RawMessage
	if err := cbor.Unmarshal(req.Msg.GetParametersCbor(), &parameters); err != nil || parameters == nil {
		return nil, invalidTask("parameters must be an object")
	}
	var name string
	if err := cbor.Unmarshal(parameters["name"], &name); err != nil || strings.TrimSpace(name) == "" || utf8.RuneCountInString(name) > 100 {
		return nil, invalidTask("name must be a nonempty string of at most 100 characters")
	}
	result, err := cbor.Marshal("Hello, " + name + "!")
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&lutrav1.GreetResponse{
		ContentType: "application/cbor",
		ResultCbor:  result,
	}), nil
}
