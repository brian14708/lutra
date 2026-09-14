// Package lutra implements the Lutra service business logic.
package lutra

import (
	"context"

	"connectrpc.com/connect"
	lutrav1 "github.com/brian14708/lutra/gen/lutra/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Service implements the Lutra ConnectRPC API.
type Service struct{}

// Ping echoes the request message, defaulting to "pong".
func (Service) Ping(_ context.Context, req *connect.Request[lutrav1.PingRequest]) (*connect.Response[lutrav1.PingResponse], error) {
	message := req.Msg.GetMessage()
	if message == "" {
		message = "pong"
	}
	return connect.NewResponse(&lutrav1.PingResponse{
		Message:   message,
		Timestamp: timestamppb.Now(),
	}), nil
}
