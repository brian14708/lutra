// Package lutra implements the Lutra service business logic.
package lutra

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/exec"
	"time"

	"connectrpc.com/connect"
	taskv1 "github.com/brian14708/lutra/gen/lutra/task/v1"
	lutrav1 "github.com/brian14708/lutra/gen/lutra/v1"
	"github.com/brian14708/lutra/internal/taskstdio"
	"github.com/google/uuid"
)

// Service implements the core Lutra ConnectRPC API.
type Service struct {
	PythonPath     string
	ReverseHandler http.Handler
}

// RunTask executes the built-in hello task in a fresh Python subprocess.
func (s Service) RunTask(ctx context.Context, req *connect.Request[lutrav1.RunTaskRequest]) (*connect.Response[lutrav1.RunTaskResponse], error) {
	if req.Msg.GetTaskName() != "hello" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("unknown task name"))
	}
	parameters := req.Msg.GetParametersCbor()
	if s.ReverseHandler == nil {
		return nil, connect.NewError(connect.CodeUnavailable, errors.New("blob service unavailable to task"))
	}
	pythonPath := s.PythonPath
	if pythonPath == "" {
		pythonPath = ".venv/bin/python"
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, pythonPath, "-c", "from lutra.task_host import main; main()", "lutra.hello:hello")
	command.Stderr = os.Stderr
	process, err := taskstdio.Start(command)
	if err != nil {
		return nil, connect.NewError(connect.CodeUnavailable, err)
	}
	process.Transport.SetReverseHandler(s.ReverseHandler)
	result, callErr := process.Client().Execute(ctx, connect.NewRequest(&taskv1.ExecuteRequest{
		InvocationId: uuid.NewString(),
		ContentType:  "application/cbor",
		Input:        parameters,
	}))
	closeErr := process.Close()
	if callErr != nil {
		return nil, callErr
	}
	if closeErr != nil {
		return nil, connect.NewError(connect.CodeUnavailable, closeErr)
	}
	return connect.NewResponse(&lutrav1.RunTaskResponse{
		ContentType: result.Msg.GetContentType(),
		ResultCbor:  result.Msg.GetOutput(),
	}), nil
}
