package taskstdio

import (
	"errors"
	"os/exec"

	"connectrpc.com/connect"
	taskv1connect "github.com/brian14708/lutra/gen/lutra/task/v1/taskv1connect"
)

// Process owns a task host and its stdio Connect transport.
type Process struct {
	Command   *exec.Cmd
	Transport *Transport
}

// Start launches a configured command. The caller may set Stderr before Start.
func Start(command *exec.Cmd) (*Process, error) {
	input, err := command.StdoutPipe()
	if err != nil {
		return nil, err
	}
	output, err := command.StdinPipe()
	if err != nil {
		_ = input.Close()
		return nil, err
	}
	if err := command.Start(); err != nil {
		_ = input.Close()
		_ = output.Close()
		return nil, err
	}
	return &Process{Command: command, Transport: NewTransport(input, output)}, nil
}

// Client returns the generated task client configured for protobuf JSON.
func (p *Process) Client() taskv1connect.TaskServiceClient {
	return taskv1connect.NewTaskServiceClient(
		p.Transport,
		"http://stdio",
		connect.WithProtoJSON(),
		connect.WithReadMaxBytes(maxLine),
		connect.WithSendMaxBytes(maxLine),
	)
}

// Close ends the transport and waits for the child process.
func (p *Process) Close() error {
	writeErr := p.Transport.CloseWrite()
	waitErr := p.Command.Wait()
	return errors.Join(writeErr, waitErr, p.Transport.Shutdown())
}
