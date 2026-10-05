package taskstdio

import (
	"errors"
	"io"
	"os/exec"
	"sync"

	"connectrpc.com/connect"
	taskv1connect "github.com/brian14708/lutra/gen/lutra/task/v1/taskv1connect"
)

// Process owns a task host and its stdio Connect transport.
type Process struct {
	Command   *exec.Cmd
	Transport *Transport
	stderr    *stderrTail
}

const maxStderrTail = 32 << 10

type stderrTail struct {
	mu   sync.Mutex
	data []byte
}

func (b *stderrTail) Write(data []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	n := len(data)
	if n >= maxStderrTail {
		b.data = append(b.data[:0], data[n-maxStderrTail:]...)
	} else {
		if excess := len(b.data) + n - maxStderrTail; excess > 0 {
			copy(b.data, b.data[excess:])
			b.data = b.data[:len(b.data)-excess]
		}
		b.data = append(b.data, data...)
	}
	return n, nil
}

// StderrTail returns the last 32 KiB of process diagnostics. Close drains stderr
// before returning, so callers can include the final traceback in a failure.
func (p *Process) StderrTail() string {
	p.stderr.mu.Lock()
	defer p.stderr.mu.Unlock()
	return string(p.stderr.data)
}

func (p *Process) ResetStderrTail() {
	p.stderr.mu.Lock()
	p.stderr.data = p.stderr.data[:0]
	p.stderr.mu.Unlock()
}

// Start launches a configured command. The caller may set Stderr before Start.
func Start(command *exec.Cmd) (*Process, error) {
	stderr := &stderrTail{}
	if command.Stderr == nil {
		command.Stderr = stderr
	} else {
		command.Stderr = io.MultiWriter(stderr, command.Stderr)
	}
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
	return &Process{Command: command, Transport: NewTransport(input, output), stderr: stderr}, nil
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
