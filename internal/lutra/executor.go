package lutra

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"

	"connectrpc.com/connect"
	taskv1 "github.com/brian14708/lutra/gen/lutra/task/v1"
	lutrav1 "github.com/brian14708/lutra/gen/lutra/v1"
	"github.com/brian14708/lutra/internal/blob"
	"github.com/brian14708/lutra/internal/result"
	"github.com/brian14708/lutra/internal/taskstdio"
)

// EnvironmentExecution is the executor-neutral request. Resolved artifact
// builds are deliberately absent; image builds choose those separately.
type EnvironmentExecution struct {
	Environment    *lutrav1.EnvironmentIdentifier
	Provider       string
	Spec           *lutrav1.EnvironmentSpec
	EntrypointID   uint32
	Input          []byte
	Environments   []*lutrav1.EnvironmentIdentifier
	RunID          string
	ActionID       string
	Attempt        int32
	Config         RunConfigSnapshot
	Output         LogSink
	TaskAPIHandler http.Handler
}

type LogPhase string

const (
	LogTask  LogPhase = "task"
	LogBuild LogPhase = "build"
	LogPull  LogPhase = "pull"
)

// LogSink keeps output framing and persistence outside the executor.
type LogSink interface{ Writer(LogPhase) io.Writer }

type logSinkFunc func(LogPhase) io.Writer

func (f logSinkFunc) Writer(phase LogPhase) io.Writer { return f(phase) }

func (r *EnvironmentExecution) output(phase LogPhase) io.Writer {
	if r.Output == nil {
		return io.Discard
	}
	return r.Output.Writer(phase)
}

type TaskError struct{ Message string }

func (e *TaskError) Error() string { return e.Message }

type TerminalTaskError struct {
	Message string
	Code    int
}

func (e *TerminalTaskError) Error() string { return e.Message }

type CacheableError struct {
	Code    string
	Details []byte
}

var cacheErrorCodePattern = regexp.MustCompile(`^[a-z][a-z0-9_.-]{0,127}$`)

func (e *CacheableError) Error() string { return e.Code }
func (*CacheableError) Cacheable() bool { return true }

func (e *CacheableError) output() ([]byte, error) {
	return result.EncodeFailure(result.Failure{Cacheable: true, Message: e.Code, Details: e.Details})
}

// Image references an artifact resolved by the selected runner.
type Image struct {
	ArtifactURI string
}

// Executor builds images and starts tasks in its runtime.
type Executor interface {
	ImageKey(spec *lutrav1.EnvironmentSpec) ([]byte, error)
	Build(context.Context, *EnvironmentExecution) (*Image, error)
	Run(context.Context, *Image, *EnvironmentExecution) (Job, error)
}

// Job is a started task execution. The provider owns its lifetime: Wait
// blocks until the task finishes, releasing resources, and Kill terminates it
// early, e.g. on cancellation. Recovery replays tasks in fresh sandboxes.
type Job interface {
	ID() string
	Wait(context.Context) ([]byte, error)
	Kill(context.Context) error
}

func executeProcess(ctx context.Context, process *taskstdio.Process, req *EnvironmentExecution) ([]byte, error) {
	response, callErr := process.Client().Execute(ctx, connect.NewRequest(&taskv1.ExecuteRequest{InvocationId: req.ActionID, RunId: req.RunID, ActionId: req.ActionID, Attempt: req.Attempt, ContentType: "application/cbor", Input: req.Input}))
	closeErr := process.Close()
	if err := errors.Join(callErr, closeErr); err != nil {
		return nil, processError(process, req, err)
	}
	return decodeTaskResult(response.Msg, req)
}

func processError(process *taskstdio.Process, req *EnvironmentExecution, err error) error {
	if stderr := strings.TrimSpace(process.StderrTail()); stderr != "" {
		return errors.New(req.Config.filter().String(fmt.Sprintf("%s\ntask stderr (last 32 KiB):\n%s", err, stderr)))
	}
	return req.Config.redactError(err)
}

func decodeTaskResult(response *taskv1.ExecuteResponse, req *EnvironmentExecution) ([]byte, error) {
	filter := req.Config.filter()
	raw := response.GetResultCbor()
	failure, tagged, err := result.DecodeFailure(raw)
	if err != nil {
		return nil, err
	}
	if tagged {
		message := filter.String(failure.Message)
		details, err := filter.CBOR(failure.Details)
		if err != nil {
			return nil, &ConfigError{Code: "config.invalid"}
		}
		if failure.Cacheable {
			if !cacheErrorCodePattern.MatchString(message) || len(details) > 64<<10 {
				return nil, errors.New("task returned invalid cacheable error")
			}
			failure := &CacheableError{Code: message, Details: details}
			encoded, err := failure.output()
			if err != nil {
				return nil, err
			}
			return encoded, failure
		}
		if failure.Terminal {
			encoded, err := result.EncodeFailure(result.Failure{Terminal: true, Code: failure.Code, Message: message, Details: details})
			if err != nil {
				return nil, err
			}
			return encoded, &TerminalTaskError{Message: message, Code: failure.Code}
		}
		if message == "config.missing" || message == "config.invalid" {
			return nil, &ConfigError{Code: message}
		}
		return nil, &TaskError{Message: message}
	}
	if response.GetContentType() != "application/cbor" {
		return nil, errors.New("task returned unsupported content type")
	}
	return filter.CBOR(raw)
}

func unpackSource(ctx context.Context, openBundle func(context.Context, []byte) (io.ReadCloser, error), uri, dir string) error {
	digest, mimeType, err := blob.ParseURI(uri)
	if err != nil || mimeType != archiveMIME || uri != sourceURI(digest) {
		return invalid("invalid source bundle URI")
	}
	if openBundle == nil {
		return errors.New("source blob store unavailable")
	}
	input, err := openBundle(ctx, digest)
	if err != nil {
		return err
	}
	defer func() { _ = input.Close() }()
	limited := &io.LimitedReader{R: input, N: maxSourceSize + 1}
	hash := sha256.New()
	verified := io.TeeReader(limited, hash)
	if err := extractBundle(verified, dir); err != nil {
		return err
	}
	if _, err := io.Copy(io.Discard, verified); err != nil {
		return err
	}
	if limited.N == 0 {
		return invalid("source bundle exceeds size limit")
	}
	if !bytes.Equal(hash.Sum(nil), digest) {
		return invalid("source bundle checksum mismatch")
	}
	return nil
}

func ensureImage(ctx context.Context, imageKey []byte, executor Executor, req *EnvironmentExecution) (*Image, error) {
	if len(imageKey) != 32 {
		return nil, invalid("invalid image key")
	}
	currentKey, err := executor.ImageKey(req.Spec)
	if err != nil {
		return nil, invalid(err.Error())
	}
	if !bytes.Equal(imageKey, currentKey) {
		return nil, invalid("image recipe differs from registered environment")
	}
	return executor.Build(ctx, req)
}
