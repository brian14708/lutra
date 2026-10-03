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
	"github.com/brian14708/lutra/internal/taskstdio"
	"github.com/fxamacker/cbor/v2"
)

// EnvironmentExecution is the executor-neutral request. Resolved artifact
// builds are deliberately absent; image builds choose those separately.
type EnvironmentExecution struct {
	Environment  *lutrav1.EnvironmentIdentifier
	Provider     string
	Spec         *lutrav1.EnvironmentSpec
	EntrypointID uint32
	Input        []byte
	Environments []*lutrav1.EnvironmentIdentifier
	RunID        string
	ActionID     string
	Attempt      int32
	// Stderr receives build and task output. TaskAPIHandler answers the
	// task's callbacks; remote providers may inject direct API access instead.
	Stderr         io.Writer
	PullOutput     io.Writer
	TaskAPIHandler http.Handler
}

type CacheableError struct {
	Code    string
	Details []byte
}

var cacheErrorCodePattern = regexp.MustCompile(`^[a-z][a-z0-9_.-]{0,127}$`)

func (e *CacheableError) Error() string { return e.Code }
func (*CacheableError) Cacheable() bool { return true }

func (e *CacheableError) output() ([]byte, error) {
	mode, err := cbor.CanonicalEncOptions().EncMode()
	if err != nil {
		return nil, err
	}
	return mode.Marshal([]any{"lutra.cacheable-error.v1", e.Code, cbor.RawMessage(e.Details)})
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
	result, callErr := process.Client().Execute(ctx, connect.NewRequest(&taskv1.ExecuteRequest{InvocationId: req.ActionID, RunId: req.RunID, ActionId: req.ActionID, Attempt: req.Attempt, ContentType: "application/cbor", Input: req.Input}))
	closeErr := process.Close()
	if err := errors.Join(callErr, closeErr); err != nil {
		if stderr := strings.TrimSpace(process.StderrTail()); stderr != "" {
			return nil, fmt.Errorf("%w\ntask stderr (last 32 KiB):\n%s", err, stderr)
		}
		return nil, err
	}
	if result.Msg.GetErrorCode() != "" {
		if !cacheErrorCodePattern.MatchString(result.Msg.GetErrorCode()) || len(result.Msg.GetErrorDetails()) > 64<<10 || len(result.Msg.GetOutput()) != 0 {
			return nil, errors.New("task returned invalid cacheable error")
		}
		var details any
		if err := cbor.Unmarshal(result.Msg.GetErrorDetails(), &details); err != nil {
			return nil, errors.New("task returned invalid cacheable error details")
		}
		failure := &CacheableError{Code: result.Msg.GetErrorCode(), Details: result.Msg.GetErrorDetails()}
		output, err := failure.output()
		if err != nil {
			return nil, err
		}
		return output, failure
	}
	if result.Msg.GetContentType() != "application/cbor" {
		return nil, errors.New("task returned unsupported content type")
	}
	return result.Msg.GetOutput(), nil
}

func unpackSource(ctx context.Context, openBundle func(context.Context, []byte) (io.ReadCloser, error), uri, dir string) error {
	digest, mimeType, err := blob.ParseURI(uri)
	if err != nil || mimeType != archiveMIME || uri != sourceURI(digest) {
		return errors.New("invalid source bundle URI")
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
		return errors.New("source bundle exceeds size limit")
	}
	if !bytes.Equal(hash.Sum(nil), digest) {
		return errors.New("source bundle checksum mismatch")
	}
	return nil
}

func ensureImage(ctx context.Context, imageKey []byte, executor Executor, req *EnvironmentExecution) (*Image, error) {
	if len(imageKey) != 32 {
		return nil, errors.New("invalid image key")
	}
	currentKey, err := executor.ImageKey(req.Spec)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(imageKey, currentKey) {
		return nil, errors.New("image recipe differs from registered environment")
	}
	return executor.Build(ctx, req)
}
