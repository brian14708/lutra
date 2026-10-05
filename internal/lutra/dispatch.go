package lutra

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"connectrpc.com/connect"
	lutrav1 "github.com/brian14708/lutra/gen/lutra/v1"
	"github.com/google/uuid"
)

type Dispatcher struct{ Ingress, Admin string }

// CBOR values are base64 encoded in JSON; allow overhead for inspection responses.
const maxRestateResponseBytes = 8 << 20

type restateHTTPError struct {
	Status    int
	URL, Body string
}

func (e *restateHTTPError) Error() string {
	return fmt.Sprintf("restate %s: %d %s", e.URL, e.Status, e.Body)
}

func (d *Dispatcher) request(ctx context.Context, method, url string, payload any, key string) ([]byte, error) {
	var data []byte
	if payload != nil {
		var err error
		data, err = json.Marshal(payload)
		if err != nil {
			return nil, err
		}
	}
	req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = response.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(response.Body, maxRestateResponseBytes+1))
	if err != nil {
		return nil, err
	}
	if len(body) > maxRestateResponseBytes {
		return nil, fmt.Errorf("restate response exceeds %d bytes", maxRestateResponseBytes)
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, &restateHTTPError{Status: response.StatusCode, URL: url, Body: string(body)}
	}
	return body, nil
}

func (d *Dispatcher) submit(ctx context.Context, id uuid.UUID) (string, error) {
	return d.submitDelayed(ctx, id, 0)
}

func (d *Dispatcher) submitDelayed(ctx context.Context, id uuid.UUID, delay time.Duration) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	endpoint := d.Ingress + "/" + actionService + "/" + id.String() + "/Run/send"
	if delay > 0 {
		endpoint += "?delay=" + url.QueryEscape(fmt.Sprintf("%dms", delay.Milliseconds()))
	}
	data, err := d.request(ctx, http.MethodPost, endpoint, nil, "")
	if err != nil {
		return "", err
	}
	var response struct {
		InvocationID string `json:"invocationId"`
	}
	if err := json.Unmarshal(data, &response); err != nil {
		return "", err
	}
	if response.InvocationID == "" {
		return "", errors.New("restate returned no invocation ID")
	}
	return response.InvocationID, nil
}

func (d *Dispatcher) cancelInvocation(ctx context.Context, id string) error {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	_, err := d.request(ctx, http.MethodPatch, d.Admin+"/invocations/"+id+"/cancel", nil, "")
	var response *restateHTTPError
	if errors.As(err, &response) && (response.Status == http.StatusNotFound || response.Status == http.StatusConflict) {
		return nil
	}
	return err
}

// Only call after committing canceled status. A new invocation then sees a
// terminal row and cannot launch code, even if its original acceptance was lost.
func (d *Dispatcher) cancelAction(ctx context.Context, id uuid.UUID) error {
	invocation, err := d.submit(ctx, id)
	if err != nil {
		return err
	}
	return d.cancelInvocation(ctx, invocation)
}

func dispatchFailure(runID string, err error) error {
	failure := connect.NewError(connect.CodeUnavailable, fmt.Errorf("run %s committed; retry delivery: %w", runID, err))
	detail, detailErr := connect.NewErrorDetail(&lutrav1.RunDispatchFailure{RunId: runID})
	if detailErr == nil {
		failure.AddDetail(detail)
	}
	return failure
}
