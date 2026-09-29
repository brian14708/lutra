package taskstdio

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/textproto"
)

type reverseCall struct {
	path     string
	headers  http.Header
	messages []json.RawMessage
	ctx      context.Context
	cancel   context.CancelFunc
}

// SetReverseHandler serves Python-initiated unary Connect JSON calls.
func (t *Transport) SetReverseHandler(handler http.Handler) {
	t.mu.Lock()
	t.reverseHandler = handler
	t.mu.Unlock()
}

func (t *Transport) handleReverseFrame(f frame) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.err != nil {
		return
	}
	if f.Type == "request" {
		if _, exists := t.reverseCalls[f.ID]; exists {
			return
		}
		ctx, cancel := context.WithCancel(context.Background())
		headers := make(http.Header, len(f.Headers))
		for key, values := range f.Headers {
			headers[textproto.CanonicalMIMEHeaderKey(key)] = values
		}
		c := &reverseCall{path: f.Path, headers: headers, ctx: ctx, cancel: cancel, messages: []json.RawMessage{f.Value}}
		t.reverseCalls[f.ID] = c
		handler := t.reverseHandler
		t.workers.Add(1)
		go func() {
			defer t.workers.Done()
			t.serveReverse(f.ID, c, handler)
		}()
		return
	}
	c := t.reverseCalls[f.ID]
	if c == nil {
		return
	}
	switch f.Type {
	case "cancel":
		c.cancel()
		delete(t.reverseCalls, f.ID)
	}
}

func (t *Transport) serveReverse(id string, c *reverseCall, handler http.Handler) {
	defer func() {
		c.cancel()
		t.mu.Lock()
		delete(t.reverseCalls, id)
		t.mu.Unlock()
	}()
	if len(c.messages) != 1 {
		t.sendReverseError(id, http.StatusBadRequest, "invalid_argument", "reverse unary call requires one message")
		return
	}
	if handler == nil {
		t.sendReverseError(id, http.StatusNotFound, "unimplemented", "reverse handler is unavailable")
		return
	}
	if len(c.path) == 0 || c.path[0] != '/' {
		t.sendReverseError(id, http.StatusBadRequest, "invalid_argument", "reverse path must be absolute")
		return
	}
	req, err := http.NewRequestWithContext(c.ctx, http.MethodPost, "http://stdio"+c.path, bytes.NewReader(c.messages[0]))
	if err != nil {
		t.sendReverseError(id, http.StatusBadRequest, "invalid_argument", "invalid reverse path")
		return
	}
	req.Header = c.headers
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)
	result := recorder.Result()
	defer func() { _ = result.Body.Close() }()
	if c.ctx.Err() != nil {
		return
	}
	body := recorder.Body.Bytes()
	if len(body) > 0 && !json.Valid(body) {
		t.sendReverseError(id, http.StatusInternalServerError, "internal", "reverse handler returned non-JSON")
		return
	}
	response := frame{ID: id, Type: "response", Status: result.StatusCode, Headers: result.Header}
	if result.StatusCode == http.StatusOK {
		response.Value = json.RawMessage(body)
	} else {
		response.Error = json.RawMessage(body)
	}
	if err := t.send(response); err != nil {
		t.fail(err)
	}
}

func (t *Transport) sendReverseError(id string, status int, code, message string) {
	value, _ := json.Marshal(map[string]string{"code": code, "message": message})
	if err := t.send(frame{ID: id, Type: "response", Status: status, Headers: http.Header{"Content-Type": {"application/json"}}, Error: value}); err != nil {
		t.fail(err)
	}
}
