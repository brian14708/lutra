package taskstdio

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/textproto"
)

type incomingCall struct {
	path     string
	headers  http.Header
	messages []json.RawMessage
	ctx      context.Context
	cancel   context.CancelFunc
}

// SetHandler serves Python-initiated unary Connect JSON calls.
func (t *Transport) SetHandler(handler http.Handler) {
	t.mu.Lock()
	t.handler = handler
	t.mu.Unlock()
}

func (t *Transport) handleIncomingFrame(f frame) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.err != nil {
		return
	}
	if f.Type == "request" {
		if _, exists := t.incomingCalls[f.ID]; exists {
			return
		}
		ctx, cancel := context.WithCancel(context.Background())
		headers := make(http.Header, len(f.Headers))
		for key, values := range f.Headers {
			headers[textproto.CanonicalMIMEHeaderKey(key)] = values
		}
		c := &incomingCall{path: f.Path, headers: headers, ctx: ctx, cancel: cancel, messages: []json.RawMessage{f.Value}}
		t.incomingCalls[f.ID] = c
		handler := t.handler
		t.workers.Add(1)
		go func() {
			defer t.workers.Done()
			t.serveIncoming(f.ID, c, handler)
		}()
		return
	}
	c := t.incomingCalls[f.ID]
	if c == nil {
		return
	}
	switch f.Type {
	case "cancel":
		c.cancel()
		delete(t.incomingCalls, f.ID)
	}
}

func (t *Transport) serveIncoming(id string, c *incomingCall, handler http.Handler) {
	defer func() {
		c.cancel()
		t.mu.Lock()
		delete(t.incomingCalls, id)
		t.mu.Unlock()
	}()
	if len(c.messages) != 1 {
		t.sendIncomingError(id, http.StatusBadRequest, "invalid_argument", "incoming unary call requires one message")
		return
	}
	if handler == nil {
		t.sendIncomingError(id, http.StatusNotFound, "unimplemented", "incoming handler is unavailable")
		return
	}
	if len(c.path) == 0 || c.path[0] != '/' {
		t.sendIncomingError(id, http.StatusBadRequest, "invalid_argument", "incoming path must be absolute")
		return
	}
	req, err := http.NewRequestWithContext(c.ctx, http.MethodPost, "http://stdio"+c.path, bytes.NewReader(c.messages[0]))
	if err != nil {
		t.sendIncomingError(id, http.StatusBadRequest, "invalid_argument", "invalid incoming path")
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
		t.sendIncomingError(id, http.StatusInternalServerError, "internal", "incoming handler returned non-JSON")
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

func (t *Transport) sendIncomingError(id string, status int, code, message string) {
	value, _ := json.Marshal(map[string]string{"code": code, "message": message})
	if err := t.send(frame{ID: id, Type: "response", Status: status, Headers: http.Header{"Content-Type": {"application/json"}}, Error: value}); err != nil {
		t.fail(err)
	}
}
