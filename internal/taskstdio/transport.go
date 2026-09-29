// Package taskstdio carries Connect JSON RPCs over a multiplexed NDJSON pipe pair.
package taskstdio

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/textproto"
	"os"
	"strconv"
	"strings"
	"sync"

	"connectrpc.com/connect"
)

const maxLine = 32 << 20

type frame struct {
	ID       string          `json:"id"`
	Type     string          `json:"type"`
	Path     string          `json:"path,omitempty"`
	Headers  http.Header     `json:"headers,omitempty"`
	Status   int             `json:"status,omitempty"`
	Value    json.RawMessage `json:"value,omitempty"`
	Trailers http.Header     `json:"trailers,omitempty"`
	Error    json.RawMessage `json:"error,omitempty"`
}

type callResult struct {
	response *http.Response
	err      error
}

type call struct {
	ready          chan callResult
	reader         *io.PipeReader
	writer         *io.PipeWriter
	requestStream  bool
	responseStream bool
	queueMu        sync.Mutex
	queue          []frame
	notify         chan struct{}
	done           chan struct{}
	closeOnce      sync.Once
}

func (c *call) push(f frame) {
	c.queueMu.Lock()
	c.queue = append(c.queue, f)
	c.queueMu.Unlock()
	select {
	case c.notify <- struct{}{}:
	default:
	}
}

func (c *call) pop() (frame, bool) {
	c.queueMu.Lock()
	defer c.queueMu.Unlock()
	if len(c.queue) == 0 {
		return frame{}, false
	}
	f := c.queue[0]
	c.queue[0] = frame{}
	c.queue = c.queue[1:]
	return f, true
}

func (c *call) close() {
	c.closeOnce.Do(func() { close(c.done) })
}

// Transport implements connect.HTTPClient for JSON-encoded Connect RPCs.
type Transport struct {
	input          io.ReadCloser
	output         io.WriteCloser
	write          sync.Mutex
	mu             sync.Mutex
	calls          map[string]*call
	reverseCalls   map[string]*reverseCall
	reverseHandler http.Handler
	next           uint64
	err            error
	workers        sync.WaitGroup
}

func NewTransport(input io.ReadCloser, output io.WriteCloser) *Transport {
	t := &Transport{input: input, output: output, calls: make(map[string]*call), reverseCalls: make(map[string]*reverseCall)}
	t.workers.Add(1)
	go func() {
		defer t.workers.Done()
		t.readLoop()
	}()
	return t
}

func (t *Transport) send(f frame) error {
	data, err := json.Marshal(f)
	if err != nil {
		return err
	}
	if len(data) >= maxLine {
		return errors.New("stdio frame exceeds limit")
	}
	t.write.Lock()
	defer t.write.Unlock()
	_, err = t.output.Write(append(data, '\n'))
	return err
}

func (t *Transport) Do(req *http.Request) (*http.Response, error) {
	if req.Method != http.MethodPost {
		return nil, fmt.Errorf("stdio transport supports POST only: %s", req.Method)
	}
	contentType := req.Header.Get("Content-Type")
	if contentType != "application/json" && contentType != "application/connect+json" {
		return nil, fmt.Errorf("stdio transport requires Connect JSON: %s", contentType)
	}
	reader, writer := io.Pipe()
	c := &call{ready: make(chan callResult, 1), reader: reader, writer: writer, requestStream: strings.HasPrefix(contentType, "application/connect+"), notify: make(chan struct{}, 1), done: make(chan struct{})}
	t.mu.Lock()
	if t.err != nil {
		err := t.err
		t.mu.Unlock()
		_ = reader.Close()
		_ = writer.Close()
		return nil, err
	}
	t.next++
	id := "g" + strconv.FormatUint(t.next, 10)
	t.calls[id] = c
	t.workers.Add(2)
	t.mu.Unlock()
	if err := t.send(frame{ID: id, Type: "open", Path: req.URL.Path, Headers: req.Header}); err != nil {
		t.remove(id)
		_ = reader.Close()
		_ = writer.Close()
		t.workers.Done()
		t.workers.Done()
		return nil, err
	}
	go func() {
		defer t.workers.Done()
		t.receiveCall(id, c, req)
	}()
	go func() {
		defer t.workers.Done()
		t.sendRequest(id, req, c.requestStream)
	}()
	select {
	case result := <-c.ready:
		return result.response, result.err
	case <-req.Context().Done():
		t.cancel(id)
		return nil, req.Context().Err()
	}
}

func (t *Transport) sendRequest(id string, req *http.Request, stream bool) {
	defer func() { _ = req.Body.Close() }()
	if stream {
		for {
			var prefix [5]byte
			_, err := io.ReadFull(req.Body, prefix[:])
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil || prefix[0] != 0 {
				t.cancel(id)
				return
			}
			size := binary.BigEndian.Uint32(prefix[1:])
			if size >= maxLine {
				t.cancel(id)
				return
			}
			body := make([]byte, size)
			if _, err := io.ReadFull(req.Body, body); err != nil {
				t.cancel(id)
				return
			}
			if err := t.send(frame{ID: id, Type: "message", Value: body}); err != nil {
				t.fail(err)
				return
			}
		}
	} else {
		body, err := io.ReadAll(io.LimitReader(req.Body, maxLine))
		if err != nil {
			t.cancel(id)
			return
		}
		if err := t.send(frame{ID: id, Type: "message", Value: body}); err != nil {
			t.fail(err)
			return
		}
	}
	if err := t.send(frame{ID: id, Type: "half_close"}); err != nil {
		t.fail(err)
	}
}

func (t *Transport) receiveCall(id string, c *call, req *http.Request) {
	defer t.remove(id)
	defer func() { _ = c.writer.Close() }()
	started := false
	defer func() {
		if !started {
			c.ready <- callResult{err: errors.New("stdio response ended before headers")}
		}
	}()
	for {
		f, hasFrame := c.pop()
		if !hasFrame {
			select {
			case <-req.Context().Done():
				_ = c.writer.CloseWithError(req.Context().Err())
				_ = req.Body.Close()
				t.cancel(id)
				return
			case <-c.done:
				_ = c.writer.CloseWithError(errors.New("stdio connection closed"))
				_ = req.Body.Close()
				return
			case <-c.notify:
				continue
			}
		}
		switch f.Type {
		case "headers":
			if started {
				return
			}
			started = true
			headers := make(http.Header, len(f.Headers))
			for key, values := range f.Headers {
				headers[textproto.CanonicalMIMEHeaderKey(key)] = values
			}
			c.responseStream = strings.HasPrefix(headers.Get("Content-Type"), "application/connect+")
			c.ready <- callResult{response: &http.Response{StatusCode: f.Status, Proto: "HTTP/2.0", ProtoMajor: 2, Header: headers, Body: c.reader, Request: req}}
		case "message":
			if !started {
				return
			}
			body := []byte(f.Value)
			if c.responseStream {
				body = envelope(0, body)
			}
			if _, err := c.writer.Write(body); err != nil {
				return
			}
		case "error":
			if _, err := c.writer.Write(f.Value); err != nil {
				return
			}
		case "end":
			if c.responseStream {
				payload := map[string]any{}
				if len(f.Trailers) > 0 {
					payload["metadata"] = f.Trailers
				}
				if len(f.Error) > 0 && string(f.Error) != "null" {
					payload["error"] = f.Error
				}
				body, _ := json.Marshal(payload)
				_, _ = c.writer.Write(envelope(2, body))
			}
			return
		}
	}
}

func envelope(flags byte, payload []byte) []byte {
	data := make([]byte, 5+len(payload))
	data[0] = flags
	binary.BigEndian.PutUint32(data[1:], uint32(len(payload)))
	copy(data[5:], payload)
	return data
}

func (t *Transport) readLoop() {
	scanner := bufio.NewScanner(t.input)
	scanner.Buffer(make([]byte, 64*1024), maxLine)
	for scanner.Scan() {
		var f frame
		if err := json.Unmarshal(scanner.Bytes(), &f); err != nil || f.ID == "" {
			t.fail(errors.New("invalid stdio response frame"))
			return
		}
		if strings.HasPrefix(f.ID, "p") {
			t.handleReverseFrame(f)
			continue
		}
		t.mu.Lock()
		c := t.calls[f.ID]
		if c != nil {
			c.push(f)
		}
		t.mu.Unlock()
	}
	if err := scanner.Err(); err != nil {
		t.fail(err)
	} else {
		t.fail(io.EOF)
	}
}

func (t *Transport) remove(id string) {
	t.mu.Lock()
	if c := t.calls[id]; c != nil {
		c.close()
	}
	delete(t.calls, id)
	t.mu.Unlock()
}

func (t *Transport) cancel(id string) {
	_ = t.send(frame{ID: id, Type: "cancel"})
	t.remove(id)
}

func (t *Transport) fail(err error) {
	t.mu.Lock()
	if t.err == nil {
		t.err = err
		for _, c := range t.calls {
			c.close()
		}
		for _, c := range t.reverseCalls {
			c.cancel()
		}
	}
	t.mu.Unlock()
}

func (t *Transport) closeResponses(err error) {
	t.mu.Lock()
	for _, c := range t.calls {
		_ = c.reader.CloseWithError(err)
	}
	t.mu.Unlock()
}

// CloseWrite closes the stream carrying requests to the peer.
func (t *Transport) CloseWrite() error {
	err := t.output.Close()
	if errors.Is(err, os.ErrClosed) {
		return nil
	}
	return err
}

// Shutdown fails all in-flight calls, closes the inbound stream, and waits
// for the transport's workers to finish.
func (t *Transport) Shutdown() error {
	t.fail(io.ErrClosedPipe)
	t.closeResponses(io.ErrClosedPipe)
	err := t.input.Close()
	if errors.Is(err, os.ErrClosed) {
		err = nil
	}
	t.workers.Wait()
	return err
}

func (t *Transport) Close() error {
	return errors.Join(t.CloseWrite(), t.Shutdown())
}

var _ connect.HTTPClient = (*Transport)(nil)
