package taskstdio

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	taskv1 "github.com/brian14708/lutra/gen/lutra/task/v1"
)

type bufferWriteCloser struct{ bytes.Buffer }

func (*bufferWriteCloser) Close() error { return nil }

func TestMalformedIncomingPath(t *testing.T) {
	output := &bufferWriteCloser{}
	transport := &Transport{output: output, incomingCalls: make(map[string]*incomingCall)}
	ctx, cancel := context.WithCancel(context.Background())
	call := &incomingCall{
		path:     "/%",
		messages: []json.RawMessage{json.RawMessage(`{}`)},
		ctx:      ctx,
		cancel:   cancel,
	}
	handlerCalled := false
	transport.serveIncoming("p1", call, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		handlerCalled = true
	}))
	if handlerCalled {
		t.Fatal("malformed path reached incoming handler")
	}
	reader := bufio.NewScanner(bytes.NewReader(output.Bytes()))
	var frames []frame
	for reader.Scan() {
		var f frame
		if err := json.Unmarshal(reader.Bytes(), &f); err != nil {
			t.Fatal(err)
		}
		frames = append(frames, f)
	}
	if err := reader.Err(); err != nil {
		t.Fatal(err)
	}
	if len(frames) != 1 || frames[0].Status != http.StatusBadRequest || frames[0].Type != "response" || !bytes.Contains(frames[0].Error, []byte(`"invalid_argument"`)) {
		t.Fatalf("unexpected incoming error frames: %+v", frames)
	}
}

func TestCloseWaitsForIncomingHandler(t *testing.T) {
	fromServer, serverOutput := io.Pipe()
	serverInput, toServer := io.Pipe()
	defer func() { _ = serverInput.Close() }()
	transport := NewTransport(fromServer, toServer)
	started := make(chan struct{})
	finished := make(chan struct{})
	transport.SetHandler(http.HandlerFunc(func(_ http.ResponseWriter, req *http.Request) {
		close(started)
		<-req.Context().Done()
		close(finished)
	}))
	encoder := json.NewEncoder(serverOutput)
	for _, f := range []frame{
		{ID: "p1", Type: "request", Path: "/test.TaskAPI/Echo", Headers: http.Header{"Content-Type": {"application/json"}}, Value: json.RawMessage(`{}`)},
	} {
		if err := encoder.Encode(f); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("incoming handler did not start")
	}
	if err := transport.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-finished:
	default:
		t.Fatal("Close returned before incoming handler finished")
	}
	_ = serverOutput.Close()
}

func TestTransportConnectionFailure(t *testing.T) {
	for _, tc := range []struct {
		name string
		data string
	}{
		{name: "malformed frame", data: "{bad\n"},
		{name: "unexpected EOF"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fromServer, serverOutput := io.Pipe()
			serverInput, toServer := io.Pipe()
			transport := NewTransport(fromServer, toServer)
			defer func() { _ = transport.Close() }()
			go func() {
				_, _ = bufio.NewReader(serverInput).ReadBytes('\n')
				_, _ = serverOutput.Write([]byte(tc.data))
				_ = serverOutput.Close()
				_ = serverInput.Close()
			}()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			req := httptest.NewRequest(http.MethodPost, "http://stdio/test.Service/Method", strings.NewReader("{}"))
			req = req.WithContext(ctx)
			req.Header.Set("Content-Type", "application/json")
			response, err := transport.Do(req)
			if err == nil {
				_, err = io.ReadAll(response.Body)
				_ = response.Body.Close()
			}
			if err == nil {
				t.Fatal("expected connection failure")
			}
		})
	}
}

func TestResponseMessageBeforeHeaders(t *testing.T) {
	fromServer, serverOutput := io.Pipe()
	serverInput, toServer := io.Pipe()
	defer func() { _ = serverOutput.Close() }()
	defer func() { _ = serverInput.Close() }()
	transport := NewTransport(fromServer, toServer)
	defer func() { _ = transport.Close() }()

	serverDone := make(chan error, 1)
	go func() {
		if _, err := bufio.NewReader(serverInput).ReadBytes('\n'); err != nil {
			serverDone <- err
			return
		}
		_, err := io.WriteString(serverOutput, "{\"id\":\"g1\",\"type\":\"message\",\"value\":{}}\n")
		serverDone <- err
	}()

	req := httptest.NewRequest(http.MethodPost, "http://stdio/test.Service/Method", strings.NewReader("{}"))
	req.Header.Set("Content-Type", "application/json")
	result := make(chan error, 1)
	go func() {
		_, err := transport.Do(req)
		result <- err
	}()
	select {
	case err := <-result:
		if err == nil || !strings.Contains(err.Error(), "before headers") {
			t.Fatalf("expected a response frame error, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Do blocked after a response message before headers")
	}
	if err := <-serverDone; err != nil {
		t.Fatal(err)
	}
}

func TestConnectModes(t *testing.T) {
	for _, mode := range []string{"unary", "client", "server", "bidi"} {
		t.Run(mode, func(t *testing.T) {
			fromServer, serverOutput := io.Pipe()
			serverInput, toServer := io.Pipe()
			transport := NewTransport(fromServer, toServer)
			defer func() { _ = transport.Close() }()
			serverDone := make(chan error, 1)
			go func() {
				defer func() { _ = serverOutput.Close() }()
				reader := bufio.NewScanner(serverInput)
				id := ""
				count := 0
				write := func(f frame) error {
					data, err := json.Marshal(f)
					if err != nil {
						return err
					}
					_, err = serverOutput.Write(append(data, '\n'))
					return err
				}
				for reader.Scan() {
					var f frame
					if err := json.Unmarshal(reader.Bytes(), &f); err != nil {
						serverDone <- err
						return
					}
					switch f.Type {
					case "open":
						id = f.ID
						contentType := "application/connect+json"
						if mode == "unary" {
							contentType = "application/json"
						}
						if err := write(frame{ID: id, Type: "headers", Status: 200, Headers: http.Header{"content-type": {contentType}}}); err != nil {
							serverDone <- err
							return
						}
					case "message":
						count++
						if mode == "bidi" {
							if err := write(frame{ID: id, Type: "message", Value: json.RawMessage(`{"output":"YQ=="}`)}); err != nil {
								serverDone <- err
								return
							}
						}
					case "half_close":
						if (mode == "client" || mode == "bidi") && count != 2 || (mode != "client" && mode != "bidi") && count != 1 {
							serverDone <- fmt.Errorf("got %d request messages", count)
							return
						}
						n := 1
						switch mode {
						case "server":
							n = 2
						case "bidi":
							n = 0
						}
						for range n {
							if err := write(frame{ID: id, Type: "message", Value: json.RawMessage(`{"output":"YQ=="}`)}); err != nil {
								serverDone <- err
								return
							}
						}
						serverDone <- write(frame{ID: id, Type: "end"})
						return
					}
				}
				serverDone <- reader.Err()
			}()
			client := connect.NewClient[taskv1.ExecuteRequest, taskv1.ExecuteResponse](transport, "http://stdio/test.Service/Method", connect.WithProtoJSON())
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			request := &taskv1.ExecuteRequest{Input: []byte("a")}
			switch mode {
			case "unary":
				res, err := client.CallUnary(ctx, connect.NewRequest(request))
				if err != nil || string(res.Msg.GetOutput()) != "a" {
					t.Fatalf("unary: %v, %v", res, err)
				}
			case "client":
				stream := client.CallClientStream(ctx)
				for range 2 {
					if err := stream.Send(request); err != nil {
						t.Fatal(err)
					}
				}
				res, err := stream.CloseAndReceive()
				if err != nil || string(res.Msg.GetOutput()) != "a" {
					t.Fatalf("client stream: %v, %v", res, err)
				}
			case "server":
				stream, err := client.CallServerStream(ctx, connect.NewRequest(request))
				if err != nil {
					t.Fatal(err)
				}
				n := 0
				for stream.Receive() {
					n++
					if string(stream.Msg().GetOutput()) != "a" {
						t.Fatalf("server stream message: %v", stream.Msg())
					}
				}
				if err := stream.Err(); err != nil || n != 2 {
					t.Fatalf("server stream: %d, %v", n, err)
				}
			case "bidi":
				stream := client.CallBidiStream(ctx)
				for range 2 {
					if err := stream.Send(request); err != nil {
						t.Fatal(err)
					}
					response, err := stream.Receive()
					if err != nil || string(response.GetOutput()) != "a" {
						t.Fatalf("bidi message: %v, %v", response, err)
					}
				}
				if err := stream.CloseRequest(); err != nil {
					t.Fatal(err)
				}
				if _, err := stream.Receive(); !errors.Is(err, io.EOF) {
					t.Fatalf("bidi end: %v", err)
				}
			}
			if err := <-serverDone; err != nil {
				t.Fatal(err)
			}
		})
	}
}
