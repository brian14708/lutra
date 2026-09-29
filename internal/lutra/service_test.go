package lutra

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"connectrpc.com/connect"
	lutrav1 "github.com/brian14708/lutra/gen/lutra/v1"
	lutrav1connect "github.com/brian14708/lutra/gen/lutra/v1/lutrav1connect"
)

func sha256Bytes(value []byte) []byte {
	digest := sha256.Sum256(value)
	return digest[:]
}

type testBlobStore struct {
	lutrav1connect.UnimplementedBlobServiceHandler
	putURL  string
	digest  []byte
	uri     string
	pending []byte
	stored  []byte
	uploads int
}

func (s *testBlobStore) CreateUpload(_ context.Context, req *connect.Request[lutrav1.CreateUploadRequest]) (*connect.Response[lutrav1.CreateUploadResponse], error) {
	if req.Msg.GetMimeType() != "text/plain; charset=utf-8" {
		return nil, errors.New("unexpected blob MIME type")
	}
	s.digest = req.Msg.GetContentSha256()
	if len(s.digest) != 32 {
		return nil, errors.New("invalid upload URI digest")
	}
	s.uri = "blob:" + req.Msg.GetMimeType() + "," + base64.StdEncoding.EncodeToString(s.digest)
	if len(s.stored) > 0 && string(s.digest) == string(sha256Bytes(s.stored)) {
		return connect.NewResponse(&lutrav1.CreateUploadResponse{AlreadyExists: true}), nil
	}
	s.uploads++
	return connect.NewResponse(&lutrav1.CreateUploadResponse{SessionId: "session", PartCount: 1}), nil
}

func (s *testBlobStore) PresignPart(_ context.Context, req *connect.Request[lutrav1.PresignPartRequest]) (*connect.Response[lutrav1.PresignPartResponse], error) {
	if req.Msg.GetSessionId() != "session" || req.Msg.GetPartNumber() != 1 {
		return nil, errors.New("unexpected upload part")
	}
	return connect.NewResponse(&lutrav1.PresignPartResponse{Url: s.putURL, Headers: map[string]string{"Content-Type": "text/plain; charset=utf-8"}}), nil
}

func (s *testBlobStore) CompleteUpload(context.Context, *connect.Request[lutrav1.CompleteUploadRequest]) (*connect.Response[lutrav1.CompleteUploadResponse], error) {
	if string(sha256Bytes(s.pending)) != string(s.digest) {
		return nil, errors.New("uploaded bytes differ from digest")
	}
	s.stored = s.pending
	return connect.NewResponse(&lutrav1.CompleteUploadResponse{Uri: s.uri}), nil
}

func TestRunTaskStoresPythonGreeting(t *testing.T) {
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Join(filepath.Dir(file), "..", "..")
	python := filepath.Join(root, ".venv", "bin", "python")
	if _, err := os.Stat(python); err != nil {
		t.Skip("project Python environment is unavailable")
	}
	store := &testBlobStore{}
	putServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPut {
			t.Errorf("upload method = %s", req.Method)
		}
		body, err := io.ReadAll(req.Body)
		if err != nil {
			t.Error(err)
		}
		store.pending = body
		w.WriteHeader(http.StatusOK)
	}))
	defer putServer.Close()
	store.putURL = putServer.URL
	_, reverseHandler := lutrav1connect.NewBlobServiceHandler(store)
	service := Service{PythonPath: python, ReverseHandler: reverseHandler}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	input := []byte{0xa1, 0x64, 'n', 'a', 'm', 'e', 0x63, 'A', 'd', 'a'}
	for range 2 {
		response, err := service.RunTask(ctx, connect.NewRequest(&lutrav1.RunTaskRequest{TaskName: "hello", ParametersCbor: input}))
		if err != nil {
			t.Fatalf("RunTask failed: %v", err)
		}
		if response.Msg.GetContentType() != "application/cbor" || string(response.Msg.GetResultCbor()) == "" {
			t.Fatalf("unexpected blob result: %v, stored %q", response.Msg, store.stored)
		}
	}
	if store.uploads != 0 {
		t.Fatalf("expected embedded greeting without upload, got %d uploads", store.uploads)
	}
	_, err := service.RunTask(ctx, connect.NewRequest(&lutrav1.RunTaskRequest{TaskName: "hello", ParametersCbor: []byte{0xff}}))
	if connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("invalid CBOR returned %v", err)
	}
}
