package lutra

import (
	"path/filepath"
	"testing"

	"connectrpc.com/connect"
	"connectrpc.com/grpchealth"
	lutrav1connect "github.com/brian14708/lutra/gen/lutra/v1/lutrav1connect"
)

func TestExecutorLockPreventsConcurrentRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "executor.lock")
	first, err := lockExecutorFile(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = first.Close() }()
	if second, err := lockExecutorFile(path); err == nil {
		_ = second.Close()
		t.Fatal("second executor acquired an active lock")
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := lockExecutorFile(path)
	if err != nil {
		t.Fatalf("restart could not acquire released lock: %v", err)
	}
	_ = restarted.Close()
}

func TestEngineReadiness(t *testing.T) {
	e := &Engine{}
	for _, ready := range []bool{false, true} {
		e.ready.Store(ready)
		for _, service := range []string{"", lutrav1connect.LutraServiceName, lutrav1connect.BlobServiceName} {
			response, err := e.Check(t.Context(), &grpchealth.CheckRequest{Service: service})
			want := grpchealth.StatusServing
			if !ready && service != lutrav1connect.BlobServiceName {
				want = grpchealth.StatusNotServing
			}
			if err != nil || response.Status != want {
				t.Fatalf("ready %v, service %q: %v, %v", ready, service, response, err)
			}
		}
	}
	if _, err := e.Check(t.Context(), &grpchealth.CheckRequest{Service: "unknown"}); connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("unknown service: %v", err)
	}
}
