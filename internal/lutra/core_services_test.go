package lutra

import (
	"encoding/json"
	"strings"
	"testing"

	lutrav1 "github.com/brian14708/lutra/gen/lutra/v1"
	"github.com/brian14708/lutra/internal/db"
)

func TestActionStateMapsTerminalStates(t *testing.T) {
	tests := map[db.LutraActionState]lutrav1.ActionState{
		db.LutraActionStateTimedOut: lutrav1.ActionState_ACTION_STATE_TIMED_OUT,
	}
	for state, want := range tests {
		if got := actionState(state); got != want {
			t.Errorf("actionState(%q) = %v, want %v", state, got, want)
		}
	}
}

func TestWorkerEnvironmentDoesNotExposeServerCredentials(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://secret")
	t.Setenv("AWS_ACCESS_KEY_ID", "secret")
	t.Setenv("LUTRA_JWT_KEY", "secret")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://collector:4318")
	t.Setenv("OTEL_EXPORTER_OTLP_HEADERS", "authorization=secret")
	t.Setenv("PATH", "/usr/bin")

	env := workerEnvironment()
	for _, entry := range env {
		if strings.HasPrefix(entry, "DATABASE_URL=") || strings.HasPrefix(entry, "AWS_ACCESS_KEY_ID=") || strings.HasPrefix(entry, "LUTRA_JWT_KEY=") {
			t.Errorf("worker environment leaked credential %q", entry)
		}
	}
	if !containsEnv(env, "PATH=/usr/bin") {
		t.Errorf("worker environment did not preserve PATH: %v", env)
	}
	if !containsEnv(env, "OTEL_EXPORTER_OTLP_ENDPOINT=http://collector:4318") {
		t.Errorf("worker environment did not preserve OTEL endpoint: %v", env)
	}
	for _, entry := range env {
		if strings.HasPrefix(entry, "OTEL_EXPORTER_OTLP_HEADERS=") {
			t.Errorf("worker environment leaked telemetry credentials %q", entry)
		}
	}
}

func TestWorkerContextCarriesTracePropagation(t *testing.T) {
	original := workerContext{TraceParent: "00-11111111111111111111111111111111-2222222222222222-01", TraceState: "vendor=value"}
	encoded, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	var decoded workerContext
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.TraceParent != original.TraceParent || decoded.TraceState != original.TraceState {
		t.Fatalf("trace context did not round-trip: %#v", decoded)
	}
}

func containsEnv(env []string, wanted string) bool {
	for _, entry := range env {
		if entry == wanted {
			return true
		}
	}
	return false
}
