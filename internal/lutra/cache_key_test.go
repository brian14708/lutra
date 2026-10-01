package lutra

import (
	"bytes"
	"testing"

	lutrav1 "github.com/brian14708/lutra/gen/lutra/v1"
	"github.com/brian14708/lutra/internal/db"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
)

func TestTaskCacheKeyContract(t *testing.T) {
	environment := &lutrav1.EnvironmentSpec{
		Name: "work", SourceUri: "blob:source,aaa",
		Image:       &lutrav1.ImageSpec{Name: "local-python", Reference: "python:3.14"},
		Entrypoints: []*lutrav1.Entrypoint{{Command: &lutrav1.StartupCommand{Args: []string{"python", "task"}}, Cache: true}},
	}
	encoded, err := proto.Marshal(environment)
	if err != nil {
		t.Fatal(err)
	}
	row := db.LoadRunGraphRow{EnvironmentSpec: encoded, EnvironmentName: "work", EntrypointID: 1, Provider: "local", Version: "registration-a", NamespaceID: uuid.New()}
	spec := &lutrav1.ActionSpec{Cache: true, InputCbor: []byte{0x82, 0x80, 0xa0}, DependencyDigest: bytes.Repeat([]byte{7}, 32)}
	baseline := cacheKey(row, spec)
	if len(baseline) != 32 {
		t.Fatalf("invalid key length: %d", len(baseline))
	}
	otherNamespace := row
	otherNamespace.NamespaceID = uuid.New()
	otherNamespace.Version = "registration-b"
	if !bytes.Equal(baseline, cacheKey(otherNamespace, spec)) {
		t.Fatal("namespace and registration version changed the key")
	}
	changedInput := proto.Clone(spec).(*lutrav1.ActionSpec)
	changedInput.InputCbor = []byte{0x82, 0x81, 0x01, 0xa0}
	if bytes.Equal(baseline, cacheKey(row, changedInput)) {
		t.Fatal("distinct inputs shared a key despite identical dependency digest")
	}
	environment.SourceUri = "blob:source,bbb"
	row.EnvironmentSpec, err = proto.Marshal(environment)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(baseline, cacheKey(row, spec)) {
		t.Fatal("source change did not invalidate source-derived version")
	}
	spec.TaskVersion = "1.2.3"
	explicit := cacheKey(row, spec)
	row.EnvironmentSpec = encoded
	if !bytes.Equal(explicit, cacheKey(row, spec)) {
		t.Fatal("explicit version did not preserve reuse across source changes")
	}
}

func TestResolvedActionCachePolicy(t *testing.T) {
	entry := &lutrav1.Entrypoint{Cache: true, MaxAttempts: 1}
	spec := &lutrav1.ActionSpec{Cache: true, DependencyDigest: bytes.Repeat([]byte{1}, 32)}
	encoded, err := resolvedActionSpec(entry, spec)
	if err != nil {
		t.Fatal(err)
	}
	var stored lutrav1.ActionSpec
	if err := proto.Unmarshal(encoded, &stored); err != nil || stored.GetTaskVersion() != "" {
		t.Fatalf("source-derived action version: %v", err)
	}
	spec.Cache = false
	if _, err := resolvedActionSpec(entry, spec); err == nil {
		t.Fatal("cache mode mismatch was accepted")
	}
	spec.Cache = true
	spec.TaskVersion = "1.0.0"
	if _, err := resolvedActionSpec(entry, spec); err == nil {
		t.Fatal("task version mismatch was accepted")
	}
}
