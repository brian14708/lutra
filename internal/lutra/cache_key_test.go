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
		Image:       &lutrav1.ImageSpec{Name: "container", FromImage: "python:3.14", BuildContextUri: "blob:build,aaa"},
		Entrypoints: []*lutrav1.Entrypoint{{Command: &lutrav1.Command{Args: []string{"python", "task"}}, Cache: true}},
	}
	encoded, err := proto.Marshal(environment)
	if err != nil {
		t.Fatal(err)
	}
	row := db.LoadRunTasksRow{EnvironmentSpec: encoded, EnvironmentName: "work", EntrypointID: 1, Provider: "local", Version: "registration-a", NamespaceID: uuid.New(), ImageKey: bytes.Repeat([]byte{1}, 32)}
	spec := &lutrav1.ActionSpec{Cache: true, InputCbor: []byte{0x82, 0x80, 0xa0}}
	baseline := cacheKey(row, spec, "docker", nil)
	if len(baseline) != 32 {
		t.Fatalf("invalid key length: %d", len(baseline))
	}
	otherNamespace := row
	otherNamespace.NamespaceID = uuid.New()
	otherNamespace.Version = "registration-b"
	if !bytes.Equal(baseline, cacheKey(otherNamespace, spec, "docker", nil)) {
		t.Fatal("namespace and registration version changed the key")
	}
	otherRuntime := row
	otherRuntime.ImageKey = bytes.Repeat([]byte{2}, 32)
	if bytes.Equal(baseline, cacheKey(otherRuntime, spec, "docker", nil)) {
		t.Fatal("runtime image change reused the task result")
	}
	if bytes.Equal(baseline, cacheKey(row, spec, "podman", nil)) {
		t.Fatal("Docker and Podman shared a task result")
	}
	changedInput := proto.Clone(spec).(*lutrav1.ActionSpec)
	changedInput.InputCbor = []byte{0x82, 0x81, 0x01, 0xa0}
	if bytes.Equal(baseline, cacheKey(row, changedInput, "docker", nil)) {
		t.Fatal("distinct inputs shared a key")
	}
	environment.SourceUri = "blob:source,bbb"
	row.EnvironmentSpec, err = proto.Marshal(environment)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(baseline, cacheKey(row, spec, "docker", nil)) {
		t.Fatal("source change did not invalidate source-derived version")
	}
	spec.TaskVersion = "1.2.3"
	explicit := cacheKey(row, spec, "docker", nil)
	row.EnvironmentSpec = encoded
	if !bytes.Equal(explicit, cacheKey(row, spec, "docker", nil)) {
		t.Fatal("explicit version did not preserve reuse across source changes")
	}
	environment.Image.BuildEnv = map[string]string{"PYTHONPATH": "/workspace/src"}
	row.EnvironmentSpec, err = proto.Marshal(environment)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(explicit, cacheKey(row, spec, "docker", nil)) {
		t.Fatal("changed Python paths reused the task result")
	}
	environment.Image.BuildEnv = nil
	environment.Image.BuildContextUri = "blob:build,bbb"
	row.EnvironmentSpec, err = proto.Marshal(environment)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(explicit, cacheKey(row, spec, "docker", nil)) {
		t.Fatal("changed build context reused the task result")
	}
	environment.Entrypoints[0].Config = []*lutrav1.ConfigBinding{{Name: "token", SettingRef: "service/token"}}
	row.EnvironmentSpec, _ = proto.Marshal(environment)
	a := RunConfigSnapshot{"service/token": {configBytes(t, "secret-a"), true}}
	b := RunConfigSnapshot{"service/token": {configBytes(t, "secret-b"), true}}
	if bytes.Equal(cacheKey(row, spec, "docker", a), cacheKey(row, spec, "docker", b)) {
		t.Fatal("sensitive values reused cache key")
	}
	if cacheKey(row, spec, "docker", nil) != nil {
		t.Fatal("missing required configuration allowed cache key")
	}
}

func TestResolvedActionCachePolicy(t *testing.T) {
	entry := &lutrav1.Entrypoint{Cache: true, MaxAttempts: 1}
	spec := &lutrav1.ActionSpec{Cache: true}
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
