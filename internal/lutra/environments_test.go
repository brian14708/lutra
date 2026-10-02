package lutra

import (
	"bytes"
	"testing"

	lutrav1 "github.com/brian14708/lutra/gen/lutra/v1"
	"github.com/google/uuid"
)

func TestEnvironmentImportRootsAreValidated(t *testing.T) {
	uri := sourceURI(bytes.Repeat([]byte{1}, 32))
	spec := &lutrav1.EnvironmentSpec{
		NamespaceId: uuid.NewString(), Name: "tasks", SourceUri: uri,
		Image: &lutrav1.ImageSpec{
			Name: "local-python", BuildContextUri: uri, Workdir: ".",
			BuildCommand: &lutrav1.StartupCommand{Args: []string{"uv", "pip", "sync"}},
		},
		Entrypoints: []*lutrav1.Entrypoint{{Command: &lutrav1.StartupCommand{Args: []string{"python", "-m", "lutra.serve"}}, MaxAttempts: 1}},
		ImportRoots: []string{".", "packages/worker/src"},
	}
	normalized, err := normalizeEnvironment(spec)
	if err != nil {
		t.Fatal(err)
	}
	if len(normalized.spec.ImportRoots) != 2 || normalized.spec.ImportRoots[1] != "packages/worker/src" {
		t.Fatalf("normalized import roots = %v", normalized.spec.ImportRoots)
	}
	firstVersion, err := normalized.version([]byte{1})
	if err != nil {
		t.Fatal(err)
	}
	secondVersion, err := normalized.version([]byte{2})
	if err != nil || firstVersion == secondVersion {
		t.Fatal("runtime image change did not change the environment version")
	}
	for _, root := range []string{"../outside", "/absolute", "a:b", "a\\b", "a/../b"} {
		spec.ImportRoots = []string{root}
		if _, err := normalizeEnvironment(spec); err == nil {
			t.Fatalf("unsafe import root %q accepted", root)
		}
	}
}
