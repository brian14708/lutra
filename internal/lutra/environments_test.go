package lutra

import (
	"bytes"
	"testing"

	lutrav1 "github.com/brian14708/lutra/gen/lutra/v1"
	"github.com/google/uuid"
)

func TestEnvironmentPythonPathsAreValidated(t *testing.T) {
	uri := sourceURI(bytes.Repeat([]byte{1}, 32))
	spec := &lutrav1.EnvironmentSpec{
		NamespaceId: uuid.NewString(), Name: "tasks", SourceUri: uri, Workdir: ".",
		Image: &lutrav1.ImageSpec{
			Name: "container", FromImage: "python:3.12-slim", BuildContextUri: uri,
		},
		Entrypoints: []*lutrav1.Entrypoint{{Command: &lutrav1.Command{Args: []string{"python", "-m", "lutra.serve"}}, MaxAttempts: 1}},
		PythonPaths: []string{".", "packages/worker/src"},
	}
	normalized, err := normalizeEnvironment(spec)
	if err != nil {
		t.Fatal(err)
	}
	if len(normalized.spec.PythonPaths) != 2 || normalized.spec.PythonPaths[1] != "packages/worker/src" {
		t.Fatalf("normalized Python paths = %v", normalized.spec.PythonPaths)
	}
	spec.Image.Name = "local-python"
	if _, err := normalizeEnvironment(spec); err == nil {
		t.Fatal("removed local process provider was accepted")
	}
	spec.Image.Name = containerTaskImage
	firstVersion, err := normalized.version([]byte{1})
	if err != nil {
		t.Fatal(err)
	}
	secondVersion, err := normalized.version([]byte{2})
	if err != nil || firstVersion == secondVersion {
		t.Fatal("runtime image change did not change the environment version")
	}
	for _, root := range []string{"../outside", "/absolute", "a:b", "a\\b", "a/../b"} {
		spec.PythonPaths = []string{root}
		if _, err := normalizeEnvironment(spec); err == nil {
			t.Fatalf("unsafe Python path %q accepted", root)
		}
	}
}
