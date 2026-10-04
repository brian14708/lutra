package lutra

import (
	"bytes"
	"testing"

	lutrav1 "github.com/brian14708/lutra/gen/lutra/v1"
	"github.com/google/uuid"
)

func TestEnvironmentBuildInputsAreValidated(t *testing.T) {
	uri := sourceURI(bytes.Repeat([]byte{1}, 32))
	spec := &lutrav1.EnvironmentSpec{
		NamespaceId: uuid.NewString(), Name: "tasks", SourceUri: uri, Workdir: ".",
		Image: &lutrav1.ImageSpec{
			Name: "container", FromImage: "python:3.12-slim", BuildContextUri: uri,
			OciCopies: []*lutrav1.OciCopy{{Image: "example/tool:1", Source: "/tool", Destination: "/usr/local/bin/tool"}},
		},
		Entrypoints: []*lutrav1.Entrypoint{{Command: &lutrav1.Command{Args: []string{"python", "-m", "lutra.serve"}}, MaxAttempts: 1}},
	}
	normalized, err := normalizeEnvironment(spec)
	if err != nil {
		t.Fatal(err)
	}
	if len(normalized.spec.Image.OciCopies) != 1 {
		t.Fatalf("normalized copies = %v", normalized.spec.Image.OciCopies)
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
	for _, root := range []string{"../outside", "/", "/workspace/tool", "/a\\b", "/a/../b", "/opt/lutra/bootstrap", "/opt/lutra", "/tools/*"} {
		spec.Image.OciCopies[0].Destination = root
		if _, err := normalizeEnvironment(spec); err == nil {
			t.Fatalf("unsafe OCI path %q accepted", root)
		}
	}
}
