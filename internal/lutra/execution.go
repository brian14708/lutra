package lutra

import (
	"errors"

	lutrav1 "github.com/brian14708/lutra/gen/lutra/v1"
	"github.com/brian14708/lutra/internal/db"
	"github.com/brian14708/lutra/internal/multihash"
	"github.com/fxamacker/cbor/v2"
	"google.golang.org/protobuf/proto"
)

func cacheKey(row db.LoadActionRow, spec *lutrav1.ActionSpec, runtime string, config RunConfigSnapshot) []byte {
	var environment lutrav1.EnvironmentSpec
	if err := proto.Unmarshal(row.EnvironmentSpec, &environment); err != nil {
		return nil
	}
	if row.EntrypointID < 1 || row.EntrypointID > int64(len(environment.Entrypoints)) {
		return nil
	}
	entrypoint := environment.Entrypoints[row.EntrypointID-1]
	req := &EnvironmentExecution{Spec: &environment, EntrypointID: uint32(row.EntrypointID), Config: config}
	env, projection, err := executionConfig(req)
	if err != nil {
		return nil
	}
	declaration, err := proto.MarshalOptions{Deterministic: true}.Marshal(entrypoint)
	if err != nil {
		return nil
	}
	version := spec.GetTaskVersion()
	if version == "" {
		version = environment.GetSourceUri()
	}
	image := environment.GetImage()
	imageDeclaration, err := proto.MarshalOptions{Deterministic: true}.Marshal(image)
	if err != nil {
		return nil
	}
	effective := RunConfigSnapshot{}
	for _, d := range declarations(&environment) {
		effective[d.path] = config[d.path]
	}
	dependencies := make([]map[string]string, 0, len(environment.GetDependencies()))
	for _, dependency := range environment.GetDependencies() {
		dependencies = append(dependencies, map[string]string{
			"name": dependency.GetName(), "version": dependency.GetVersion(),
		})
	}
	server := map[string]any{
		"profile":       "lutra.task-cache.v2",
		"environment":   environment.GetName(),
		"workdir":       environment.GetWorkdir(),
		"entrypoint_id": row.EntrypointID,
		"command":       entrypoint.GetCommand().GetArgs(),
		"task_version":  version,
		"input_cbor":    spec.GetInputCbor(),
		"provider":      row.Provider,
		"runtime":       runtime,
		"image_key":     row.ImageKey,
		"image": map[string]any{
			"name": image.GetName(), "from_image": image.GetFromImage(),
			"resources": map[string]uint64{
				"cpu_millis":   uint64(image.GetResources().GetCpuMillis()),
				"memory_bytes": image.GetResources().GetMemoryBytes(),
			},
			"declaration": imageDeclaration, "build_context_uri": image.GetBuildContextUri(),
			"platform": image.GetPlatform(),
		},
		"dependencies":       dependencies,
		"config":             projection,
		"config_declaration": declaration,
		"effective_env":      env,
		"effective_config":   effective,
	}
	mode, err := cbor.CanonicalEncOptions().EncMode()
	if err != nil {
		return nil
	}
	serverBytes, err := mode.Marshal(server)
	if err != nil {
		return nil
	}
	digest := multihash.Sum(serverBytes)
	return digest[:]
}

func environmentExecution(row db.LoadActionRow, config RunConfigSnapshot) (*EnvironmentExecution, *lutrav1.ActionSpec, error) {
	var spec lutrav1.EnvironmentSpec
	var action lutrav1.ActionSpec
	if err := proto.Unmarshal(row.EnvironmentSpec, &spec); err != nil {
		return nil, nil, err
	}
	if err := proto.Unmarshal(row.ActionSpec, &action); err != nil {
		return nil, nil, err
	}
	if row.EntrypointID < 1 || row.EntrypointID > int64(len(spec.Entrypoints)) {
		return nil, nil, errors.New("entrypoint is not registered")
	}
	task := &EnvironmentExecution{Environment: &lutrav1.EnvironmentIdentifier{NamespaceId: row.NamespaceID.String(), Name: row.EnvironmentName, Version: row.Version}, EntrypointID: uint32(row.EntrypointID), Provider: row.Provider, Spec: &spec, Input: action.InputCbor, RunID: row.RunID.String(), ActionID: row.ID.String()}
	task.Config = config
	task.Environments = append([]*lutrav1.EnvironmentIdentifier{task.Environment}, spec.Dependencies...)
	return task, &action, nil
}
