package lutra

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"path"
	"regexp"
	"sort"
	"strings"

	"connectrpc.com/connect"
	lutrav1 "github.com/brian14708/lutra/gen/lutra/v1"
	"github.com/brian14708/lutra/internal/blob"
	"github.com/brian14708/lutra/internal/db"
	"github.com/fxamacker/cbor/v2"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/proto"
)

var environmentVariablePattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z_0-9]*$`)

// environmentSpec is the validated, normalized EnvironmentSpec alongside its
// deterministic encoding, which is both stored and hashed.
type environmentSpec struct {
	spec         *lutrav1.EnvironmentSpec
	specBytes    []byte
	source       []byte
	buildContext []byte
}

// version hashes the canonical declaration, never a resolved image build.
func (e *environmentSpec) version() (string, error) {
	mode, err := cbor.CanonicalEncOptions().EncMode()
	if err != nil {
		return "", err
	}
	canonical, err := mode.Marshal(map[string]any{"profile": "lutra.environment.v0", "spec": e.specBytes})
	if err != nil {
		return "", err
	}
	hash := sha256.Sum256(canonical)
	return hex.EncodeToString(hash[:]), nil
}

func validateEnvironmentIdentifier(id *lutrav1.EnvironmentIdentifier) error {
	if id == nil || !namespacePattern.MatchString(id.Project) || !namespacePattern.MatchString(id.Domain) ||
		!namespacePattern.MatchString(id.Name) || !versionPattern.MatchString(id.Version) {
		return invalidTask("invalid environment identifier")
	}
	return nil
}

func normalizeEnvironment(spec *lutrav1.EnvironmentSpec) (*environmentSpec, error) {
	if spec == nil || !namespacePattern.MatchString(spec.Project) || !namespacePattern.MatchString(spec.Domain) || !namespacePattern.MatchString(spec.Name) {
		return nil, invalidTask("invalid environment spec")
	}
	digest, mimeType, err := blob.ParseURI(spec.SourceUri)
	if err != nil || mimeType != archiveMIME || spec.SourceUri != sourceURI(digest) {
		return nil, invalidTask("invalid environment source")
	}
	image := spec.GetImage()
	var buildContext []byte
	switch image.GetName() {
	case localTaskImage:
		if image.GetReference() != "" {
			return nil, invalidTask("local-python does not accept an image reference")
		}
		buildContext, mimeType, err = blob.ParseURI(image.GetBuildContextUri())
		if err != nil || mimeType != archiveMIME || image.GetBuildContextUri() != sourceURI(buildContext) {
			return nil, invalidTask("invalid image build context")
		}
		if err := validateCommand(image.GetBuildCommand()); err != nil {
			return nil, err
		}
		if image.GetWorkdir() == "" || path.IsAbs(image.GetWorkdir()) || path.Clean(image.GetWorkdir()) != image.GetWorkdir() || image.GetWorkdir() == ".." || strings.HasPrefix(image.GetWorkdir(), "../") || strings.ContainsRune(image.GetWorkdir(), 0) {
			return nil, invalidTask("invalid image workdir")
		}
	case "docker", "e2b":
		if image.GetReference() == "" {
			return nil, invalidTask("image reference is required")
		}
	default:
		return nil, invalidTask("invalid task image provider")
	}
	if len(image.GetReference()) > 2048 || strings.ContainsRune(image.GetReference(), 0) {
		return nil, invalidTask("invalid image reference")
	}
	cpu, memory := image.GetResources().GetCpuMillis(), image.GetResources().GetMemoryBytes()
	if cpu == 0 {
		cpu = 1000
	}
	if memory == 0 {
		memory = 1 << 30
	}
	if cpu > 1_000_000 || memory > 1<<50 {
		return nil, invalidTask("resources exceed limits")
	}
	variables := make(map[string]string, len(image.GetEnvVars()))
	var variableSize int
	for key, value := range image.GetEnvVars() {
		if !environmentVariablePattern.MatchString(key) || strings.ContainsRune(value, 0) || strings.HasPrefix(key, "LUTRA_") || strings.HasPrefix(key, "UV_") || key == "PATH" || key == "HOME" || key == "PYTHONPATH" {
			return nil, invalidTask("invalid or reserved environment variable")
		}
		variableSize += len(key) + len(value)
		variables[key] = value
	}
	if len(variables) > 128 || variableSize > 64<<10 || len(spec.Dependencies) > 128 {
		return nil, invalidTask("environment declaration exceeds limits")
	}
	if err := validateEntrypoints(spec.Entrypoints); err != nil {
		return nil, err
	}
	deps := append([]*lutrav1.EnvironmentIdentifier(nil), spec.Dependencies...)
	names := map[string]bool{spec.Name: true}
	for _, dep := range deps {
		if err := validateEnvironmentIdentifier(dep); err != nil {
			return nil, err
		}
		if dep.Project != spec.Project || dep.Domain != spec.Domain || names[dep.Name] {
			return nil, invalidTask("dependencies must have unique names in the same project and domain")
		}
		names[dep.Name] = true
	}
	sort.Slice(deps, func(i, j int) bool { return deps[i].Name < deps[j].Name })
	normalized := &lutrav1.EnvironmentSpec{
		Project: spec.Project, Domain: spec.Domain, Name: spec.Name,
		SourceUri: sourceURI(digest),
		Image: &lutrav1.ImageSpec{
			Name: image.Name, Reference: image.Reference,
			Resources: &lutrav1.Resources{CpuMillis: cpu, MemoryBytes: memory},
			EnvVars:   variables, BuildContextUri: image.BuildContextUri, BuildCommand: image.BuildCommand, Workdir: image.Workdir,
		},
		Dependencies: deps,
		Entrypoints:  spec.Entrypoints,
	}
	specBytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(normalized)
	if err != nil {
		return nil, err
	}
	return &environmentSpec{spec: normalized, specBytes: specBytes, source: digest, buildContext: buildContext}, nil
}

func validateEntrypoints(entries []*lutrav1.StartupCommand) error {
	if len(entries) == 0 || len(entries) > 1000 {
		return invalidTask("environment requires between 1 and 1000 entrypoints")
	}
	for _, command := range entries {
		if err := validateCommand(command); err != nil {
			return err
		}
	}
	return nil
}

func validateCommand(command *lutrav1.StartupCommand) error {
	if command == nil || len(command.Args) == 0 || len(command.Args) > 128 || command.Args[0] == "" {
		return invalidTask("invalid command")
	}
	var size int
	for _, arg := range command.Args {
		if strings.ContainsRune(arg, 0) {
			return invalidTask("invalid command")
		}
		size += len(arg)
	}
	if size > 64<<10 {
		return invalidTask("command exceeds limits")
	}
	return nil
}

func (s Service) RegisterEnvironment(ctx context.Context, req *connect.Request[lutrav1.RegisterEnvironmentRequest]) (*connect.Response[lutrav1.RegisterEnvironmentResponse], error) {
	if s.Worker == nil {
		return nil, connect.NewError(connect.CodeUnavailable, errors.New("worker unavailable"))
	}
	spec, err := normalizeEnvironment(req.Msg.GetSpec())
	if err != nil {
		return nil, err
	}
	version, err := spec.version()
	if err != nil {
		return nil, err
	}
	identifier := &lutrav1.EnvironmentIdentifier{Project: spec.spec.Project, Domain: spec.spec.Domain, Name: spec.spec.Name, Version: version}
	// Matching versions include the commands, so repeat registrations skip source loading.
	_, err = db.New(s.Worker.DB).GetEnvironment(ctx, db.GetEnvironmentParams{Project: identifier.Project, Domain: identifier.Domain, Name: identifier.Name, Version: version})
	if err == nil {
		return connect.NewResponse(&lutrav1.RegisterEnvironmentResponse{Environment: identifier}), nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, err
	}
	executor, err := s.Worker.executor(spec.spec.GetImage().GetName())
	if err != nil {
		return nil, err
	}
	if _, err := db.New(s.Worker.DB).GetBlobBySHA256(ctx, spec.source); errors.Is(err, pgx.ErrNoRows) {
		return nil, invalidTask("environment source blob is not uploaded")
	} else if err != nil {
		return nil, err
	}
	if _, err := db.New(s.Worker.DB).GetBlobBySHA256(ctx, spec.buildContext); errors.Is(err, pgx.ErrNoRows) {
		return nil, invalidTask("image build context blob is not uploaded")
	} else if err != nil {
		return nil, err
	}
	imageKey, err := executor.ImageKey(spec.spec)
	if err != nil {
		return nil, err
	}
	tx, err := s.Worker.DB.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	q := db.New(tx)
	id := uuid.New()
	_, err = q.InsertEnvironment(ctx, db.InsertEnvironmentParams{ID: id, Project: identifier.Project, Domain: identifier.Domain, Name: identifier.Name, Version: version, Provider: spec.spec.GetImage().GetName(), Spec: spec.specBytes, ImageKey: imageKey})
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return nil, err
	}
	if err == nil {
		for _, dep := range spec.spec.Dependencies {
			if _, err := lookupEnvironment(ctx, q, dep); err != nil {
				return nil, err
			}
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return connect.NewResponse(&lutrav1.RegisterEnvironmentResponse{Environment: identifier}), nil
}

func lookupEnvironment(ctx context.Context, q *db.Queries, id *lutrav1.EnvironmentIdentifier) (db.LutraTaskEnvironment, error) {
	if err := validateEnvironmentIdentifier(id); err != nil {
		return db.LutraTaskEnvironment{}, err
	}
	row, err := q.GetEnvironment(ctx, db.GetEnvironmentParams{Project: id.Project, Domain: id.Domain, Name: id.Name, Version: id.Version})
	if errors.Is(err, pgx.ErrNoRows) {
		return row, connect.NewError(connect.CodeNotFound, errors.New("environment is not registered"))
	}
	return row, err
}

func lookupTask(ctx context.Context, q *db.Queries, id *lutrav1.EnvironmentIdentifier, entrypointID uint32) (db.LutraTaskEnvironment, error) {
	environment, err := lookupEnvironment(ctx, q, id)
	if err != nil {
		return environment, err
	}
	var spec lutrav1.EnvironmentSpec
	if err := proto.Unmarshal(environment.Spec, &spec); err != nil {
		return environment, err
	}
	if entrypointID == 0 || entrypointID > uint32(len(spec.Entrypoints)) {
		return environment, connect.NewError(connect.CodeNotFound, errors.New("entrypoint is not registered in this environment"))
	}
	return environment, nil
}
