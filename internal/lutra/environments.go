package lutra

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"path"
	"regexp"
	"sort"
	"strings"

	"connectrpc.com/connect"
	lutrav1 "github.com/brian14708/lutra/gen/lutra/v1"
	"github.com/brian14708/lutra/internal/blob"
	"github.com/brian14708/lutra/internal/db"
	"github.com/distribution/reference"
	"github.com/fxamacker/cbor/v2"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/proto"
)

var (
	environmentVariablePattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z_0-9]*$`)
	containerPlatformPattern   = regexp.MustCompile(`^linux/[a-z0-9_]+(?:/v[0-9]+)?$`)
)

func validUUID(value string) bool {
	id, err := uuid.Parse(value)
	return err == nil && id != uuid.Nil
}

// environmentSpec is the validated, normalized EnvironmentSpec alongside its
// deterministic encoding, which is both stored and hashed.
type environmentSpec struct {
	spec         *lutrav1.EnvironmentSpec
	specBytes    []byte
	source       []byte
	buildContext []byte
}

// version hashes the declaration and the local toolchain selected for its image.
func (e *environmentSpec) version(imageKey []byte) (string, error) {
	mode, err := cbor.CanonicalEncOptions().EncMode()
	if err != nil {
		return "", err
	}
	canonical, err := mode.Marshal(map[string]any{"profile": "lutra.environment.v2", "spec": e.specBytes, "image_key": imageKey})
	if err != nil {
		return "", err
	}
	hash := sha256.Sum256(canonical)
	return hex.EncodeToString(hash[:]), nil
}

func validateEnvironmentIdentifier(id *lutrav1.EnvironmentIdentifier) error {
	if id == nil || !validUUID(id.NamespaceId) ||
		!slugPattern.MatchString(id.Name) || !versionPattern.MatchString(id.Version) {
		return invalid("invalid environment identifier")
	}
	return nil
}

func normalizeEnvironment(spec *lutrav1.EnvironmentSpec) (*environmentSpec, error) {
	if spec == nil || !validUUID(spec.NamespaceId) || !slugPattern.MatchString(spec.Name) {
		return nil, invalid("invalid environment spec")
	}
	digest, mimeType, err := blob.ParseURI(spec.SourceUri)
	if err != nil || mimeType != archiveMIME || spec.SourceUri != sourceURI(digest) {
		return nil, invalid("invalid environment source")
	}
	image := spec.GetImage()
	var buildContext []byte
	switch image.GetName() {
	case containerTaskImage:
		if !containerPlatformPattern.MatchString(imagePlatform(image)) {
			return nil, invalid("invalid container platform")
		}
		if len(image.GetFromImage()) > 2048 {
			return nil, invalid("invalid container base image")
		}
		if _, err := reference.ParseNormalizedNamed(image.GetFromImage()); err != nil {
			return nil, invalid("invalid container base image")
		}
		buildContext, mimeType, err = blob.ParseURI(image.GetBuildContextUri())
		if err != nil || mimeType != archiveMIME || image.GetBuildContextUri() != sourceURI(buildContext) {
			return nil, invalid("invalid image build context")
		}
		if len(image.GetPythonRequires()) > 200 || strings.ContainsRune(image.GetPythonRequires(), 0) {
			return nil, invalid("invalid Python requirement")
		}
		if spec.GetWorkdir() == "" || path.IsAbs(spec.GetWorkdir()) || path.Clean(spec.GetWorkdir()) != spec.GetWorkdir() || spec.GetWorkdir() == ".." || strings.HasPrefix(spec.GetWorkdir(), "../") || strings.ContainsAny(spec.GetWorkdir(), ":\\\x00") {
			return nil, invalid("invalid environment workdir")
		}
	case "e2b":
		if image.GetFromImage() == "" {
			return nil, invalid("image reference is required")
		}
	default:
		return nil, invalid("invalid task image provider")
	}
	if len(image.GetFromImage()) > 2048 || strings.ContainsRune(image.GetFromImage(), 0) {
		return nil, invalid("invalid image reference")
	}
	cpu, memory := image.GetResources().GetCpuMillis(), image.GetResources().GetMemoryBytes()
	if cpu == 0 {
		cpu = 1000
	}
	if memory == 0 {
		memory = 1 << 30
	}
	if cpu > 1_000_000 || memory > 1<<50 {
		return nil, invalid("resources exceed limits")
	}
	variables := make(map[string]string, len(image.GetEnvVars()))
	var variableSize int
	for key, value := range image.GetEnvVars() {
		if !environmentVariablePattern.MatchString(key) || strings.ContainsRune(value, 0) || strings.HasPrefix(key, "LUTRA_") || strings.HasPrefix(key, "UV_") || key == "PATH" || key == "HOME" || key == "PYTHONPATH" {
			return nil, invalid("invalid or reserved environment variable")
		}
		variableSize += len(key) + len(value)
		variables[key] = value
	}
	if len(variables) > 128 || variableSize > 64<<10 || len(spec.Dependencies) > 128 {
		return nil, invalid("environment declaration exceeds limits")
	}
	if err := validateEntrypoints(spec.Entrypoints); err != nil {
		return nil, err
	}
	if spec.PrepareCommand != nil {
		if err := validateCommand(spec.PrepareCommand); err != nil {
			return nil, err
		}
	}
	pythonPaths := append([]string(nil), spec.GetPythonPaths()...)
	if len(pythonPaths) > 256 {
		return nil, invalid("environment requires at most 256 Python paths")
	}
	for _, root := range pythonPaths {
		if root == "" || path.IsAbs(root) || path.Clean(root) != root || root == ".." || strings.HasPrefix(root, "../") || strings.ContainsAny(root, ":\\\x00") {
			return nil, invalid("invalid environment Python path")
		}
	}
	deps := append([]*lutrav1.EnvironmentIdentifier(nil), spec.Dependencies...)
	names := map[string]bool{spec.Name: true}
	for _, dep := range deps {
		if err := validateEnvironmentIdentifier(dep); err != nil {
			return nil, err
		}
		if dep.NamespaceId != spec.NamespaceId || names[dep.Name] {
			return nil, invalid("dependencies must have unique names in the same namespace")
		}
		names[dep.Name] = true
	}
	sort.Slice(deps, func(i, j int) bool { return deps[i].Name < deps[j].Name })
	normalized := &lutrav1.EnvironmentSpec{
		NamespaceId: spec.NamespaceId, Name: spec.Name,
		SourceUri: sourceURI(digest),
		Image: &lutrav1.ImageSpec{
			Name: image.Name, FromImage: image.FromImage,
			Resources: &lutrav1.Resources{CpuMillis: cpu, MemoryBytes: memory},
			EnvVars:   variables, BuildContextUri: image.BuildContextUri, PythonRequires: image.PythonRequires, Platform: imagePlatform(image),
		},
		Dependencies:   deps,
		PrepareCommand: spec.PrepareCommand,
		Entrypoints:    spec.Entrypoints,
		PythonPaths:    pythonPaths,
		Workdir:        spec.Workdir,
	}
	specBytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(normalized)
	if err != nil {
		return nil, err
	}
	return &environmentSpec{spec: normalized, specBytes: specBytes, source: digest, buildContext: buildContext}, nil
}

func validateEntrypoints(entries []*lutrav1.Entrypoint) error {
	if len(entries) == 0 || len(entries) > 1000 {
		return invalid("environment requires between 1 and 1000 entrypoints")
	}
	for _, entry := range entries {
		if entry == nil {
			return invalid("invalid entrypoint")
		}
		if entry.GetCache() {
			if len(entry.GetTaskVersion()) > 200 || (entry.GetTaskVersion() != "" && !semanticVersionPattern.MatchString(entry.GetTaskVersion())) {
				return invalid("cached tasks require a semantic or source-derived task version")
			}
		} else if entry.GetTaskVersion() != "" {
			return invalid("uncached tasks cannot include a cache version")
		}
		if _, err := resolveAttempts(entry, 0); err != nil {
			return err
		}
		if err := validateCommand(entry.Command); err != nil {
			return err
		}
	}
	return nil
}

func validateCommand(command *lutrav1.Command) error {
	if command == nil || len(command.Args) == 0 || len(command.Args) > 128 || command.Args[0] == "" {
		return invalid("invalid command")
	}
	var size int
	for _, arg := range command.Args {
		if strings.ContainsRune(arg, 0) {
			return invalid("invalid command")
		}
		size += len(arg)
	}
	if size > 64<<10 {
		return invalid("command exceeds limits")
	}
	return nil
}

func (s Service) RegisterEnvironment(ctx context.Context, req *connect.Request[lutrav1.RegisterEnvironmentRequest]) (*connect.Response[lutrav1.RegisterEnvironmentResponse], error) {
	if s.DB == nil {
		return nil, connect.NewError(connect.CodeUnavailable, errors.New("worker unavailable"))
	}
	spec, err := normalizeEnvironment(req.Msg.GetSpec())
	if err != nil {
		return nil, err
	}
	var executor Executor
	switch spec.spec.GetImage().GetName() {
	case containerTaskImage:
		executor = &ContainerExecutor{}
	default:
		return nil, connect.NewError(connect.CodeUnimplemented, fmt.Errorf("%s executor is not configured", spec.spec.GetImage().GetName()))
	}
	imageKey, err := executor.ImageKey(spec.spec)
	if err != nil {
		return nil, err
	}
	version, err := spec.version(imageKey)
	if err != nil {
		return nil, err
	}
	namespaceID, err := uuid.Parse(spec.spec.NamespaceId)
	if err != nil || namespaceID == uuid.Nil {
		return nil, invalid("invalid namespace ID")
	}
	namespace, err := db.New(s.DB).GetNamespaceByID(ctx, namespaceID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("namespace not found"))
	}
	if err != nil {
		return nil, err
	}
	identifier := &lutrav1.EnvironmentIdentifier{NamespaceId: spec.spec.NamespaceId, Name: spec.spec.Name, Version: version}
	// Matching versions include the commands, so repeat registrations skip source loading.
	_, err = db.New(s.DB).GetEnvironment(ctx, db.GetEnvironmentParams{NamespaceID: namespaceID, Name: identifier.Name, Version: version})
	if err == nil {
		return connect.NewResponse(&lutrav1.RegisterEnvironmentResponse{Environment: identifier}), nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, err
	}
	if _, err := db.New(s.DB).GetBlobBySHA256(ctx, spec.source); errors.Is(err, pgx.ErrNoRows) {
		return nil, invalid("environment source blob is not uploaded")
	} else if err != nil {
		return nil, err
	}
	if _, err := db.New(s.DB).GetBlobBySHA256(ctx, spec.buildContext); errors.Is(err, pgx.ErrNoRows) {
		return nil, invalid("image build context blob is not uploaded")
	} else if err != nil {
		return nil, err
	}
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	q := db.New(tx)
	id, err := uuid.NewV7()
	if err != nil {
		return nil, err
	}
	_, err = q.InsertEnvironment(ctx, db.InsertEnvironmentParams{ID: id, NamespaceID: namespace.ID, Name: identifier.Name, Version: version, Provider: spec.spec.GetImage().GetName(), Spec: spec.specBytes, ImageKey: imageKey})
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
	namespaceID, err := uuid.Parse(id.NamespaceId)
	if err != nil {
		return db.LutraTaskEnvironment{}, invalid("invalid environment identifier")
	}
	row, err := q.GetEnvironment(ctx, db.GetEnvironmentParams{NamespaceID: namespaceID, Name: id.Name, Version: id.Version})
	if errors.Is(err, pgx.ErrNoRows) {
		return row, connect.NewError(connect.CodeNotFound, errors.New("environment is not registered"))
	}
	return row, err
}

func lookupTask(ctx context.Context, q *db.Queries, id *lutrav1.EnvironmentIdentifier, entrypointID uint32) (db.LutraTaskEnvironment, *lutrav1.Entrypoint, error) {
	environment, err := lookupEnvironment(ctx, q, id)
	if err != nil {
		return environment, nil, err
	}
	var spec lutrav1.EnvironmentSpec
	if err := proto.Unmarshal(environment.Spec, &spec); err != nil {
		return environment, nil, err
	}
	if entrypointID == 0 || entrypointID > uint32(len(spec.Entrypoints)) {
		return environment, nil, connect.NewError(connect.CodeNotFound, errors.New("entrypoint is not registered in this environment"))
	}
	return environment, spec.Entrypoints[entrypointID-1], nil
}
