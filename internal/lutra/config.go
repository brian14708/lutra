package lutra

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"strconv"
	"strings"

	"connectrpc.com/connect"
	lutrav1 "github.com/brian14708/lutra/gen/lutra/v1"
	"github.com/brian14708/lutra/internal/db"
	"github.com/brian14708/lutra/internal/redact"
	"github.com/fxamacker/cbor/v2"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
)

type ConfigValue struct {
	Value     []byte `cbor:"value"`
	Sensitive bool   `cbor:"sensitive"`
}
type (
	RunConfigSnapshot map[string]ConfigValue
	configDeclaration struct {
		path      string
		required  bool
		sensitive *bool
	}
)
type ConfigError struct{ Code string }

func (s RunConfigSnapshot) filter() *redact.Filter {
	var values [][]byte
	for _, v := range s {
		if v.Sensitive && len(v.Value) != 0 {
			values = append(values, v.Value)
		}
	}
	return redact.New(values)
}

// redactError passes structured worker errors through unchanged and rewrites
// any other error message through the sensitive-value filter.
func (s RunConfigSnapshot) redactError(err error) error {
	var config *ConfigError
	var cacheable *CacheableError
	var task *TaskError
	var terminal *TerminalTaskError
	if errors.As(err, &config) || errors.As(err, &cacheable) || errors.As(err, &task) || errors.As(err, &terminal) {
		return err
	}
	return errors.New(s.filter().String(err.Error()))
}

func (e *ConfigError) Error() string { return e.Code }
func (*ConfigError) Retryable() bool { return false }

func configFailure(code string) error {
	return connect.NewError(connect.CodeFailedPrecondition, &ConfigError{Code: code})
}

func canonicalConfig(value []byte) ([]byte, error) {
	if len(value) == 0 || len(value) > 65536 {
		return nil, configFailure("config.invalid")
	}
	mode, err := (cbor.DecOptions{DupMapKey: cbor.DupMapKeyEnforcedAPF, MaxNestedLevels: 32, MaxArrayElements: 65536, MaxMapPairs: 4096}).DecMode()
	if err != nil {
		return nil, err
	}
	var decoded any
	if err := mode.Unmarshal(value, &decoded); err != nil {
		return nil, configFailure("config.invalid")
	}
	enc, _ := cbor.CanonicalEncOptions().EncMode()
	canonical, err := enc.Marshal(decoded)
	if err != nil || !bytes.Equal(value, canonical) {
		return nil, configFailure("config.invalid")
	}
	return canonical, nil
}

func configOverrides(value []byte) (map[string]cbor.RawMessage, error) {
	if len(value) == 0 {
		return map[string]cbor.RawMessage{}, nil
	}
	if _, err := canonicalConfig(value); err != nil {
		return nil, err
	}
	var out map[string]cbor.RawMessage
	if err := cbor.Unmarshal(value, &out); err != nil || out == nil {
		return nil, configFailure("config.invalid")
	}
	for path := range out {
		if validatePath(path) != nil {
			return nil, configFailure("config.invalid")
		}
	}
	return out, nil
}

func declarations(spec *lutrav1.EnvironmentSpec) []configDeclaration {
	var out []configDeclaration
	for _, v := range spec.GetImage().GetEnv() {
		if _, ok := v.GetSource().(*lutrav1.EnvValue_SettingRef); ok {
			out = append(out, configDeclaration{v.GetSettingRef(), true, v.Sensitive})
		}
	}
	for _, entry := range spec.Entrypoints {
		for _, b := range entry.Config {
			out = append(out, configDeclaration{b.SettingRef, b.Required == nil || *b.Required, b.Sensitive})
		}
	}
	return out
}

func ResolveRunConfig(settings RunConfigSnapshot, declarations []configDeclaration, overrides map[string]cbor.RawMessage) (RunConfigSnapshot, error) {
	out := RunConfigSnapshot{}
	for _, d := range declarations {
		v, exists := settings[d.path]
		if d.required && !exists {
			if _, ok := overrides[d.path]; !ok {
				return nil, configFailure("config.missing")
			}
		}
		if !exists {
			if d.sensitive == nil {
				return nil, configFailure("config.invalid")
			}
			v.Sensitive = *d.sensitive
		}
		if override, ok := overrides[d.path]; ok {
			if !exists && d.sensitive == nil {
				return nil, configFailure("config.invalid")
			}
			v.Value = override
		}
		if d.required && len(v.Value) == 0 {
			return nil, configFailure("config.missing")
		}
		if previous, ok := out[d.path]; ok && previous.Sensitive != v.Sensitive {
			return nil, configFailure("config.invalid")
		}
		out[d.path] = v
	}
	for path := range overrides {
		if _, ok := out[path]; !ok {
			return nil, configFailure("config.invalid")
		}
	}
	return out, nil
}

func snapshotRunConfig(ctx context.Context, q *db.Queries, runID uuid.UUID, environment db.LutraTaskEnvironment, overrides []byte) error {
	visited := map[uuid.UUID]bool{}
	var all []configDeclaration
	var specs []*lutrav1.EnvironmentSpec
	var visit func(db.LutraTaskEnvironment) error
	visit = func(row db.LutraTaskEnvironment) error {
		if visited[row.ID] {
			return nil
		}
		visited[row.ID] = true
		if len(visited) > 1024 {
			return configFailure("config.invalid")
		}
		var spec lutrav1.EnvironmentSpec
		if err := proto.Unmarshal(row.Spec, &spec); err != nil {
			return err
		}
		all = append(all, declarations(&spec)...)
		specs = append(specs, &spec)
		for _, dep := range spec.Dependencies {
			row, err := lookupEnvironment(ctx, q, dep)
			if err != nil {
				return err
			}
			if err := visit(row); err != nil {
				return err
			}
		}
		return nil
	}
	if err := visit(environment); err != nil {
		return err
	}
	paths := []string{}
	for _, d := range all {
		paths = append(paths, d.path)
	}
	values := RunConfigSnapshot{}
	if len(paths) > 0 {
		rows, err := q.ListSettings(ctx, db.ListSettingsParams{NamespaceID: environment.NamespaceID, Paths: paths})
		if err != nil {
			return err
		}
		for _, row := range rows {
			values[row.Path] = ConfigValue{row.Value, row.Sensitive}
		}
	}
	override, err := configOverrides(overrides)
	if err != nil {
		return err
	}
	snapshot, err := ResolveRunConfig(values, all, override)
	if err != nil {
		return err
	}
	for _, spec := range specs {
		for index := range spec.Entrypoints {
			_, _, err := executionConfig(&EnvironmentExecution{Spec: spec, EntrypointID: uint32(index + 1), Config: snapshot})
			if err != nil {
				return configFailure("config.invalid")
			}
		}
	}
	for path, v := range snapshot {
		if err := q.InsertRunSetting(ctx, db.InsertRunSettingParams{RunID: runID, Path: path, ValueCbor: v.Value, Sensitive: v.Sensitive}); err != nil {
			return err
		}
	}
	return nil
}

func loadRunConfig(ctx context.Context, q *db.Queries, id uuid.UUID) (RunConfigSnapshot, error) {
	rows, err := q.ListRunSettings(ctx, id)
	if err != nil {
		return nil, err
	}
	out := RunConfigSnapshot{}
	for _, row := range rows {
		out[row.Path] = ConfigValue{row.ValueCbor, row.Sensitive}
	}
	return out, nil
}

// Environment references accept scalar CBOR: strings verbatim, booleans and
// numbers in decimal, bytes in base64, and null as an empty string.
func configEnvString(raw []byte) (string, error) {
	var v any
	if err := cbor.Unmarshal(raw, &v); err != nil {
		return "", &ConfigError{Code: "config.invalid"}
	}
	var s string
	switch v := v.(type) {
	case nil:
		s = ""
	case string:
		s = v
	case bool:
		s = strconv.FormatBool(v)
	case uint64:
		s = strconv.FormatUint(v, 10)
	case int64:
		s = strconv.FormatInt(v, 10)
	case float64:
		s = strconv.FormatFloat(v, 'g', -1, 64)
	case []byte:
		s = base64.StdEncoding.EncodeToString(v)
	default:
		return "", &ConfigError{Code: "config.invalid"}
	}
	if strings.ContainsRune(s, 0) {
		return "", &ConfigError{Code: "config.invalid"}
	}
	return s, nil
}

func executionConfig(req *EnvironmentExecution) (map[string]string, string, error) {
	env := map[string]string{}
	size := 0
	for key, v := range req.Spec.GetImage().GetEnv() {
		value := v.GetStaticValue()
		if _, ok := v.GetSource().(*lutrav1.EnvValue_SettingRef); ok {
			resolved := req.Config[v.GetSettingRef()]
			if len(resolved.Value) == 0 {
				return nil, "", &ConfigError{Code: "config.missing"}
			}
			var err error
			value, err = configEnvString(resolved.Value)
			if err != nil {
				return nil, "", err
			}
		}
		size += len(key) + len(value)
		env[key] = value
	}
	if size > 64<<10 {
		return nil, "", &ConfigError{Code: "config.invalid"}
	}
	projection := map[string]any{}
	if req.EntrypointID > 0 && req.EntrypointID <= uint32(len(req.Spec.Entrypoints)) {
		for _, b := range req.Spec.Entrypoints[req.EntrypointID-1].Config {
			v := req.Config[b.SettingRef]
			required := b.Required == nil || *b.Required
			if required && len(v.Value) == 0 {
				return nil, "", &ConfigError{Code: "config.missing"}
			}
			var value any
			present := len(v.Value) != 0
			if present {
				if err := cbor.Unmarshal(v.Value, &value); err != nil {
					return nil, "", errors.New("config.invalid")
				}
			}
			projection[b.Name] = map[string]any{"present": present, "value": value, "sensitive": v.Sensitive, "required": required}
		}
	}
	enc, _ := cbor.CanonicalEncOptions().EncMode()
	payload, err := enc.Marshal(projection)
	if len(payload) > 65536 {
		return nil, "", &ConfigError{Code: "config.invalid"}
	}
	return env, base64.StdEncoding.EncodeToString(payload), err
}
