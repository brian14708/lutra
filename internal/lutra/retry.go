package lutra

import (
	lutrav1 "github.com/brian14708/lutra/gen/lutra/v1"
	"github.com/fxamacker/cbor/v2"
	"google.golang.org/protobuf/proto"
)

func resolveAttempts(entry *lutrav1.Entrypoint, override int32) (int32, error) {
	attempts := entry.MaxAttempts
	if override != 0 {
		attempts = override
	}
	if attempts < 1 || attempts > 100 {
		return 0, invalid("max_attempts must be between 1 and 100")
	}
	if entry.GetWorkflow() && attempts != 1 {
		return 0, invalid("workflows do not accept task retry overrides")
	}
	return attempts, nil
}

func resolvedActionSpec(entry *lutrav1.Entrypoint, requested *lutrav1.ActionSpec) ([]byte, error) {
	if err := validateActionCache(requested); err != nil {
		return nil, err
	}
	if entry.GetCache() != requested.GetCache() || entry.GetTaskVersion() != requested.GetTaskVersion() {
		return nil, invalid("action cache policy must match the registered task")
	}
	attempts, err := resolveAttempts(entry, requested.GetMaxAttempts())
	if err != nil {
		return nil, err
	}
	overrides, err := configOverrides(requested.GetConfigOverridesCbor())
	if err != nil {
		return nil, err
	}
	overrideBytes := []byte(nil)
	if len(overrides) != 0 {
		mode, _ := cbor.CanonicalEncOptions().EncMode()
		overrideBytes, err = mode.Marshal(overrides)
		if err != nil {
			return nil, err
		}
	}
	return proto.Marshal(&lutrav1.ActionSpec{InputCbor: requested.GetInputCbor(), MaxAttempts: attempts, Cache: requested.GetCache(), TaskVersion: requested.GetTaskVersion(), ConfigOverridesCbor: overrideBytes})
}
