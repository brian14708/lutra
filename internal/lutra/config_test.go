package lutra

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"connectrpc.com/connect"
	lutrav1 "github.com/brian14708/lutra/gen/lutra/v1"
	"github.com/brian14708/lutra/internal/db"
	"github.com/brian14708/lutra/internal/runlog"
	"github.com/fxamacker/cbor/v2"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/proto"
)

func configBytes(t *testing.T, value any) []byte {
	t.Helper()
	enc, _ := cbor.CanonicalEncOptions().EncMode()
	raw, err := enc.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestResolveRunConfiguration(t *testing.T) {
	sensitive := true
	decl := []configDeclaration{{"service/token", true, nil}, {"optional/value", false, &sensitive}, {"null/value", true, nil}}
	settings := RunConfigSnapshot{"service/token": {configBytes(t, "secret"), true}, "null/value": {[]byte{0xf6}, false}}
	snapshot, err := ResolveRunConfig(settings, decl, map[string]cbor.RawMessage{"service/token": configBytes(t, "override")})
	if err != nil {
		t.Fatal(err)
	}
	if !snapshot["service/token"].Sensitive || !bytes.Equal(snapshot["service/token"].Value, configBytes(t, "override")) {
		t.Fatal("override lost value or sensitivity")
	}
	if snapshot["optional/value"].Value != nil || !snapshot["optional/value"].Sensitive || !bytes.Equal(snapshot["null/value"].Value, []byte{0xf6}) {
		t.Fatal("absence or null changed")
	}
	if !bytes.Equal(settings["service/token"].Value, configBytes(t, "secret")) {
		t.Fatal("resolver mutated settings")
	}
	if _, err := ResolveRunConfig(nil, []configDeclaration{{"missing/value", true, nil}}, nil); err == nil || !strings.Contains(err.Error(), "config.missing") {
		t.Fatalf("missing error: %v", err)
	}
	if _, err := ResolveRunConfig(nil, []configDeclaration{{"missing/value", true, nil}}, map[string]cbor.RawMessage{"missing/value": []byte{0xf6}}); err == nil {
		t.Fatal("override inferred sensitivity")
	}
	for _, value := range [][]byte{{0xf6}, {0xa1, 0x61, 'x', 0x18, 0x01}, {0xa2, 0x61, 'x', 1, 0x61, 'x', 2}} {
		if _, err := configOverrides(value); err == nil {
			t.Fatalf("invalid overrides accepted: %x", value)
		}
	}
}

func TestConfigurationProviderAndRedaction(t *testing.T) {
	req := &EnvironmentExecution{EntrypointID: 1, Config: RunConfigSnapshot{"service/token": {configBytes(t, "secret-token"), true}}, Spec: &lutrav1.EnvironmentSpec{Image: &lutrav1.ImageSpec{Env: map[string]*lutrav1.EnvValue{"TOKEN": {Source: &lutrav1.EnvValue_SettingRef{SettingRef: "service/token"}}, "STATIC": {Source: &lutrav1.EnvValue_StaticValue{StaticValue: "literal"}}}}, Entrypoints: []*lutrav1.Entrypoint{{Config: []*lutrav1.ConfigBinding{{Name: "token", SettingRef: "service/token"}}}}}}
	env, payload, err := executionConfig(req)
	if err != nil {
		t.Fatal(err)
	}
	if env["TOKEN"] != "secret-token" || env["STATIC"] != "literal" || payload == "" {
		t.Fatal("provider config missing")
	}
	if got := req.Config.filter().String("failure: secret-token"); got != "failure: [redacted]" {
		t.Fatal(got)
	}
	filtered, err := req.Config.filter().CBOR(configBytes(t, map[string]any{"message": "secret-token", "value": []any{"secret-token"}}))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(filtered, []byte("secret-token")) {
		t.Fatal("CBOR diagnostics leaked sensitive setting")
	}
	setting := settingMessage(db.LutraSetting{Value: configBytes(t, "secret-token"), Sensitive: true})
	if len(setting.ValueCbor) != 0 || !setting.Sensitive {
		t.Fatal("public settings exposed value")
	}
	stored, _ := proto.Marshal(&lutrav1.ActionSpec{InputCbor: []byte{0x82, 0x80, 0xa0}, ConfigOverridesCbor: configBytes(t, map[string]string{"service/token": "secret-token"})})
	action, err := actionFromRow(actionRow{ActionSpec: stored})
	if err != nil {
		t.Fatal(err)
	}
	if len(action.ActionSpec.ConfigOverridesCbor) != 0 {
		t.Fatal("public action exposed overrides")
	}
}

func TestRunConfigurationSnapshot(t *testing.T) {
	url := os.Getenv("LUTRA_CONFIG_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("set LUTRA_CONFIG_TEST_DATABASE_URL to an empty migrated database")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	q := db.New(pool)
	namespace := uuid.New()
	if _, err := q.CreateNamespace(ctx, db.CreateNamespaceParams{ID: namespace, Slug: "config-test", Name: "Config test"}); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"invocationId":"inv_config"}`))
	}))
	defer server.Close()
	service := Service{DB: pool, Logs: runlog.Service{DB: pool}, Durable: &DurableAdapter{Dispatch: &Dispatcher{Ingress: server.URL}}}
	upsert := func(value string) {
		t.Helper()
		res, err := service.UpsertSetting(ctx, connect.NewRequest(&lutrav1.UpsertSettingRequest{NamespaceId: namespace.String(), Path: "service/token", ValueCbor: configBytes(t, value), Sensitive: true}))
		if err != nil {
			t.Fatal(err)
		}
		if len(res.Msg.Setting.ValueCbor) != 0 {
			t.Fatal("upsert leaked")
		}
	}
	upsert("initial-secret")
	makeEnv := func(name string, dependencies []*lutrav1.EnvironmentIdentifier, config []*lutrav1.ConfigBinding) *lutrav1.EnvironmentIdentifier {
		t.Helper()
		id := uuid.New()
		version := strings.Repeat("a", 64)
		spec, err := proto.Marshal(&lutrav1.EnvironmentSpec{Name: name, NamespaceId: namespace.String(), Image: &lutrav1.ImageSpec{}, Dependencies: dependencies, Entrypoints: []*lutrav1.Entrypoint{{MaxAttempts: 1, Command: &lutrav1.Command{Args: []string{"python"}}, Config: config}}})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := q.InsertEnvironment(ctx, db.InsertEnvironmentParams{ID: id, NamespaceID: namespace, Name: name, Version: version, Spec: spec, Provider: "container", ImageKey: bytes.Repeat([]byte{1}, 32)}); err != nil {
			t.Fatal(err)
		}
		return &lutrav1.EnvironmentIdentifier{NamespaceId: namespace.String(), Name: name, Version: version}
	}
	dep := makeEnv("dependency", nil, []*lutrav1.ConfigBinding{{Name: "token", SettingRef: "service/token"}})
	root := makeEnv("root", []*lutrav1.EnvironmentIdentifier{dep}, nil)
	request := &lutrav1.CreateRunRequest{Environment: root, EntrypointId: 1, IdempotencyKey: "same", ActionSpec: &lutrav1.ActionSpec{InputCbor: []byte{0x82, 0x80, 0xa0}, MaxAttempts: 1}}
	first, err := service.CreateRun(ctx, connect.NewRequest(request))
	if err != nil {
		t.Fatal(err)
	}
	runID := uuid.MustParse(first.Msg.Run.Id)
	upsert("changed-secret")
	replay, err := service.CreateRun(ctx, connect.NewRequest(request))
	if err != nil {
		t.Fatal(err)
	}
	if replay.Msg.Run.Id != first.Msg.Run.Id {
		t.Fatal("idempotent replay created another run")
	}
	snapshot, err := loadRunConfig(ctx, q, runID)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(snapshot["service/token"].Value, configBytes(t, "initial-secret")) || !snapshot["service/token"].Sensitive {
		t.Fatal("snapshot changed after setting update")
	}
	request.IdempotencyKey = "override"
	request.ActionSpec.ConfigOverridesCbor = configBytes(t, map[string]string{"service/token": "override-secret"})
	overridden, err := service.CreateRun(ctx, connect.NewRequest(request))
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err = loadRunConfig(ctx, q, uuid.MustParse(overridden.Msg.Run.Id))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(snapshot["service/token"].Value, configBytes(t, "override-secret")) {
		t.Fatal("dependency override missing")
	}
	if _, err := q.DeleteSetting(ctx, db.DeleteSettingParams{NamespaceID: namespace, Path: "service/token"}); err != nil {
		t.Fatal(err)
	}
	request.IdempotencyKey = "missing"
	request.ActionSpec.ConfigOverridesCbor = nil
	if _, err := service.CreateRun(ctx, connect.NewRequest(request)); err == nil || !strings.Contains(err.Error(), "config.missing") {
		t.Fatalf("missing config: %v", err)
	}
	var count int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM lutra.runs WHERE namespace_id=$1", namespace).Scan(&count); err != nil || count != 2 {
		t.Fatalf("invalid run persisted: %d, %v", count, err)
	}
}
