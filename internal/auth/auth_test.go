package auth

import (
	"context"
	"testing"
)

func TestRequireUsesProjectRolePolicy(t *testing.T) {
	enforcer, err := newEnforcer()
	if err != nil {
		t.Fatal(err)
	}
	store := &Store{}
	store.enforcer.Store(enforcer)
	if _, err := enforcer.AddGroupingPolicy("user-1", "viewer", "project-1"); err != nil {
		t.Fatal(err)
	}
	ctx := withPrincipal(context.Background(), Principal{ID: "user-1"})
	if err := store.Require(ctx, "project-1", "project", "read"); err != nil {
		t.Fatalf("expected read permission: %v", err)
	}
	if err := store.Require(ctx, "project-2", "project", "read"); err == nil {
		t.Fatal("expected unrelated project to be denied")
	}
}

func TestRandomSecretIsHashedAndPrefixed(t *testing.T) {
	secret, err := randomSecret()
	if err != nil {
		t.Fatal(err)
	}
	if len(secret) < 20 || secretPrefix(secret) != secret[:11] || len(secretHash(secret)) != 32 {
		t.Fatalf("unexpected secret metadata: secret length %d, hash length %d", len(secret), len(secretHash(secret)))
	}
}

// Casbin keeps policy rules inside the model, so newEnforcer must hand each
// enforcer its own copy. Otherwise a reload would inherit the previous
// enforcer's memberships instead of replacing them.
func TestNewEnforcerDoesNotSharePolicyState(t *testing.T) {
	first, err := newEnforcer()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.AddGroupingPolicy("user-1", "admin", "project-1"); err != nil {
		t.Fatal(err)
	}
	second, err := newEnforcer()
	if err != nil {
		t.Fatal(err)
	}
	rules, err := second.GetGroupingPolicy()
	if err != nil {
		t.Fatal(err)
	}
	if len(rules) != 0 {
		t.Fatalf("new enforcer inherited %d grouping policies", len(rules))
	}
	if ok, err := second.Enforce("user-1", "project-1", "project", "read"); err != nil || ok {
		t.Fatalf("new enforcer authorized a stale membership: allowed=%v err=%v", ok, err)
	}
}
