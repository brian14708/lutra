// Package auth contains Lutra authentication and project authorization.
package auth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"embed"
	"encoding/base64"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"uuid"

	"connectrpc.com/connect"
	lutrav1 "github.com/brian14708/lutra/gen/lutra/v1"
	"github.com/brian14708/lutra/internal/db"
	"github.com/brian14708/lutra/internal/rpcutil"
	"github.com/casbin/casbin/v2"
	casbinmodel "github.com/casbin/casbin/v2/model"
	"github.com/coreos/go-oidc/v3/oidc"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

type contextKey struct{}

// Principal identifies the authenticated caller.
type Principal struct {
	ID string
}

func withPrincipal(ctx context.Context, principal Principal) context.Context {
	return context.WithValue(ctx, contextKey{}, principal)
}

// PrincipalFromContext returns the authenticated principal, or an error when the
// request carries no credential.
func PrincipalFromContext(ctx context.Context) (Principal, error) {
	principal, ok := ctx.Value(contextKey{}).(Principal)
	if !ok {
		return Principal{}, rpcutil.Unauthenticated()
	}
	return principal, nil
}

// Store persists identities and keeps the Casbin policy in memory.
type Store struct {
	pool     *pgxpool.Pool
	queries  *db.Queries
	enforcer atomic.Pointer[casbin.Enforcer]
	reloadMu sync.Mutex
	verifier *oidc.IDTokenVerifier
}

//go:embed model.conf
var modelFS embed.FS

// authorizationModel parses the embedded Casbin model once. The model is
// immutable, so every enforcer this process builds shares one parsed copy.
var authorizationModel = sync.OnceValues(func() (casbinmodel.Model, error) {
	modelText, err := fs.ReadFile(modelFS, "model.conf")
	if err != nil {
		return nil, fmt.Errorf("read authorization model: %w", err)
	}
	model, err := casbinmodel.NewModelFromString(string(modelText))
	if err != nil {
		return nil, fmt.Errorf("create authorization model: %w", err)
	}
	return model, nil
})

// roleActions maps each database role to the actions it grants. The project and
// resource dimensions are wildcarded, so a role applies to every project.
var roleActions = map[string][]string{
	string(db.LutraProjectRoleViewer):    {"read"},
	string(db.LutraProjectRoleDeveloper): {"read", "register"},
	string(db.LutraProjectRoleRunner):    {"read", "register", "execute"},
	string(db.LutraProjectRoleOperator):  {"read", "register", "execute", "operate"},
	string(db.LutraProjectRoleAdmin):     {"*"},
}

// NewStore initializes authentication and the in-memory policy cache.
func NewStore(ctx context.Context, pool *pgxpool.Pool) (*Store, error) {
	store := &Store{pool: pool, queries: db.New(pool)}
	if issuer := os.Getenv("OIDC_ISSUER"); issuer != "" {
		provider, err := oidc.NewProvider(ctx, issuer)
		if err != nil {
			return nil, fmt.Errorf("initialize OIDC provider: %w", err)
		}
		audience := os.Getenv("OIDC_AUDIENCE")
		if audience == "" {
			return nil, errors.New("OIDC_AUDIENCE is required when OIDC_ISSUER is configured")
		}
		store.verifier = provider.Verifier(&oidc.Config{ClientID: audience})
	}
	if err := store.ReloadPolicies(ctx); err != nil {
		return nil, err
	}
	return store, nil
}

// newEnforcer builds an enforcer carrying the static role policies. Membership
// policies are loaded separately, by ReloadPolicies.
func newEnforcer() (*casbin.Enforcer, error) {
	model, err := authorizationModel()
	if err != nil {
		return nil, err
	}
	// Casbin stores policy rules inside the model, so each enforcer gets its own
	// copy; sharing one would carry the previous enforcer's rules into the next.
	enforcer, err := casbin.NewEnforcer(model.Copy())
	if err != nil {
		return nil, fmt.Errorf("create authorizer: %w", err)
	}
	for role, actions := range roleActions {
		for _, action := range actions {
			if _, err := enforcer.AddPolicy(role, "*", "*", action, "allow"); err != nil {
				return nil, fmt.Errorf("add authorization policy: %w", err)
			}
		}
	}
	return enforcer, nil
}

// Bootstrap initializes the first administrator during server startup. It is
// intentionally not exposed as an RPC. The configured secret becomes the
// first user's API key and is never stored in plaintext.
func (s *Store) Bootstrap(ctx context.Context) error {
	// The transaction lock serializes initializers across server replicas.
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin bootstrap: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	q := s.queries.WithTx(tx)
	if err := q.AcquireBootstrapLock(ctx); err != nil {
		return fmt.Errorf("lock bootstrap: %w", err)
	}
	adminExists, err := q.AdminExists(ctx)
	if err != nil {
		return fmt.Errorf("check administrator: %w", err)
	}
	if adminExists {
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit bootstrap check: %w", err)
		}
		return s.ReloadPolicies(ctx)
	}
	// Removing the last administrator must never re-enable bootstrap.
	populated, err := q.UsersExist(ctx)
	if err != nil {
		return fmt.Errorf("check existing users: %w", err)
	}
	if populated {
		return errors.New("database has users but no administrator; restore an administrator role")
	}
	secret := os.Getenv("LUTRA_BOOTSTRAP_API_KEY")
	if secret == "" {
		return errors.New("LUTRA_BOOTSTRAP_API_KEY is required for an uninitialized database")
	}
	const userName = "Administrator"
	const projectName = "default"
	user, err := q.InsertUser(ctx, db.InsertUserParams{UserID: uuid.New(), DisplayName: userName, Email: ""})
	if err != nil {
		return fmt.Errorf("create bootstrap user: %w", err)
	}
	project, err := q.CreateProject(ctx, db.CreateProjectParams{ProjectID: uuid.New(), Name: projectName, Metadata: []byte(`{}`)})
	if err != nil {
		return fmt.Errorf("create bootstrap project: %w", err)
	}
	if err := q.InsertProjectMembership(ctx, db.InsertProjectMembershipParams{ProjectID: project.ProjectID, UserID: user.UserID, Role: db.LutraProjectRoleAdmin}); err != nil {
		return fmt.Errorf("grant bootstrap role: %w", err)
	}
	if _, err := q.InsertAPIKey(ctx, db.InsertAPIKeyParams{ApiKeyID: uuid.New(), UserID: user.UserID, Name: "bootstrap-admin", Prefix: secretPrefix(secret), SecretHash: secretHash(secret), ExpireTime: pgtype.Timestamptz{}}); err != nil {
		return fmt.Errorf("create bootstrap api key: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit bootstrap: %w", err)
	}
	return s.ReloadPolicies(ctx)
}

// ReloadPolicies replaces all Casbin memberships from PostgreSQL atomically from
// callers' perspective. A failed load leaves the existing policy untouched.
func (s *Store) ReloadPolicies(ctx context.Context) error {
	s.reloadMu.Lock()
	defer s.reloadMu.Unlock()

	rows, err := s.queries.ListActiveProjectMemberships(ctx)
	if err != nil {
		return fmt.Errorf("load project memberships: %w", err)
	}
	enforcer, err := newEnforcer()
	if err != nil {
		return err
	}
	for _, m := range rows {
		if _, err := enforcer.AddGroupingPolicy(m.UserID.String(), string(m.Role), m.ProjectID.String()); err != nil {
			return fmt.Errorf("add project membership policy: %w", err)
		}
	}
	s.enforcer.Store(enforcer)
	return nil
}

// Require checks a user's role for a project operation.
func (s *Store) Require(ctx context.Context, projectID, resource, action string) error {
	principal, err := PrincipalFromContext(ctx)
	if err != nil {
		return err
	}
	// Recheck membership and project status so removed memberships and
	// archived projects cannot remain authorized through a stale cache.
	if s.queries != nil {
		active, err := s.queries.HasActiveProjectMembership(ctx, db.HasActiveProjectMembershipParams{UserID: rpcutil.UUID(principal.ID), ProjectID: rpcutil.UUID(projectID)})
		if err != nil {
			return rpcutil.Internal(fmt.Errorf("check project membership: %w", err))
		}
		if !active {
			return rpcutil.NotFound()
		}
	}
	ok, err := s.enforcer.Load().Enforce(principal.ID, projectID, resource, action)
	if err != nil {
		return rpcutil.Internal(fmt.Errorf("authorize request: %w", err))
	}
	if !ok {
		return rpcutil.NotFound()
	}
	return nil
}

// RequireAnyAdmin checks that the caller administers at least one project.
func (s *Store) RequireAnyAdmin(ctx context.Context) error {
	principal, err := PrincipalFromContext(ctx)
	if err != nil {
		return err
	}
	admin, err := s.queries.AnyAdmin(ctx, rpcutil.UUID(principal.ID))
	if err != nil {
		return rpcutil.Internal(err)
	}
	if !admin {
		return rpcutil.NotFound()
	}
	return nil
}

// Authenticate resolves an Authorization bearer value to a principal.
func (s *Store) Authenticate(ctx context.Context, authorization string) (Principal, error) {
	const prefix = "Bearer "
	if !strings.HasPrefix(authorization, prefix) {
		return Principal{}, rpcutil.Unauthenticated()
	}
	secret := strings.TrimSpace(strings.TrimPrefix(authorization, prefix))
	if secret == "" {
		return Principal{}, rpcutil.Unauthenticated()
	}
	userID, err := s.queries.TouchAPIKeyPrincipal(ctx, secretHash(secret))
	if err == nil {
		return Principal{ID: userID.String()}, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return Principal{}, rpcutil.Internal(errors.New("authenticate api key"))
	}
	if s.verifier == nil {
		return Principal{}, rpcutil.Unauthenticated()
	}
	idToken, err := s.verifier.Verify(ctx, secret)
	if err != nil {
		return Principal{}, rpcutil.Unauthenticated()
	}
	var claims struct {
		Subject string `json:"sub"`
	}
	if err := idToken.Claims(&claims); err != nil || claims.Subject == "" {
		return Principal{}, rpcutil.Unauthenticated()
	}
	userID, err = s.queries.FindOIDCUser(ctx, db.FindOIDCUserParams{Issuer: idToken.Issuer, Subject: claims.Subject})
	if err != nil {
		return Principal{}, rpcutil.Unauthenticated()
	}
	return Principal{ID: userID.String()}, nil
}

// Interceptor authenticates every business RPC.
func (s *Store) Interceptor() connect.Interceptor {
	return connect.UnaryInterceptorFunc(func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			principal, err := s.Authenticate(ctx, req.Header().Get("Authorization"))
			if err != nil {
				return nil, err
			}
			return next(withPrincipal(ctx, principal), req)
		}
	})
}

// randomSecret returns a new API key secret. Its prefix and hash are derived
// from the secret by secretPrefix and secretHash.
func randomSecret() (string, error) {
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		return "", err
	}
	return "lt_" + base64.RawURLEncoding.EncodeToString(secret), nil
}

func secretPrefix(secret string) string { return secret[:min(len(secret), 11)] }

func secretHash(secret string) []byte {
	hash := sha256.Sum256([]byte(secret))
	return hash[:]
}

func userStatus(status db.LutraUserStatus) lutrav1.UserStatus {
	switch status {
	case db.LutraUserStatusActive:
		return lutrav1.UserStatus_USER_STATUS_ACTIVE
	case db.LutraUserStatusSuspended:
		return lutrav1.UserStatus_USER_STATUS_SUSPENDED
	default:
		// The PostgreSQL enum prevents this on a healthy database. Keep
		// corruption visible to API clients instead of silently claiming ACTIVE.
		return lutrav1.UserStatus_USER_STATUS_UNSPECIFIED
	}
}

var roleNames = map[lutrav1.ProjectRole]db.LutraProjectRole{
	lutrav1.ProjectRole_PROJECT_ROLE_VIEWER:    db.LutraProjectRoleViewer,
	lutrav1.ProjectRole_PROJECT_ROLE_DEVELOPER: db.LutraProjectRoleDeveloper,
	lutrav1.ProjectRole_PROJECT_ROLE_RUNNER:    db.LutraProjectRoleRunner,
	lutrav1.ProjectRole_PROJECT_ROLE_OPERATOR:  db.LutraProjectRoleOperator,
	lutrav1.ProjectRole_PROJECT_ROLE_ADMIN:     db.LutraProjectRoleAdmin,
}

func roleString(role lutrav1.ProjectRole) (db.LutraProjectRole, error) {
	value, ok := roleNames[role]
	if !ok {
		return "", errors.New("invalid project role")
	}
	return value, nil
}

// roleProto is the inverse of roleString; an unrecognized name maps to
// PROJECT_ROLE_UNSPECIFIED.
func roleProto(role db.LutraProjectRole) lutrav1.ProjectRole {
	for proto, name := range roleNames {
		if name == role {
			return proto
		}
	}
	return lutrav1.ProjectRole_PROJECT_ROLE_UNSPECIFIED
}

// Queries exposes the typed database queries to sibling services.
func (s *Store) Queries() *db.Queries { return s.queries }

// Begin starts a transaction for operations that need to update multiple
// records atomically.
func (s *Store) Begin(ctx context.Context) (pgx.Tx, error) { return s.pool.Begin(ctx) }
