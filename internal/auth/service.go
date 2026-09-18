package auth

import (
	"context"
	"errors"
	"uuid"

	"connectrpc.com/connect"
	lutrav1 "github.com/brian14708/lutra/gen/lutra/v1"
	lutrav1connect "github.com/brian14708/lutra/gen/lutra/v1/lutrav1connect"
	"github.com/brian14708/lutra/internal/db"
	"github.com/brian14708/lutra/internal/rpcutil"
	"github.com/jackc/pgx/v5/pgtype"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Service implements AuthService against PostgreSQL.
type Service struct {
	lutrav1connect.UnimplementedAuthServiceHandler
	store *Store
}

// NewService creates an authentication RPC service backed by store.
func NewService(store *Store) *Service { return &Service{store: store} }

// CreateUser creates a user account.
func (s *Service) CreateUser(ctx context.Context, req *connect.Request[lutrav1.CreateUserRequest]) (*connect.Response[lutrav1.CreateUserResponse], error) {
	if err := s.store.RequireAnyAdmin(ctx); err != nil {
		return nil, err
	}
	user, err := s.store.Queries().InsertUser(ctx, db.InsertUserParams{UserID: uuid.New(), DisplayName: req.Msg.GetDisplayName(), Email: req.Msg.GetEmail()})
	if err != nil {
		return nil, rpcutil.Conflict(err)
	}
	return connect.NewResponse(&lutrav1.CreateUserResponse{User: userProto(user)}), nil
}

// ListUsers returns all user accounts.
func (s *Service) ListUsers(ctx context.Context, _ *connect.Request[lutrav1.ListUsersRequest]) (*connect.Response[lutrav1.ListUsersResponse], error) {
	if err := s.store.RequireAnyAdmin(ctx); err != nil {
		return nil, err
	}
	users, err := s.store.Queries().ListUsers(ctx)
	if err != nil {
		return nil, rpcutil.Internal(err)
	}
	response := &lutrav1.ListUsersResponse{Users: make([]*lutrav1.User, 0, len(users))}
	for _, user := range users {
		response.Users = append(response.Users, userProto(user))
	}
	return connect.NewResponse(response), nil
}

// SetUserStatus changes a user's account status.
func (s *Service) SetUserStatus(ctx context.Context, req *connect.Request[lutrav1.SetUserStatusRequest]) (*connect.Response[lutrav1.SetUserStatusResponse], error) {
	if err := s.store.RequireAnyAdmin(ctx); err != nil {
		return nil, err
	}
	var status db.LutraUserStatus
	switch req.Msg.GetStatus() {
	case lutrav1.UserStatus_USER_STATUS_ACTIVE:
		status = db.LutraUserStatusActive
	case lutrav1.UserStatus_USER_STATUS_SUSPENDED:
		status = db.LutraUserStatusSuspended
	default:
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("invalid user status"))
	}
	user, err := s.store.Queries().UpdateUserStatus(ctx, db.UpdateUserStatusParams{UserID: rpcutil.UUID(req.Msg.GetUserId()), Status: status})
	if err != nil {
		return nil, rpcutil.DB(err)
	}
	return connect.NewResponse(&lutrav1.SetUserStatusResponse{User: userProto(user)}), nil
}

// LinkOIDCIdentity associates an OIDC subject with a user account.
func (s *Service) LinkOIDCIdentity(ctx context.Context, req *connect.Request[lutrav1.LinkOIDCIdentityRequest]) (*connect.Response[lutrav1.LinkOIDCIdentityResponse], error) {
	if err := s.store.RequireAnyAdmin(ctx); err != nil {
		return nil, err
	}
	user, err := s.store.Queries().GetUser(ctx, rpcutil.UUID(req.Msg.GetUserId()))
	if err != nil {
		return nil, rpcutil.DB(err)
	}
	if err := s.store.Queries().InsertOIDCIdentity(ctx, db.InsertOIDCIdentityParams{Issuer: req.Msg.GetIssuer(), Subject: req.Msg.GetSubject(), UserID: rpcutil.UUID(req.Msg.GetUserId())}); err != nil {
		return nil, rpcutil.Internal(err)
	}
	return connect.NewResponse(&lutrav1.LinkOIDCIdentityResponse{User: userProto(user)}), nil
}

// CreateAPIKey creates an API key and returns its one-time secret.
func (s *Service) CreateAPIKey(ctx context.Context, req *connect.Request[lutrav1.CreateAPIKeyRequest]) (*connect.Response[lutrav1.CreateAPIKeyResponse], error) {
	if err := s.requireUserOrAdmin(ctx, req.Msg.GetUserId()); err != nil {
		return nil, err
	}
	secret, err := randomSecret()
	if err != nil {
		return nil, rpcutil.Internal(err)
	}
	key, err := s.store.Queries().InsertAPIKey(ctx, db.InsertAPIKeyParams{ApiKeyID: uuid.New(), UserID: rpcutil.UUID(req.Msg.GetUserId()), Name: req.Msg.GetName(), Prefix: secretPrefix(secret), SecretHash: secretHash(secret), ExpireTime: toPGTime(req.Msg.GetExpireTime())})
	if err != nil {
		return nil, rpcutil.Internal(err)
	}
	return connect.NewResponse(&lutrav1.CreateAPIKeyResponse{ApiKey: apiKeyProto(key.ApiKeyID, key.UserID, key.Name, key.Prefix, key.CreateTime, key.ExpireTime, key.RevokeTime), Secret: secret}), nil
}

// ListAPIKeys returns the API keys visible to the caller.
func (s *Service) ListAPIKeys(ctx context.Context, req *connect.Request[lutrav1.ListAPIKeysRequest]) (*connect.Response[lutrav1.ListAPIKeysResponse], error) {
	if err := s.requireUserOrAdmin(ctx, req.Msg.GetUserId()); err != nil {
		return nil, err
	}
	keys, err := s.store.Queries().ListAPIKeys(ctx, rpcutil.UUID(req.Msg.GetUserId()))
	if err != nil {
		return nil, rpcutil.Internal(err)
	}
	response := &lutrav1.ListAPIKeysResponse{ApiKeys: make([]*lutrav1.APIKey, 0, len(keys))}
	for _, key := range keys {
		response.ApiKeys = append(response.ApiKeys, apiKeyProto(key.ApiKeyID, key.UserID, key.Name, key.Prefix, key.CreateTime, key.ExpireTime, key.RevokeTime))
	}
	return connect.NewResponse(response), nil
}

// RevokeAPIKey revokes an API key.
func (s *Service) RevokeAPIKey(ctx context.Context, req *connect.Request[lutrav1.RevokeAPIKeyRequest]) (*connect.Response[lutrav1.RevokeAPIKeyResponse], error) {
	if _, err := s.requireKeyAccess(ctx, req.Msg.GetApiKeyId()); err != nil {
		return nil, err
	}
	key, err := s.store.Queries().RevokeAPIKey(ctx, rpcutil.UUID(req.Msg.GetApiKeyId()))
	if err != nil {
		return nil, rpcutil.Internal(err)
	}
	return connect.NewResponse(&lutrav1.RevokeAPIKeyResponse{ApiKey: apiKeyProto(key.ApiKeyID, key.UserID, key.Name, key.Prefix, key.CreateTime, key.ExpireTime, key.RevokeTime)}), nil
}

// RotateAPIKey revokes an API key and creates its replacement.
func (s *Service) RotateAPIKey(ctx context.Context, req *connect.Request[lutrav1.RotateAPIKeyRequest]) (*connect.Response[lutrav1.RotateAPIKeyResponse], error) {
	owner, err := s.requireKeyAccess(ctx, req.Msg.GetApiKeyId())
	if err != nil {
		return nil, err
	}
	secret, err := randomSecret()
	if err != nil {
		return nil, rpcutil.Internal(err)
	}
	tx, err := s.store.Begin(ctx)
	if err != nil {
		return nil, rpcutil.Internal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	q := s.store.Queries().WithTx(tx)
	if _, err := q.RevokeAPIKey(ctx, rpcutil.UUID(req.Msg.GetApiKeyId())); err != nil {
		return nil, rpcutil.Internal(err)
	}
	key, err := q.InsertAPIKey(ctx, db.InsertAPIKeyParams{ApiKeyID: uuid.New(), UserID: owner.UserID, Name: owner.Name, Prefix: secretPrefix(secret), SecretHash: secretHash(secret), ExpireTime: owner.ExpireTime})
	if err != nil {
		return nil, rpcutil.Internal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, rpcutil.Internal(err)
	}
	return connect.NewResponse(&lutrav1.RotateAPIKeyResponse{ApiKey: apiKeyProto(key.ApiKeyID, key.UserID, key.Name, key.Prefix, key.CreateTime, key.ExpireTime, key.RevokeTime), Secret: secret}), nil
}

// SetProjectRole assigns a user's role in a project.
func (s *Service) SetProjectRole(ctx context.Context, req *connect.Request[lutrav1.SetProjectRoleRequest]) (*connect.Response[lutrav1.SetProjectRoleResponse], error) {
	if err := s.store.Require(ctx, req.Msg.GetProjectId(), "project_membership", "admin"); err != nil {
		return nil, err
	}
	role, err := roleString(req.Msg.GetRole())
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	membership, err := s.store.Queries().AssignProjectRole(ctx, db.AssignProjectRoleParams{ProjectID: rpcutil.UUID(req.Msg.GetProjectId()), UserID: rpcutil.UUID(req.Msg.GetUserId()), Role: role})
	if err != nil {
		return nil, rpcutil.Internal(err)
	}
	if err := s.store.ReloadPolicies(ctx); err != nil {
		return nil, rpcutil.Internal(err)
	}
	return connect.NewResponse(&lutrav1.SetProjectRoleResponse{Membership: &lutrav1.ProjectMembership{ProjectId: membership.ProjectID.String(), UserId: membership.UserID.String(), Role: roleProto(membership.Role)}}), nil
}

// requireKeyAccess resolves the key owner and checks the caller is the owner or an admin.
func (s *Service) requireKeyAccess(ctx context.Context, apiKeyID string) (db.GetAPIKeyOwnerRow, error) {
	owner, err := s.store.Queries().GetAPIKeyOwner(ctx, rpcutil.UUID(apiKeyID))
	if err != nil {
		return db.GetAPIKeyOwnerRow{}, rpcutil.DB(err)
	}
	if err := s.requireUserOrAdmin(ctx, owner.UserID.String()); err != nil {
		return db.GetAPIKeyOwnerRow{}, err
	}
	return owner, nil
}

func (s *Service) requireUserOrAdmin(ctx context.Context, userID string) error {
	principal, err := PrincipalFromContext(ctx)
	if err != nil {
		return err
	}
	if principal.ID == userID {
		return nil
	}
	return s.store.RequireAnyAdmin(ctx)
}

func userProto(user db.LutraUser) *lutrav1.User {
	return &lutrav1.User{UserId: user.UserID.String(), DisplayName: user.DisplayName, Email: user.Email, Status: userStatus(user.Status), CreateTime: rpcutil.Timestamp(user.CreateTime), UpdateTime: rpcutil.Timestamp(user.UpdateTime)}
}

func apiKeyProto(id, userID uuid.UUID, name, prefix string, create, expire, revoke pgtype.Timestamptz) *lutrav1.APIKey {
	return &lutrav1.APIKey{ApiKeyId: id.String(), UserId: userID.String(), Name: name, Prefix: prefix, CreateTime: rpcutil.Timestamp(create), ExpireTime: rpcutil.Timestamp(expire), RevokeTime: rpcutil.Timestamp(revoke)}
}

func toPGTime(value *timestamppb.Timestamp) pgtype.Timestamptz {
	if value == nil {
		return pgtype.Timestamptz{}
	}
	return pgtype.Timestamptz{Time: value.AsTime(), Valid: true}
}
