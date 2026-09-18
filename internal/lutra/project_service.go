package lutra

import (
	"context"
	"encoding/json"
	"uuid"

	"connectrpc.com/connect"
	lutrav1 "github.com/brian14708/lutra/gen/lutra/v1"
	lutrav1connect "github.com/brian14708/lutra/gen/lutra/v1/lutrav1connect"
	"github.com/brian14708/lutra/internal/auth"
	"github.com/brian14708/lutra/internal/db"
	"github.com/brian14708/lutra/internal/rpcutil"
	"github.com/jackc/pgx/v5/pgtype"
)

// ProjectService implements project lifecycle operations.
type ProjectService struct {
	lutrav1connect.UnimplementedProjectServiceHandler
	store *auth.Store
}

// NewProjectService creates a project service backed by store.
func NewProjectService(store *auth.Store) *ProjectService {
	return &ProjectService{store: store}
}

// CreateProject creates a project and grants its creator administrator access.
func (s *ProjectService) CreateProject(ctx context.Context, req *connect.Request[lutrav1.CreateProjectRequest]) (*connect.Response[lutrav1.CreateProjectResponse], error) {
	principal, err := auth.PrincipalFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if err := s.store.RequireAnyAdmin(ctx); err != nil {
		return nil, err
	}
	metadataJSON, err := json.Marshal(req.Msg.GetMetadata())
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	tx, err := s.store.Begin(ctx)
	if err != nil {
		return nil, rpcutil.Internal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	q := s.store.Queries().WithTx(tx)
	project, err := q.CreateProject(ctx, db.CreateProjectParams{ProjectID: uuid.New(), Name: req.Msg.GetName(), Metadata: metadataJSON})
	if err != nil {
		return nil, rpcutil.Conflict(err)
	}
	if err := q.InsertProjectMembership(ctx, db.InsertProjectMembershipParams{ProjectID: project.ProjectID, UserID: rpcutil.UUID(principal.ID), Role: db.LutraProjectRoleAdmin}); err != nil {
		return nil, rpcutil.Internal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, rpcutil.Internal(err)
	}
	if err := s.store.ReloadPolicies(ctx); err != nil {
		return nil, rpcutil.Internal(err)
	}
	return connect.NewResponse(&lutrav1.CreateProjectResponse{Project: projectMessage(project.ProjectID, project.Name, req.Msg.GetMetadata(), project.CreateTime, project.UpdateTime)}), nil
}

// GetProject returns a project visible to the caller.
func (s *ProjectService) GetProject(ctx context.Context, req *connect.Request[lutrav1.GetProjectRequest]) (*connect.Response[lutrav1.GetProjectResponse], error) {
	if err := s.store.Require(ctx, req.Msg.GetProjectId(), "project", "read"); err != nil {
		return nil, err
	}
	project, err := s.store.Queries().GetProject(ctx, rpcutil.UUID(req.Msg.GetProjectId()))
	if err != nil {
		return nil, rpcutil.DB(err)
	}
	metadata, err := metadataMap(project.Metadata)
	if err != nil {
		return nil, rpcutil.Internal(err)
	}
	return connect.NewResponse(&lutrav1.GetProjectResponse{Project: projectMessage(project.ProjectID, project.Name, metadata, project.CreateTime, project.UpdateTime)}), nil
}

// ListProjects returns projects visible to the caller.
func (s *ProjectService) ListProjects(ctx context.Context, _ *connect.Request[lutrav1.ListProjectsRequest]) (*connect.Response[lutrav1.ListProjectsResponse], error) {
	principal, err := auth.PrincipalFromContext(ctx)
	if err != nil {
		return nil, err
	}
	projects, err := s.store.Queries().ListProjectsForUser(ctx, rpcutil.UUID(principal.ID))
	if err != nil {
		return nil, rpcutil.Internal(err)
	}
	response := &lutrav1.ListProjectsResponse{Projects: make([]*lutrav1.Project, 0, len(projects))}
	for _, p := range projects {
		metadata, err := metadataMap(p.Metadata)
		if err != nil {
			return nil, rpcutil.Internal(err)
		}
		response.Projects = append(response.Projects, projectMessage(p.ProjectID, p.Name, metadata, p.CreateTime, p.UpdateTime))
	}
	return connect.NewResponse(response), nil
}

// UpdateProject changes a project's name and metadata.
func (s *ProjectService) UpdateProject(ctx context.Context, req *connect.Request[lutrav1.UpdateProjectRequest]) (*connect.Response[lutrav1.UpdateProjectResponse], error) {
	if err := s.store.Require(ctx, req.Msg.GetProjectId(), "project", "admin"); err != nil {
		return nil, err
	}
	metadataJSON, err := json.Marshal(req.Msg.GetMetadata())
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	project, err := s.store.Queries().UpdateProject(ctx, db.UpdateProjectParams{ProjectID: rpcutil.UUID(req.Msg.GetProjectId()), Name: req.Msg.GetName(), Metadata: metadataJSON})
	if err != nil {
		return nil, rpcutil.DB(err)
	}
	return connect.NewResponse(&lutrav1.UpdateProjectResponse{Project: projectMessage(project.ProjectID, project.Name, req.Msg.GetMetadata(), project.CreateTime, project.UpdateTime)}), nil
}

// DeleteProject archives a project.
func (s *ProjectService) DeleteProject(ctx context.Context, req *connect.Request[lutrav1.DeleteProjectRequest]) (*connect.Response[lutrav1.DeleteProjectResponse], error) {
	if err := s.store.Require(ctx, req.Msg.GetProjectId(), "project", "admin"); err != nil {
		return nil, err
	}
	// Archiving leaves every membership untouched, so the policy cache already
	// reflects the result and needs no reload.
	if err := s.store.Queries().ArchiveProject(ctx, rpcutil.UUID(req.Msg.GetProjectId())); err != nil {
		return nil, rpcutil.Internal(err)
	}
	return connect.NewResponse(&lutrav1.DeleteProjectResponse{}), nil
}

func projectMessage(id uuid.UUID, name string, metadata map[string]string, created, updated pgtype.Timestamptz) *lutrav1.Project {
	return &lutrav1.Project{ProjectId: id.String(), Name: name, Metadata: metadata, CreateTime: rpcutil.Timestamp(created), UpdateTime: rpcutil.Timestamp(updated)}
}

func metadataMap(raw []byte) (map[string]string, error) {
	values := map[string]string{}
	if err := json.Unmarshal(raw, &values); err != nil {
		return nil, err
	}
	return values, nil
}
