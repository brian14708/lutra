package settings

import (
	"context"
	"errors"
	"regexp"
	"sort"
	"strings"
	"time"

	"connectrpc.com/connect"
	lutrav1 "github.com/brian14708/lutra/gen/lutra/v1"
	lutrav1connect "github.com/brian14708/lutra/gen/lutra/v1/lutrav1connect"
	"github.com/brian14708/lutra/internal/db"
	"github.com/fxamacker/cbor/v2"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	slugPattern = regexp.MustCompile(`^[a-z][a-z0-9-]{0,62}$`)
	pathPattern = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,63}(/[a-z][a-z0-9_-]{0,63})*$`)
)

type Service struct{ DB *pgxpool.Pool }

var _ lutrav1connect.SettingsServiceHandler = Service{}

func invalid(message string) error {
	return connect.NewError(connect.CodeInvalidArgument, errors.New(message))
}

func parseID(value, field string) (uuid.UUID, error) {
	id, err := uuid.Parse(value)
	if err != nil || id == uuid.Nil {
		return uuid.Nil, invalid("invalid " + field)
	}
	return id, nil
}

func validateSlug(value, field string) error {
	if !slugPattern.MatchString(value) {
		return invalid("invalid " + field)
	}
	return nil
}

func validateName(value string) error {
	if n := len(strings.TrimSpace(value)); n == 0 || n > 200 {
		return invalid("invalid name")
	}
	return nil
}

func validatePath(value string) error {
	if !pathPattern.MatchString(value) || len(value) > 512 {
		return invalid("invalid setting path")
	}
	return nil
}

func validateCBOR(value []byte) error {
	if len(value) == 0 || len(value) > 65536 {
		return invalid("invalid CBOR value size")
	}
	var decoded any
	if err := cbor.Unmarshal(value, &decoded); err != nil {
		return invalid("invalid CBOR value")
	}
	return nil
}

func timestamp(t pgtype.Timestamptz) string {
	if !t.Valid {
		return ""
	}
	return t.Time.UTC().Format(time.RFC3339Nano)
}

func projectMessage(row db.LutraProject) *lutrav1.Project {
	return &lutrav1.Project{Id: row.ID.String(), Slug: row.Slug, Name: row.Name, CreatedAt: timestamp(row.CreatedAt)}
}

func domainMessage(row db.LutraDomain) *lutrav1.Domain {
	return &lutrav1.Domain{Id: row.ID.String(), ProjectId: row.ProjectID.String(), Slug: row.Slug, Name: row.Name, CreatedAt: timestamp(row.CreatedAt)}
}

func settingMessage(row db.LutraSetting) *lutrav1.Setting {
	domain := ""
	if row.DomainID != nil {
		domain = row.DomainID.String()
	}
	return &lutrav1.Setting{ProjectId: row.ProjectID.String(), DomainId: domain, Path: row.Path, ValueCbor: row.Value, UpdatedAt: timestamp(row.UpdatedAt)}
}

func (s Service) CreateProject(ctx context.Context, req *connect.Request[lutrav1.CreateProjectRequest]) (*connect.Response[lutrav1.CreateProjectResponse], error) {
	if err := validateSlug(req.Msg.GetSlug(), "project slug"); err != nil {
		return nil, err
	}
	if err := validateName(req.Msg.GetName()); err != nil {
		return nil, err
	}
	row, err := db.New(s.DB).CreateProject(ctx, db.CreateProjectParams{ID: uuid.New(), Slug: req.Msg.GetSlug(), Name: req.Msg.GetName()})
	if err != nil {
		return nil, connect.NewError(connect.CodeAlreadyExists, err)
	}
	return connect.NewResponse(&lutrav1.CreateProjectResponse{Project: projectMessage(row)}), nil
}

func (s Service) ListProjects(ctx context.Context, _ *connect.Request[lutrav1.ListProjectsRequest]) (*connect.Response[lutrav1.ListProjectsResponse], error) {
	rows, err := db.New(s.DB).ListProjects(ctx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	result := &lutrav1.ListProjectsResponse{Projects: make([]*lutrav1.Project, 0, len(rows))}
	for _, row := range rows {
		result.Projects = append(result.Projects, projectMessage(row))
	}
	return connect.NewResponse(result), nil
}

func (s Service) CreateDomain(ctx context.Context, req *connect.Request[lutrav1.CreateDomainRequest]) (*connect.Response[lutrav1.CreateDomainResponse], error) {
	projectID, err := parseID(req.Msg.GetProjectId(), "project ID")
	if err != nil {
		return nil, err
	}
	if err := validateSlug(req.Msg.GetSlug(), "domain slug"); err != nil {
		return nil, err
	}
	if err := validateName(req.Msg.GetName()); err != nil {
		return nil, err
	}
	row, err := db.New(s.DB).CreateDomain(ctx, db.CreateDomainParams{ID: uuid.New(), ProjectID: projectID, Slug: req.Msg.GetSlug(), Name: req.Msg.GetName()})
	if err != nil {
		return nil, connect.NewError(connect.CodeAlreadyExists, err)
	}
	return connect.NewResponse(&lutrav1.CreateDomainResponse{Domain: domainMessage(row)}), nil
}

func (s Service) ListDomains(ctx context.Context, req *connect.Request[lutrav1.ListDomainsRequest]) (*connect.Response[lutrav1.ListDomainsResponse], error) {
	projectID, err := parseID(req.Msg.GetProjectId(), "project ID")
	if err != nil {
		return nil, err
	}
	rows, err := db.New(s.DB).ListDomains(ctx, projectID)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	result := &lutrav1.ListDomainsResponse{Domains: make([]*lutrav1.Domain, 0, len(rows))}
	for _, row := range rows {
		result.Domains = append(result.Domains, domainMessage(row))
	}
	return connect.NewResponse(result), nil
}

func optionalDomain(value string) (*uuid.UUID, error) {
	if value == "" {
		return nil, nil
	}
	id, err := parseID(value, "domain ID")
	return &id, err
}

func (s Service) UpsertSetting(ctx context.Context, req *connect.Request[lutrav1.UpsertSettingRequest]) (*connect.Response[lutrav1.UpsertSettingResponse], error) {
	projectID, err := parseID(req.Msg.GetProjectId(), "project ID")
	if err != nil {
		return nil, err
	}
	domainID, err := optionalDomain(req.Msg.GetDomainId())
	if err != nil {
		return nil, err
	}
	if err := validatePath(req.Msg.GetPath()); err != nil {
		return nil, err
	}
	if err := validateCBOR(req.Msg.GetValueCbor()); err != nil {
		return nil, err
	}
	row, err := db.New(s.DB).UpsertSetting(ctx, db.UpsertSettingParams{ProjectID: projectID, DomainID: domainID, Path: req.Msg.GetPath(), Value: req.Msg.GetValueCbor()})
	if err != nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition, err)
	}
	return connect.NewResponse(&lutrav1.UpsertSettingResponse{Setting: settingMessage(row)}), nil
}

func (s Service) DeleteSetting(ctx context.Context, req *connect.Request[lutrav1.DeleteSettingRequest]) (*connect.Response[lutrav1.DeleteSettingResponse], error) {
	projectID, err := parseID(req.Msg.GetProjectId(), "project ID")
	if err != nil {
		return nil, err
	}
	domainID, err := optionalDomain(req.Msg.GetDomainId())
	if err != nil {
		return nil, err
	}
	if err := validatePath(req.Msg.GetPath()); err != nil {
		return nil, err
	}
	if _, err := db.New(s.DB).DeleteSetting(ctx, db.DeleteSettingParams{ProjectID: projectID, DomainID: domainID, Path: req.Msg.GetPath()}); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&lutrav1.DeleteSettingResponse{}), nil
}

func (s Service) ListSettings(ctx context.Context, req *connect.Request[lutrav1.ListSettingsRequest]) (*connect.Response[lutrav1.ListSettingsResponse], error) {
	projectID, err := parseID(req.Msg.GetProjectId(), "project ID")
	if err != nil {
		return nil, err
	}
	domainID, err := optionalDomain(req.Msg.GetDomainId())
	if err != nil {
		return nil, err
	}
	rows, err := db.New(s.DB).ListSettings(ctx, db.ListSettingsParams{ProjectID: projectID, DomainID: domainID})
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	result := &lutrav1.ListSettingsResponse{Settings: make([]*lutrav1.Setting, 0, len(rows))}
	for _, row := range rows {
		result.Settings = append(result.Settings, settingMessage(row))
	}
	return connect.NewResponse(result), nil
}

func (s Service) ResolveSettings(ctx context.Context, req *connect.Request[lutrav1.ResolveSettingsRequest]) (*connect.Response[lutrav1.ResolveSettingsResponse], error) {
	projectID, err := parseID(req.Msg.GetProjectId(), "project ID")
	if err != nil {
		return nil, err
	}
	domainID, err := optionalDomain(req.Msg.GetDomainId())
	if err != nil {
		return nil, err
	}
	for _, path := range req.Msg.GetPaths() {
		if err := validatePath(path); err != nil {
			return nil, err
		}
	}
	rows, err := db.New(s.DB).ResolveSettings(ctx, db.ResolveSettingsParams{ProjectID: projectID, DomainID: domainID, Column3: req.Msg.GetPaths()})
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	merged := make(map[string]db.LutraSetting, len(rows))
	for _, row := range rows {
		if _, ok := merged[row.Path]; !ok || row.DomainID != nil {
			merged[row.Path] = row
		}
	}
	result := &lutrav1.ListSettingsResponse{Settings: make([]*lutrav1.Setting, 0, len(merged))}
	paths := make([]string, 0, len(merged))
	for path := range merged {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	for _, path := range paths {
		result.Settings = append(result.Settings, settingMessage(merged[path]))
	}
	return connect.NewResponse(&lutrav1.ResolveSettingsResponse{Settings: result.Settings}), nil
}
