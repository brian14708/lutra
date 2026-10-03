package lutra

import (
	"context"
	"errors"
	"regexp"
	"strings"
	"time"

	"connectrpc.com/connect"
	lutrav1 "github.com/brian14708/lutra/gen/lutra/v1"
	lutrav1connect "github.com/brian14708/lutra/gen/lutra/v1/lutrav1connect"
	"github.com/brian14708/lutra/internal/db"
	"github.com/fxamacker/cbor/v2"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

var (
	slugPattern = regexp.MustCompile(`^[a-z][a-z0-9-]{0,62}$`)
	pathPattern = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,63}(/[a-z][a-z0-9_-]{0,63})*$`)
)

var _ lutrav1connect.SettingsServiceHandler = Service{}

func namespaceID(ctx context.Context, q *db.Queries, value string) (uuid.UUID, error) {
	id, err := uuid.Parse(value)
	if err != nil || id == uuid.Nil {
		return uuid.Nil, invalid("invalid namespace ID")
	}
	row, err := q.GetNamespaceByID(ctx, id)
	if errors.Is(err, pgx.ErrNoRows) {
		return uuid.Nil, connect.NewError(connect.CodeNotFound, errors.New("namespace not found"))
	}
	return row.ID, err
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

func namespaceMessage(row db.LutraNamespace) *lutrav1.Namespace {
	return &lutrav1.Namespace{Id: row.ID.String(), Slug: row.Slug, Name: row.Name, CreatedAt: timestamp(row.CreatedAt)}
}

func settingMessage(row db.LutraSetting) *lutrav1.Setting {
	return &lutrav1.Setting{NamespaceId: row.NamespaceID.String(), Path: row.Path, ValueCbor: row.Value, UpdatedAt: timestamp(row.UpdatedAt)}
}

func (s Service) CreateNamespace(ctx context.Context, req *connect.Request[lutrav1.CreateNamespaceRequest]) (*connect.Response[lutrav1.CreateNamespaceResponse], error) {
	if !slugPattern.MatchString(req.Msg.GetSlug()) {
		return nil, invalid("invalid namespace slug")
	}
	if err := validateName(req.Msg.GetName()); err != nil {
		return nil, err
	}
	id, err := uuid.NewV7()
	if err != nil {
		return nil, err
	}
	row, err := db.New(s.DB).CreateNamespace(ctx, db.CreateNamespaceParams{ID: id, Slug: req.Msg.GetSlug(), Name: req.Msg.GetName()})
	if err != nil {
		var databaseError *pgconn.PgError
		if errors.As(err, &databaseError) && databaseError.Code == "23505" {
			return nil, connect.NewError(connect.CodeAlreadyExists, err)
		}
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&lutrav1.CreateNamespaceResponse{Namespace: namespaceMessage(row)}), nil
}

func (s Service) ListNamespaces(ctx context.Context, _ *connect.Request[lutrav1.ListNamespacesRequest]) (*connect.Response[lutrav1.ListNamespacesResponse], error) {
	rows, err := db.New(s.DB).ListNamespaces(ctx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	out := &lutrav1.ListNamespacesResponse{}
	for _, row := range rows {
		out.Namespaces = append(out.Namespaces, namespaceMessage(row))
	}
	return connect.NewResponse(out), nil
}

func (s Service) UpsertSetting(ctx context.Context, req *connect.Request[lutrav1.UpsertSettingRequest]) (*connect.Response[lutrav1.UpsertSettingResponse], error) {
	q := db.New(s.DB)
	id, err := namespaceID(ctx, q, req.Msg.GetNamespaceId())
	if err != nil {
		return nil, err
	}
	if err := validatePath(req.Msg.GetPath()); err != nil {
		return nil, err
	}
	if err := validateCBOR(req.Msg.GetValueCbor()); err != nil {
		return nil, err
	}
	row, err := q.UpsertSetting(ctx, db.UpsertSettingParams{NamespaceID: id, Path: req.Msg.GetPath(), Value: req.Msg.GetValueCbor()})
	if err != nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition, err)
	}
	return connect.NewResponse(&lutrav1.UpsertSettingResponse{Setting: settingMessage(row)}), nil
}

func (s Service) DeleteSetting(ctx context.Context, req *connect.Request[lutrav1.DeleteSettingRequest]) (*connect.Response[lutrav1.DeleteSettingResponse], error) {
	q := db.New(s.DB)
	id, err := namespaceID(ctx, q, req.Msg.GetNamespaceId())
	if err != nil {
		return nil, err
	}
	if err := validatePath(req.Msg.GetPath()); err != nil {
		return nil, err
	}
	if _, err := q.DeleteSetting(ctx, db.DeleteSettingParams{NamespaceID: id, Path: req.Msg.GetPath()}); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&lutrav1.DeleteSettingResponse{}), nil
}

func (s Service) ListSettings(ctx context.Context, req *connect.Request[lutrav1.ListSettingsRequest]) (*connect.Response[lutrav1.ListSettingsResponse], error) {
	q := db.New(s.DB)
	id, err := namespaceID(ctx, q, req.Msg.GetNamespaceId())
	if err != nil {
		return nil, err
	}
	for _, p := range req.Msg.GetPaths() {
		if err := validatePath(p); err != nil {
			return nil, err
		}
	}
	rows, err := q.ListSettings(ctx, db.ListSettingsParams{NamespaceID: id, Paths: req.Msg.GetPaths()})
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	out := &lutrav1.ListSettingsResponse{}
	for _, row := range rows {
		out.Settings = append(out.Settings, settingMessage(row))
	}
	return connect.NewResponse(out), nil
}
