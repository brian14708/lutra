// Package rpcutil provides helpers shared by the Connect RPC services.
package rpcutil

import (
	"errors"
	"uuid"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// UUID converts an API UUID string to the database UUID type. RPC validation
// rejects malformed IDs before handlers run; returning uuid.Nil here keeps
// direct service calls safe and makes them resolve to no database row.
func UUID(value string) uuid.UUID {
	parsed, err := uuid.Parse(value)
	if err != nil {
		return uuid.Nil()
	}
	return parsed
}

// Internal wraps err as an internal RPC error.
func Internal(err error) error { return connect.NewError(connect.CodeInternal, err) }

// Conflict maps a PostgreSQL unique constraint violation to AlreadyExists and
// preserves all other database failures as Internal.
func Conflict(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		return connect.NewError(connect.CodeAlreadyExists, errors.New("resource already exists"))
	}
	return Internal(err)
}

// DB maps a database error onto a Connect code: a missing row is NotFound and
// everything else is Internal.
func DB(err error) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return NotFound()
	}
	return Internal(err)
}

// NotFound reports a missing resource.
func NotFound() error {
	return connect.NewError(connect.CodeNotFound, errors.New("resource not found"))
}

// Unauthenticated reports a missing or invalid credential.
func Unauthenticated() error {
	return connect.NewError(connect.CodeUnauthenticated, errors.New("authentication required"))
}

// Timestamp converts a nullable database timestamp to protobuf.
func Timestamp(value pgtype.Timestamptz) *timestamppb.Timestamp {
	if !value.Valid {
		return nil
	}
	return timestamppb.New(value.Time.UTC())
}
