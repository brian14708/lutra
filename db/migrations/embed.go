package migrations

import "embed"

// FS contains the embedded database migration files.
//
//go:embed *.sql
var FS embed.FS
