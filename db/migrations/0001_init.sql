-- +goose Up
CREATE SCHEMA IF NOT EXISTS lutra;

-- +goose Down
DROP SCHEMA IF EXISTS lutra;
