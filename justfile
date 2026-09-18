set shell := ["bash", "-eu", "-o", "pipefail", "-c"]

default: test lint

build: generate
    pnpm --filter @lutra/console build

generate: init
    go tool buf generate
    go tool sqlc generate

fmt:
    go tool golangci-lint fmt
    go tool sqlc fmt
    uv run ruff format
    pnpm format

lint:
    go tool buf lint
    go tool sqlc vet
    go tool golangci-lint run
    uv run ruff check
    uv run --all-packages --directory sdk pyrefly check
    pnpm --filter @lutra/console lint

test:
    go test ./...
    uv run --package lutra pytest

init:
    uv sync
    pnpm install
