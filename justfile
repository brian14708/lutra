set shell := ["bash", "-eu", "-o", "pipefail", "-c"]

generate:
    go tool buf generate

fmt:
    go tool golangci-lint fmt
    uv run ruff format

lint: generate
    go tool buf lint
    go tool golangci-lint run ./...
    uv run ruff check
    uv run --all-packages --directory sdk pyrefly check

test:
    go test ./...
    uv run --package lutra pytest

check: fmt lint test
