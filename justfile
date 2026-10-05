set shell := ["bash", "-eu", "-o", "pipefail", "-c"]

default: test lint

build: generate
    pnpm --filter @lutra/console build

generate: init
    go tool buf generate
    rm -rf internal/db/
    go tool sqlc generate

fmt:
    go tool buf format -w
    go tool golangci-lint fmt
    uv run ruff format
    pnpm format

lint:
    go tool buf lint
    go tool golangci-lint run
    uv run ruff check
    uv run ruff format --check
    uv run --all-packages --all-extras --directory sdk pyrefly check
    pnpm --filter @lutra/console lint

test:
    go test ./...
    uv run --all-extras --package lutra pytest
    pnpm --filter @lutra/console test

init:
    uv sync --all-packages --all-extras
    pnpm install

# Prove workflow recovery and task parity with disposable real services.
verify-workflow: generate
    mkdir -p .data/workflow-proof
    go build -o .data/workflow-proof/server ./cmd/server
    LUTRA_WORKFLOW_E2E=1 LUTRA_WORKFLOW_MANAGED=1 uv run --all-extras --package lutra pytest -v sdk/tests/test_workflow_e2e.py
